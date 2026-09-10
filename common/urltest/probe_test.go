package urltest

import (
	"testing"
	"time"
)

// An outbound belongs to several groups at once and they all probe their own
// members, so the claim — not the delay history — is what keeps one interval to
// one probe. See HistoryStorage.ClaimProbe.
func TestClaimProbe_OneClaimPerWindow(t *testing.T) {
	s := NewHistoryStorage()
	const window = time.Minute

	if !s.ClaimProbe("node-a", window) {
		t.Fatal("first claim refused")
	}
	// A second group arriving while the probe is in flight must not dial again.
	if s.ClaimProbe("node-a", window) {
		t.Fatal("claimed while a probe was in flight")
	}
	// Another member is unrelated.
	if !s.ClaimProbe("node-b", window) {
		t.Fatal("claim on a different tag refused")
	}

	s.ReleaseProbe("node-a")
	// Released, but the window has not elapsed.
	if s.ClaimProbe("node-a", window) {
		t.Fatal("claimed inside the window after release")
	}
	// A zero window means "no coalescing", which is what a force check wants.
	if !s.ClaimProbe("node-a", 0) {
		t.Fatal("zero window must not coalesce")
	}
	s.ReleaseProbe("node-a")
	if !s.ClaimProbe("node-a", time.Nanosecond) {
		t.Fatal("claim refused after the window elapsed")
	}
}

// The whole reason the guard is keyed on the attempt: a probe that fails (or a
// member in dial cooldown) has its delay history DELETED, so a freshness check
// can never dedupe exactly the members that are probed most pointlessly.
func TestClaimProbe_FailedProbeStillClosesTheWindow(t *testing.T) {
	s := NewHistoryStorage()
	const window = time.Minute

	if !s.ClaimProbe("dead", window) {
		t.Fatal("first claim refused")
	}
	// Probe failed: nothing is stored, and sing-box deletes any earlier entry.
	s.DeleteURLTestHistory("dead")
	s.ReleaseProbe("dead")

	if h := s.LoadURLTestHistory("dead"); h != nil {
		t.Fatalf("history present after a failed probe: %v", h)
	}
	if s.ClaimProbe("dead", window) {
		t.Fatal("a failed probe left the member claimable again inside the window")
	}
}

// The storage is per-box (groups take it from the box's service context), so
// two boxes in one process — the test binary, selftest — must not coalesce each
// other's probes even though node tags are identical.
func TestClaimProbe_SeparateStoragesAreIndependent(t *testing.T) {
	a, b := NewHistoryStorage(), NewHistoryStorage()
	if !a.ClaimProbe("shared-tag", time.Minute) {
		t.Fatal("first storage refused the claim")
	}
	if !b.ClaimProbe("shared-tag", time.Minute) {
		t.Fatal("second storage coalesced against the first")
	}
}

func TestClaimProbe_ForgetMakesItClaimableAgain(t *testing.T) {
	s := NewHistoryStorage()
	if !s.ClaimProbe("gone", time.Minute) {
		t.Fatal("first claim refused")
	}
	s.ReleaseProbe("gone")
	s.ForgetProbe("gone")
	if !s.ClaimProbe("gone", time.Minute) {
		t.Fatal("forgotten member still not claimable")
	}
}

// A nil storage must behave as "no coalescing" rather than panic: the group
// holds a pointer it took from the context.
func TestClaimProbe_NilStorage(t *testing.T) {
	var s *HistoryStorage
	if !s.ClaimProbe("x", time.Minute) {
		t.Fatal("nil storage refused a claim")
	}
	s.ReleaseProbe("x")
	s.ForgetProbe("x")
}

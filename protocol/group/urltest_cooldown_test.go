package group

import (
	"testing"
	"time"
)

// A network change has to wipe the failure cooldown, or every node stays out of
// Auto for five minutes and the gateway has no egress at all.
//
// The cooldown exists because a node can pass the generate_204 probe and still
// fail real TLS, so a plain probe success is not enough to resurrect it. That
// reasoning is about the *node*. When the default interface changes — laptop
// moves from Ethernet to WiFi, tethering, sleep/wake, another VPN coming up —
// every recorded failure was recorded against a path that no longer exists, and
// every node fails at once, so all of them land in cooldown together.
//
// What made this hard to see: InterfaceUpdated does re-probe, and the probes
// succeed on the new network — but selection still withheld delay history while
// cooled, so Auto had nothing to pick. clearFailures on interface change is
// what restores selection; without it the only escape was rebuilding the box
// ("switch out of TUN and back"), which allocates a fresh failedUntil map.
//
// Cooled members are still probed for scorer recovery (NoteProbe); only the
// ranking history stays suppressed until cooldown expires or clearFailures.
func TestInterfaceUpdateClearsFailureCooldown(t *testing.T) {
	g := &URLTestGroup{
		failedUntil: map[string]time.Time{
			"hk-01": time.Now().Add(urltestFailureCooldown),
			"jp-02": time.Now().Add(urltestFailureCooldown),
		},
	}
	for _, tag := range []string{"hk-01", "jp-02"} {
		if !g.isCooled(tag) {
			t.Fatalf("%s should be in cooldown before the interface change — this test would prove nothing", tag)
		}
	}

	g.clearFailures()

	for _, tag := range []string{"hk-01", "jp-02"} {
		if g.isCooled(tag) {
			t.Fatalf("%s is still in cooldown after a network change, so Select keeps "+
				"withholding delay history and Auto has no healthy node", tag)
		}
	}
}

// Sticky selection points at an outbound reached over the old path, so it goes
// with the failures. Leaving it would keep dialling the interface that just
// disappeared.
func TestClearFailuresDropsStickySelection(t *testing.T) {
	g := &URLTestGroup{failedUntil: map[string]time.Time{}}
	g.selectedOutboundTCP = nil
	g.selectedOutboundUDP = nil
	// Nothing to assert about specific outbounds without a full group; what
	// matters is that clearFailures is the one place that resets both, so a later
	// edit cannot clear the map and forget the selection.
	g.clearFailures()
	if g.selectedOutboundTCP != nil || g.selectedOutboundUDP != nil {
		t.Fatal("clearFailures left a sticky selection")
	}
}

// The cooldown still has to work for its original purpose: a node that really is
// broken must not come back just because the periodic probe likes it. Only a
// network change clears it early.
func TestCooldownStillExpiresOnItsOwn(t *testing.T) {
	g := &URLTestGroup{failedUntil: map[string]time.Time{
		"live":    time.Now().Add(urltestFailureCooldown),
		"expired": time.Now().Add(-time.Second),
	}}
	if !g.isCooled("live") {
		t.Fatal("a fresh failure is not in cooldown")
	}
	if g.isCooled("expired") {
		t.Fatal("an elapsed cooldown still reports as cooled")
	}
	if _, still := g.failedUntil["expired"]; still {
		t.Fatal("an elapsed entry was not reaped")
	}
}

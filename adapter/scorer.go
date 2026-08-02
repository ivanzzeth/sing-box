package adapter

import "time"

// OutboundScorer lets an embedder rank group members by observed real-traffic
// quality instead of by generate_204 latency alone. It is symmetric with
// ConnectionTracker / DNSQueryTracker: sing-box calls it, sing-box never
// implements it.
//
// It is installed through the service registry
// (service.ContextWith[adapter.OutboundScorer]) and read once per group at
// construction. When absent, groups behave exactly as they always have — that
// nil case is the regression insurance and must stay byte-identical.
//
// trust-proxy addition; not upstream.
type OutboundScorer interface {
	// Score returns the member's quality score (0..100, higher is better) and
	// whether it may be preferred right now.
	//
	// A false `preferred` means "demote to last resort", NEVER "exclude". A
	// group that excludes its unhealthy members can end up with no members at
	// all — see urltest_cooldown_test.go, where exactly that left a gateway
	// with no egress for five minutes. Callers must keep such a member in the
	// candidate list, merely sorted last.
	//
	// Called on the dial path: it must not block, allocate heavily, or re-enter
	// the router.
	Score(tag string) (score float64, preferred bool)

	// Observe reports one real dial outcome. Latency is meaningful only when
	// success is true — a failure's duration measures a timeout, not speed.
	// Must return immediately.
	//
	// A successful dial must NOT clear a blackhole verdict: blackholes complete
	// their own handshake (so dials "succeed") and then relay nothing. Clearing
	// on dial success would undo the verdict on the very next attempt.
	Observe(tag string, success bool, latency time.Duration, err error)

	// NoteProbe reports one urltest / delay probe result. A successful probe
	// fetched bytes through the member (generate_204), which is conclusive
	// counter-evidence for a blackhole — and the only recovery path that does
	// not require demoted members to already be carrying user traffic (they
	// never are: score 0 sorts them last). Probe failures are ignored: they
	// must not re-open breakers on a path that is not the user's.
	// Optional: older scorers without this method are fine — groups type-assert.
	// Kept on the interface so the production scorer always implements it.
	NoteProbe(tag string, success bool, latency time.Duration)

	// TieMargin is the score difference below which two members count as
	// equal, leaving the existing latency tolerance to break the tie. Read
	// live rather than baked into the group at construction, so changing the
	// setting does not require rebuilding the instance.
	TieMargin() float64
}

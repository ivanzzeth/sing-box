package group

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/urltest"
	"github.com/sagernet/sing-box/log"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

// fakeOutbound is the minimum adapter.Outbound a group needs for selection.
type fakeOutbound struct {
	tag string
}

func (f *fakeOutbound) Type() string           { return "fake" }
func (f *fakeOutbound) Tag() string            { return f.tag }
func (f *fakeOutbound) Network() []string      { return []string{N.NetworkTCP, N.NetworkUDP} }
func (f *fakeOutbound) Dependencies() []string { return nil }
func (f *fakeOutbound) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	return nil, nil
}

func (f *fakeOutbound) ListenPacket(ctx context.Context, destination M.Socksaddr) (net.PacketConn, error) {
	return nil, nil
}

// fakeScorer is a scripted adapter.OutboundScorer.
type fakeScorer struct {
	scores    map[string]float64
	blocked   map[string]bool
	margin    float64
	observed  []string
	lastOK    map[string]bool
	lastDelay map[string]time.Duration
}

func newFakeScorer() *fakeScorer {
	return &fakeScorer{
		scores:    map[string]float64{},
		blocked:   map[string]bool{},
		margin:    5,
		lastOK:    map[string]bool{},
		lastDelay: map[string]time.Duration{},
	}
}

func (f *fakeScorer) Score(tag string) (float64, bool) {
	s, ok := f.scores[tag]
	if !ok {
		s = 100
	}
	return s, !f.blocked[tag]
}

func (f *fakeScorer) Observe(tag string, success bool, latency time.Duration, err error) {
	f.observed = append(f.observed, tag)
	f.lastOK[tag] = success
	f.lastDelay[tag] = latency
}

func (f *fakeScorer) TieMargin() float64 { return f.margin }

// buildGroup wires a group with the given members, probe delays and scorer.
func buildGroup(t *testing.T, delays map[string]uint16, scorer adapter.OutboundScorer, tags ...string) *URLTestGroup {
	t.Helper()
	history := urltest.NewHistoryStorage()
	outbounds := make([]adapter.Outbound, 0, len(tags))
	for _, tag := range tags {
		outbounds = append(outbounds, &fakeOutbound{tag: tag})
		if d, ok := delays[tag]; ok {
			history.StoreURLTestHistory(tag, &adapter.URLTestHistory{Time: time.Now(), Delay: d})
		}
	}
	return &URLTestGroup{
		outbounds:   outbounds,
		history:     history,
		tolerance:   150,
		failedUntil: map[string]time.Time{},
		scorer:      scorer,
	}
}

// Without a scorer the group must behave exactly as upstream: pure latency with
// the existing tolerance. This is the regression insurance for every deployment
// that does not install one.
func TestNoScorerKeepsLatencyOrdering(t *testing.T) {
	g := buildGroup(t, map[string]uint16{"slow": 400, "fast": 60}, nil, "slow", "fast")
	out, _ := g.Select(N.NetworkTCP)
	if out == nil || out.Tag() != "fast" {
		t.Fatalf("selected %v, want the lowest-latency member", out)
	}
	if pick := g.pickForDial(N.NetworkTCP, map[string]bool{}); pick == nil || pick.Tag() != "fast" {
		t.Fatalf("pickForDial chose %v, want fast", pick)
	}
}

// A materially better score must win even when the other node probes faster —
// that is the whole point: generate_204 latency is not availability.
func TestScoreBeatsLatency(t *testing.T) {
	sc := newFakeScorer()
	sc.scores["quick-but-broken"] = 20
	sc.scores["slower-but-solid"] = 90
	g := buildGroup(t, map[string]uint16{"quick-but-broken": 40, "slower-but-solid": 300}, sc,
		"quick-but-broken", "slower-but-solid")

	out, _ := g.Select(N.NetworkTCP)
	if out == nil || out.Tag() != "slower-but-solid" {
		t.Fatalf("Select chose %v, want the higher-scoring member", out)
	}
	if pick := g.pickForDial(N.NetworkTCP, map[string]bool{}); pick == nil || pick.Tag() != "slower-but-solid" {
		t.Fatalf("pickForDial chose %v, want the higher-scoring member", pick)
	}
}

// "相同分数选择延迟最低的": scores inside the tie margin count as equal and the
// existing latency tolerance decides.
func TestEqualScoresFallBackToLatency(t *testing.T) {
	sc := newFakeScorer()
	sc.scores["a"] = 80
	sc.scores["b"] = 83 // within the 5-point margin => same bucket
	g := buildGroup(t, map[string]uint16{"a": 500, "b": 60}, sc, "a", "b")
	out, _ := g.Select(N.NetworkTCP)
	if out == nil || out.Tag() != "b" {
		t.Fatalf("selected %v, want the lower-latency member of the same score bucket", out)
	}
}

// Warm-up: every member reports the neutral 100, so ordering must collapse to
// latency. A cold start must not reshuffle anything.
func TestWarmUpEqualScoresBehaveLikeUpstream(t *testing.T) {
	sc := newFakeScorer() // unknown tags => 100 for everyone
	g := buildGroup(t, map[string]uint16{"a": 500, "b": 60, "c": 200}, sc, "a", "b", "c")
	out, _ := g.Select(N.NetworkTCP)
	if out == nil || out.Tag() != "b" {
		t.Fatalf("selected %v during warm-up, want the lowest-latency member", out)
	}
}

// An open breaker demotes a member to last resort. It must NOT be excluded:
// see urltest_cooldown_test.go — a group that drops its unhealthy members can
// end up with nothing to dial and no egress at all.
func TestBreakerDemotesButNeverExcludes(t *testing.T) {
	sc := newFakeScorer()
	sc.scores["broken"] = 100 // still scores perfectly (e.g. still warming)
	sc.blocked["broken"] = true
	sc.scores["ok"] = 40
	g := buildGroup(t, map[string]uint16{"broken": 20, "ok": 900}, sc, "broken", "ok")

	out, _ := g.Select(N.NetworkTCP)
	if out == nil || out.Tag() != "ok" {
		t.Fatalf("selected %v, want the member whose breaker is closed", out)
	}

	// Now trip every breaker: the group must still return something.
	sc.blocked["ok"] = true
	out, _ = g.Select(N.NetworkTCP)
	if out == nil {
		t.Fatal("every breaker open left the group with no selection: this is the no-egress incident")
	}
	if pick := g.pickForDial(N.NetworkTCP, map[string]bool{}); pick == nil {
		t.Fatal("every breaker open left pickForDial with nothing to dial")
	}
}

// With no probe history at all (fresh box, probes not back yet) the breaker
// still steers away from a known-broken member — but never to nothing.
func TestNoHistoryStillAvoidsTrippedBreaker(t *testing.T) {
	sc := newFakeScorer()
	sc.blocked["broken"] = true
	g := buildGroup(t, nil, sc, "broken", "healthy")
	out, _ := g.Select(N.NetworkTCP)
	if out == nil || out.Tag() != "healthy" {
		t.Fatalf("selected %v with no probe history, want the member whose breaker is closed", out)
	}
	if pick := g.pickForDial(N.NetworkTCP, map[string]bool{}); pick == nil || pick.Tag() != "healthy" {
		t.Fatalf("pickForDial chose %v, want healthy", pick)
	}
	sc.blocked["healthy"] = true
	if out, _ := g.Select(N.NetworkTCP); out == nil {
		t.Fatal("all breakers open + no history => no selection at all")
	}
}

// Sticky selection yields to a tripped breaker — otherwise a node that just
// started failing keeps receiving every new connection until the periodic probe
// notices, which is minutes away.
func TestStickySelectionYieldsToTrippedBreaker(t *testing.T) {
	sc := newFakeScorer()
	g := buildGroup(t, map[string]uint16{"a": 50, "b": 60}, sc, "a", "b")
	a := g.outbounds[0]
	g.selectedOutboundTCP = a

	if pick := g.pickForDial(N.NetworkTCP, map[string]bool{}); pick != a {
		t.Fatalf("sticky selection not honoured while healthy: got %v", pick)
	}
	sc.blocked["a"] = true
	if pick := g.pickForDial(N.NetworkTCP, map[string]bool{}); pick == nil || pick.Tag() != "b" {
		t.Fatalf("pickForDial stayed on the tripped member: got %v", pick)
	}
}

// markFailed / markUnhealthy must reach the scorer, otherwise the score can
// only ever go up and a node that stops working is never demoted.
func TestFailuresAreObserved(t *testing.T) {
	sc := newFakeScorer()
	g := buildGroup(t, map[string]uint16{"a": 50}, sc, "a")
	g.logger = log.NewNOPFactory().Logger()
	g.markFailed(g.outbounds[0])
	if len(sc.observed) == 0 || sc.lastOK["a"] {
		t.Fatalf("markFailed did not report a failure to the scorer: %+v", sc.lastOK)
	}
	sc.observed = nil
	g.markUnhealthy(g.outbounds[0])
	if len(sc.observed) == 0 || sc.lastOK["a"] {
		t.Fatal("markUnhealthy did not report a failure to the scorer")
	}
}

// Scoring must only affect which member a NEW connection is dialed on. Nothing
// in the scoring path may interrupt live connections — that was the "logging in
// died halfway" bug, and re-introducing it through scoring would be the same
// failure wearing a new hat.
func TestScoringNeverInterruptsLiveConnections(t *testing.T) {
	sc := newFakeScorer()
	g := buildGroup(t, map[string]uint16{"a": 50, "b": 60}, sc, "a", "b")
	g.logger = log.NewNOPFactory().Logger()
	g.interruptGroup = nil // any Interrupt() call would panic here

	// A score change is not an event at all: nothing observes it but the next
	// pickForDial. Drive one through the full selection path to be sure.
	sc.scores["a"] = 10
	sc.scores["b"] = 95
	if pick := g.pickForDial(N.NetworkTCP, map[string]bool{}); pick == nil || pick.Tag() != "b" {
		t.Fatalf("new connection did not move to the better-scoring member: %v", pick)
	}
}

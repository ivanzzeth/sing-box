package adapter

import (
	"context"
	"time"
)

// ConnectionTiming accumulates raw phase timestamps for one outbound
// connection attempt, so a ConnectionTracker (trust-proxy's own detector) can
// report a latency breakdown (DNS / TCP connect / TLS handshake) once the
// connection closes, instead of only knowing when the connection opened and
// closed.
//
// Deliberately doesn't also cover time-to-first-byte: that would require
// wrapping remoteConn all the way into the data-copy path, which broke a
// copy-loop fast path for at least shadowsocks2's AEAD-2022 client (a real,
// reproducible data race — see the comment above the ConnectDone assignment
// in route/conn.go's NewConnection for the full story).
//
// The router creates one of these and puts it in the context right before
// calling ConnectionTracker.RoutedConnection (see route.routeConnection), so
// a tracker can read the SAME pointer back out of the ctx it's handed and
// stash it (e.g. on its own per-connection event) for later — the dialer/TLS
// code downstream fills in the fields as the connection actually gets
// established, all on the same pointer.
//
// Every field is a raw timestamp, not a pre-computed duration: deriving
// human-meaningful phase lengths (e.g. TLSDone - TCPDone) is left to the
// consumer, which keeps this fork patch minimal and free of interpretation
// logic that might need to change as trust-proxy's needs evolve.
type ConnectionTiming struct {
	// DialStart is when the router is about to hand the connection to its
	// tracker(s) and then dial — effectively "matched a rule, about to
	// connect" time.
	DialStart time.Time
	// DNSStart/DNSDone bound the domain resolution call in
	// common/dialer/resolve.go. Both stay zero when the destination was
	// already a literal IP (no resolution needed).
	DNSStart time.Time
	DNSDone  time.Time
	// TCPDone/TLSDone are set by common/tls/client.go's defaultDialer only
	// when the outbound actually performs a TLS handshake (vless/vmess/
	// trojan/anytls with tls enabled, i.e. protocols that wrap their dialer
	// with tls.NewDialer). Both stay zero for non-TLS outbounds (shadowsocks,
	// or QUIC-based hysteria2/tuic, whose handshake isn't a separable phase
	// in the same sense) — callers should treat ConnectDone as the "finished
	// connecting" signal either way.
	TCPDone time.Time
	TLSDone time.Time
	// ConnectDone is when remoteConn is fully ready to use (set in
	// route/conn.go regardless of protocol) — the authoritative "done
	// connecting" signal, whether or not TLS was involved.
	ConnectDone time.Time
}

type connectionTimingContextKey struct{}

// ContextWithConnectionTiming attaches timing to ctx for downstream dial/TLS
// code (and the tracker that created it) to read back via
// ConnectionTimingFromContext.
func ContextWithConnectionTiming(ctx context.Context, timing *ConnectionTiming) context.Context {
	return context.WithValue(ctx, connectionTimingContextKey{}, timing)
}

// ConnectionTimingFromContext returns the timing attached by
// ContextWithConnectionTiming, or nil if none (e.g. UDP/packet connections,
// which don't currently get one).
func ConnectionTimingFromContext(ctx context.Context) *ConnectionTiming {
	timing, _ := ctx.Value(connectionTimingContextKey{}).(*ConnectionTiming)
	return timing
}

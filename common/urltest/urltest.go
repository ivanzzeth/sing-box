package urltest

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/sagernet/sing-box/adapter"
	C "github.com/sagernet/sing-box/constant"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/common/ntp"
	"github.com/sagernet/sing/common/observable"
)

type HistoryStorage struct {
	access       sync.RWMutex
	delayHistory map[string]*adapter.URLTestHistory
	updateHooks  []*observable.Subscriber[struct{}]

	probeAccess sync.Mutex
	probes      map[string]probeState
}

// probeState is the per-outbound probe bookkeeping behind ClaimProbe.
type probeState struct {
	inFlight bool
	done     time.Time // when the last probe finished
}

func NewHistoryStorage() *HistoryStorage {
	return &HistoryStorage{
		delayHistory: make(map[string]*adapter.URLTestHistory),
		probes:       make(map[string]probeState),
	}
}

// ClaimProbe reserves tag for a URL test, returning false when another group is
// already probing it or finished one less than window ago.
//
// One outbound usually belongs to several groups at once — Auto, a region
// group, its per-country group — and each group probes all of its own members.
// The delay history cannot deduplicate that work:
//
//   - Every group's ticker starts from the same box, so they fire together and
//     all of them read the history before any probe has written to it. Measured
//     on a 36-node subscription with 18 groups (103 memberships): a box start
//     produced 103 probe connections in one second, every node dialled three
//     times.
//   - A probe that fails, or a member in dial cooldown, has its history entry
//     *deleted* — so from then on the freshness check never matches and every
//     group re-probes it every interval, forever. The nodes being probed most
//     pointlessly are exactly the ones the old check stopped covering.
//
// This guard is keyed on the attempt rather than on its result, so both cases
// collapse to one probe per outbound per window. The storage is per-box (groups
// take it from the box's service context), so separate boxes in one process —
// tests, selftest — never coalesce each other's probes.
func (s *HistoryStorage) ClaimProbe(tag string, window time.Duration) bool {
	if s == nil {
		return true
	}
	s.probeAccess.Lock()
	defer s.probeAccess.Unlock()
	st := s.probes[tag]
	if st.inFlight {
		return false
	}
	if window > 0 && !st.done.IsZero() && time.Since(st.done) < window {
		return false
	}
	st.inFlight = true
	s.probes[tag] = st
	return true
}

// ReleaseProbe ends the claim taken by ClaimProbe and starts the window. It
// must run whatever the probe's outcome was; see ClaimProbe.
func (s *HistoryStorage) ReleaseProbe(tag string) {
	if s == nil {
		return
	}
	s.probeAccess.Lock()
	defer s.probeAccess.Unlock()
	s.probes[tag] = probeState{done: time.Now()}
}

// ForgetProbe drops the bookkeeping for tag, so the next ClaimProbe succeeds
// immediately. For members that left the group.
func (s *HistoryStorage) ForgetProbe(tag string) {
	if s == nil {
		return
	}
	s.probeAccess.Lock()
	defer s.probeAccess.Unlock()
	delete(s.probes, tag)
}

func (s *HistoryStorage) AddUpdateHook(hook *observable.Subscriber[struct{}]) {
	s.access.Lock()
	defer s.access.Unlock()
	s.updateHooks = append(s.updateHooks, hook)
}

func (s *HistoryStorage) NotifyUpdated() {
	s.access.RLock()
	defer s.access.RUnlock()
	s.notifyUpdated()
}

func (s *HistoryStorage) LoadURLTestHistory(tag string) *adapter.URLTestHistory {
	if s == nil {
		return nil
	}
	s.access.RLock()
	defer s.access.RUnlock()
	return s.delayHistory[tag]
}

func (s *HistoryStorage) DeleteURLTestHistory(tag string) {
	s.access.Lock()
	delete(s.delayHistory, tag)
	s.notifyUpdated()
	s.access.Unlock()
}

func (s *HistoryStorage) StoreURLTestHistory(tag string, history *adapter.URLTestHistory) {
	s.access.Lock()
	s.delayHistory[tag] = history
	s.notifyUpdated()
	s.access.Unlock()
}

func (s *HistoryStorage) notifyUpdated() {
	for _, updateHook := range s.updateHooks {
		updateHook.Emit(struct{}{})
	}
}

func (s *HistoryStorage) Close() error {
	s.access.Lock()
	defer s.access.Unlock()
	s.updateHooks = nil
	return nil
}

func URLTest(ctx context.Context, link string, detour N.Dialer) (t uint16, err error) {
	if link == "" {
		link = "https://www.gstatic.com/generate_204"
	}
	linkURL, err := url.Parse(link)
	if err != nil {
		return
	}
	hostname := linkURL.Hostname()
	port := linkURL.Port()
	if port == "" {
		switch linkURL.Scheme {
		case "http":
			port = "80"
		case "https":
			port = "443"
		}
	}

	start := time.Now()
	instance, err := detour.DialContext(ctx, "tcp", M.ParseSocksaddrHostPortStr(hostname, port))
	if err != nil {
		return
	}
	defer instance.Close()
	if N.NeedHandshakeForWrite(instance) {
		start = time.Now()
	}
	req, err := http.NewRequest(http.MethodHead, link, nil)
	if err != nil {
		return
	}
	client := http.Client{
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
				return instance, nil
			},
			TLSClientConfig: &tls.Config{
				Time:    ntp.TimeFuncFromContext(ctx),
				RootCAs: adapter.RootPoolFromContext(ctx),
			},
		},
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
		Timeout: C.TCPTimeout,
	}
	defer client.CloseIdleConnections()
	resp, err := client.Do(req.WithContext(ctx))
	if err != nil {
		return
	}
	resp.Body.Close()
	// Any HTTP answer used to count as success, so a node that dials a
	// landing page (or returns 403/5xx) could win urltest with a tiny delay
	// and then break real destinations. Require 2xx — generate_204 /
	// cp.cloudflare.com answer 204 when the exit actually reaches them.
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		err = fmt.Errorf("unexpected status %d", resp.StatusCode)
		return
	}
	t = uint16(time.Since(start) / time.Millisecond)
	return
}

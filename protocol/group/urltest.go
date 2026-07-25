package group

import (
	"context"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/outbound"
	"github.com/sagernet/sing-box/common/interrupt"
	"github.com/sagernet/sing-box/common/urltest"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common"
	"github.com/sagernet/sing/common/batch"
	E "github.com/sagernet/sing/common/exceptions"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/common/x/list"
	"github.com/sagernet/sing/service"
	"github.com/sagernet/sing/service/pause"
)

func RegisterURLTest(registry *outbound.Registry) {
	outbound.Register[option.URLTestOutboundOptions](registry, C.TypeURLTest, NewURLTest)
}

var (
	_ adapter.OutboundGroup           = (*URLTest)(nil)
	_ adapter.InterfaceUpdateListener = (*URLTest)(nil)
)

type URLTest struct {
	outbound.Adapter
	ctx                          context.Context
	outbound                     adapter.OutboundManager
	connection                   adapter.ConnectionManager
	logger                       log.ContextLogger
	tags                         []string
	link                         string
	interval                     time.Duration
	tolerance                    uint16
	idleTimeout                  time.Duration
	group                        *URLTestGroup
	interruptExternalConnections bool
}

func NewURLTest(ctx context.Context, router adapter.Router, logger log.ContextLogger, tag string, options option.URLTestOutboundOptions) (adapter.Outbound, error) {
	outbound := &URLTest{
		Adapter:                      outbound.NewAdapter(C.TypeURLTest, tag, []string{N.NetworkTCP, N.NetworkUDP}, options.Outbounds),
		ctx:                          ctx,
		outbound:                     service.FromContext[adapter.OutboundManager](ctx),
		connection:                   service.FromContext[adapter.ConnectionManager](ctx),
		logger:                       logger,
		tags:                         options.Outbounds,
		link:                         options.URL,
		interval:                     time.Duration(options.Interval),
		tolerance:                    options.Tolerance,
		idleTimeout:                  time.Duration(options.IdleTimeout),
		interruptExternalConnections: options.InterruptExistConnections,
	}
	if len(outbound.tags) == 0 {
		return nil, E.New("missing tags")
	}
	return outbound, nil
}

func (s *URLTest) Start() error {
	outbounds := make([]adapter.Outbound, 0, len(s.tags))
	for i, tag := range s.tags {
		detour, loaded := s.outbound.Outbound(tag)
		if !loaded {
			return E.New("outbound ", i, " not found: ", tag)
		}
		outbounds = append(outbounds, detour)
	}
	group, err := NewURLTestGroup(s.ctx, s.outbound, s.logger, outbounds, s.link, s.interval, s.tolerance, s.idleTimeout, s.interruptExternalConnections)
	if err != nil {
		return err
	}
	s.group = group
	return nil
}

func (s *URLTest) PostStart() error {
	s.group.PostStart()
	return nil
}

func (s *URLTest) Close() error {
	return common.Close(
		common.PtrOrNil(s.group),
	)
}

func (s *URLTest) Now() string {
	if s.group.selectedOutboundTCP != nil {
		return s.group.selectedOutboundTCP.Tag()
	} else if s.group.selectedOutboundUDP != nil {
		return s.group.selectedOutboundUDP.Tag()
	}
	return ""
}

func (s *URLTest) All() []string {
	return s.tags
}

func (s *URLTest) URLTest(ctx context.Context) (map[string]uint16, error) {
	return s.group.URLTest(ctx)
}

func (s *URLTest) CheckOutbounds() {
	s.group.CheckOutbounds(true)
}

func (s *URLTest) InterfaceUpdated() {
	group := s.group
	if group == nil {
		return
	}
	if group.pause.IsDevicePaused() || group.pause.IsNetworkPaused() {
		return
	}
	go group.CheckOutbounds(true)
}

func (s *URLTest) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	s.group.Touch()
	networkName := N.NetworkName(network)
	switch networkName {
	case N.NetworkTCP, N.NetworkUDP:
	default:
		return nil, E.Extend(N.ErrUnknownNetwork, network)
	}
	tried := make(map[string]bool)
	var lastErr error
	for {
		outbound := s.group.pickForDial(networkName, tried)
		if outbound == nil {
			if lastErr != nil {
				return nil, lastErr
			}
			return nil, E.New("missing supported outbound")
		}
		tag := RealTag(outbound)
		tried[tag] = true
		conn, err := outbound.DialContext(ctx, network, destination)
		if err != nil {
			s.logger.ErrorContext(ctx, err)
			s.group.markFailed(outbound)
			lastErr = err
			continue
		}
		s.group.setSelected(networkName, outbound)
		conn = s.group.wrapFailoverConn(conn, outbound)
		return s.group.interruptGroup.NewConn(conn, interrupt.IsExternalConnectionFromContext(ctx)), nil
	}
}

func (s *URLTest) ListenPacket(ctx context.Context, destination M.Socksaddr) (net.PacketConn, error) {
	s.group.Touch()
	tried := make(map[string]bool)
	var lastErr error
	for {
		outbound := s.group.pickForDial(N.NetworkUDP, tried)
		if outbound == nil {
			if lastErr != nil {
				return nil, lastErr
			}
			return nil, E.New("missing supported outbound")
		}
		tag := RealTag(outbound)
		tried[tag] = true
		conn, err := outbound.ListenPacket(ctx, destination)
		if err != nil {
			s.logger.ErrorContext(ctx, err)
			s.group.markFailed(outbound)
			lastErr = err
			continue
		}
		s.group.setSelected(N.NetworkUDP, outbound)
		conn = s.group.wrapFailoverPacketConn(conn, outbound)
		return s.group.interruptGroup.NewPacketConn(conn, interrupt.IsExternalConnectionFromContext(ctx)), nil
	}
}

func (s *URLTest) NewConnection(ctx context.Context, conn net.Conn, metadata adapter.InboundContext, onClose N.CloseHandlerFunc) {
	ctx = interrupt.ContextWithIsExternalConnection(ctx)
	s.connection.NewConnection(ctx, s, conn, metadata, onClose)
}

func (s *URLTest) NewPacketConnection(ctx context.Context, conn N.PacketConn, metadata adapter.InboundContext, onClose N.CloseHandlerFunc) {
	ctx = interrupt.ContextWithIsExternalConnection(ctx)
	s.connection.NewPacketConnection(ctx, s, conn, metadata, onClose)
}

type URLTestGroup struct {
	ctx                          context.Context
	outbound                     adapter.OutboundManager
	pause                        pause.Manager
	pauseCallback                *list.Element[pause.Callback]
	logger                       log.Logger
	outbounds                    []adapter.Outbound
	link                         string
	interval                     time.Duration
	tolerance                    uint16
	idleTimeout                  time.Duration
	history                      *urltest.HistoryStorage
	checking                     atomic.Bool
	selectedOutboundTCP          adapter.Outbound
	selectedOutboundUDP          adapter.Outbound
	interruptGroup               *interrupt.Group
	interruptExternalConnections bool
	access                       sync.Mutex
	ticker                       *time.Ticker
	close                        chan struct{}
	started                      bool
	lastActive                   common.TypedValue[time.Time]
	// failedUntil excludes a node from selection/probes after a real dial/IO
	// failure. Without this, CheckOutbounds immediately re-probes generate_204
	// and resurrects nodes that pass the health URL but fail real TLS (e.g. to
	// api2.cursor.sh / accounts.google.com).
	failedUntil map[string]time.Time
}

func NewURLTestGroup(ctx context.Context, outboundManager adapter.OutboundManager, logger log.Logger, outbounds []adapter.Outbound, link string, interval time.Duration, tolerance uint16, idleTimeout time.Duration, interruptExternalConnections bool) (*URLTestGroup, error) {
	if interval == 0 {
		interval = C.DefaultURLTestInterval
	}
	if tolerance == 0 {
		tolerance = 50
	}
	if idleTimeout == 0 {
		idleTimeout = C.DefaultURLTestIdleTimeout
	}
	if interval > idleTimeout {
		return nil, E.New("interval must be less or equal than idle_timeout")
	}
	history := service.PtrFromContext[urltest.HistoryStorage](ctx)
	if history == nil {
		return nil, E.New("missing URL test history storage")
	}
	return &URLTestGroup{
		ctx:                          ctx,
		outbound:                     outboundManager,
		logger:                       logger,
		outbounds:                    outbounds,
		link:                         link,
		interval:                     interval,
		tolerance:                    tolerance,
		idleTimeout:                  idleTimeout,
		history:                      history,
		close:                        make(chan struct{}),
		pause:                        service.FromContext[pause.Manager](ctx),
		interruptGroup:               interrupt.NewGroup(),
		interruptExternalConnections: interruptExternalConnections,
		failedUntil:                  make(map[string]time.Time),
	}, nil
}

func (g *URLTestGroup) PostStart() {
	g.access.Lock()
	defer g.access.Unlock()
	g.started = true
	g.lastActive.Store(time.Now())
	go g.CheckOutbounds(false)
}

func (g *URLTestGroup) Touch() {
	if !g.started {
		return
	}
	g.access.Lock()
	defer g.access.Unlock()
	if g.ticker != nil {
		g.lastActive.Store(time.Now())
		return
	}
	ticker := time.NewTicker(g.interval)
	g.ticker = ticker
	g.pauseCallback = pause.RegisterTicker(g.pause, ticker, g.interval, nil)
	go g.loopCheck(ticker, g.close)
}

func (g *URLTestGroup) Close() error {
	g.access.Lock()
	defer g.access.Unlock()
	if g.ticker == nil {
		return nil
	}
	g.ticker.Stop()
	g.ticker = nil
	g.pause.UnregisterCallback(g.pauseCallback)
	g.pauseCallback = nil
	close(g.close)
	return nil
}

func (g *URLTestGroup) Select(network string) (adapter.Outbound, bool) {
	var minDelay uint16
	var minOutbound adapter.Outbound
	switch network {
	case N.NetworkTCP:
		if g.selectedOutboundTCP != nil && !g.isCooled(RealTag(g.selectedOutboundTCP)) {
			if history := g.history.LoadURLTestHistory(RealTag(g.selectedOutboundTCP)); history != nil {
				minOutbound = g.selectedOutboundTCP
				minDelay = history.Delay
			}
		}
	case N.NetworkUDP:
		if g.selectedOutboundUDP != nil && !g.isCooled(RealTag(g.selectedOutboundUDP)) {
			if history := g.history.LoadURLTestHistory(RealTag(g.selectedOutboundUDP)); history != nil {
				minOutbound = g.selectedOutboundUDP
				minDelay = history.Delay
			}
		}
	}
	for _, detour := range g.outbounds {
		if !common.Contains(detour.Network(), network) || g.isCooled(RealTag(detour)) {
			continue
		}
		history := g.history.LoadURLTestHistory(RealTag(detour))
		if history == nil {
			continue
		}
		if minDelay == 0 || minDelay > history.Delay+g.tolerance {
			minDelay = history.Delay
			minOutbound = detour
		}
	}
	if minOutbound == nil {
		for _, detour := range g.outbounds {
			if !common.Contains(detour.Network(), network) || g.isCooled(RealTag(detour)) {
				continue
			}
			return detour, false
		}
		// Everything cooled: last resort, ignore cooldown so traffic is not blackholed.
		for _, detour := range g.outbounds {
			if !common.Contains(detour.Network(), network) {
				continue
			}
			return detour, false
		}
		return nil, false
	}
	return minOutbound, true
}

func (g *URLTestGroup) loopCheck(ticker *time.Ticker, closeChan <-chan struct{}) {
	if time.Since(g.lastActive.Load()) > g.interval {
		g.lastActive.Store(time.Now())
		g.CheckOutbounds(false)
	}
	for {
		select {
		case <-closeChan:
			return
		case <-ticker.C:
		}
		if time.Since(g.lastActive.Load()) > g.idleTimeout {
			g.access.Lock()
			if g.ticker == ticker {
				g.ticker.Stop()
				g.ticker = nil
				g.pause.UnregisterCallback(g.pauseCallback)
				g.pauseCallback = nil
			}
			g.access.Unlock()
			return
		}
		g.CheckOutbounds(false)
	}
}

func (g *URLTestGroup) CheckOutbounds(force bool) {
	_, _ = g.urlTest(g.ctx, force)
}

func (g *URLTestGroup) URLTest(ctx context.Context) (map[string]uint16, error) {
	return g.urlTest(ctx, false)
}

func (g *URLTestGroup) urlTest(ctx context.Context, force bool) (map[string]uint16, error) {
	result := make(map[string]uint16)
	if g.checking.Swap(true) {
		return result, nil
	}
	defer g.checking.Store(false)
	b, _ := batch.New(ctx, batch.WithConcurrencyNum[any](10))
	checked := make(map[string]bool)
	var resultAccess sync.Mutex
	for _, detour := range g.outbounds {
		tag := detour.Tag()
		realTag := RealTag(detour)
		if checked[realTag] {
			continue
		}
		if g.isCooled(realTag) {
			// Real traffic already failed this node; do not let generate_204
			// resurrect it until the cooldown expires.
			g.history.DeleteURLTestHistory(realTag)
			continue
		}
		history := g.history.LoadURLTestHistory(realTag)
		if !force && history != nil && time.Since(history.Time) < g.interval {
			continue
		}
		checked[realTag] = true
		p, loaded := g.outbound.Outbound(realTag)
		if !loaded {
			continue
		}
		b.Go(realTag, func() (any, error) {
			testCtx, cancel := context.WithTimeout(g.ctx, C.TCPTimeout)
			defer cancel()
			t, err := urltest.URLTest(testCtx, g.link, p)
			if err != nil {
				g.logger.Debug("outbound ", tag, " unavailable: ", err)
				g.history.DeleteURLTestHistory(realTag)
			} else if g.isCooled(realTag) {
				g.logger.Debug("outbound ", tag, " available but in failure cooldown, ignoring probe")
				g.history.DeleteURLTestHistory(realTag)
			} else {
				g.logger.Debug("outbound ", tag, " available: ", t, "ms")
				g.history.StoreURLTestHistory(realTag, &adapter.URLTestHistory{
					Time:  time.Now(),
					Delay: t,
				})
				resultAccess.Lock()
				result[tag] = t
				resultAccess.Unlock()
			}
			return nil, nil
		})
	}
	b.Wait()
	g.performUpdateCheck()
	return result, nil
}

func (g *URLTestGroup) performUpdateCheck() {
	var updated bool
	if outbound, exists := g.Select(N.NetworkTCP); outbound != nil && (g.selectedOutboundTCP == nil || (exists && outbound != g.selectedOutboundTCP)) {
		if g.selectedOutboundTCP != nil {
			updated = true
		}
		g.selectedOutboundTCP = outbound
	}
	if outbound, exists := g.Select(N.NetworkUDP); outbound != nil && (g.selectedOutboundUDP == nil || (exists && outbound != g.selectedOutboundUDP)) {
		if g.selectedOutboundUDP != nil {
			updated = true
		}
		g.selectedOutboundUDP = outbound
	}
	if updated {
		g.interruptGroup.Interrupt(g.interruptExternalConnections)
	}
}

// urltestFailureCooldownout keeps a node out of Auto after a real dial/IO failure
// so the periodic generate_204 probe cannot immediately put it back.
const urltestFailureCooldown = 5 * time.Minute

func (g *URLTestGroup) isCooled(tag string) bool {
	g.access.Lock()
	defer g.access.Unlock()
	until, ok := g.failedUntil[tag]
	if !ok {
		return false
	}
	if time.Now().Before(until) {
		return true
	}
	delete(g.failedUntil, tag)
	return false
}

// pickForDial chooses the next outbound to try, skipping tags already failed in
// this dial attempt. Prefers the current selection, then lowest urltest delay,
// then any remaining member — so a dial/IO failure can immediately fail over
// without waiting for the periodic probe.
func (g *URLTestGroup) pickForDial(network string, exclude map[string]bool) adapter.Outbound {
	var preferred adapter.Outbound
	switch network {
	case N.NetworkTCP:
		preferred = g.selectedOutboundTCP
	case N.NetworkUDP:
		preferred = g.selectedOutboundUDP
	}
	if preferred != nil && common.Contains(preferred.Network(), network) && !exclude[RealTag(preferred)] && !g.isCooled(RealTag(preferred)) {
		return preferred
	}
	var minDelay uint16
	var minOutbound adapter.Outbound
	for _, detour := range g.outbounds {
		tag := RealTag(detour)
		if !common.Contains(detour.Network(), network) || exclude[tag] || g.isCooled(tag) {
			continue
		}
		history := g.history.LoadURLTestHistory(tag)
		if history == nil {
			continue
		}
		if minOutbound == nil || minDelay > history.Delay+g.tolerance {
			minDelay = history.Delay
			minOutbound = detour
		}
	}
	if minOutbound != nil {
		return minOutbound
	}
	for _, detour := range g.outbounds {
		tag := RealTag(detour)
		if !common.Contains(detour.Network(), network) || exclude[tag] || g.isCooled(tag) {
			continue
		}
		return detour
	}
	// All cooled or excluded: allow a cooled node rather than failing hard.
	for _, detour := range g.outbounds {
		tag := RealTag(detour)
		if !common.Contains(detour.Network(), network) || exclude[tag] {
			continue
		}
		return detour
	}
	return nil
}

func (g *URLTestGroup) setSelected(network string, outbound adapter.Outbound) {
	switch network {
	case N.NetworkTCP:
		g.selectedOutboundTCP = outbound
	case N.NetworkUDP:
		g.selectedOutboundUDP = outbound
	}
}

// markFailed drops the outbound from urltest history, puts it in cooldown, and
// clears sticky selection. Does NOT force an immediate urltest — that would
// re-probe generate_204 and undo the failure.
func (g *URLTestGroup) markFailed(outbound adapter.Outbound) {
	if outbound == nil {
		return
	}
	tag := RealTag(outbound)
	g.history.DeleteURLTestHistory(tag)
	g.access.Lock()
	if g.failedUntil == nil {
		g.failedUntil = make(map[string]time.Time)
	}
	g.failedUntil[tag] = time.Now().Add(urltestFailureCooldown)
	if g.selectedOutboundTCP == outbound {
		g.selectedOutboundTCP = nil
	}
	if g.selectedOutboundUDP == outbound {
		g.selectedOutboundUDP = nil
	}
	g.access.Unlock()
	g.logger.Info("outbound ", tag, " marked failed for ", urltestFailureCooldown, " (real dial/IO error)")
	// Promote another member from existing history; skip force re-probe.
	g.performUpdateCheck()
}

func (g *URLTestGroup) wrapFailoverConn(conn net.Conn, outbound adapter.Outbound) net.Conn {
	return &urltestFailoverConn{Conn: conn, group: g, outbound: outbound}
}

func (g *URLTestGroup) wrapFailoverPacketConn(conn net.PacketConn, outbound adapter.Outbound) net.PacketConn {
	return &urltestFailoverPacketConn{PacketConn: conn, group: g, outbound: outbound}
}

// urltestFailoverConn treats early I/O failure (before any successful read) as
// a probe failure — e.g. TLS RST after a successful dial through a dead proxy.
type urltestFailoverConn struct {
	net.Conn
	group    *URLTestGroup
	outbound adapter.Outbound
	readOK   atomic.Bool
	once     sync.Once
}

func (c *urltestFailoverConn) noticeFail() {
	c.once.Do(func() { c.group.markFailed(c.outbound) })
}

func (c *urltestFailoverConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	if n > 0 {
		c.readOK.Store(true)
	}
	if err != nil && err != io.EOF && !c.readOK.Load() {
		c.noticeFail()
	}
	return n, err
}

func (c *urltestFailoverConn) Write(p []byte) (int, error) {
	n, err := c.Conn.Write(p)
	if err != nil && !c.readOK.Load() {
		c.noticeFail()
	}
	return n, err
}

type urltestFailoverPacketConn struct {
	net.PacketConn
	group    *URLTestGroup
	outbound adapter.Outbound
	readOK   atomic.Bool
	once     sync.Once
}

func (c *urltestFailoverPacketConn) noticeFail() {
	c.once.Do(func() { c.group.markFailed(c.outbound) })
}

func (c *urltestFailoverPacketConn) ReadFrom(p []byte) (int, net.Addr, error) {
	n, addr, err := c.PacketConn.ReadFrom(p)
	if n > 0 {
		c.readOK.Store(true)
	}
	if err != nil && err != io.EOF && !c.readOK.Load() {
		c.noticeFail()
	}
	return n, addr, err
}

func (c *urltestFailoverPacketConn) WriteTo(p []byte, addr net.Addr) (int, error) {
	n, err := c.PacketConn.WriteTo(p, addr)
	if err != nil && !c.readOK.Load() {
		c.noticeFail()
	}
	return n, err
}

package group

import (
	"context"
	"hash/fnv"
	"net"
	"strings"
	"sync/atomic"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/outbound"
	"github.com/sagernet/sing-box/common/interrupt"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common"
	E "github.com/sagernet/sing/common/exceptions"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/service"
	"golang.org/x/net/publicsuffix"
)

func RegisterLoadBalance(registry *outbound.Registry) {
	outbound.Register[option.LoadBalanceOutboundOptions](registry, C.TypeLoadBalance, NewLoadBalance)
	// Accept Clash/Mihomo's spelling as well when importing compatible configs.
	outbound.Register[option.LoadBalanceOutboundOptions](registry, C.TypeLoadBalanceClash, NewLoadBalance)
}

var _ adapter.OutboundGroup = (*LoadBalance)(nil)

type LoadBalance struct {
	outbound.Adapter
	ctx                          context.Context
	outbound                     adapter.OutboundManager
	connection                   adapter.ConnectionManager
	logger                       log.ContextLogger
	tags                         []string
	strategy                     string
	link                         string
	interval                     time.Duration
	idleTimeout                  time.Duration
	group                        *URLTestGroup
	interruptGroup               *interrupt.Group
	interruptExternalConnections bool
	counter                      atomic.Uint64
}

func NewLoadBalance(ctx context.Context, router adapter.Router, logger log.ContextLogger, tag string, options option.LoadBalanceOutboundOptions) (adapter.Outbound, error) {
	strategy := strings.ToLower(options.Strategy)
	if strategy == "" {
		strategy = "consistent-hashing"
	}
	if strategy != "round-robin" && strategy != "consistent-hashing" {
		return nil, E.New("unsupported load balance strategy: ", options.Strategy)
	}
	if len(options.Outbounds) == 0 {
		return nil, E.New("missing tags")
	}
	return &LoadBalance{
		Adapter:                      outbound.NewAdapter(C.TypeLoadBalance, tag, []string{N.NetworkTCP, N.NetworkUDP}, options.Outbounds),
		ctx:                          ctx,
		outbound:                     service.FromContext[adapter.OutboundManager](ctx),
		connection:                   service.FromContext[adapter.ConnectionManager](ctx),
		logger:                       logger,
		tags:                         options.Outbounds,
		strategy:                     strategy,
		link:                         options.URL,
		interval:                     time.Duration(options.Interval),
		idleTimeout:                  time.Duration(options.IdleTimeout),
		interruptGroup:               interrupt.NewGroup(),
		interruptExternalConnections: options.InterruptExistConnections,
	}, nil
}

func (s *LoadBalance) Start() error {
	outbounds := make([]adapter.Outbound, 0, len(s.tags))
	for i, tag := range s.tags {
		detour, loaded := s.outbound.Outbound(tag)
		if !loaded {
			return E.New("outbound ", i, " not found: ", tag)
		}
		outbounds = append(outbounds, detour)
	}
	group, err := NewURLTestGroup(s.ctx, s.outbound, s.logger, outbounds, s.link, s.interval, 0, s.idleTimeout, s.interruptExternalConnections)
	if err != nil {
		return err
	}
	s.group = group
	return nil
}

func (s *LoadBalance) PostStart() error { s.group.PostStart(); return nil }
func (s *LoadBalance) Close() error     { return common.Close(common.PtrOrNil(s.group)) }
func (s *LoadBalance) Now() string {
	if len(s.tags) == 0 {
		return ""
	}
	return s.tags[0]
}
func (s *LoadBalance) All() []string { return s.tags }

func (s *LoadBalance) pick(network string, destination M.Socksaddr) adapter.Outbound {
	var candidates []adapter.Outbound
	for _, detour := range s.group.outbounds {
		if common.Contains(detour.Network(), network) {
			if s.group.history.LoadURLTestHistory(RealTag(detour)) != nil {
				candidates = append(candidates, detour)
			}
		}
	}
	if len(candidates) == 0 {
		for _, detour := range s.group.outbounds {
			if common.Contains(detour.Network(), network) {
				candidates = append(candidates, detour)
			}
		}
	}
	if len(candidates) == 0 {
		return nil
	}
	if s.strategy == "round-robin" {
		return candidates[(s.counter.Add(1)-1)%uint64(len(candidates))]
	}
	key := destination.Fqdn
	if key == "" {
		key = destination.Addr.String()
	}
	if domain, err := publicsuffix.EffectiveTLDPlusOne(key); err == nil {
		key = domain
	}
	var selected adapter.Outbound
	var best uint64
	for _, candidate := range candidates {
		h := fnv.New64a()
		_, _ = h.Write([]byte(key + "\x00" + candidate.Tag()))
		score := h.Sum64()
		if selected == nil || score > best {
			selected, best = candidate, score
		}
	}
	return selected
}

func (s *LoadBalance) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	detour := s.pick(network, destination)
	if detour == nil {
		return nil, E.New("missing supported outbound")
	}
	conn, err := detour.DialContext(ctx, network, destination)
	if err != nil {
		s.group.history.DeleteURLTestHistory(RealTag(detour))
		return nil, err
	}
	return s.interruptGroup.NewConn(conn, interrupt.IsExternalConnectionFromContext(ctx)), nil
}

func (s *LoadBalance) ListenPacket(ctx context.Context, destination M.Socksaddr) (net.PacketConn, error) {
	detour := s.pick(N.NetworkUDP, destination)
	if detour == nil {
		return nil, E.New("missing supported outbound")
	}
	conn, err := detour.ListenPacket(ctx, destination)
	if err != nil {
		s.group.history.DeleteURLTestHistory(RealTag(detour))
		return nil, err
	}
	return s.interruptGroup.NewPacketConn(conn, interrupt.IsExternalConnectionFromContext(ctx)), nil
}

func (s *LoadBalance) NewConnection(ctx context.Context, conn net.Conn, metadata adapter.InboundContext, onClose N.CloseHandlerFunc) {
	ctx = interrupt.ContextWithIsExternalConnection(ctx)
	detour := s.pick(N.NetworkTCP, metadata.Destination)
	if detour == nil {
		s.connection.NewConnection(ctx, s, conn, metadata, onClose)
		return
	}
	if handler, ok := detour.(adapter.ConnectionHandler); ok {
		handler.NewConnection(ctx, conn, metadata, onClose)
		return
	}
	s.connection.NewConnection(ctx, detour, conn, metadata, onClose)
}

func (s *LoadBalance) NewPacketConnection(ctx context.Context, conn N.PacketConn, metadata adapter.InboundContext, onClose N.CloseHandlerFunc) {
	ctx = interrupt.ContextWithIsExternalConnection(ctx)
	detour := s.pick(N.NetworkUDP, metadata.Destination)
	if detour == nil {
		s.connection.NewPacketConnection(ctx, s, conn, metadata, onClose)
		return
	}
	if handler, ok := detour.(adapter.PacketConnectionHandler); ok {
		handler.NewPacketConnection(ctx, conn, metadata, onClose)
		return
	}
	s.connection.NewPacketConnection(ctx, detour, conn, metadata, onClose)
}

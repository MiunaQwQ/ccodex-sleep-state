// Package netpath owns the connection to a proxy server, before its protocol
// tunnel is established. A physical path never falls back to the system VPN.
package netpath

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"strings"
	"time"

	"github.com/metacubex/mihomo/component/dialer"
)

type Info struct {
	Mode       string   `json:"mode"`
	Interface  string   `json:"interface,omitempty"`
	DNSServers []string `json:"dns_servers,omitempty"`
}

type Path struct {
	Info   Info
	lookup func(context.Context, string, string) ([]netip.Addr, error)
	dial   func(context.Context, string, string) (net.Conn, error)
	listen func(context.Context, string, string, netip.AddrPort) (net.PacketConn, error)
}

func New(ctx context.Context, mode, name string) (*Path, error) {
	if mode == "" || mode == "system" {
		return nil, nil
	}
	if mode != "physical" {
		return nil, errors.New("未知节点网络模式")
	}
	info, err := discover(ctx, name)
	if err != nil {
		return nil, err
	}
	return newPath(info), nil
}

func newPath(info Info) *Path {
	p := &Path{Info: info}
	opts := []dialer.Option{dialer.WithInterface(info.Interface)}
	p.dial = func(ctx context.Context, network, address string) (net.Conn, error) {
		return dialer.DialContext(ctx, network, address, opts...)
	}
	p.listen = func(ctx context.Context, network, address string, remote netip.AddrPort) (net.PacketConn, error) {
		return dialer.ListenPacket(ctx, network, address, remote, opts...)
	}
	p.lookup = func(ctx context.Context, network, host string) ([]netip.Addr, error) {
		// Each resolver socket is bound too. A system resolver could otherwise
		// return the VPN's 198.18/15 fake addresses even with a bound TCP socket.
		for _, server := range info.DNSServers {
			resolveCtx, cancel := context.WithTimeout(ctx, 4*time.Second)
			r := &net.Resolver{PreferGo: true, Dial: func(ctx context.Context, network, _ string) (net.Conn, error) {
				return p.dial(ctx, network, net.JoinHostPort(server, "53"))
			}}
			ips, err := r.LookupNetIP(resolveCtx, network, host)
			cancel()
			if err == nil && len(ips) > 0 {
				return ips, nil
			}
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
		}
		return nil, errors.New("物理网络 DNS 查询失败；没有回退到 VPN DNS")
	}
	return p
}

func (p *Path) Resolve(ctx context.Context, network, host string) ([]netip.Addr, error) {
	if ip, err := netip.ParseAddr(host); err == nil {
		if virtualIP(ip) {
			return nil, errors.New("节点地址是 VPN 虚拟 IP；请使用原始域名或真实 IP")
		}
		return []netip.Addr{ip}, nil
	}
	if strings.EqualFold(strings.TrimSuffix(host, "."), "localhost") {
		return []netip.Addr{netip.MustParseAddr("127.0.0.1")}, nil
	}
	family := "ip"
	if strings.HasSuffix(network, "4") {
		family = "ip4"
	}
	if strings.HasSuffix(network, "6") {
		family = "ip6"
	}
	ips, err := p.lookup(ctx, family, host)
	if err != nil {
		return nil, err
	}
	valid := make([]netip.Addr, 0, len(ips))
	for _, ip := range ips {
		if ip.IsValid() && !virtualIP(ip) && !ip.IsUnspecified() && !ip.IsMulticast() {
			valid = append(valid, ip.Unmap())
		}
	}
	if len(valid) == 0 {
		return nil, errors.New("物理网络 DNS 未返回真实地址；没有回退到 VPN")
	}
	return valid, nil
}

func virtualIP(ip netip.Addr) bool {
	return netip.MustParsePrefix("198.18.0.0/15").Contains(ip.Unmap()) || netip.MustParsePrefix("fc00::/18").Contains(ip)
}

func (p *Path) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return nil, errors.New("节点地址格式不正确")
	}
	ips, err := p.Resolve(ctx, network, host)
	if err != nil {
		return nil, err
	}
	for _, ip := range ips {
		// Bound each individual attempt, so an unreachable first address cannot
		// consume the entire request deadline and hide a working alternate.
		attempt, cancel := context.WithTimeout(ctx, 5*time.Second)
		conn, e := p.dial(attempt, network, net.JoinHostPort(ip.String(), port))
		cancel()
		if e == nil {
			return conn, nil
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
	}
	return nil, errors.New("物理网卡无法连接节点；没有回退到 VPN")
}

func (p *Path) ListenPacket(ctx context.Context, network, address string, remote netip.AddrPort) (net.PacketConn, error) {
	if remote.IsValid() && virtualIP(remote.Addr()) {
		return nil, errors.New("拒绝向 VPN 虚拟 IP 发送节点数据")
	}
	return p.listen(ctx, network, address, remote)
}

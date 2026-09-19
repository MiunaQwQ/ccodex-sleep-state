package netpath

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"sync/atomic"
	"testing"
	"time"

	"github.com/miekg/dns"
)

func TestPhysicalDNSUsesBoundDialer(t *testing.T) {
	packet, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := &dns.Server{PacketConn: packet, Handler: dns.HandlerFunc(func(w dns.ResponseWriter, r *dns.Msg) {
		m := new(dns.Msg)
		m.SetReply(r)
		for _, q := range r.Question {
			if q.Qtype == dns.TypeA {
				m.Answer = append(m.Answer, &dns.A{Hdr: dns.RR_Header{Name: q.Name, Rrtype: dns.TypeA, Class: dns.ClassINET, Ttl: 60}, A: net.ParseIP("203.0.113.9")})
			}
		}
		w.WriteMsg(m)
	})}
	go server.ActivateAndServe()
	defer server.Shutdown()
	p := newPath(Info{Mode: "physical", Interface: "en-test", DNSServers: []string{"192.0.2.53"}})
	var calls atomic.Int32
	p.dial = func(ctx context.Context, network, address string) (net.Conn, error) {
		if address != "192.0.2.53:53" {
			t.Errorf("unexpected DNS endpoint %s", address)
		}
		calls.Add(1)
		return (&net.Dialer{}).DialContext(ctx, network, packet.LocalAddr().String())
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	ips, err := p.Resolve(ctx, "tcp4", "node.invalid.")
	if err != nil || len(ips) != 1 || ips[0].String() != "203.0.113.9" || calls.Load() == 0 {
		t.Fatalf("bound DNS not used: %v %v", ips, err)
	}
}

func TestFakeIPAndDNSFailureNeverFallBack(t *testing.T) {
	for _, literal := range []bool{false, true} {
		p := &Path{lookup: func(context.Context, string, string) ([]netip.Addr, error) {
			return []netip.Addr{netip.MustParseAddr("198.18.2.3")}, nil
		}, dial: func(context.Context, string, string) (net.Conn, error) {
			t.Fatal("dialed a fake IP or fell back")
			return nil, nil
		}}
		host := "node.invalid:443"
		if literal {
			host = "198.18.2.3:443"
		}
		if _, err := p.DialContext(context.Background(), "tcp", host); err == nil {
			t.Fatal("fake IP accepted")
		}
	}
	p := &Path{lookup: func(context.Context, string, string) ([]netip.Addr, error) { return nil, errors.New("DNS down") }, dial: func(context.Context, string, string) (net.Conn, error) {
		t.Fatal("fell back after DNS failure")
		return nil, nil
	}}
	if _, err := p.DialContext(context.Background(), "tcp", "node.invalid:443"); err == nil {
		t.Fatal("DNS failure ignored")
	}
}

func TestPhysicalPathTriesResolvedAddressesAndKeepsHostnameOutOfDial(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	calls := []string{}
	p := &Path{lookup: func(context.Context, string, string) ([]netip.Addr, error) {
		return []netip.Addr{netip.MustParseAddr("198.18.1.2"), netip.MustParseAddr("203.0.113.1"), netip.MustParseAddr("203.0.113.2")}, nil
	}, dial: func(_ context.Context, _, address string) (net.Conn, error) {
		calls = append(calls, address)
		if len(calls) == 1 {
			return nil, errors.New("first address down")
		}
		return a, nil
	}}
	conn, err := p.DialContext(context.Background(), "tcp", "node.invalid:443")
	if err != nil || conn != a || len(calls) != 2 || calls[0] != "203.0.113.1:443" || calls[1] != "203.0.113.2:443" {
		t.Fatalf("unexpected attempts: %v %v", calls, err)
	}
}

func TestPhysicalPacketRejectsVirtualDestination(t *testing.T) {
	p := &Path{listen: func(context.Context, string, string, netip.AddrPort) (net.PacketConn, error) {
		t.Fatal("virtual UDP endpoint allowed")
		return nil, nil
	}}
	if _, err := p.ListenPacket(context.Background(), "udp", ":0", netip.MustParseAddrPort("198.18.0.4:443")); err == nil {
		t.Fatal("missing virtual IP check")
	}
}

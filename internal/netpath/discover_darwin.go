package netpath

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

func command(ctx context.Context, name string, args ...string) ([]byte, error) {
	child, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	return exec.CommandContext(child, name, args...).Output()
}

func physical(i net.Interface) bool {
	if i.Flags&net.FlagUp == 0 || i.Flags&net.FlagLoopback != 0 || !strings.HasPrefix(i.Name, "en") {
		return false
	}
	addrs, _ := i.Addrs()
	for _, addr := range addrs {
		ip, _, err := net.ParseCIDR(addr.String())
		if err == nil && ip.To4() != nil && !ip.IsLinkLocalUnicast() {
			return true
		}
	}
	return false
}

func discover(ctx context.Context, name string) (Info, error) {
	interfaces, err := net.Interfaces()
	if err != nil {
		return Info{}, errors.New("无法读取本机物理网卡")
	}
	for _, iface := range interfaces {
		if (name != "" && iface.Name != name) || !physical(iface) {
			continue
		}
		route, err := command(ctx, "/sbin/route", "-n", "get", "-ifscope", iface.Name, "default")
		if err != nil || !strings.Contains(string(route), "gateway:") || routeInterface(string(route)) != iface.Name {
			continue
		}
		raw, _ := command(ctx, "/usr/sbin/ipconfig", "getoption", iface.Name, "domain_name_server")
		dns := validDNS(strings.Fields(string(raw)))
		if len(dns) == 0 {
			raw, _ = command(ctx, "/usr/sbin/scutil", "--dns")
			dns = scopedDNS(string(raw), iface.Index)
		}
		if len(dns) == 0 {
			return Info{}, errors.New("未找到物理网卡的 DNS；不会改用 VPN DNS")
		}
		return Info{Mode: "physical", Interface: iface.Name, DNSServers: dns}, nil
	}
	return Info{}, errors.New("没有找到带默认网关的物理网卡；请连接 Wi-Fi/有线网络后重试，不会改走 VPN")
}

func routeInterface(s string) string {
	for _, line := range strings.Split(s, "\n") {
		if key, value, ok := strings.Cut(line, ":"); ok && strings.TrimSpace(key) == "interface" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func validDNS(values []string) []string {
	out := []string{}
	seen := map[string]bool{}
	for _, v := range values {
		ip, err := netip.ParseAddr(v)
		if err == nil && !virtualIP(ip) && !ip.IsLoopback() && !ip.IsUnspecified() && !ip.IsMulticast() && !seen[ip.String()] {
			out = append(out, ip.String())
			seen[ip.String()] = true
		}
	}
	return out
}

func scopedDNS(raw string, index int) []string {
	_, scoped, ok := strings.Cut(raw, "DNS configuration (for scoped queries)")
	if !ok {
		return nil
	}
	for _, block := range strings.Split(scoped, "resolver #") {
		servers := []string{}
		matches := false
		for _, line := range strings.Split(block, "\n") {
			key, value, ok := strings.Cut(line, ":")
			if !ok {
				continue
			}
			key = strings.TrimSpace(key)
			fields := strings.Fields(value)
			if len(fields) == 0 {
				continue
			}
			if key == "if_index" {
				matches = fields[0] == strconv.Itoa(index)
			}
			if strings.HasPrefix(key, "nameserver[") {
				servers = append(servers, fields[0])
			}
		}
		if matches {
			return validDNS(servers)
		}
	}
	return nil
}

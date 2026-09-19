package proxyroute

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gylive/ccodex-sleep-state/internal/netpath"
	"github.com/gylive/ccodex-sleep-state/internal/settings"
)

func TestPhysicalPathPreservesNodeIdentityAndSource(t *testing.T) {
	node, err := ParseURI("anytls://fixture-password@node.invalid:443?sni=cert.invalid#fixture")
	if err != nil {
		t.Fatal(err)
	}
	system, err := Build(node, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer system.Close()
	path := &netpath.Path{Info: netpath.Info{Mode: "physical", Interface: "en-test"}}
	physical, err := Build(node, 0, path)
	if err != nil {
		t.Fatal(err)
	}
	defer physical.Close()
	if system.StableID != physical.StableID || system.Connection != physical.Connection || physical.NodePath != path {
		t.Fatal("network policy changed the saved node identity or source")
	}
	if _, ok := node["interface-name"]; ok {
		t.Fatal("runtime policy leaked into subscription")
	}
}

func TestSubscriptionFormats(t *testing.T) {
	QuietCore()
	uri := "socks5://user:password@127.0.0.1:1080#Private%20node\nhttp://127.0.0.1:8080"
	yaml := "proxies:\n  - name: private-node\n    type: socks5\n    server: 127.0.0.1\n    port: 1080\n  - name: second\n    type: http\n    server: 127.0.0.1\n    port: 8080\nrules: ['MATCH,DIRECT']\n"
	for _, source := range []string{uri, base64.StdEncoding.EncodeToString([]byte(uri)), base64.RawURLEncoding.EncodeToString([]byte(uri)), yaml} {
		nodes, err := Parse([]byte(source))
		if err != nil || len(nodes) != 2 {
			t.Fatalf("parse count=%d err=%v", len(nodes), err)
		}
		for i, n := range nodes {
			r, err := Build(n, i)
			if err != nil {
				t.Fatal(err)
			}
			r.Transport.CloseIdleConnections()
			if strings.Contains(r.ID, "private") {
				t.Fatal("node name leaked")
			}
		}
	}
	for _, bad := range []string{"", "garbage", uri + "\nunknown://secret@example.invalid", "proxies: [broken", strings.Repeat("a", MaxSubscriptionBytes+1)} {
		if _, err := Parse([]byte(bad)); err == nil {
			t.Fatal("invalid subscription silently accepted")
		}
	}
}
func TestEncryptedURIAdapters(t *testing.T) {
	QuietCore()
	vmess, _ := json.Marshal(map[string]string{"v": "2", "ps": "private-name", "add": "127.0.0.1", "port": "443", "id": "00000000-0000-4000-8000-000000000001", "aid": "0", "net": "tcp", "type": "none", "tls": "tls"})
	uris := []string{
		"anytls://synthetic-password@127.0.0.1:443?sni=example.invalid#private",
		"ss://" + base64.RawURLEncoding.EncodeToString([]byte("aes-128-gcm:synthetic-password")) + "@127.0.0.1:443#private",
		"vmess://" + base64.StdEncoding.EncodeToString(vmess),
		"vless://00000000-0000-4000-8000-000000000001@127.0.0.1:443?security=tls&type=tcp#private",
		"trojan://synthetic-password@127.0.0.1:443?sni=example.invalid#private",
		"hysteria2://synthetic-password@127.0.0.1:443?sni=example.invalid#private",
	}
	for i, uri := range uris {
		node, err := ParseURI(uri)
		if err != nil {
			t.Fatal(err)
		}
		route, err := Build(node, i)
		if err != nil {
			t.Fatalf("protocol %v: %v", node["type"], err)
		}
		route.Transport.CloseIdleConnections()
	}
}
func TestRejectLocalFileAndInsecureOverrides(t *testing.T) {
	for _, field := range []string{"certificate", "private-key", "dialer-proxy", "interface-name", "routing-mark", "ca"} {
		node := map[string]any{"type": "socks5", "server": "127.0.0.1", "port": 1080, field: "sensitive-value"}
		if _, err := Build(node, 0); err == nil || strings.Contains(err.Error(), "sensitive-value") {
			t.Fatal("unsafe field accepted or leaked")
		}
	}
	node := map[string]any{"type": "http", "skip-cert-verify": true}
	if _, err := Build(node, 0); err == nil {
		t.Fatal("insecure TLS accepted")
	}
}
func TestSubscriptionDownloadAndEnvironment(t *testing.T) {
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, "http://127.0.0.1:8080") }))
	defer source.Close()
	t.Setenv("TEST_SUBSCRIPTION_URL", source.URL+"?token=synthetic-secret")
	c := settings.Default()
	c.Direct = false
	c.Subscriptions = []settings.Source{{URLEnv: "TEST_SUBSCRIPTION_URL"}}
	routes, err := Load(context.Background(), c)
	if err != nil || len(routes) != 1 {
		t.Fatalf("routes=%d err=%v", len(routes), err)
	}
	routes[0].Transport.CloseIdleConnections()
	c.Subscriptions = []settings.Source{{URL: "http://example.invalid?secret=never-print"}}
	if _, err = Load(context.Background(), c); err == nil || strings.Contains(err.Error(), "never-print") {
		t.Fatal("unsafe URL accepted or leaked")
	}
}
func TestHTTPConnectAndSOCKS5Authentication(t *testing.T) {
	QuietCore()
	for _, protocol := range []string{"http", "socks5"} {
		t.Run(protocol, func(t *testing.T) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			result := make(chan error, 1)
			go func() {
				conn, err := listener.Accept()
				if err != nil {
					result <- err
					return
				}
				defer conn.Close()
				conn.SetDeadline(time.Now().Add(5 * time.Second))
				if protocol == "http" {
					req, err := http.ReadRequest(bufio.NewReader(conn))
					if err != nil {
						result <- err
						return
					}
					want := "Basic " + base64.StdEncoding.EncodeToString([]byte("alice:synthetic-password"))
					if req.Method != "CONNECT" || req.Host != "test.invalid:443" || req.Header.Get("Proxy-Authorization") != want {
						result <- errors.New("CONNECT auth or target mismatch")
						return
					}
					fmt.Fprint(conn, "HTTP/1.1 200 Connection Established\r\n\r\n")
				} else if err := socksHandshake(conn); err != nil {
					result <- err
					return
				}
				data := make([]byte, 4)
				if _, err = io.ReadFull(conn, data); err == nil {
					_, err = conn.Write(data)
				}
				result <- err
			}()
			node, err := ParseURI(protocol + "://alice:synthetic-password@" + listener.Addr().String())
			if err != nil {
				t.Fatal(err)
			}
			route, err := Build(node, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer route.Transport.CloseIdleConnections()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			conn, err := route.Transport.DialContext(ctx, "tcp", "test.invalid:443")
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			conn.SetDeadline(time.Now().Add(5 * time.Second))
			conn.Write([]byte("ping"))
			got := make([]byte, 4)
			if _, err = io.ReadFull(conn, got); err != nil || string(got) != "ping" {
				t.Fatalf("tunnel failed: %v", err)
			}
			if err = <-result; err != nil {
				t.Fatal(err)
			}
		})
	}
}
func socksHandshake(conn net.Conn) error {
	head := make([]byte, 2)
	if _, err := io.ReadFull(conn, head); err != nil {
		return err
	}
	methods := make([]byte, int(head[1]))
	if _, err := io.ReadFull(conn, methods); err != nil {
		return err
	}
	conn.Write([]byte{5, 2})
	if _, err := io.ReadFull(conn, head); err != nil {
		return err
	}
	user := make([]byte, int(head[1]))
	if _, err := io.ReadFull(conn, user); err != nil {
		return err
	}
	size := make([]byte, 1)
	if _, err := io.ReadFull(conn, size); err != nil {
		return err
	}
	password := make([]byte, int(size[0]))
	if _, err := io.ReadFull(conn, password); err != nil {
		return err
	}
	if string(user) != "alice" || string(password) != "synthetic-password" {
		return errors.New("SOCKS authentication mismatch")
	}
	conn.Write([]byte{1, 0})
	request := make([]byte, 4)
	if _, err := io.ReadFull(conn, request); err != nil {
		return err
	}
	if request[3] != 3 {
		return errors.New("target was not sent as a domain")
	}
	if _, err := io.ReadFull(conn, size); err != nil {
		return err
	}
	host := make([]byte, int(size[0]))
	if _, err := io.ReadFull(conn, host); err != nil {
		return err
	}
	port := make([]byte, 2)
	if _, err := io.ReadFull(conn, port); err != nil {
		return err
	}
	if string(host) != "test.invalid" || port[0] != 1 || port[1] != 187 {
		return errors.New("SOCKS target mismatch")
	}
	_, err := conn.Write([]byte{5, 0, 0, 1, 127, 0, 0, 1, 0, 0})
	return err
}

func TestEmptyClashTemplateReportsMissingNodes(t *testing.T) {
	for _, raw := range []string{
		"proxies: []\nproxy-groups: [{name: group, type: select, proxies: [DIRECT]}]\nrules: ['MATCH,DIRECT']\n",
		`{"proxies":[],"rules":["MATCH,DIRECT"]}`,
	} {
		_, err := Parse([]byte(raw))
		if err == nil || !strings.Contains(err.Error(), "empty proxies list") {
			t.Fatalf("empty template was mistaken for an encoding error: %v", err)
		}
	}
}

func TestSubscriptionUserAgentSelectsNodeFormat(t *testing.T) {
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("User-Agent") != "custom-yaml-client" {
			fmt.Fprint(w, "proxies: []\n")
			return
		}
		fmt.Fprint(w, "proxies:\n- name: private\n  type: anytls\n  server: 127.0.0.1\n  port: 443\n  password: synthetic-password\n")
	}))
	defer source.Close()
	c := settings.Default()
	c.Direct = false
	c.Subscriptions = []settings.Source{{URL: source.URL, UserAgent: "custom-yaml-client"}}
	routes, err := Load(context.Background(), c)
	if err != nil || len(routes) != 1 {
		t.Fatalf("subscription format selection failed: %v", err)
	}
	for _, r := range routes {
		r.Close()
	}
}

func TestSubscriptionFiltersRunBeforeUnsafeNodeConstruction(t *testing.T) {
	nodes := []map[string]any{
		{"name": "香港", "type": "anytls", "server": "first.invalid"},
		{"name": "台湾", "type": "anytls", "server": "second.invalid"},
		{"name": "澳门", "type": "anytls", "server": "third.invalid"},
		{"name": "Japan", "type": "anytls", "server": "fourth.invalid"},
		{"name": "US", "type": "trojan", "server": "fifth.invalid", "skip-cert-verify": true},
	}
	source := settings.Source{IncludeProtocols: []string{"anytls"}, ExcludeKeywords: []string{"香港", "台湾", "澳门"}}
	kept := filterNodes(nodes, source)
	if len(kept) != 1 || kept[0]["name"] != "Japan" {
		t.Fatal("excluded region or protocol survived filtering")
	}
	if nodes[4]["skip-cert-verify"] != true {
		t.Fatal("filter mutated node security settings")
	}
}

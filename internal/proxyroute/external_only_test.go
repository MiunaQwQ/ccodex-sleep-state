package proxyroute

import (
	"context"
	"github.com/gylive/ccodex-sleep-state/internal/settings"
	"testing"
)

func TestExternalOnlyAllowsEndpointsButRejectsEmbeddedTunnel(t *testing.T) {
	c := settings.Default()
	c.ExternalProxyOnly = true
	c.Direct = false
	for _, endpoint := range []string{"http://127.0.0.1:18080", "https://127.0.0.1:18081", "socks5://127.0.0.1:18082"} {
		c.ProxyURLs = []string{endpoint}
		rs, err := Load(context.Background(), c)
		if err != nil {
			t.Fatal(endpoint, err)
		}
		for _, r := range rs {
			r.Close()
		}
	}
	c.ProxyURLs = []string{"trojan://fixture-only@127.0.0.1:18443?sni=example.invalid"}
	rs, err := Load(context.Background(), c)
	for _, r := range rs {
		r.Close()
	}
	if err == nil {
		t.Fatal("embedded tunnel accepted in external-only mode")
	}
}

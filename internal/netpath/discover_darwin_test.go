package netpath

import "testing"

func TestScopedDNSExcludesTunnelResolver(t *testing.T) {
	raw := `DNS configuration
resolver #1
  nameserver[0] : 198.18.0.2
  if_index : 29 (utun4)
DNS configuration (for scoped queries)
resolver #1
  nameserver[0] : 192.168.31.1
  nameserver[1] : 198.18.0.2
  if_index : 15 (en1)
resolver #2
  nameserver[0] : 198.18.0.2
  if_index : 29 (utun4)
`
	got := scopedDNS(raw, 15)
	if len(got) != 1 || got[0] != "192.168.31.1" {
		t.Fatalf("scoped DNS wrong: %v", got)
	}
	if len(scopedDNS(raw, 29)) != 0 || len(scopedDNS(raw, 20)) != 0 {
		t.Fatal("tunnel or missing interface accepted")
	}
}

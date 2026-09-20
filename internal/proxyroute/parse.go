// Package proxyroute translates user-supplied subscriptions into outbound
// connections. It does not run a proxy listener, a TUN, or a system controller.
package proxyroute

import (
	"bytes"
	"encoding/base64"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"github.com/metacubex/mihomo/common/convert"
	"gopkg.in/yaml.v3"
)

const MaxSubscriptionBytes = 2 << 20
const MaxNodes = 256

func Parse(data []byte) ([]map[string]any, error) {
	if len(data) > MaxSubscriptionBytes {
		return nil, errors.New("subscription exceeds 2 MiB")
	}
	data = bytes.TrimSpace(data)
	var document struct {
		Proxies *[]map[string]any `yaml:"proxies"`
	}
	if yaml.Unmarshal(data, &document) == nil && document.Proxies != nil {
		if len(*document.Proxies) == 0 {
			return nil, errors.New("subscription has an empty proxies list; the server supplied no nodes")
		}
		if len(*document.Proxies) > MaxNodes {
			return nil, errors.New("subscription exceeds 256 nodes")
		}
		return *document.Proxies, nil
	}
	if !bytes.Contains(data, []byte("://")) {
		var decoded []byte
		var err error
		compact := strings.Join(strings.Fields(string(data)), "")
		for _, enc := range []*base64.Encoding{base64.StdEncoding, base64.RawStdEncoding, base64.URLEncoding, base64.RawURLEncoding} {
			decoded, err = enc.DecodeString(compact)
			if err == nil {
				break
			}
		}
		if err != nil {
			return nil, errors.New("expected Clash YAML or a URI/Base64 subscription")
		}
		data = decoded
	}
	var nodes []map[string]any
	for i, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		node, err := ParseURI(line)
		if err != nil {
			return nil, fmt.Errorf("subscription line %d: invalid or unsupported proxy URI", i+1)
		}
		nodes = append(nodes, node)
		if len(nodes) > MaxNodes {
			return nil, errors.New("subscription exceeds 256 nodes")
		}
	}
	if len(nodes) == 0 {
		return nil, errors.New("subscription contains no supported nodes")
	}
	return nodes, nil
}

func ParseURI(raw string) (map[string]any, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return nil, errors.New("invalid proxy URI")
	}
	scheme := strings.ToLower(u.Scheme)
	if scheme == "http" || scheme == "https" || scheme == "socks5" || scheme == "socks5h" {
		port, err := strconv.Atoi(u.Port())
		if err != nil || port < 1 || port > 65535 || u.Hostname() == "" || (u.Path != "" && u.Path != "/") || u.RawQuery != "" {
			return nil, errors.New("proxy URI requires a host and explicit port")
		}
		kind := scheme
		if scheme == "https" {
			kind = "http"
		}
		if scheme == "socks5h" {
			kind = "socks5"
		}
		node := map[string]any{"__source_uri": raw, "name": "imported", "type": kind, "server": u.Hostname(), "port": port}
		if u.Fragment != "" {
			node["name"] = u.Fragment
		}
		if scheme == "https" {
			node["tls"] = true
		}
		if u.User != nil {
			node["username"] = u.User.Username()
			node["password"], _ = u.User.Password()
		}
		return node, nil
	}
	// Convert each line separately: the core's bulk converter otherwise skips
	// malformed entries. A partial import must not look like a complete one.
	nodes, err := convert.ConvertsV2Ray([]byte(raw))
	if err != nil || len(nodes) != 1 {
		return nil, errors.New("unsupported proxy URI")
	}
	nodes[0]["__source_uri"] = raw
	return nodes[0], nil
}

func validateNode(node map[string]any) error {
	switch node["type"] {
	case "http", "socks5", "ss", "ssr", "vmess", "vless", "trojan", "hysteria", "hysteria2", "tuic", "anytls":
	default:
		return errors.New("unsupported protocol (see docs/proxies.md)")
	}
	return validateFields(node)
}
func validateFields(value any) error {
	switch v := value.(type) {
	case map[string]any:
		for k, x := range v {
			switch k {
			case "dialer-proxy", "interface-name", "routing-mark", "certificate", "private-key", "private-key-passphrase", "ca", "ca-str":
				return errors.New("subscription contains a forbidden local-file or routing override")
			case "skip-cert-verify":
				if x != false && x != "false" && x != nil {
					return errors.New("insecure TLS is not allowed; fix the node certificate")
				}
			}
			if err := validateFields(x); err != nil {
				return err
			}
		}
	case []any:
		for _, x := range v {
			if err := validateFields(x); err != nil {
				return err
			}
		}
	}
	return nil
}

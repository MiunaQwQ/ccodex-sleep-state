package service

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"github.com/gylive/ccodex-sleep-state/internal/proxyroute"
	"net/url"
	"strings"
	"unicode/utf8"
)

// Normalize subscription wrappers, not node credentials. Every entry point uses
// this parser so a Shadowrocket JSON/sub:// paste works in the ordinary form too.
func normalizeSource(v sourceRequest) (sourceRequest, error) {
	auto := v.Mode == "auto"
	raw := strings.TrimSpace(strings.TrimPrefix(v.Value, "\ufeff"))
	if len(raw) > 65536 || !utf8.ValidString(raw) {
		return v, errors.New("导入内容须为 UTF-8，最多 64 KiB")
	}
	if strings.HasPrefix(raw, "{") {
		var item struct {
			Type string `json:"type"`
			Host string `json:"host"`
		}
		if json.Unmarshal([]byte(raw), &item) != nil || !strings.EqualFold(item.Type, "Subscribe") || item.Host == "" {
			return v, errors.New("无法识别订阅 JSON；请粘贴 Subscribe 导出或节点链接")
		}
		raw = item.Host
		v.Mode = "subscription"
	}
	lines := strings.Split(raw, "\n")

	count := 0
	for i, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			lines[i] = line
			continue
		}
		if strings.HasPrefix(line, "[") {
			if end := strings.LastIndex(line, "]("); end >= 0 && strings.HasSuffix(line, ")") {
				line = line[end+2 : len(line)-1]
			}
		}
		if strings.HasPrefix(strings.ToLower(line), "sub://") {
			encoded := strings.SplitN(line[6:], "#", 2)[0]
			encoded = strings.TrimRight(strings.NewReplacer("-", "+", "_", "/").Replace(encoded), "=")
			value, err := base64.RawStdEncoding.DecodeString(encoded)
			if err != nil || !utf8.Valid(value) {
				return v, errors.New("sub:// 链接不完整或编码无效")
			}
			line = strings.TrimSpace(string(value))
			v.Mode = "subscription-list"
		}

		lines[i] = line
		count++
	}
	v.Value = strings.Join(lines, "\n")
	if auto {
		v.Mode = "mixed"
	}
	if v.Mode == "subscription" && count > 1 {
		v.Mode = "subscription-list"
	}
	return v, nil
}

func isSubscriptionSource(raw string) bool {
	u, err := url.Parse(raw)
	return err == nil && (u.Scheme == "https" || u.Scheme == "http") && u.User == nil && (u.RawQuery != "" || (u.Path != "" && u.Path != "/"))
}

func importDiff(old []map[string]any, next []proxyroute.Route) map[string]int {
	existing := map[string]bool{}
	for _, r := range old {
		existing[r["id"].(string)] = true
	}
	added, kept := 0, 0
	for _, r := range next {
		if existing[r.ID] {
			kept++
			delete(existing, r.ID)
		} else {
			added++
		}
	}
	return map[string]int{"added": added, "retained": kept, "removed": len(existing)}
}

package codexconfig

import (
	"bytes"
	"errors"
	"github.com/pelletier/go-toml/v2/unstable"
	"strconv"
)

// PatchMissingEndpoint inserts one missing key, preserving all other bytes.
// Existing endpoints/authentication are never guessed or silently replaced.
func PatchMissingEndpoint(original []byte, profile, endpoint, authMode string) ([]byte, error) {
	doc, err := document(original)
	if err != nil {
		return nil, err
	}
	if profile == "" {
		profile, _ = doc["profile"].(string)
	}
	selected := selectedTable(doc, profile)
	id, _ := selected["model_provider"].(string)
	if id == "" && profile != "" {
		id, _ = doc["model_provider"].(string)
	}
	if id == "" || id == "openai" || id == provider {
		return nil, errors.New("仅补全明确选中的中转 provider，不修改官方或已接管配置")
	}
	fields := tableAt(doc, "model_providers", id)
	if fields == nil {
		return nil, errors.New("中转 provider 定义缺失，请先在 CCS 完整配置")
	}
	if _, exists := fields["base_url"]; exists {
		return nil, errors.New("中转已有 base_url，未覆盖；请检查地址或使用完整连接向导")
	}
	var parser unstable.Parser
	parser.Reset(original)
	for parser.NextExpression() {
		n := parser.Expression()
		if n.Kind != unstable.Table {
			continue
		}
		keys := n.Key()
		parts := []string{}
		start := 0
		for keys.Next() {
			if len(parts) == 0 {
				start = int(keys.Node().Raw.Offset)
			}
			parts = append(parts, string(keys.Node().Data))
		}
		if len(parts) != 2 || parts[0] != "model_providers" || parts[1] != id {
			continue
		}
		end := bytes.IndexByte(original[start:], '\n')
		if end < 0 {
			end = len(original)
		} else {
			end += start + 1
		}
		prefix := append([]byte{}, original[:end]...)
		if len(prefix) > 0 && prefix[len(prefix)-1] != '\n' {
			prefix = append(prefix, '\n')
		}
		result := append(prefix, []byte("base_url = "+strconv.Quote(endpoint)+"\n")...)
		result = append(result, original[end:]...)
		if _, err := ResolveWithAuth(result, profile, authMode); err != nil {
			return nil, err
		}
		return result, nil
	}
	return nil, errors.New("中转使用内联/点键表，无法安全局部插入；请在 CCS 补全地址，或使用备份重建向导")
}

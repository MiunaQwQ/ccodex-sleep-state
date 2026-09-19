package service

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/gylive/ccodex-sleep-state/internal/settings"
)

// NormalizeRelayURL accepts a base URL or a pasted Responses/Models endpoint.
// It never probes guessed hosts, downgrades TLS, or follows redirects with keys.
func normalizeRelayURL(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if !strings.Contains(raw, "://") {
		raw = "https://" + raw
	}
	u, err := url.Parse(raw)
	if err != nil || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.RawPath != "" || strings.Contains(u.Path, "..") || strings.ContainsAny(u.Path, "\\\r\n") {
		return "", errors.New("请填写无密钥、查询参数的中转基础地址")
	}
	if u.Scheme != "https" && !(u.Scheme == "http" && settings.IsLoopback(u.Hostname())) {
		return "", errors.New("中转地址需要 HTTPS；只有本机测试允许 HTTP")
	}
	u.Path = strings.TrimRight(u.Path, "/")
	for _, suffix := range []string{"/responses", "/models", "/chat/completions"} {
		u.Path = strings.TrimSuffix(u.Path, suffix)
	}
	if u.Path == "" {
		u.Path = "/v1"
	}
	return u.String(), nil
}
func relayModels(ctx context.Context, base, key string) (map[string]any, error) {
	endpoint, err := normalizeRelayURL(base)
	if err != nil {
		return nil, err
	}
	key = strings.TrimSpace(key)
	if len(key) < 8 || len(key) > 16384 || strings.ContainsAny(key, "\r\n") || strings.HasPrefix(key, "eyJ") {
		return nil, errors.New("请使用该中转专用 API key，不要粘贴官方 OAuth 登录 token")
	}
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "GET", endpoint+"/models", nil)
	req.Header.Set("Authorization", "Bearer "+key)
	client := &http.Client{Timeout: 20 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	resp, err := client.Do(req)
	if err != nil {
		return nil, errors.New("无法连接模型接口；请检查地址、网络或代理。没有发送生成请求")
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return map[string]any{"status": resp.StatusCode, "models": []string{}, "message": "模型列表接口未成功。401/403 检查密钥权限；404 可能是不支持 /models；429 等待恢复。不自动换地址重试。"}, nil
	}
	b, err := io.ReadAll(io.LimitReader(resp.Body, (1<<20)+1))
	if err != nil || len(b) > 1<<20 {
		return nil, errors.New("模型列表回复过大或不完整")
	}
	var result struct {
		Data []struct {
			ID string `json:"id"`
		} `json:"data"`
	}
	if json.Unmarshal(b, &result) != nil {
		return nil, errors.New("不是兼容的 /models JSON 回复")
	}
	models := []string{}
	seen := map[string]bool{}
	for _, m := range result.Data {
		if settings.SupportedModel(m.ID) && !seen[m.ID] {
			models = append(models, m.ID)
			seen[m.ID] = true
		}
	}
	return map[string]any{"status": 200, "models": models, "message": "模型列表已读取，只展示 Astra / Sol / Terra。列出不等于已验证生成权限；不采集 state，密钥不保存，模型接口本身不发送生成请求。"}, nil
}

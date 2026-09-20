package gateway

import (
	"errors"
	"io"
	"net/http"
	"strings"
)

const codexPrefix = "/backend-api/codex/"

// Forward the provider namespace, including features introduced by later Codex
// clients. Keep management paths, proxy tunneling and path traversal outside it.
func codexEndpoint(r *http.Request) bool {
	switch r.Method {
	case http.MethodGet, http.MethodHead, http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete, http.MethodOptions:
	default:
		return false
	}
	if !strings.HasPrefix(r.URL.Path, codexPrefix) || !strings.HasPrefix(r.URL.EscapedPath(), codexPrefix) {
		return false
	}
	for _, part := range strings.Split(r.URL.Path, "/") {
		if part == "." || part == ".." || strings.ContainsAny(part, "\\\x00\r\n") {
			return false
		}
	}
	return true
}

// Use a category instead of logging arbitrary resource IDs, queries or bodies.
func endpointKind(path string) string {
	switch path {
	case codexPrefix + "responses":
		return "responses"
	case codexPrefix + "responses/compact":
		return "compact"
	case codexPrefix + "models":
		return "models"
	case codexPrefix + "images/generations":
		return "image_generation"
	case codexPrefix + "images/edits":
		return "image_edit"
	case codexPrefix + "alpha/search":
		return "search"
	default:
		return "codex_passthrough"
	}
}

func opaqueRequestBody(r *http.Request, limit int64) ([]byte, int, error) {
	defer r.Body.Close()
	body, err := io.ReadAll(io.LimitReader(r.Body, limit+1))
	if int64(len(body)) > limit {
		return nil, http.StatusRequestEntityTooLarge, errors.New("请求体超过配置上限，请在连接设置调整；请求尚未转发")
	}
	if err != nil {
		return nil, http.StatusBadRequest, errors.New("无法完整读取请求体")
	}
	return body, 0, nil
}

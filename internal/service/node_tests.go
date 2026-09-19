package service

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gylive/ccodex-sleep-state/internal/proxyroute"
)

type nodeTestResult struct {
	ID         string    `json:"id"`
	Phase      string    `json:"phase"`
	Reachable  bool      `json:"reachable"`
	Selectable bool      `json:"selectable"`
	Status     int       `json:"status,omitempty"`
	DurationMS int64     `json:"duration_ms,omitempty"`
	Message    string    `json:"message"`
	At         time.Time `json:"at"`
}
type nodeTestSnapshot struct {
	Running   bool                      `json:"running"`
	Total     int                       `json:"total"`
	Completed int                       `json:"completed"`
	Results   map[string]nodeTestResult `json:"results"`
}
type nodeTester struct {
	mu         sync.Mutex
	cancel     context.CancelFunc
	generation uint64
	state      nodeTestSnapshot
}

func (t *nodeTester) snapshot() nodeTestSnapshot {
	t.mu.Lock()
	defer t.mu.Unlock()
	s := t.state
	s.Results = make(map[string]nodeTestResult, len(t.state.Results))
	for id, result := range t.state.Results {
		s.Results[id] = result
	}
	return s
}
func (t *nodeTester) stop() {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.cancel != nil {
		t.cancel()
	}
}
func (t *nodeTester) start(parent context.Context, catalog []map[string]any, target string, timeout time.Duration) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.state.Running {
		return errors.New("已有延迟测试正在运行，请等待或先停止测试")
	}
	ctx, cancel := context.WithCancel(parent)
	t.cancel = cancel
	t.generation++
	generation := t.generation
	t.state.Running = true
	t.state.Total = len(catalog)
	t.state.Completed = 0
	if t.state.Results == nil {
		t.state.Results = map[string]nodeTestResult{}
	}
	for _, row := range catalog {
		id := row["id"].(string)
		t.state.Results[id] = nodeTestResult{ID: id, Phase: "queued", Message: "等待测试"}
	}
	go func() {
		defer cancel()
		jobs := make(chan map[string]any)
		var workers sync.WaitGroup
		for i := 0; i < min(4, len(catalog)); i++ {
			workers.Add(1)
			go func() {
				defer workers.Done()
				for row := range jobs {
					id := row["id"].(string)
					t.mu.Lock()
					t.state.Results[id] = nodeTestResult{ID: id, Phase: "testing", Message: "正在连接上游"}
					t.mu.Unlock()
					result := testNode(ctx, row, target, timeout)
					t.mu.Lock()
					if t.generation == generation {
						t.state.Results[id] = result
						t.state.Completed++
					}
					t.mu.Unlock()
				}
			}()
		}
		for _, row := range catalog {
			jobs <- row
		}
		close(jobs)
		workers.Wait()
		t.mu.Lock()
		if t.generation == generation {
			t.state.Running = false
			t.cancel = nil
		}
		t.mu.Unlock()
	}()
	return nil
}

func testNode(parent context.Context, row map[string]any, target string, timeout time.Duration) nodeTestResult {
	result := nodeTestResult{ID: row["id"].(string), Phase: "failed", At: time.Now().UTC()}
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	if parent.Err() != nil {
		result.Phase = "cancelled"
		result.Message = "测试已停止"
		return result
	}
	var route proxyroute.Route
	var err error
	if result.ID == "direct" {
		route = proxyroute.Route{Transport: &http.Transport{Proxy: nil}}
	} else {
		connection, _ := row["connection"].(string)
		var nodes []map[string]any
		nodes, err = proxyroute.Parse([]byte(connection))
		if err == nil && len(nodes) == 1 {
			route, err = proxyroute.Build(nodes[0], 0)
		} else {
			err = errors.New("invalid cached node")
		}
	}
	if err != nil {
		result.Message = "节点配置无法构建"
		return result
	}
	defer route.Close()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		result.Message = "测试目标无效"
		return result
	}
	client := http.Client{Transport: route.Transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	started := time.Now()
	response, err := client.Do(req)
	result.DurationMS = time.Since(started).Milliseconds()
	result.At = time.Now().UTC()
	if err != nil {
		if parent.Err() != nil {
			result.Phase = "cancelled"
			result.Message = "测试已停止"
		} else if ctx.Err() != nil {
			result.Phase = "timeout"
			result.Message = "连接或上游响应超时"
		} else {
			result.Message = "连接失败（代理 / DNS / TLS）"
		}
		return result
	}
	response.Body.Close()
	result.Reachable = true
	result.Status = response.StatusCode
	// /models is intentionally requested without a bearer. 401 is expected and
	// proves HTTPS transport, not account/model/state eligibility.
	if (response.StatusCode >= 200 && response.StatusCode < 300) || response.StatusCode == 401 {
		result.Phase = "reachable"
		result.Selectable = true
		result.Message = "上游可达；未验证模型或 292"
	} else {
		result.Phase = "restricted"
		result.Message = "收到 HTTP 响应，但受限或异常，需另行确认"
	}
	return result
}

func (c *control) nodeTestAction(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/admin/api/pool/test-stop" {
		c.tests.stop()
		reply(w, 200, map[string]string{"message": "正在停止节点延迟测试"})
		return
	}
	var v struct {
		IDs            []string `json:"ids"`
		TimeoutSeconds int      `json:"timeout_seconds"`
	}
	if err := decode(w, r, &v); err != nil {
		reply(w, 400, map[string]string{"error": err.Error()})
		return
	}
	if v.TimeoutSeconds == 0 {
		v.TimeoutSeconds = 6
	}
	if v.TimeoutSeconds < 1 || v.TimeoutSeconds > 30 {
		reply(w, 400, map[string]string{"error": "测试超时需为 1–30 秒"})
		return
	}
	ids := map[string]bool{}
	for _, id := range v.IDs {
		ids[id] = true
	}
	catalog := []map[string]any{}
	for _, row := range c.catalog {
		if len(v.IDs) == 0 || ids[row["id"].(string)] {
			catalog = append(catalog, row)
			delete(ids, row["id"].(string))
		}
	}
	if len(catalog) == 0 || len(ids) > 0 {
		reply(w, 400, map[string]string{"error": "没有找到待测试节点，请刷新列表"})
		return
	}
	if err := c.tests.start(c.ctx, catalog, strings.TrimRight(c.effective().Upstream, "/")+"/models", time.Duration(v.TimeoutSeconds)*time.Second); err != nil {
		reply(w, 409, map[string]string{"error": err.Error()})
		return
	}
	reply(w, 202, map[string]string{"message": "已开始批量延迟测试，最多同时测 4 个节点；不携带登录信息，不调用模型。"})
}

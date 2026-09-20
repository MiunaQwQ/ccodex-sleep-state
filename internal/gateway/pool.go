package gateway

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	"github.com/gylive/ccodex-sleep-state/internal/proxyroute"
	"github.com/gylive/ccodex-sleep-state/internal/routepool"
)

type PoolRow struct {
	ID       string `json:"id"`
	Label    string `json:"label"`
	Protocol string `json:"protocol"`
	routepool.Entry
	Connection proxyroute.ConnectionHealth `json:"connection"`
}

func (e *Engine) PoolStatus() []PoolRow {
	rows := make([]PoolRow, 0, len(e.routes))
	for _, r := range e.routes {
		rows = append(rows, PoolRow{r.ID, r.DisplayName, r.Protocol, e.pool.Get(r.ID), r.ConnectionHealth()})
	}
	return rows
}
func (e *Engine) selectEgress(harvest int, hasState bool) (int, error) {
	if e.settings().EgressMode == "fixed" {
		for i, r := range e.routes {
			if r.ID == e.settings().EgressRoute {
				state := e.pool.Get(r.ID).State
				if state == "failed" {
					for offset := 1; offset < len(e.routes); offset++ {
						next := (i + offset) % len(e.routes)
						st := e.pool.Get(e.routes[next].ID).State
						if st != "failed" && st != "disabled" {
							return next, nil
						}
					}
					return 0, errors.New("固定出口连接失败，且没有其它可用节点；请手动恢复或增加节点")
				}
				if state == "disabled" {
					return 0, errors.New("固定出口已停用，请先取消停用或选择其它节点")
				}
				return i, nil
			}
		}
		return 0, errors.New("固定出口不存在，请重新选择")
	}
	if e.settings().EgressMode != "random" {
		if hasState {
			return harvest, nil
		}
		return 0, nil
	}
	candidates := []int{}
	for i, r := range e.routes {
		st := e.pool.Get(r.ID).State
		if hasState && i == harvest {
			continue
		}

		if st == "disabled" || st == "failed" || (e.settings().PoolEnabled && st == "used") {
			continue
		}
		candidates = append(candidates, i)
	}
	if len(candidates) == 0 {
		return 0, errors.New("没有可用的独立出口；请补充节点或手动回收。也可明确选择固定出口/采集同出口模式")
	}
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return 0, errors.New("无法随机选择出口")
	}
	return candidates[binary.LittleEndian.Uint64(b[:])%uint64(len(candidates))], nil
}

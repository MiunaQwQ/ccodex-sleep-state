package settings

import (
	"errors"
	"time"
)

// CollectionPolicy applies only to additional state probes, not user requests.
type CollectionPolicy struct {
	// AutomaticDisabled stops all background and request-triggered collection.
	// A missing field remains enabled for backwards compatibility with older
	// configuration files.
	AutomaticDisabled         bool   `json:"automatic_disabled,omitempty"`
	Cadence                   string `json:"cadence"`
	StandbyTarget             int    `json:"standby_target"`
	StandbySpacingSeconds     int    `json:"standby_spacing_seconds"`
	FailureIntervalSeconds    int    `json:"failure_interval_seconds"`
	FailureMaxIntervalSeconds int    `json:"failure_max_interval_seconds"`
	SearchBudget              int    `json:"search_budget"`
	SearchPauseSeconds        int    `json:"search_pause_seconds"`
	HourlyBudget              int    `json:"hourly_budget"`
	IdleSeconds               int    `json:"idle_seconds"`
}

func DefaultCollection() CollectionPolicy {
	return CollectionPolicy{Cadence: "round", StandbyTarget: 2, StandbySpacingSeconds: 900, FailureIntervalSeconds: 5, FailureMaxIntervalSeconds: 60, SearchBudget: 12, SearchPauseSeconds: 600, HourlyBudget: 30, IdleSeconds: 300}
}

// Zero means one immediate retry, followed by a 30-second exponential backoff.
// A zero ceiling is useful for isolated scheduler tests but invalid in config.
func (c CollectionPolicy) FailureDelay(failures int) time.Duration {
	if failures < 1 || c.FailureMaxIntervalSeconds <= 0 {
		return 0
	}
	seconds := c.FailureIntervalSeconds
	if seconds == 0 {
		if failures == 1 {
			return 0
		}
		seconds, failures = 30, failures-1
	}
	for i := 1; i < failures && seconds < c.FailureMaxIntervalSeconds; i++ {
		seconds *= 2
	}
	return time.Duration(min(seconds, c.FailureMaxIntervalSeconds)) * time.Second
}

func (c CollectionPolicy) Validate() error {
	if c.Cadence != "" && c.Cadence != "round" && c.Cadence != "backoff" {
		return errors.New("采集节奏须为 round 或 backoff")
	}
	if c.StandbyTarget < 0 || c.StandbyTarget > 16 || c.StandbySpacingSeconds < 30 || c.StandbySpacingSeconds > 3600 {
		return errors.New("备用数量须为 0–16，错峰间隔须为 30–3600 秒")
	}
	if c.FailureIntervalSeconds < 0 || c.FailureMaxIntervalSeconds < 1 || c.FailureMaxIntervalSeconds < c.FailureIntervalSeconds || c.FailureMaxIntervalSeconds > 3600 {
		return errors.New("失败初始间隔至少 0 秒，最大间隔须为 1–3600 秒且不小于初始间隔")
	}
	if c.SearchBudget < 1 || c.SearchBudget > 1000 || c.SearchPauseSeconds < 30 || c.SearchPauseSeconds > 86400 || c.HourlyBudget < 1 || c.HourlyBudget > 1000 || c.IdleSeconds < 30 || c.IdleSeconds > 1800 {
		return errors.New("连续失败预算及每小时预算须为 1–1000 次，预算暂停须为 30–86400 秒，空闲暂停须为 30–1800 秒")
	}
	return nil
}

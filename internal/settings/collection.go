package settings

import "errors"

// CollectionPolicy applies only to additional state probes, not user requests.
type CollectionPolicy struct {
	StandbyTarget             int `json:"standby_target"`
	StandbySpacingSeconds     int `json:"standby_spacing_seconds"`
	FailureIntervalSeconds    int `json:"failure_interval_seconds"`
	FailureMaxIntervalSeconds int `json:"failure_max_interval_seconds"`
	SearchBudget              int `json:"search_budget"`
	SearchPauseSeconds        int `json:"search_pause_seconds"`
	HourlyBudget              int `json:"hourly_budget"`
	IdleSeconds               int `json:"idle_seconds"`
}

func DefaultCollection() CollectionPolicy {
	return CollectionPolicy{2, 900, 5, 60, 12, 600, 30, 300}
}

func (c CollectionPolicy) Validate() error {
	if c.StandbyTarget < 0 || c.StandbyTarget > 16 || c.StandbySpacingSeconds < 30 || c.StandbySpacingSeconds > 3600 {
		return errors.New("备用数量须为 0–16，错峰间隔须为 30–3600 秒")
	}
	if c.FailureIntervalSeconds < 1 || c.FailureMaxIntervalSeconds < c.FailureIntervalSeconds || c.FailureMaxIntervalSeconds > 3600 {
		return errors.New("失败间隔至少 1 秒，最大间隔须不小于初始间隔且不超过 3600 秒")
	}
	if c.SearchBudget < 1 || c.SearchBudget > 1000 || c.SearchPauseSeconds < 30 || c.SearchPauseSeconds > 86400 || c.HourlyBudget < 1 || c.HourlyBudget > 1000 || c.IdleSeconds < 30 || c.IdleSeconds > 1800 {
		return errors.New("连续失败预算及每小时预算须为 1–1000 次，预算暂停须为 30–86400 秒，空闲暂停须为 30–1800 秒")
	}
	return nil
}

package service

import (
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/gylive/ccodex-sleep-state/internal/settings"
)

func TestTimingPreferencesPersistAndPreserveOtherSettings(t *testing.T) {
	c, h := panelControl(t, func(c *settings.Config) {
		c.Model = "gpt-5.6-sol"
		c.StateFallback = "passthrough"
		c.InjectionDisabled = true
	})
	before := c.config
	payload := `{"probe_timeout_seconds":15,"probe_cooldown_seconds":600,"state_ttl_seconds":1800,"refresh_before_seconds":300,"max_probes_per_round":2}`
	w := panelPost(h, "timing", payload)
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	got, err := settings.Load(c.path)
	if err != nil {
		t.Fatal(err)
	}
	want := before
	want.ProbeSeconds, want.CooldownSeconds, want.TTLSeconds, want.RefreshSeconds, want.MaxProbes = 15, 600, 1800, 300, 2
	a, _ := json.Marshal(got)
	b, _ := json.Marshal(want)
	if string(a) != string(b) {
		t.Fatalf("unexpected persisted settings: %s", a)
	}
	backups, err := filepath.Glob(filepath.Join(c.dir, "backups", "config-*.json"))
	if err != nil || len(backups) != 1 {
		t.Fatal("missing backup", backups, err)
	}
	backup, err := settings.Load(backups[0])
	if err != nil || !reflect.DeepEqual(timingFrom(backup), timingFrom(before)) {
		t.Fatal("backup incorrect", err)
	}
	status := c.status()
	if !reflect.DeepEqual(status["timing"], timingFrom(want)) || !reflect.DeepEqual(status["timing_defaults"], timingFrom(settings.Default())) {
		t.Fatal("status missing timing", status)
	}
}

func TestAutomaticCollectionTogglePersistsWithoutRebuildingEngine(t *testing.T) {
	c, h := panelControl(t, func(cfg *settings.Config) {
		cfg.Model = "gpt-5.6-sol"
		cfg.Collection.HourlyBudget = 17
	})
	engine := c.engine
	c.activeRequests.Store(1) // The switch is safe while a reply is in flight.
	w := panelPost(h, "automatic-collection", `{"enabled":false}`)
	if w.Code != 200 {
		t.Fatalf("disable: %d %s", w.Code, w.Body.String())
	}
	if c.engine != engine || !c.config.Collection.AutomaticDisabled || c.config.Collection.HourlyBudget != 17 {
		t.Fatal("toggle rebuilt the engine or changed unrelated collection settings")
	}
	disk, err := settings.Load(c.path)
	if err != nil || !disk.Collection.AutomaticDisabled || disk.Collection.HourlyBudget != 17 {
		t.Fatalf("disabled setting was not persisted: %+v %v", disk, err)
	}
	status := c.status()
	if status["automatic_collection_enabled"] != false || timingFrom(c.config).Collection.AutomaticDisabled != true {
		t.Fatalf("status did not expose disabled state: %+v", status)
	}
	if w = panelPost(h, "automatic-collection", `{"enabled":true}`); w.Code != 200 {
		t.Fatalf("enable: %d %s", w.Code, w.Body.String())
	}
	if c.config.Collection.AutomaticDisabled {
		t.Fatal("enabled setting was not applied")
	}
	if disk, err = settings.Load(c.path); err != nil || disk.Collection.AutomaticDisabled {
		t.Fatalf("enabled setting was not persisted: %+v %v", disk, err)
	}
	if w = panelPost(h, "automatic-collection", `{}`); w.Code != 400 {
		t.Fatalf("missing explicit switch value accepted: %d", w.Code)
	}
}

func TestTimingRejectsInvalidWithoutWriting(t *testing.T) {
	for _, body := range []string{
		`{}`, `null`,
		`{"probe_timeout_seconds":0,"probe_cooldown_seconds":600,"state_ttl_seconds":1800,"refresh_before_seconds":300,"max_probes_per_round":2}`,
		`{"probe_timeout_seconds":61,"probe_cooldown_seconds":600,"state_ttl_seconds":1800,"refresh_before_seconds":300,"max_probes_per_round":2}`,
		`{"probe_timeout_seconds":20,"probe_cooldown_seconds":29,"state_ttl_seconds":1800,"refresh_before_seconds":300,"max_probes_per_round":2}`,
		`{"probe_timeout_seconds":20,"probe_cooldown_seconds":600,"state_ttl_seconds":3601,"refresh_before_seconds":300,"max_probes_per_round":2}`,
		`{"probe_timeout_seconds":20,"probe_cooldown_seconds":600,"state_ttl_seconds":1800,"refresh_before_seconds":1800,"max_probes_per_round":2}`,
		`{"probe_timeout_seconds":20,"probe_cooldown_seconds":600,"state_ttl_seconds":1800,"refresh_before_seconds":300,"max_probes_per_round":21}`,
		`{"probe_timeout_seconds":1.5}`, `{"unknown":true}`, `{} {}`,
	} {
		t.Run(body, func(t *testing.T) {
			c, h := panelControl(t, nil)
			before := diskConfig(t, c)
			engine := c.engine
			w := panelPost(h, "timing", body)
			if w.Code != 400 {
				t.Fatal(w.Code, w.Body.String())
			}
			if diskConfig(t, c) != before || c.engine != engine {
				t.Fatal("invalid update mutated config or engine")
			}
		})
	}
}

func TestTimingExternalEditNotOverwritten(t *testing.T) {
	c, h := panelControl(t, nil)
	edited := c.config
	edited.ProbeSeconds = 7
	data, _ := json.Marshal(edited)
	if err := os.WriteFile(c.path, data, 0600); err != nil {
		t.Fatal(err)
	}
	engine := c.engine
	payload, _ := json.Marshal(timingFrom(settings.Default()))
	w := panelPost(h, "timing", string(payload))
	if w.Code != 400 || diskConfig(t, c) != string(data) || c.engine != engine {
		t.Fatal(w.Code, w.Body.String())
	}
}

func TestTimingRequiresAuthentication(t *testing.T) {
	_, h := panelControl(t, nil)
	w := panelRequest(h, http.MethodPost, "/admin/api/timing", `{}`, nil)
	if w.Code != 401 {
		t.Fatal(w.Code)
	}
}

func TestTimingMissingPinnedRoutePreservesWorkingEngine(t *testing.T) {
	c, h := panelControl(t, nil)
	// Model a previously pinned route disappearing on subscription reload.
	c.config.PinnedRoute = "removed-route"
	data, _ := json.Marshal(c.config)
	if err := os.WriteFile(c.path, data, 0600); err != nil {
		t.Fatal(err)
	}
	engine := c.engine
	payload, _ := json.Marshal(timingFrom(settings.Default()))
	w := panelPost(h, "timing", string(payload))
	if w.Code != 400 || c.engine != engine || diskConfig(t, c) != string(data) || c.cancel == nil {
		t.Fatal("lost working generation", w.Code, w.Body.String())
	}
}

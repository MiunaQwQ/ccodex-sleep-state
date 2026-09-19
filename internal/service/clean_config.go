package service

import (
	"errors"
	"os"
	"path/filepath"
	"regexp"

	"github.com/gylive/ccodex-sleep-state/internal/codexconfig"
	"github.com/pelletier/go-toml/v2"
)

type rebuildRequest struct {
	Kind     string `json:"kind"`
	Upstream string `json:"upstream"`
	EnvKey   string `json:"env_key"`
	SHA      string `json:"expected_config_sha256"`
	Exists   bool   `json:"expected_exists"`
}

func (c *control) cleanPreview() (codexconfig.CleanConfigPreview, error) {
	if !c.configure {
		return codexconfig.CleanConfigPreview{}, errors.New("只读模式不允许重建 Codex 配置，请用 setup 启动")
	}
	home, err := c.config.CodexDir()
	if err != nil {
		return codexconfig.CleanConfigPreview{}, err
	}
	return codexconfig.PreviewCleanConfig(c.dir, home)
}
func (c *control) rebuildConfig(v rebuildRequest) error {
	if !c.configure {
		return errors.New("只读模式不允许重建 Codex 配置")
	}
	if c.config.UpstreamMode == "manual" {
		return errors.New("当前服务固定了手动上游，请先在服务配置中改为 auto，再使用引导重建，避免连接到错误的上游")
	}
	home, err := c.config.CodexDir()
	if err != nil {
		return err
	}
	authMode, err := codexconfig.ReadAuthMode(home)
	if err != nil {
		return err
	}
	doc := map[string]any{"model": c.config.SelectedModel(), "model_provider": "openai"}
	switch v.Kind {
	case "official":
		if authMode != "chatgpt" {
			return errors.New("没有识别到明确的官方 ChatGPT 登录。请先在 Codex 登录，或选择 API / 中转；不会猜测你的认证方式")
		}
	case "relay":
		if v.EnvKey != "" {
			if !regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,127}$`).MatchString(v.EnvKey) {
				return errors.New("API key 环境变量名格式不正确；不要在这里填写密钥本身")
			}
			authMode = "api_key"
		} else if authMode != "api_key" {
			return errors.New("请先通过 Codex / CCS 配置 API key，或填写现有 key 的环境变量名。官方登录不能发送给中转")
		}
		provider := map[string]any{"name": "User API", "base_url": v.Upstream, "wire_api": "responses", "requires_openai_auth": v.EnvKey == ""}
		if v.EnvKey != "" {
			provider["env_key"] = v.EnvKey
		}
		doc["model_provider"] = "user-api"
		doc["model_providers"] = map[string]any{"user-api": provider}
	default:
		return errors.New("请明确选择官方 ChatGPT 或 API / 中转，不能从损坏文件猜测")
	}
	if profile := c.config.CodexProfile; profile != "" {
		doc["profiles"] = map[string]any{profile: map[string]any{"model_provider": doc["model_provider"], "model": c.config.SelectedModel()}}
	}
	replacement, err := toml.Marshal(doc)
	if err != nil {
		return errors.New("无法生成新配置")
	}
	_, err = codexconfig.ResetConfig(c.dir, home, codexconfig.CleanConfigOptions{ExpectedConfigSHA256: v.SHA, ExpectedExists: v.Exists, Replacement: replacement, AuthMode: authMode})
	return err
}

func (c *control) repairEndpoint(v rebuildRequest) error {
	if !c.configure || c.managed {
		return errors.New("仅在尚未接管时补全地址；运行中的连接请先停止，避免凭据送错上游")
	}
	home, err := c.config.CodexDir()
	if err != nil {
		return err
	}
	original, err := os.ReadFile(filepath.Join(home, "config.toml"))
	if err != nil {
		return err
	}
	auth, err := codexconfig.ReadAuthMode(home)
	if err != nil {
		return err
	}
	replacement, err := codexconfig.PatchMissingEndpoint(original, c.config.CodexProfile, v.Upstream, auth)
	if err != nil {
		return err
	}
	selection, err := codexconfig.ResolveWithAuth(replacement, c.config.CodexProfile, auth)
	if err != nil {
		return err
	}
	if selection.EnvKey != "" {
		auth = "api_key"
	}
	_, err = codexconfig.ResetConfig(c.dir, home, codexconfig.CleanConfigOptions{ExpectedConfigSHA256: v.SHA, ExpectedExists: v.Exists, Replacement: replacement, AuthMode: auth})
	return err
}

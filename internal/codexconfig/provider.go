package codexconfig

import (
	"errors"
	"net/url"
	"strings"

	"github.com/pelletier/go-toml/v2"
)

// Options selects the profile used by the Codex invocation. An empty Profile
// follows the config's default profile, if any; it never guesses from the list.
type Options struct {
	Profile  string
	AuthMode string
	// Model chooses the managed model. Empty keeps the Astra default.
	Model string
	// ExpectedConfigSHA256 binds installation to the document resolved by the
	// service. Empty retains the standalone Patch/Install API.
	ExpectedConfigSHA256 string
}

// Selection describes the selected upstream. It deliberately contains no
// resolved environment values or auth.json contents. Upstream is private to
// service wiring: even a URL path can contain a relay credential.
type Selection struct {
	ProviderID   string `json:"provider"`
	Profile      string `json:"profile,omitempty"`
	Upstream     string `json:"-"`
	AuthKind     string `json:"auth_kind"`
	EnvKey       string `json:"env_key,omitempty"`
	Official     bool   `json:"official"`
	ConfigSHA256 string `json:"-"`
	fields       map[string]any
}

// Resolve reads only the supplied TOML. A named relay is never silently treated
// as OpenAI when its definition is missing or malformed.
func Resolve(original []byte, profile string) (Selection, error) {
	return ResolveWithAuth(original, profile, "unknown")
}

// ResolveWithAuth accepts a category from ClassifyAuth, never an API key/token.
func ResolveWithAuth(original []byte, profile, authMode string) (Selection, error) {
	switch authMode {
	case "", "unknown", "api_key", "chatgpt", "ambiguous":
	default:
		return Selection{}, errors.New("unknown Codex authentication category")
	}

	var doc map[string]any
	if err := toml.Unmarshal(original, &doc); err != nil {
		return Selection{}, errors.New("Codex config is not valid TOML; left unchanged")
	}
	if profile == "" {
		if value, exists := doc["profile"]; exists {
			var ok bool
			profile, ok = value.(string)
			if !ok {
				return Selection{}, errors.New("Codex profile must be a string")
			}
		}
	}
	selected := doc
	if profile != "" {
		profiles, _ := doc["profiles"].(map[string]any)
		p, ok := profiles[profile].(map[string]any)
		if !ok {
			return Selection{}, errors.New("selected Codex profile does not exist")
		}
		selected = make(map[string]any, len(doc))
		for k, v := range doc {
			selected[k] = v
		}
		for k, v := range p {
			selected[k] = v
		}
	}
	id, validID := selected["model_provider"].(string)
	if _, exists := selected["model_provider"]; exists && !validID {
		return Selection{}, errors.New("Codex model_provider must be a string")
	}
	if id == "" {
		id = "openai"
	}
	if id == provider {
		return Selection{}, errors.New("managed provider is already selected; restore the previous transaction first")
	}
	result := Selection{ProviderID: id, Profile: profile, ConfigSHA256: digest(original), fields: map[string]any{}}
	providers, _ := doc["model_providers"].(map[string]any)
	fields, exists := providers[id].(map[string]any)
	if !exists && id != "openai" {
		return Selection{}, errors.New("selected provider has no definition")
	}
	for k, v := range fields {
		result.fields[k] = v
	}
	if wire, ok := fields["wire_api"].(string); ok && wire != "responses" {
		return Selection{}, errors.New("only Responses API providers are supported")
	}
	endpoint, _ := fields["base_url"].(string)
	if endpoint == "" {
		endpoint, _ = selected["openai_base_url"].(string)
	}
	if endpoint == "" {
		if id != "openai" {
			return Selection{}, errors.New("所选中转 provider 缺少 base_url。请在连接设置中补全 Responses API 地址；不会猜测上游或将官方登录凭据发给中转")
		}
		endpoint = "https://chatgpt.com/backend-api/codex"
		if authMode == "api_key" {
			endpoint = "https://api.openai.com/v1"
		}
	}
	u, err := url.Parse(endpoint)
	if err != nil || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Scheme != "https" && !(u.Scheme == "http" && (u.Hostname() == "127.0.0.1" || u.Hostname() == "::1" || u.Hostname() == "localhost"))) {
		return Selection{}, errors.New("provider base_url must use HTTPS (or loopback HTTP), without credentials, query or fragment")
	}
	result.Upstream = strings.TrimRight(endpoint, "/")
	result.Official = u.Scheme == "https" && strings.EqualFold(u.Host, "chatgpt.com") && strings.TrimRight(u.Path, "/") == "/backend-api/codex"
	result.EnvKey, _ = fields["env_key"].(string)
	requires, explicit := fields["requires_openai_auth"].(bool)
	if !explicit {
		requires = id == "openai" && (result.Official || authMode == "api_key")
	}
	if id == "openai" && !result.Official && !explicit && result.EnvKey == "" && authMode != "api_key" {
		return Selection{}, errors.New("OpenAI base URL is overridden but authentication is ambiguous; select an explicit relay provider with env_key or requires_openai_auth=false")
	}
	if requires && !result.Official && authMode != "api_key" {
		return Selection{}, errors.New("relay provider requests OpenAI account authentication; refusing to forward account credentials to a different host")
	}
	if requires && authMode == "ambiguous" {
		return Selection{}, errors.New("Codex authentication contains mixed or invalid credentials; choose a single login method before connecting")
	}
	result.fields["requires_openai_auth"] = requires
	switch {
	case requires && authMode != "api_key":
		result.AuthKind = "chatgpt"
	case result.EnvKey != "":
		result.AuthKind = "api_key"
	default:
		result.AuthKind = "api_key"
	}
	return result, nil
}

package codexconfig

import (
	"bytes"
	"strings"
	"testing"
)

func TestPatchMissingEndpointPreservesConfig(t *testing.T) {
	source := []byte("# user comment\nmodel_provider='relay'\n[model_providers.relay]\nname='Test'\nenv_key='RELAY_KEY'\n[[skills.config]]\nname='keep'\n")
	out, err := PatchMissingEndpoint(source, "", "https://relay.invalid/v1", "unknown")
	if err != nil {
		t.Fatal(err)
	}
	removed := bytes.Replace(out, []byte("base_url = \"https://relay.invalid/v1\"\n"), nil, 1)
	if !bytes.Equal(removed, source) {
		t.Fatal("unrelated config changed")
	}
	if _, err = PatchMissingEndpoint(out, "", "https://different.invalid/v1", "unknown"); err == nil {
		t.Fatal("overwrote existing endpoint")
	}
}
func TestPatchMissingEndpointRejectsOAuthRelay(t *testing.T) {
	src := []byte("model_provider='r'\n[model_providers.r]\nrequires_openai_auth=true\n")
	if _, err := PatchMissingEndpoint(src, "", "https://relay.invalid/v1", "chatgpt"); err == nil {
		t.Fatal("sent official auth to relay")
	}
}
func TestEndpointProfileAndNoFinalNewline(t *testing.T) {
	src := []byte("profile='work'\n[profiles.work]\nmodel_provider='r'\n[model_providers.r]\nenv_key='KEY'")
	out, err := PatchMissingEndpoint(src, "", "https://relay.invalid/v1", "unknown")
	if err != nil || !strings.Contains(string(out), "env_key='KEY'") {
		t.Fatal(err)
	}
}

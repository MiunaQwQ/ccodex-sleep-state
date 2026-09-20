package gateway

import (
	"bytes"
	"encoding/json"
	"strings"
	"time"
	"unicode"
)

// Upstream-declared metadata from the latest finished forwarding attempt.
// Missing metadata is recorded too, so a previous model is never shown as new.
type ResponseModelObservation struct {
	Model    string    `json:"model,omitempty"`
	At       time.Time `json:"at"`
	Endpoint string    `json:"endpoint"`
	Result   string    `json:"result"`
	Received bool      `json:"received"`
}

func declaredResponseModel(data []byte, eventName string) string {
	if !bytes.Contains(data, []byte(`"model"`)) {
		return ""
	}
	var envelope struct {
		Type     string `json:"type"`
		Object   string `json:"object"`
		Model    string `json:"model"`
		Response struct {
			Model string `json:"model"`
		} `json:"response"`
	}
	if json.Unmarshal(data, &envelope) != nil {
		return ""
	}
	kind := envelope.Type
	if kind == "" {
		kind = eventName
	}
	model := ""
	switch kind {
	case "response.created", "response.in_progress", "response.completed", "response.failed", "response.incomplete", "response.queued":
		model = envelope.Response.Model
	case "":
		if envelope.Object == "response" || envelope.Object == "response.compaction" {
			model = envelope.Model
		}
	}
	model = strings.TrimSpace(model)
	if len(model) > 256 || strings.IndexFunc(model, func(r rune) bool { return unicode.IsControl(r) || unicode.IsSpace(r) }) >= 0 {
		return ""
	}
	return model
}

func (b *bootstrapStream) inspectResponseModel() {
	walkSSE(b.event, func(name string, data []byte) {
		if model := declaredResponseModel(data, name); model != "" {
			b.responseModel = model
		}
	})
}

package server_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/chasedputnam/go-kiro-gateway/gateway/internal/cache"
	"github.com/chasedputnam/go-kiro-gateway/gateway/internal/config"
	"github.com/chasedputnam/go-kiro-gateway/gateway/internal/debug"
	"github.com/chasedputnam/go-kiro-gateway/gateway/internal/resolver"
	"github.com/chasedputnam/go-kiro-gateway/gateway/internal/server"
	"github.com/chasedputnam/go-kiro-gateway/gateway/internal/truncation"
)

func TestMessages_VercelBackend(t *testing.T) {
	// 1. Create a mock Vercel server.
	var capturedAuth string
	var capturedBody map[string]any
	vercelMock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedAuth = r.Header.Get("Authorization")
		json.NewDecoder(r.Body).Decode(&capturedBody)

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{
			"id": "vercel_123",
			"object": "chat.completion",
			"created": 1677652288,
			"model": "anthropic/claude-fable-5",
			"choices": [
				{
					"index": 0,
					"message": {
						"role": "assistant",
						"content": "hello from vercel"
					},
					"finish_reason": "stop"
				}
			],
			"usage": {
				"prompt_tokens": 10,
				"completion_tokens": 10,
				"total_tokens": 20
			}
		}`))
	}))
	defer vercelMock.Close()

	// 2. Build configuration with Vercel backend.
	cfg := &config.Config{
		Host:                 "127.0.0.1",
		Port:                 0,
		ProxyAPIKey:          "test-gateway-key",
		BackendMode:          "vercel",
		VercelAPIKey:         "v1:abc:vcp_123",
		VercelURL:            vercelMock.URL,
		TruncationRecovery:   false,
		FakeReasoningEnabled: false,
	}

	modelCache := cache.New(time.Hour)
	modelResolver := resolver.New(modelCache, resolver.Config{
		Aliases: map[string]string{
			"fable-5": "anthropic/claude-fable-5",
		},
	})
	debugLogger := debug.NewDebugLogger("off", "")
	truncState := truncation.NewState()

	srv := server.New(cfg, &mockAuthManager{}, modelCache, modelResolver, &mockStreamingClient{}, debugLogger, truncState)

	// 3. Make the API request.
	reqBody := map[string]any{
		"model":      "fable-5",
		"max_tokens": 1024,
		"messages": []map[string]any{
			{"role": "user", "content": "hello"},
		},
	}
	bodyBytes, _ := json.Marshal(reqBody)

	req := httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(bodyBytes))
	req.Header.Set("Authorization", "Bearer test-gateway-key")
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	srv.Router().ServeHTTP(rr, req)

	// 4. Assertions.
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rr.Code, rr.Body.String())
	}

	if capturedAuth != "Bearer v1:abc:vcp_123" {
		t.Errorf("expected Authorization header Bearer v1:abc:vcp_123, got %q", capturedAuth)
	}

	if capturedBody["model"] != "anthropic/claude-fable-5" {
		t.Errorf("expected model to be mapped to anthropic/claude-fable-5, got %v", capturedBody["model"])
	}

	// Verify it was translated to OpenAI messages format
	messages, ok := capturedBody["messages"].([]any)
	if !ok || len(messages) == 0 {
		t.Fatalf("expected translated messages list, got %v", capturedBody["messages"])
	}
	firstMsg := messages[0].(map[string]any)
	if firstMsg["role"] != "user" || firstMsg["content"] != "hello" {
		t.Errorf("expected first message to be role:user, content:hello, got %v", firstMsg)
	}

	var resp map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}

	if resp["id"] != "msg_vercel_123" {
		t.Errorf("expected id msg_vercel_123, got %v", resp["id"])
	}

	contentList, ok := resp["content"].([]any)
	if !ok || len(contentList) == 0 {
		t.Fatalf("expected content array in response, got %v", resp["content"])
	}
	firstContent := contentList[0].(map[string]any)
	if firstContent["type"] != "text" || firstContent["text"] != "hello from vercel" {
		t.Errorf("expected text: 'hello from vercel', got %v", firstContent)
	}
}

func TestMessages_VercelBackend_Streaming(t *testing.T) {
	// 1. Create a mock Vercel server.
	var capturedAuth string
	var capturedBody map[string]any
	vercelMock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedAuth = r.Header.Get("Authorization")
		json.NewDecoder(r.Body).Decode(&capturedBody)

		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		w.WriteHeader(http.StatusOK)
		
		// Write simulated OpenAI SSE events
		w.Write([]byte("data: {\"id\": \"vercel_stream_123\", \"object\": \"chat.completion.chunk\", \"choices\": [{\"index\": 0, \"delta\": {\"content\": \"hello from vercel stream\"}, \"finish_reason\": null}]}\n\n"))
		w.Write([]byte("data: [DONE]\n\n"))
	}))
	defer vercelMock.Close()

	// 2. Build configuration with Vercel backend.
	cfg := &config.Config{
		Host:                 "127.0.0.1",
		Port:                 0,
		ProxyAPIKey:          "test-gateway-key",
		BackendMode:          "vercel",
		VercelAPIKey:         "v1:abc:vcp_123",
		VercelURL:            vercelMock.URL,
		TruncationRecovery:   false,
		FakeReasoningEnabled: false,
	}

	modelCache := cache.New(time.Hour)
	modelResolver := resolver.New(modelCache, resolver.Config{
		Aliases: map[string]string{
			"fable-5": "anthropic/claude-fable-5",
		},
	})
	debugLogger := debug.NewDebugLogger("off", "")
	truncState := truncation.NewState()

	srv := server.New(cfg, &mockAuthManager{}, modelCache, modelResolver, &mockStreamingClient{}, debugLogger, truncState)

	// 3. Make the API request.
	reqBody := map[string]any{
		"model":      "fable-5",
		"max_tokens": 1024,
		"stream":     true,
		"messages": []map[string]any{
			{"role": "user", "content": "hello"},
		},
	}
	bodyBytes, _ := json.Marshal(reqBody)

	req := httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(bodyBytes))
	req.Header.Set("Authorization", "Bearer test-gateway-key")
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	srv.Router().ServeHTTP(rr, req)

	// 4. Assertions.
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rr.Code, rr.Body.String())
	}

	if capturedAuth != "Bearer v1:abc:vcp_123" {
		t.Errorf("expected Authorization header Bearer v1:abc:vcp_123, got %q", capturedAuth)
	}

	if capturedBody["model"] != "anthropic/claude-fable-5" {
		t.Errorf("expected model to be mapped to anthropic/claude-fable-5, got %v", capturedBody["model"])
	}

	// Verify SSE headers
	if rr.Header().Get("Content-Type") != "text/event-stream" {
		t.Errorf("expected Content-Type text/event-stream, got %q", rr.Header().Get("Content-Type"))
	}

	responseBody := rr.Body.String()
	if !strings.Contains(responseBody, "event: message_start") {
		t.Errorf("expected event: message_start, got: %s", responseBody)
	}
	if !strings.Contains(responseBody, "hello from vercel stream") {
		t.Errorf("expected hello from vercel stream content, got: %s", responseBody)
	}
}

// Package backend — tests for ACPBackend using a fake kiro-cli subprocess.
package backend

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/chasedputnam/go-kiro-gateway/gateway/internal/config"
	"github.com/chasedputnam/go-kiro-gateway/gateway/internal/streaming"
)

// ---------------------------------------------------------------------------
// Fake kiro-cli helper process
// ---------------------------------------------------------------------------
//
// Tests that need a subprocess call os/exec to re-invoke the test binary with
// the FAKE_KIRO_CLI env var set. The TestFakeKiroCLI function acts as the
// fake binary, reading JSON-RPC requests and writing scripted responses.

// TestFakeKiroCLI is the entrypoint for the fake kiro-cli subprocess.
// It is not a real test — it exits when FAKE_KIRO_CLI is not set.
func TestFakeKiroCLI(t *testing.T) {
	if os.Getenv("FAKE_KIRO_CLI") != "1" {
		t.Skip("not a fake kiro-cli invocation")
	}
	fakeKiroCLIMain()
}

// fakeKiroCLIMain simulates kiro-cli acp over stdin/stdout. FAKE_KIRO_MODE
// selects scripted behaviors used by individual tests; the empty default
// streams two text chunks and ends the turn cleanly.
func fakeKiroCLIMain() {
	mode := os.Getenv("FAKE_KIRO_MODE")

	// Append handled events to FAKE_KIRO_LOG so tests can observe how many
	// sessions were created and which slash commands were issued.
	logf := func(string) {}
	if path := os.Getenv("FAKE_KIRO_LOG"); path != "" {
		if f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644); err == nil {
			defer f.Close()
			logf = func(line string) { fmt.Fprintln(f, line) }
		}
	}

	sessionCounter := 0
	r := bufio.NewReader(os.Stdin)
	for {
		msg, err := readMessage(r)
		if err != nil {
			if err != io.EOF {
				fmt.Fprintf(os.Stderr, "fake kiro-cli read error: %v\n", err)
			}
			return
		}

		switch msg.Method {
		case "initialize":
			version := 1
			if mode == "version_mismatch" {
				version = 2 // newer than the gateway supports
			}
			respond(os.Stdout, msg.ID, map[string]any{
				"protocolVersion":   version,
				"agentCapabilities": map[string]any{},
				"agentInfo":         map[string]any{"name": "fake-kiro-cli"},
			})

		case "session/new":
			sessionCounter++
			logf("session/new")
			respond(os.Stdout, msg.ID, map[string]any{
				"sessionId": fmt.Sprintf("test-session-%d", sessionCounter),
			})

		case "session/prompt":
			// Parse sessionId and the first prompt block's text.
			var params struct {
				SessionID string `json:"sessionId"`
				Prompt    []struct {
					Text string `json:"text"`
				} `json:"prompt"`
			}
			_ = json.Unmarshal(msg.Params, &params)
			promptText := ""
			if len(params.Prompt) > 0 {
				promptText = params.Prompt[0].Text
			}
			logf("prompt:" + promptText)

			// /clear wipes session context on reuse.
			if promptText == "/clear" {
				if mode == "clear_fail" {
					respondError(os.Stdout, msg.ID, -32001, "clear failed")
					continue
				}
				sendNotification(os.Stdout, params.SessionID, AgentMessageChunk{
					SessionUpdate: "agent_message_chunk",
					Content:       ContentBlock{Type: "text", Text: "Conversation cleared\n"},
				})
				respond(os.Stdout, msg.ID, PromptResponse{StopReason: "end_turn"})
				continue
			}

			// Model selection arrives as a `/model <name>` slash command turn.
			if strings.HasPrefix(promptText, "/model ") {
				reply := "Model set.\n"
				if mode == "model_unavailable" {
					reply = "Model '" + strings.TrimPrefix(promptText, "/model ") + "' not found. Run /model to browse.\n"
				}
				sendNotification(os.Stdout, params.SessionID, AgentMessageChunk{
					SessionUpdate: "agent_message_chunk",
					Content:       ContentBlock{Type: "text", Text: reply},
				})
				respond(os.Stdout, msg.ID, PromptResponse{StopReason: "end_turn"})
				continue
			}

			if mode == "prompt_error" {
				respondError(os.Stdout, msg.ID, -32000, "internal error")
				continue
			}

			if mode == "tool_call" {
				sendNotification(os.Stdout, params.SessionID, ToolCallNotification{
					SessionUpdate: "tool_call",
					ToolCallID:    "call_1",
					Title:         "read_file",
					Kind:          "read",
					Status:        "in_progress",
				})
				respond(os.Stdout, msg.ID, PromptResponse{StopReason: "end_turn"})
				continue
			}

			// Default: stream updates first, then respond to the prompt request
			// with a stopReason — the response is the turn-completion signal.
			sendNotification(os.Stdout, params.SessionID, AgentMessageChunk{
				SessionUpdate: "agent_message_chunk",
				Content:       ContentBlock{Type: "text", Text: "Hello"},
			})
			sendNotification(os.Stdout, params.SessionID, AgentMessageChunk{
				SessionUpdate: "agent_message_chunk",
				Content:       ContentBlock{Type: "text", Text: " world"},
			})
			respond(os.Stdout, msg.ID, PromptResponse{StopReason: "end_turn"})

		case "session/cancel":
			respond(os.Stdout, msg.ID, map[string]any{})
		}
	}
}

func respond(w io.Writer, id int64, result any) {
	data, _ := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"id":      id,
		"result":  result,
	})
	_, _ = w.Write(append(data, '\n'))
}

func respondError(w io.Writer, id int64, code int, message string) {
	data, _ := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"id":      id,
		"error":   map[string]any{"code": code, "message": message},
	})
	_, _ = w.Write(append(data, '\n'))
}

func sendNotification(w io.Writer, sessionID string, update any) {
	params := map[string]any{
		"sessionId": sessionID,
		"update":    update,
	}
	data, _ := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"method":  "session/update",
		"params":  params,
	})
	_, _ = w.Write(append(data, '\n'))
}

// ---------------------------------------------------------------------------
// Helper: newTestACPBackend
// ---------------------------------------------------------------------------

// newTestACPBackend creates an ACPBackend (default mode, reuse disabled) using
// the test binary as the fake kiro-cli subprocess and fails on startup error.
func newTestACPBackend(t *testing.T) *ACPBackend {
	t.Helper()
	b, err := startFakeACPBackend(t, "")
	if err != nil {
		t.Fatalf("initialize handshake failed: %v", err)
	}
	return b
}

// startFakeACPBackend spins up the fake kiro-cli with reuse disabled (maxIdle 0).
func startFakeACPBackend(t *testing.T, mode string) (*ACPBackend, error) {
	b, _, err := startFakeACPBackendOpts(t, mode, 0)
	return b, err
}

// startFakeACPBackendOpts spins up the fake kiro-cli in the given FAKE_KIRO_MODE
// with the given idle-pool size. It returns the backend, the path to the fake's
// event log (handled methods and prompt texts, one per line), and the initialize
// handshake error (if any), so tests can assert on startup failures and reuse.
func startFakeACPBackendOpts(t *testing.T, mode string, maxIdle int) (*ACPBackend, string, error) {
	t.Helper()

	testBin, err := os.Executable()
	if err != nil {
		t.Fatalf("failed to get test binary path: %v", err)
	}

	cfg := &config.Config{KiroCLIPath: testBin, ACPAgent: "", ACPMaxIdleSessions: maxIdle}
	logPath := filepath.Join(t.TempDir(), "fake-kiro.log")

	// Build the backend manually to inject the fake subprocess.
	args := []string{"-test.run=TestFakeKiroCLI", "-test.v=false"}
	cmd := exec.Command(testBin, args...)
	cmd.Env = append(os.Environ(), "FAKE_KIRO_CLI=1", "FAKE_KIRO_MODE="+mode, "FAKE_KIRO_LOG="+logPath)

	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatalf("stdin pipe: %v", err)
	}
	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("stdout pipe: %v", err)
	}
	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		t.Fatalf("stderr pipe: %v", err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatalf("cmd.Start: %v", err)
	}

	b := &ACPBackend{
		cmd:         cmd,
		stdin:       stdin,
		stdout:      bufio.NewReader(stdoutPipe),
		cfg:         cfg,
		idleByModel: make(map[string][]string),
		maxIdle:     cfg.ACPMaxIdleSessions,
		done:        make(chan struct{}),
	}
	import_zerolog(b, t)

	go b.drainStderr(stderrPipe)
	go b.dispatchLoop()
	go func() {
		_ = cmd.Wait()
		close(b.done)
	}()

	return b, logPath, b.initialize()
}

// import_zerolog sets up the logger field (zerolog is not importable as an
// expression, so we use the package-level log via a small helper).
func import_zerolog(b *ACPBackend, t *testing.T) {
	// The zerolog.Logger zero value is a no-op logger — fine for tests.
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func TestACPBackend_Complete_StreamsChunks(t *testing.T) {
	b := newTestACPBackend(t)
	defer b.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req := &Request{
		Payload: map[string]any{
			"messages": []any{
				map[string]any{"role": "user", "content": "hello"},
			},
		},
		Model: "claude-sonnet-4-6",
	}

	ch, err := b.Complete(ctx, req)
	if err != nil {
		t.Fatalf("Complete() error: %v", err)
	}

	var contents []string
	for event := range ch {
		if event.Type == streaming.EventTypeContent {
			contents = append(contents, event.Content)
		}
		if event.Type == streaming.EventTypeError {
			t.Fatalf("received error event: %v", event.Error)
		}
	}

	full := strings.Join(contents, "")
	if full != "Hello world" {
		t.Errorf("accumulated content = %q, want %q", full, "Hello world")
	}
}

func TestACPBackend_Complete_ChannelClosedAfterStopReason(t *testing.T) {
	b := newTestACPBackend(t)
	defer b.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ch, err := b.Complete(ctx, &Request{
		Payload: map[string]any{"messages": []any{
			map[string]any{"role": "user", "content": "ping"},
		}},
	})
	if err != nil {
		t.Fatalf("Complete() error: %v", err)
	}

	// Drain until closed.
	for range ch {
	}
	// If we reach here the channel was closed cleanly.
}

func TestACPBackend_Complete_EmitsToolCall(t *testing.T) {
	b, err := startFakeACPBackend(t, "tool_call")
	if err != nil {
		t.Fatalf("startup error: %v", err)
	}
	defer b.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ch, err := b.Complete(ctx, &Request{
		Payload: map[string]any{"messages": []any{
			map[string]any{"role": "user", "content": "read it"},
		}},
	})
	if err != nil {
		t.Fatalf("Complete() error: %v", err)
	}

	var toolCalls []streaming.ToolCallInfo
	for event := range ch {
		if event.Type == streaming.EventTypeError {
			t.Fatalf("unexpected error event: %v", event.Error)
		}
		if event.Type == streaming.EventTypeToolCall && event.ToolCall != nil {
			toolCalls = append(toolCalls, *event.ToolCall)
		}
	}

	if len(toolCalls) != 1 {
		t.Fatalf("got %d tool calls, want 1", len(toolCalls))
	}
	if toolCalls[0].Name != "read_file" {
		t.Errorf("tool call Name = %q, want %q", toolCalls[0].Name, "read_file")
	}
	if toolCalls[0].ID != "call_1" {
		t.Errorf("tool call ID = %q, want %q", toolCalls[0].ID, "call_1")
	}
}

func TestACPBackend_Complete_PromptErrorEmitsErrorEvent(t *testing.T) {
	b, err := startFakeACPBackend(t, "prompt_error")
	if err != nil {
		t.Fatalf("startup error: %v", err)
	}
	defer b.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ch, err := b.Complete(ctx, &Request{
		Payload: map[string]any{"messages": []any{
			map[string]any{"role": "user", "content": "hi"},
		}},
	})
	if err != nil {
		t.Fatalf("Complete() error: %v", err)
	}

	var sawError bool
	for event := range ch {
		if event.Type == streaming.EventTypeError {
			sawError = true
			if event.Error == nil {
				t.Error("error event has nil Error")
			}
		}
	}
	if !sawError {
		t.Error("expected an EventTypeError when session/prompt fails, got none")
	}
}

func TestACPBackend_VersionMismatch_FailsStartup(t *testing.T) {
	b, err := startFakeACPBackend(t, "version_mismatch")
	if b != nil {
		defer b.Close()
	}
	if err == nil {
		t.Fatal("expected initialize to fail on a newer protocol version, got nil")
	}
	if !strings.Contains(err.Error(), "protocol version") {
		t.Errorf("error = %q, want it to mention the protocol version mismatch", err)
	}
}

func TestACPBackend_Complete_ModelUnavailableFallsBack(t *testing.T) {
	b, err := startFakeACPBackend(t, "model_unavailable")
	if err != nil {
		t.Fatalf("startup error: %v", err)
	}
	defer b.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Model is set, so /model selection runs and reports "not found" — the
	// request must still complete on the session default.
	ch, err := b.Complete(ctx, &Request{
		Model: "claude-sonnet-4-6",
		Payload: map[string]any{"messages": []any{
			map[string]any{"role": "user", "content": "hi"},
		}},
	})
	if err != nil {
		t.Fatalf("Complete() error: %v", err)
	}

	var contents []string
	for event := range ch {
		if event.Type == streaming.EventTypeError {
			t.Fatalf("unexpected error event: %v", event.Error)
		}
		if event.Type == streaming.EventTypeContent {
			contents = append(contents, event.Content)
		}
	}
	if strings.Join(contents, "") != "Hello world" {
		t.Errorf("content = %q, want %q", strings.Join(contents, ""), "Hello world")
	}
}

func TestToKiroModelID(t *testing.T) {
	cases := map[string]string{
		"claude-sonnet-4-6": "claude-sonnet-4.6",
		"claude-opus-4-5":   "claude-opus-4.5",
		"claude-haiku-4-5":  "claude-haiku-4.5",
		"claude-sonnet-4":   "claude-sonnet-4", // single version segment unchanged
		"auto":              "auto",
		"":                  "",
	}
	for in, want := range cases {
		if got := toKiroModelID(in); got != want {
			t.Errorf("toKiroModelID(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestExtractPromptText_KiroPayload(t *testing.T) {
	payload := map[string]any{
		"conversationState": map[string]any{
			"currentMessage": map[string]any{
				"userInputMessage": map[string]any{
					"content": "the real prompt",
				},
			},
		},
	}
	if got := extractPromptText(payload); got != "the real prompt" {
		t.Errorf("extractPromptText = %q, want %q", got, "the real prompt")
	}
}

// drainRequest runs a Complete request to completion, returning the accumulated
// content. Fails the test on a Complete error or an error event.
func drainRequest(t *testing.T, b *ACPBackend, model string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ch, err := b.Complete(ctx, &Request{
		Model: model,
		Payload: map[string]any{"messages": []any{
			map[string]any{"role": "user", "content": "hi"},
		}},
	})
	if err != nil {
		t.Fatalf("Complete() error: %v", err)
	}
	var sb strings.Builder
	for event := range ch {
		if event.Type == streaming.EventTypeError {
			t.Fatalf("unexpected error event: %v", event.Error)
		}
		if event.Type == streaming.EventTypeContent {
			sb.WriteString(event.Content)
		}
	}
	return sb.String()
}

// countLog returns how many lines in the fake CLI log equal want.
func countLog(t *testing.T, logPath, want string) int {
	t.Helper()
	data, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("read fake log: %v", err)
	}
	n := 0
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		if line == want {
			n++
		}
	}
	return n
}

func TestACPBackend_ReusesPooledSession(t *testing.T) {
	b, logPath, err := startFakeACPBackendOpts(t, "", 4)
	if err != nil {
		t.Fatalf("startup error: %v", err)
	}
	defer b.Close()

	// Two sequential requests on the default model: the second reuses the
	// session pooled by the first, so session/new runs once and /clear once.
	if got := drainRequest(t, b, "auto"); got != "Hello world" {
		t.Fatalf("first content = %q", got)
	}
	if got := drainRequest(t, b, "auto"); got != "Hello world" {
		t.Fatalf("second content = %q", got)
	}

	if n := countLog(t, logPath, "session/new"); n != 1 {
		t.Errorf("session/new count = %d, want 1 (second request should reuse)", n)
	}
	if n := countLog(t, logPath, "prompt:/clear"); n != 1 {
		t.Errorf("/clear count = %d, want 1 (issued on reuse)", n)
	}
}

func TestACPBackend_ReuseDisabledWhenMaxIdleZero(t *testing.T) {
	b, logPath, err := startFakeACPBackendOpts(t, "", 0)
	if err != nil {
		t.Fatalf("startup error: %v", err)
	}
	defer b.Close()

	drainRequest(t, b, "auto")
	drainRequest(t, b, "auto")

	if n := countLog(t, logPath, "session/new"); n != 2 {
		t.Errorf("session/new count = %d, want 2 (reuse disabled)", n)
	}
	if n := countLog(t, logPath, "prompt:/clear"); n != 0 {
		t.Errorf("/clear count = %d, want 0 (no reuse)", n)
	}
}

func TestACPBackend_NoReuseAcrossModels(t *testing.T) {
	b, logPath, err := startFakeACPBackendOpts(t, "", 4)
	if err != nil {
		t.Fatalf("startup error: %v", err)
	}
	defer b.Close()

	// Different models don't share a session, so each creates a new one.
	drainRequest(t, b, "auto")              // key ""
	drainRequest(t, b, "claude-sonnet-4-6") // key "claude-sonnet-4.6"

	if n := countLog(t, logPath, "session/new"); n != 2 {
		t.Errorf("session/new count = %d, want 2 (no cross-model reuse)", n)
	}
	if n := countLog(t, logPath, "prompt:/clear"); n != 0 {
		t.Errorf("/clear count = %d, want 0 (no reuse)", n)
	}
}

func TestACPBackend_FailedClearFallsBackToNewSession(t *testing.T) {
	b, logPath, err := startFakeACPBackendOpts(t, "clear_fail", 4)
	if err != nil {
		t.Fatalf("startup error: %v", err)
	}
	defer b.Close()

	// First request pools a session; on the second, /clear fails so the backend
	// discards the poisoned session and creates a new one — the request still
	// succeeds.
	drainRequest(t, b, "auto")
	if got := drainRequest(t, b, "auto"); got != "Hello world" {
		t.Fatalf("second content = %q, want completion despite failed /clear", got)
	}

	if n := countLog(t, logPath, "session/new"); n != 2 {
		t.Errorf("session/new count = %d, want 2 (fallback after failed /clear)", n)
	}
}

func TestACPBackend_ConcurrentReuse(t *testing.T) {
	b, _, err := startFakeACPBackendOpts(t, "", 2)
	if err != nil {
		t.Fatalf("startup error: %v", err)
	}
	defer b.Close()

	// Fire many concurrent same-model requests against a small pool. Every one
	// must complete — a turn whose pooled session was clobbered by another
	// request's notif cleanup would stream nothing and time out.
	const n = 12
	results := make([]string, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			ch, e := b.Complete(ctx, &Request{
				Model: "auto",
				Payload: map[string]any{"messages": []any{
					map[string]any{"role": "user", "content": "hi"},
				}},
			})
			if e != nil {
				errs[i] = e
				return
			}
			var sb strings.Builder
			for event := range ch {
				if event.Type == streaming.EventTypeError {
					errs[i] = event.Error
				}
				if event.Type == streaming.EventTypeContent {
					sb.WriteString(event.Content)
				}
			}
			results[i] = sb.String()
		}(i)
	}
	wg.Wait()

	for i := 0; i < n; i++ {
		if errs[i] != nil {
			t.Errorf("request %d error: %v", i, errs[i])
		}
		if results[i] != "Hello world" {
			t.Errorf("request %d content = %q, want %q", i, results[i], "Hello world")
		}
	}
}

func TestACPBackend_Close_TerminatesProcess(t *testing.T) {
	b := newTestACPBackend(t)
	if err := b.Close(); err != nil {
		t.Errorf("Close() error: %v", err)
	}
}

func TestACPBackend_Complete_AfterClose_ReturnsError(t *testing.T) {
	b := newTestACPBackend(t)
	_ = b.Close()

	// Wait for done channel to be closed.
	select {
	case <-b.done:
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for subprocess to exit")
	}

	_, err := b.Complete(context.Background(), &Request{})
	if err == nil {
		t.Error("expected error after Close(), got nil")
	}
}

// ---------------------------------------------------------------------------
// resolveKiroCLI tests (no subprocess needed)
// ---------------------------------------------------------------------------

func TestResolveKiroCLI_ExplicitPathNotFound(t *testing.T) {
	_, err := resolveKiroCLI("/nonexistent/path/kiro-cli")
	if err == nil {
		t.Error("expected error for nonexistent path, got nil")
	}
}

func TestResolveKiroCLI_ExplicitPathFound(t *testing.T) {
	// Use the test binary itself as a stand-in for an existing file.
	self, _ := os.Executable()
	path, err := resolveKiroCLI(self)
	if err != nil {
		t.Errorf("resolveKiroCLI(%q) error: %v", self, err)
	}
	if path != self {
		t.Errorf("path = %q, want %q", path, self)
	}
}

// ---------------------------------------------------------------------------
// extractPromptText tests
// ---------------------------------------------------------------------------

func TestExtractPromptText_LastUserMessage(t *testing.T) {
	payload := map[string]any{
		"messages": []any{
			map[string]any{"role": "system", "content": "You are helpful."},
			map[string]any{"role": "user", "content": "Hello"},
			map[string]any{"role": "assistant", "content": "Hi"},
			map[string]any{"role": "user", "content": "How are you?"},
		},
	}
	got := extractPromptText(payload)
	if got != "How are you?" {
		t.Errorf("extractPromptText = %q, want %q", got, "How are you?")
	}
}

func TestExtractPromptText_ContentBlock(t *testing.T) {
	payload := map[string]any{
		"messages": []any{
			map[string]any{
				"role": "user",
				"content": []any{
					map[string]any{"type": "text", "text": "block text"},
				},
			},
		},
	}
	got := extractPromptText(payload)
	if got != "block text" {
		t.Errorf("extractPromptText = %q, want %q", got, "block text")
	}
}

func TestExtractPromptText_EmptyMessages(t *testing.T) {
	payload := map[string]any{"messages": []any{}}
	got := extractPromptText(payload)
	if got != "" {
		t.Errorf("extractPromptText = %q, want empty", got)
	}
}

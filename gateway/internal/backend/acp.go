// Package backend — ACP backend implementation.
//
// ACPBackend spawns kiro-cli as a subprocess and communicates with it over
// JSON-RPC 2.0 via stdin/stdout. Each call to Complete creates a new ACP
// session, sends the prompt, streams back KiroEvents, and discards the
// session when done (stateless per request).
package backend

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/chasedputnam/go-kiro-gateway/gateway/internal/config"
	"github.com/chasedputnam/go-kiro-gateway/gateway/internal/streaming"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// ---------------------------------------------------------------------------
// ACPBackend
// ---------------------------------------------------------------------------

// ACPBackend fulfills requests by communicating with a kiro-cli subprocess
// over JSON-RPC 2.0 (newline-delimited JSON over stdin/stdout).
type ACPBackend struct {
	cmd    *exec.Cmd
	stdin  io.WriteCloser
	stdout *bufio.Reader
	cfg    *config.Config
	logger zerolog.Logger

	mu      sync.Mutex              // serialises JSON-RPC writes
	nextID  atomic.Int64            // monotonically increasing request IDs
	pending sync.Map                // int64 → chan *rpcMessage (in-flight requests)
	notifs  sync.Map                // string (sessionID) → chan *SessionNotification

	poolMu      sync.Mutex          // guards idleByModel
	idleByModel map[string][]string // kiro model id ("" = default) → idle session IDs
	maxIdle     int                 // max idle sessions per model; 0 disables reuse

	done chan struct{} // closed when subprocess exits
}

// NewACPBackend locates kiro-cli, spawns it with the acp subcommand,
// performs the initialize handshake, and returns a ready-to-use backend.
func NewACPBackend(cfg *config.Config) (*ACPBackend, error) {
	cliPath, err := resolveKiroCLI(cfg.KiroCLIPath)
	if err != nil {
		return nil, err
	}

	args := []string{"acp", "-a"}
	if cfg.ACPAgent != "" {
		args = append(args, "--agent", cfg.ACPAgent)
	}

	cmd := exec.Command(cliPath, args...)

	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, fmt.Errorf("acp: create stdin pipe: %w", err)
	}
	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("acp: create stdout pipe: %w", err)
	}
	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		return nil, fmt.Errorf("acp: create stderr pipe: %w", err)
	}

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("acp: start kiro-cli (%s): %w", cliPath, err)
	}

	b := &ACPBackend{
		cmd:         cmd,
		stdin:       stdin,
		stdout:      bufio.NewReader(stdoutPipe),
		cfg:         cfg,
		logger:      log.With().Str("component", "acp").Logger(),
		idleByModel: make(map[string][]string),
		maxIdle:     cfg.ACPMaxIdleSessions,
		done:        make(chan struct{}),
	}

	// Capture stderr and log at WARN level.
	go b.drainStderr(stderrPipe)

	// Dispatch loop: reads messages from stdout and routes them.
	go b.dispatchLoop()

	// Watch for subprocess exit.
	go func() {
		_ = cmd.Wait()
		close(b.done)
	}()

	// Perform the initialize handshake with a 10s timeout.
	if err := b.initialize(); err != nil {
		_ = b.Close()
		return nil, fmt.Errorf("acp: initialize handshake failed: %w", err)
	}

	log.Info().Str("cli_path", cliPath).Msg("ACP backend ready")
	return b, nil
}

// Close terminates the kiro-cli subprocess gracefully.
func (b *ACPBackend) Close() error {
	if b.cmd == nil || b.cmd.Process == nil {
		return nil
	}
	// Send SIGTERM, then wait for the subprocess-exit watcher to signal b.done.
	// We reuse the existing watcher goroutine (which calls cmd.Wait()) rather than
	// calling Wait() a second time, which would return an error on most platforms.
	_ = b.cmd.Process.Signal(os.Interrupt)
	select {
	case <-b.done:
	case <-time.After(5 * time.Second):
		_ = b.cmd.Process.Kill()
	}
	return nil
}

// ---------------------------------------------------------------------------
// Complete — per-request session flow
// ---------------------------------------------------------------------------

// Complete creates a new ACP session, sends the prompt, and returns a channel
// of KiroEvents. The channel is closed on TurnEnd or context cancellation.
func (b *ACPBackend) Complete(ctx context.Context, req *Request) (<-chan streaming.KiroEvent, error) {
	// Check subprocess is alive.
	select {
	case <-b.done:
		return nil, fmt.Errorf("acp: kiro-cli subprocess has exited")
	default:
	}

	// 1. Check out a session for the target model — a cleared idle session from
	//    the pool, or a freshly created one with the model selected.
	key := modelKey(req.Model)
	sessionID, poolKey, reused, err := b.checkoutSession(ctx, key)
	if err != nil {
		return nil, err
	}

	// 3. Subscribe to notifications BEFORE sending the prompt so no streamed
	//    updates are missed. notifCh is not closed here — the drain goroutine is
	//    its only reader and dispatchLoop sends non-blocking, so deleting it from
	//    the map (below) is enough to stop delivery without risking a
	//    send-on-closed-channel panic.
	notifCh := make(chan *SessionNotification, 64)
	b.notifs.Store(sessionID, notifCh)

	promptText := extractPromptText(req.Payload)
	if promptText == "" {
		b.logger.Warn().Str("session_id", sessionID).Msg("ACP: extractPromptText returned empty string; sending empty prompt to kiro-cli")
	}

	// 4. Issue session/prompt concurrently with the notification drain. The
	//    prompt response carries stopReason and only returns after all updates
	//    have streamed, so it must not be awaited ahead of reading updates.
	type promptResult struct {
		stopReason string
		err        error
	}
	promptDone := make(chan promptResult, 1)
	go func() {
		stopReason, err := b.sessionPrompt(ctx, sessionID, promptText)
		promptDone <- promptResult{stopReason: stopReason, err: err}
	}()

	// 5. Translate notifications to KiroEvents until the prompt turn completes.
	events := make(chan streaming.KiroEvent, 64)
	start := time.Now()
	go func() {
		defer close(events)
		// Note: notifs.Delete(sessionID) is NOT deferred. It must run before the
		// session is handed back to the pool (returnSession), otherwise a
		// concurrent request that reuses this sessionID and re-registers its own
		// notif channel could have it clobbered by a late delete here.

		emit := func(notif *SessionNotification) {
			update, err := ParseUpdate(notif.Update)
			if err != nil {
				b.logger.Warn().Err(err).Msg("skipping malformed ACP update")
				return
			}
			if update == nil {
				return // unhandled variant
			}
			switch u := update.(type) {
			case *AgentMessageChunk:
				events <- streaming.KiroEvent{
					Type:    streaming.EventTypeContent,
					Content: u.Content.Text,
				}
			case *ToolCallNotification:
				if u.Status == "pending" || u.Status == "in_progress" {
					events <- streaming.KiroEvent{
						Type: streaming.EventTypeToolCall,
						ToolCall: &streaming.ToolCallInfo{
							ID:        u.ToolCallID,
							Name:      u.Title,
							Arguments: "{}",
						},
					}
				}
			case *ToolCallUpdate:
				// Progress update — no KiroEvent equivalent, skip.
			}
		}

		var res promptResult
	loop:
		for {
			select {
			case <-ctx.Done():
				_ = b.sessionCancel(context.Background(), sessionID)
				b.notifs.Delete(sessionID) // drop session (not pooled)
				return

			case <-b.done:
				events <- streaming.KiroEvent{
					Type:  streaming.EventTypeError,
					Error: fmt.Errorf("acp: kiro-cli subprocess exited unexpectedly"),
				}
				b.notifs.Delete(sessionID)
				return

			case notif := <-notifCh:
				emit(notif)

			case res = <-promptDone:
				break loop
			}
		}

		// Turn complete. dispatchLoop enqueues updates before the prompt
		// response, so any updates belonging to this turn are already buffered —
		// drain them non-blockingly before finishing.
		for drained := false; !drained; {
			select {
			case notif := <-notifCh:
				emit(notif)
			default:
				drained = true
			}
		}

		if res.err != nil {
			// Don't pool a session whose turn errored.
			events <- streaming.KiroEvent{
				Type:  streaming.EventTypeError,
				Error: fmt.Errorf("acp: session/prompt failed: %w", res.err),
			}
			b.notifs.Delete(sessionID)
			return
		}

		// Clean turn — deregister this turn's notif channel BEFORE pooling the
		// session, so a concurrent reuse can safely re-register its own.
		b.notifs.Delete(sessionID)
		b.returnSession(poolKey, sessionID)

		b.logger.Info().
			Str("session_id", sessionID).
			Str("model", req.Model).
			Str("stop_reason", res.stopReason).
			Bool("reused", reused).
			Dur("duration", time.Since(start)).
			Msg("ACP session complete")
	}()

	return events, nil
}

// ---------------------------------------------------------------------------
// ACP method calls
// ---------------------------------------------------------------------------

// acpProtocolVersion is the Agent Client Protocol version the gateway speaks.
// ProtocolVersion is an integer in ACP; kiro-cli 2.8.1 negotiates version 1.
const acpProtocolVersion = 1

func (b *ACPBackend) initialize() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	params := map[string]any{
		"protocolVersion": acpProtocolVersion,
		"clientCapabilities": map[string]any{
			"fs":       map[string]any{"readTextFile": false, "writeTextFile": false},
			"terminal": false,
		},
		"clientInfo": map[string]any{
			"name":    "go-kiro-gateway",
			"version": "1.0",
		},
	}
	result, err := b.call(ctx, "initialize", params)
	if err != nil {
		return err
	}

	// Log the negotiated protocol version; fail fast on an unsupported newer one.
	var r struct {
		ProtocolVersion int `json:"protocolVersion"`
	}
	if err := json.Unmarshal(result, &r); err != nil {
		return fmt.Errorf("acp: parse initialize result: %w", err)
	}
	b.logger.Info().Int("protocol_version", r.ProtocolVersion).Msg("ACP protocol negotiated")
	if r.ProtocolVersion > acpProtocolVersion {
		return fmt.Errorf(
			"acp: kiro-cli requires protocol version %d but the gateway supports %d; please update go-kiro-gateway",
			r.ProtocolVersion, acpProtocolVersion,
		)
	}
	return nil
}

func (b *ACPBackend) sessionNew(ctx context.Context) (string, error) {
	cwd, _ := os.Getwd()
	// cwd and mcpServers are both required by ACP session/new. Omitting
	// mcpServers makes kiro-cli's deserialize fail and the subprocess exit.
	result, err := b.call(ctx, "session/new", map[string]any{
		"cwd":        cwd,
		"mcpServers": []any{},
	})
	if err != nil {
		return "", err
	}
	var r struct {
		SessionID string `json:"sessionId"`
	}
	if err := json.Unmarshal(result, &r); err != nil {
		return "", fmt.Errorf("acp: parse session/new result: %w", err)
	}
	return r.SessionID, nil
}

// checkoutSession returns a session ready for a fresh turn for the given model
// key, reusing a cleared idle pooled session when one is available or creating a
// new one otherwise. The returned poolKey is the model the session actually has
// selected (it falls back to "" if a requested model was unavailable) and must
// be the key used when returning the session to the pool.
func (b *ACPBackend) checkoutSession(ctx context.Context, key string) (sessionID, poolKey string, reused bool, err error) {
	if id, ok := b.popIdle(key); ok {
		// Wipe prior context before reuse. A failure (including subprocess exit)
		// means the session is dead/poisoned — drop it and create a fresh one.
		if cerr := b.clearSession(ctx, id); cerr == nil {
			return id, key, true, nil
		}
		b.logger.Warn().Str("session_id", id).Msg("ACP: /clear failed on pooled session, creating a new one")
	}

	id, err := b.sessionNew(ctx)
	if err != nil {
		return "", "", false, fmt.Errorf("acp: session/new failed: %w", err)
	}
	poolKey = key
	if key != "" {
		if serr := b.selectModelKiro(ctx, id, key); serr != nil {
			b.logger.Warn().Err(serr).Str("model", key).Msg("model selection failed, using session default")
			poolKey = "" // session is on the default model now
		}
	}
	return id, poolKey, false, nil
}

// popIdle removes and returns one idle session for key, if any.
func (b *ACPBackend) popIdle(key string) (string, bool) {
	b.poolMu.Lock()
	defer b.poolMu.Unlock()
	ids := b.idleByModel[key]
	if len(ids) == 0 {
		return "", false
	}
	id := ids[len(ids)-1]
	b.idleByModel[key] = ids[:len(ids)-1]
	return id, true
}

// returnSession pools a session for reuse under key, dropping it when the idle
// pool for that key is full. With maxIdle == 0 every session is dropped, which
// reduces to the per-request behavior (no reuse).
func (b *ACPBackend) returnSession(key, sessionID string) {
	b.poolMu.Lock()
	defer b.poolMu.Unlock()
	if len(b.idleByModel[key]) >= b.maxIdle {
		return
	}
	b.idleByModel[key] = append(b.idleByModel[key], sessionID)
}

// clearSession wipes a session's conversation history via the /clear command.
func (b *ACPBackend) clearSession(ctx context.Context, sessionID string) error {
	_, err := b.runCommandTurn(ctx, sessionID, "/clear")
	return err
}

// selectModelKiro switches the session to kiroModel (already in kiro-cli's
// naming) via the `/model` slash command. kiro-cli has no session/set_model
// method (sending one crashes the subprocess), so selection is just another
// prompt turn whose output is discarded.
func (b *ACPBackend) selectModelKiro(ctx context.Context, sessionID, kiroModel string) error {
	out, err := b.runCommandTurn(ctx, sessionID, "/model "+kiroModel)
	if err != nil {
		return err
	}
	if strings.Contains(strings.ToLower(out), "not found") {
		return fmt.Errorf("acp: model %q unavailable: %s", kiroModel, strings.TrimSpace(out))
	}
	return nil
}

// modelKey maps a gateway model ID to the pool key (kiro model id), normalizing
// empty/"auto" to "" (the session default, which skips /model selection).
func modelKey(gatewayModel string) string {
	k := toKiroModelID(gatewayModel)
	if k == "auto" {
		return ""
	}
	return k
}

// runCommandTurn sends text as a complete prompt turn and returns the
// concatenated agent text plus any error. Streamed updates are consumed but not
// forwarded — used for kiro slash commands like /model. It mirrors the
// concurrent prompt/drain model of Complete: the prompt response (stopReason)
// signals turn completion, so the prompt is driven on its own goroutine while
// this function drains updates.
func (b *ACPBackend) runCommandTurn(ctx context.Context, sessionID, text string) (string, error) {
	notifCh := make(chan *SessionNotification, 64)
	b.notifs.Store(sessionID, notifCh)
	defer b.notifs.Delete(sessionID)

	done := make(chan error, 1)
	go func() {
		_, err := b.sessionPrompt(ctx, sessionID, text)
		done <- err
	}()

	var sb strings.Builder
	collect := func(n *SessionNotification) {
		if u, err := ParseUpdate(n.Update); err == nil {
			if c, ok := u.(*AgentMessageChunk); ok {
				sb.WriteString(c.Content.Text)
			}
		}
	}

	for {
		select {
		case <-ctx.Done():
			return sb.String(), ctx.Err()
		case <-b.done:
			return sb.String(), fmt.Errorf("acp: subprocess exited during command turn")
		case n := <-notifCh:
			collect(n)
		case err := <-done:
			for drained := false; !drained; {
				select {
				case n := <-notifCh:
					collect(n)
				default:
					drained = true
				}
			}
			return sb.String(), err
		}
	}
}

// toKiroModelID maps a gateway model ID to kiro-cli's naming, which uses a dot
// before the version (e.g. claude-sonnet-4-6 → claude-sonnet-4.6). Names whose
// final two hyphen-separated segments aren't both numeric (e.g. claude-sonnet-4,
// auto) are returned unchanged.
func toKiroModelID(model string) string {
	parts := strings.Split(model, "-")
	n := len(parts)
	if n >= 2 && isAllDigits(parts[n-1]) && isAllDigits(parts[n-2]) {
		return strings.Join(parts[:n-1], "-") + "." + parts[n-1]
	}
	return model
}

func isAllDigits(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

// sessionPrompt sends a session/prompt request and returns the turn's
// stopReason. In ACP, kiro-cli streams session/update notifications while the
// turn runs and only responds to this request once the turn ends — so the
// response (carrying stopReason) is the completion signal, not a notification.
// Callers must therefore drive this concurrently with the notification drain.
func (b *ACPBackend) sessionPrompt(ctx context.Context, sessionID, text string) (string, error) {
	result, err := b.call(ctx, "session/prompt", map[string]any{
		"sessionId": sessionID,
		"prompt": []map[string]any{
			{"type": "text", "text": text},
		},
	})
	if err != nil {
		return "", err
	}
	var r PromptResponse
	if err := json.Unmarshal(result, &r); err != nil {
		return "", fmt.Errorf("acp: parse session/prompt result: %w", err)
	}
	return r.StopReason, nil
}

func (b *ACPBackend) sessionCancel(ctx context.Context, sessionID string) error {
	_, err := b.call(ctx, "session/cancel", map[string]any{
		"sessionId": sessionID,
	})
	return err
}

// ---------------------------------------------------------------------------
// JSON-RPC call helper
// ---------------------------------------------------------------------------

// call sends a JSON-RPC request and waits for the corresponding response.
func (b *ACPBackend) call(ctx context.Context, method string, params any) (json.RawMessage, error) {
	id := b.nextID.Add(1)
	respCh := make(chan *rpcMessage, 1)
	b.pending.Store(id, respCh)
	defer b.pending.Delete(id)

	req := rpcRequest{
		JSONRPC: "2.0",
		ID:      id,
		Method:  method,
		Params:  params,
	}

	b.mu.Lock()
	err := writeRequest(b.stdin, req)
	b.mu.Unlock()
	if err != nil {
		return nil, fmt.Errorf("acp: write %s: %w", method, err)
	}

	b.logger.Debug().Str("method", method).Int64("id", id).Msg("ACP → sent")

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-b.done:
		return nil, fmt.Errorf("acp: subprocess exited while waiting for %s response", method)
	case msg := <-respCh:
		if msg.Error != nil {
			return nil, msg.Error
		}
		b.logger.Debug().Str("method", method).Int64("id", id).Msg("ACP ← received")
		return msg.Result, nil
	}
}

// ---------------------------------------------------------------------------
// Dispatch loop
// ---------------------------------------------------------------------------

// dispatchLoop reads messages from stdout and routes them to the appropriate
// pending response channel or active session notification channel.
func (b *ACPBackend) dispatchLoop() {
	for {
		msg, err := readMessage(b.stdout)
		if err != nil {
			if err != io.EOF {
				b.logger.Warn().Err(err).Msg("ACP read error")
			}
			return
		}

		if msg.Method == "" {
			// Response to an in-flight request.
			if ch, ok := b.pending.Load(msg.ID); ok {
				ch.(chan *rpcMessage) <- msg
			}
		} else if msg.Method == "session/update" {
			// Notification — route to the matching session channel.
			var notif SessionNotification
			if err := json.Unmarshal(msg.Params, &notif); err != nil {
				b.logger.Warn().Err(err).Msg("ACP: failed to parse session/update params")
				continue
			}
			if ch, ok := b.notifs.Load(notif.SessionID); ok {
				select {
				case ch.(chan *SessionNotification) <- &notif:
				default:
					b.logger.Warn().Str("session_id", notif.SessionID).Msg("ACP: notification channel full, dropping")
				}
			}
		}
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// resolveKiroCLI returns the absolute path to kiro-cli, checking the explicit
// path first, then falling back to PATH lookup.
func resolveKiroCLI(explicitPath string) (string, error) {
	if explicitPath != "" {
		if _, err := os.Stat(explicitPath); err != nil {
			return "", fmt.Errorf("acp: kiro-cli not found at KIRO_CLI_PATH=%q: %w", explicitPath, err)
		}
		return explicitPath, nil
	}
	p, err := exec.LookPath("kiro-cli")
	if err != nil {
		return "", fmt.Errorf(
			"acp: kiro-cli not found on PATH and KIRO_CLI_PATH is not set.\n" +
				"Install kiro-cli from https://kiro.dev/downloads/ or set KIRO_CLI_PATH to its location.",
		)
	}
	return p, nil
}

// drainStderr reads stderr from the subprocess and logs each line at WARN.
func (b *ACPBackend) drainStderr(r io.Reader) {
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		b.logger.Warn().Str("stderr", scanner.Text()).Msg("kiro-cli stderr")
	}
}

// extractPromptText pulls the prompt text the gateway should send to kiro-cli.
// The backend receives the Kiro/CodeWhisperer payload, so the primary path
// reads conversationState.currentMessage.userInputMessage.content. The legacy
// OpenAI-style messages walk is kept as a fallback for callers that pass it.
func extractPromptText(payload map[string]any) string {
	if cs, ok := payload["conversationState"].(map[string]any); ok {
		if cm, ok := cs["currentMessage"].(map[string]any); ok {
			if uim, ok := cm["userInputMessage"].(map[string]any); ok {
				if c, ok := uim["content"].(string); ok && c != "" {
					return c
				}
			}
		}
	}

	msgs, ok := payload["messages"].([]any)
	if !ok || len(msgs) == 0 {
		return ""
	}
	// Walk backwards to find the last user message.
	for i := len(msgs) - 1; i >= 0; i-- {
		msg, ok := msgs[i].(map[string]any)
		if !ok {
			continue
		}
		if msg["role"] != "user" {
			continue
		}
		switch c := msg["content"].(type) {
		case string:
			return c
		case []any:
			for _, block := range c {
				b, ok := block.(map[string]any)
				if !ok {
					continue
				}
				if b["type"] == "text" {
					if t, ok := b["text"].(string); ok {
						return t
					}
				}
			}
		}
	}
	return ""
}

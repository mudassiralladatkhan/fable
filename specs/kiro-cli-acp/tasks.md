# Tasks: Kiro CLI ACP Backend

## Implementation Plan

- [x] 1. Add ACP configuration fields and update startup validation
  - Add `BackendMode`, `KiroCLIPath`, and `ACPAgent` fields to `Config` struct in `internal/config/config.go`
  - Load from env vars `BACKEND_MODE` (enum: `http`/`acp`, default `http`), `KIRO_CLI_PATH`, `ACP_AGENT`
  - Update `validate()` to skip credential field requirement when `BackendMode == "acp"`
  - Add tests in `config_test.go` covering: ACP mode skips cred validation, HTTP mode still requires creds, enum rejects invalid values
  - References: Req 1.1, 1.2, 1.6

- [x] 2. Define the Backend interface and HTTP backend wrapper
  - Create `internal/backend/` package
  - Define `Backend` interface with `Complete(ctx, *Request) (<-chan streaming.KiroEvent, error)` and `Close() error`
  - Define `Request` struct with `Payload`, `Model`, `Stream`, `ProfileARN`, `ConversationID` fields
  - Implement `HTTPBackend` in `internal/backend/http.go` wrapping the existing `client.KiroClient` — call `RequestWithRetry`, pipe body into `streaming.ParseKiroStream`, return the event channel
  - Add unit tests for `HTTPBackend` verifying it produces the same KiroEvents as the existing streaming path
  - References: Design — Backend Interface, HTTP Backend

- [x] 3. Implement JSON-RPC 2.0 transport layer
  - Create `internal/backend/jsonrpc.go` with `rpcRequest`, `rpcResponse`, `rpcError` structs
  - Implement `writeRequest(w io.Writer, req rpcRequest) error` — marshal to single-line JSON + `\n`
  - Implement `readMessage(r *bufio.Reader) (*rpcResponse, error)` — read one line, unmarshal
  - Add unit tests using `io.Pipe` to verify round-trip encode/decode for requests, responses, and notifications
  - References: Design — JSON-RPC Transport

- [x] 4. Implement ACP message types
  - Create `internal/backend/acp_types.go` with typed structs: `SessionNotification`, `AgentMessageChunk`, `ToolCallNotification`, `TurnEndNotification`
  - Add helper `ParseUpdate(raw json.RawMessage) (any, error)` that decodes the `type` field and returns the appropriate concrete type
  - Add unit tests for `ParseUpdate` covering all notification types and unknown types
  - References: Design — ACP Message Types

- [x] 5. Implement ACP backend — process lifecycle
  - Create `internal/backend/acp.go` with `ACPBackend` struct
  - Implement `NewACPBackend(cfg *config.Config) (*ACPBackend, error)`:
    - Locate `kiro-cli` via `cfg.KiroCLIPath` or `exec.LookPath("kiro-cli")`; return clear error if not found (Req 1.5)
    - Spawn `kiro-cli acp [--agent <name>]` with stdin/stdout pipes
    - Start stderr-reading goroutine that logs at WARN level (Req 6.4)
    - Start stdout-reading goroutine that dispatches responses to `pending` map and notifications to active session channels
    - Send `initialize` handshake with 10s timeout; fail fast if no response (Req 2.4)
  - Implement `Close() error`: send SIGTERM to subprocess, wait up to 5s, then SIGKILL (Req 2.2)
  - Add unit tests using a fake `kiro-cli` binary (a small Go test helper that speaks ACP over stdio) to verify: startup succeeds, initialize handshake completes, Close terminates the process
  - References: Req 2.1–2.5, Design — ACP Backend startup sequence

- [x] 6. Implement ACP backend — per-request session flow
  - Implement `Complete(ctx context.Context, req *Request) (<-chan streaming.KiroEvent, error)` on `ACPBackend`:
    - Send `session/new` → get `sessionId`; return error on failure (Req 3.1, 3.4)
    - Send `session/set_model` with `req.Model`; log warning on failure, continue (Req 5.1–5.3)
    - Extract prompt text from `req.Payload` and send `session/prompt` (Req 3.2)
    - Subscribe to `session/notification` messages for this `sessionId`
    - Start goroutine that reads notifications, translates to `KiroEvent` via the mapping table, writes to returned channel
    - On `TurnEnd`, close the channel (Req 3.3, 4.2)
    - On context cancellation, cancel the session via `session/cancel` and close the channel (Req 4.5)
  - Add unit tests with the fake CLI helper covering: streaming chunks arrive in order, TurnEnd closes channel, concurrent sessions are isolated (Req 3.5)
  - References: Req 3.1–3.5, 4.1–4.5, Design — per-request flow

- [x] 7. Refactor Server to use Backend interface
  - Add `backend backend.Backend` field to `Server` struct in `internal/server/server.go`
  - Update `New()` constructor to accept `backend.Backend` as a parameter
  - Refactor `handleOpenAIStreaming`, `handleOpenAINonStreaming`, `handleAnthropicStreaming`, `handleAnthropicNonStreaming` to call `s.backend.Complete()` and consume the returned `KiroEvent` channel instead of calling `s.httpClient.RequestWithRetry()` directly
  - Update `handleListModels` in ACP mode: the model list still comes from `s.cache`/`s.resolver` unchanged (Req 5.4)
  - Update all existing server tests to inject a mock `Backend` instead of a mock HTTP client
  - References: Design — Server Changes, Req 5.4

- [x] 8. Wire backend selection in main.go
  - Update `cmd/gateway/main.go` to select backend based on `cfg.BackendMode`:
    - `"acp"`: call `backend.NewACPBackend(cfg)`, skip auth/HTTP client init, register `defer backend.Close()`
    - `"http"` (default): call `backend.NewHTTPBackend(authMgr, kiroClient, cfg)`
  - Update startup banner to show `backend: acp (kiro-cli: <resolved path>)` when in ACP mode (Req 6.1)
  - Update graceful shutdown to call `backend.Close()` (Req 2.2)
  - References: Req 1.3–1.5, 2.2, 6.1, Design — Startup Changes

- [x] 9. Add observability for ACP mode
  - Log every JSON-RPC send/receive at DEBUG level with structured fields (method, id, session_id) (Req 6.2)
  - When `DEBUG_MODE=all`, write ACP request/response payloads to debug log files via `DebugLogger` (Req 6.3)
  - Log session completion (session ID, model, duration) at INFO level on `TurnEnd` (Req 6.5)
  - Add tests verifying debug log entries are written in `all` mode and suppressed in `off` mode
  - References: Req 6.2–6.5

- [x] 10. Update configuration documentation
  - Update `.env.example` with `BACKEND_MODE`, `KIRO_CLI_PATH`, `ACP_AGENT` entries and inline comments explaining each
  - Update `README.md` ACP section (or add one) describing the ACP backend, prerequisites, and example configuration
  - References: Req 1.1–1.6

---

## ACP v1 Protocol Conformance Fix (issue #21)

The original implementation was written against a guessed protocol shape. These tasks align the wire format with Agent Client Protocol v1 (`schema/v1/schema.json`) so `kiro-cli` 2.8.1 sessions complete instead of the subprocess exiting silently.

- [x] 11. Fix the `initialize` and `session/new` request shapes
  - In `acp.go` `initialize()`: send `protocolVersion` as integer `1`, move capabilities under `clientCapabilities` (with `fs`/`terminal` defaults); keep `clientInfo`
  - Capture the agent's `initialize` response, log the negotiated `protocolVersion`, and fail startup with a clear error if it exceeds the supported version (Req 7.5)
  - In `sessionNew()`: add required `mcpServers: []` alongside `cwd` (Req 7.2)
  - References: Req 7.1, 7.2, 7.5

- [x] 12. Rewrite ACP notification types for the `sessionUpdate` discriminator
  - In `acp_types.go`: replace the `type`/PascalCase structs with `sessionUpdate`/snake_case (`agent_message_chunk`, `tool_call`, `tool_call_update`); add a nested `ContentBlock`; add `PromptResponse{StopReason}`; remove `TurnEndNotification`
  - Update `ParseUpdate` to discriminate on `sessionUpdate` and return `nil`/skip for unknown variants (e.g. `agent_thought_chunk`, `plan`) rather than erroring
  - Update `acp_test.go` notification fixtures to the real shapes
  - References: Req 7.4, Design — ACP Message Types

- [x] 13. Fix `session/prompt` and the turn-completion control flow
  - In `acp.go` `sessionPrompt()`: carry content blocks in the `prompt` field (not `content`)
  - In `Complete()`: start the notification-draining goroutine and issue `session/prompt` concurrently so updates stream while the prompt is in flight; the prompt response (`stopReason`) — not a `TurnEnd` notification — closes the channel
  - Map `agent_message_chunk` → text KiroEvent (from nested `content.text`), `tool_call` (status `in_progress`) → tool-use KiroEvent, `tool_call_update` → skip
  - Handle abnormal `stopReason` per the error-handling table
  - References: Req 7.3, 4.1–4.5, Design — per-request flow

- [x] 14. Fix the dispatch loop method name
  - In `acp.go` `dispatchLoop()`: route notifications whose method is `session/update` (was `session/notification`) to the session channel
  - References: Req 7.4, Design — JSON-RPC Transport

- [x] 15. Verify against kiro-cli and update tests/docs
  - Update the fake-CLI test helper to speak ACP v1 (integer `protocolVersion`, `session/update` notifications, prompt response with `stopReason`); ensure `acp_test.go` covers the concurrent prompt/drain flow and stop-reason channel close
  - Run `go test ./...` and a manual end-to-end check against `kiro-cli acp` confirming `session/new` returns and a prompt streams to completion
  - Note the resolution in the README ACP/troubleshooting section
  - References: Req 7, issue #21

---

## Live-validation fixes (kiro-cli 2.8.1 end-to-end, issue #21)

Validating against a real `kiro-cli` 2.8.1 confirmed the `session/new` fix but surfaced three further breakages in the original backend that block a working request.

- [x] 16. Remove the unsupported `session/set_model` call
  - kiro-cli has no `session/set_model` method; sending it crashes the subprocess mid-request. Delete the `sessionSetModel` call and method.
  - References: Req 5 (note), issue #21

- [x] 17. Fix prompt extraction from the Kiro payload
  - `extractPromptText` must read `conversationState.currentMessage.userInputMessage.content` (string) from the Kiro/CodeWhisperer payload, with the existing `messages` walk kept as a fallback. Without this the gateway sends an empty prompt.
  - Add a unit test covering the Kiro payload shape.
  - References: Req 3.2, Design — Prompt extraction

- [x] 18. Implement model selection via the `/model` slash command
  - Add `toKiroModelID` (gateway → kiro name mapping: join final two numeric version segments with a dot) and `selectModel`, which runs `/model <kiro-id>` as a command turn via a shared `runCommandTurn` helper, discarding streamed output. Skip for empty/`auto`; warn and continue on "not found"/failure.
  - Wire `selectModel` into `Complete` in place of `sessionSetModel`.
  - Add unit tests for `toKiroModelID` and a fake-CLI mode covering successful selection and the unavailable-model fallback.
  - Re-validate live against `kiro-cli` 2.8.1: a streaming chat completion returns model output end-to-end.
  - References: Req 5.1–5.4, Design — Model selection

---

## Session reuse for latency (issue follow-up)

- [x] 19. Add the `ACP_MAX_IDLE_SESSIONS` configuration
  - Add `ACPMaxIdleSessions int` to `Config` and load it via `envInt("ACP_MAX_IDLE_SESSIONS", 8)`
  - Add a `.env.example` entry with an inline comment (default 8, `0` disables reuse)
  - Add a config test asserting the default and an override
  - References: Req 8.5, 8.8, Design — Configuration

- [x] 20. Implement the session pool and wire it into `Complete`
  - Add pool state to `ACPBackend`: `poolMu sync.Mutex`, `idleByModel map[string][]string`, `maxIdle int` (from `cfg.ACPMaxIdleSessions`); initialize the map in `NewACPBackend`
  - Add `modelKey(gatewayModel) string` (= `toKiroModelID` normalized so `auto`/empty → `""`)
  - Add `checkoutSession(ctx, key) (sessionID, effectiveKey string, err error)`: pop an idle id for `key`; if found, `/clear` via `runCommandTurn` and reuse (on error discard and create); else `session/new` + `/model` (effectiveKey falls back to `""` if the model is unavailable)
  - Add `returnSession(key, sessionID)`: append under `poolMu` if below `maxIdle`, else drop
  - Refactor `Complete` to use checkout/return in place of the inline `session/new` + `selectModel`; return the session after the turn completes, drop it on turn error / subprocess exit
  - References: Req 8.1–8.8, Design — Session pooling

- [x] 21. Test and live-validate session reuse
  - Unit tests with the fake CLI: a second request reuses the pooled session (assert `session/new` is called once across two sequential same-model requests, and `/clear` is issued on the reuse); `maxIdle == 0` creates a new session each time; a different model does not reuse a mismatched session; a failed `/clear` falls back to a new session
  - Run `go test ./...` (race) and re-validate live against `kiro-cli` 2.8.1: two sequential requests, confirming the second skips `session/new` and is faster
  - Note reuse behavior in the README ACP section
  - References: Req 8, Design — Session pooling

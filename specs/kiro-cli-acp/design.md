# Design: Kiro CLI ACP Backend

## Overview

This feature adds an ACP (Agent Control Protocol) backend to the gateway alongside the existing HTTP backend. When configured, the gateway spawns `kiro-cli acp` as a subprocess and routes all chat completion requests through it via JSON-RPC 2.0 over stdin/stdout instead of making direct HTTP calls to the Kiro API.

The key architectural principle is **backend abstraction**: the existing OpenAI and Anthropic adapters, converters, and streaming formatters are reused unchanged. Only the backend that fulfills the request changes. This is achieved by introducing a `Backend` interface that both the HTTP client and the new ACP client satisfy.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                          Clients                                │
│  OpenAI SDK / Anthropic SDK / Cursor / Claude Code / etc.       │
└───────────────────────────┬─────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│                   Go Kiro Gateway                               │
│                                                                 │
│  ┌─────────────────────┐       ┌─────────────────────┐          │
│  │  OpenAI Adapter     │       │  Anthropic Adapter  │          │
│  │  /v1/chat/...       │       │  /v1/messages       │          │
│  └──────────┬──────────┘       └──────────┬──────────┘          │
│             └──────────────┬───────────────┘                    │
│                            ▼                                    │
│             ┌─────────────────────────────┐                     │
│             │      Backend Interface      │                     │
│             └──────────┬──────────────────┘                     │
│                        │                                        │
│           ┌────────────┴────────────┐                           │
│           ▼                         ▼                           │
│  ┌─────────────────┐    ┌──────────────────────┐                │
│  │  HTTP Backend   │    │    ACP Backend       │                │
│  │  (existing)     │    │  (new, stdio/JSON-RPC)│               │
│  └────────┬────────┘    └──────────┬───────────┘                │
└───────────┼──────────────────────── ┼───────────────────────────┘
            ▼                         ▼
┌─────────────────────┐    ┌──────────────────────┐
│    Kiro HTTP API    │    │   kiro-cli process   │
│  (AWS CodeWhisperer)│    │  (local subprocess)  │
└─────────────────────┘    └──────────────────────┘
```

---

## Components and Interfaces

### Backend Interface (`internal/backend/`)

A new package introduces the `Backend` interface that abstracts how the gateway fulfills a chat completion request:

```go
// Backend abstracts the downstream fulfillment mechanism.
// The HTTP backend calls the Kiro API directly; the ACP backend
// communicates with a local kiro-cli process over JSON-RPC stdio.
type Backend interface {
    // Complete sends a unified request and returns a channel of KiroEvents.
    // The channel is closed when the response is complete or an error occurs.
    Complete(ctx context.Context, req *Request) (<-chan streaming.KiroEvent, error)
    // Close releases any resources held by the backend (e.g. subprocess).
    Close() error
}

// Request is the backend-agnostic representation of a completion request.
type Request struct {
    Payload    map[string]any   // Kiro-format payload (from converter)
    Model      string           // resolved internal model ID
    Stream     bool
    SessionHint string          // optional: client-supplied session ID hint
}
```

The existing route handlers (`handleOpenAIStreaming`, `handleOpenAINonStreaming`, etc.) are refactored to call `s.backend.Complete(ctx, req)` instead of `s.httpClient.RequestWithRetry(...)` directly. This keeps the adapter layer unchanged.

### HTTP Backend (`internal/backend/http.go`)

A thin wrapper around the existing `client.KiroClient`. It:
1. Calls `RequestWithRetry` to get an `*http.Response`
2. Pipes `resp.Body` into `streaming.ParseKiroStream` to produce a `KiroEvent` channel
3. Handles non-streaming by collecting all events via `streaming.CollectResponse`

This is a straightforward extraction of logic already in the route handlers.

### ACP Backend (`internal/backend/acp.go`)

The new backend that communicates with `kiro-cli acp` over stdio JSON-RPC 2.0:

```go
type ACPBackend struct {
    cmd        *exec.Cmd
    stdin      io.WriteCloser
    stdout     *bufio.Reader
    mu         sync.Mutex          // serializes JSON-RPC writes
    pending    map[int64]chan *rpcResponse  // in-flight request tracking
    nextID     atomic.Int64
    cfg        *config.Config
    logger     zerolog.Logger
    done       chan struct{}        // closed when subprocess exits
}
```

**Startup sequence:**
1. Locate `kiro-cli` (via `KIRO_CLI_PATH` or `exec.LookPath`)
2. Spawn `kiro-cli acp [--agent <name>]` with stdin/stdout pipes
3. Start a goroutine to read stderr and log at WARN level
4. Start a goroutine to read stdout responses and route to `pending` channels
5. Send `initialize` JSON-RPC request and await response
6. Backend is ready to serve requests

**Per-request flow:**
1. Check out a session for the request's target model (see *Session pooling* below). Either an idle pooled session for that model — `/clear`ed before reuse — or a freshly created one (`session/new` + `/model`).
2. Model selection happens only when creating a new session (best-effort): map the requested model to kiro's ID and run a `/model <kiro-id>` slash command as a `session/prompt` turn, discarding its output. Warn and continue on failure. Skipped for empty/`auto`. (See *Model selection* below.)
3. Extract the prompt text from the Kiro payload (see *Prompt extraction* below)
4. Subscribe to `session/update` notifications for this `sessionId`, then start the notification-draining goroutine
5. `session/prompt` with the user message in the `prompt` field. This call is issued **without blocking** the notification drain — in ACP the prompt response is the turn-completion signal and only returns after all updates have streamed, so it must not be awaited ahead of reading updates
6. While the prompt is in flight, translate each `session/update` (`agent_message_chunk`, `tool_call`, `tool_call_update`) into `KiroEvent` objects. kiro-cli also emits proprietary `_kiro.dev/*` notifications (commands, mcp init, metadata); these are ignored
7. When the `session/prompt` response arrives, inspect its `stopReason`, emit the terminal event, and close the channel
8. Return the session to the idle pool for reuse (or drop it if the turn errored, the subprocess exited, or the pool is full)

> **Critical ordering note (root cause of issue #21):** the original implementation awaited the `session/prompt` response *before* reading notifications and waited for a non-existent `TurnEnd` notification. ACP has no `TurnEnd` notification — the prompt response carries `stopReason`. The prompt request and the update drain must run concurrently.

### Model selection (`/model` slash command)

kiro-cli has no `session/set_model` method (sending it crashes the subprocess). Models are selected via the `/model <name>` slash command, issued as an ordinary `session/prompt` turn. `selectModel` maps the gateway model ID to kiro's naming (`toKiroModelID`: join the final two numeric version segments with a dot, e.g. `claude-sonnet-4-6` → `claude-sonnet-4.6`), then runs a *command turn* that streams a confirmation/error chunk and ends with a `stopReason`. A response containing "not found" is treated as a soft failure (warn, fall back to the session default). A shared `runCommandTurn` helper drives this turn and discards the streamed text rather than forwarding it to the client. Note: because sessions are stateless per request, this adds one extra round-trip per request when a concrete model is selected.

### Prompt extraction

The backend receives the Kiro/CodeWhisperer payload, not an OpenAI-style `messages` array. `extractPromptText` reads `conversationState.currentMessage.userInputMessage.content` (a string), falling back to the legacy `messages` walk for any caller that still passes that shape.

### Session pooling (latency reuse)

All ACP sessions live inside the one `kiro-cli` subprocess, so pooling reuses logical session IDs. `session/new` is the expensive step (it initializes the session's MCP servers, ≈3.7s); `/clear` is a cheap turn that fully wipes conversation history. Reuse therefore swaps an expensive create + `/model` for a cheap `/clear`.

State added to `ACPBackend`:

```go
poolMu      sync.Mutex
idleByModel map[string][]string // kiro model id ("" = default) → idle session IDs
maxIdle     int                 // cfg.ACPMaxIdleSessions; 0 disables reuse
```

The pool key is the kiro model id the session currently has selected — `modelKey(req.Model)` = `toKiroModelID(req.Model)` normalized so `auto`/empty both map to `""` (the session default). A session is "leased" simply by being absent from `idleByModel` while a turn runs; checkout pops it, return pushes it back, all under `poolMu`. No session is ever in the pool while in use, so concurrent requests never collide.

```
checkoutSession(ctx, key):
  pop an idle id for key under poolMu
  if found:
    err = runCommandTurn(ctx, id, "/clear")     // wipe prior context
    if err == nil: return id, keyModel           // reuse
    // poisoned/dead session: drop it, fall through to create
  id = sessionNew(ctx)
  if key != "": err = selectModel; if "not found" → effective key = ""
  return id, effectiveKey

returnSession(key, id):
  under poolMu: if len(idleByModel[key]) < maxIdle → append, else drop
```

- **Exclusivity (Req 8.4):** enforced by removal-from-pool on checkout.
- **Poisoned sessions (Req 8.6):** a failed `/clear` (including subprocess exit, surfaced as an error by `runCommandTurn`) discards the session and creates a fresh one; if `session/new` then also fails the request returns an error as today.
- **Bounding (Req 8.5):** `returnSession` drops sessions beyond `maxIdle`. Dropped sessions are abandoned in the subprocess (ACP v1 has no `session/close`); steady-state session count is bounded by peak concurrency + `maxIdle`, and a subprocess restart clears them. This is an accepted limitation.
- **Disable (Req 8.8):** `maxIdle == 0` means `returnSession` always drops, so every request creates a fresh session — the prior per-request behavior.
- **Shutdown (Req 8.7):** `Close` terminates the subprocess, which invalidates all pooled IDs; the pool is cleared.

The effective model returned by checkout (which may differ from the requested model if `/model` reported the model unavailable) is the key used when returning the session, so a session that fell back to the default is pooled under `""` and not mistakenly reused as the requested model.

### Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `ACP_MAX_IDLE_SESSIONS` | `8` | Max idle ACP sessions kept warm per model for reuse. `0` disables reuse (new session per request). |

### JSON-RPC Transport (`internal/backend/jsonrpc.go`)

Handles the low-level framing for JSON-RPC 2.0 over stdio (newline-delimited JSON):

```go
type rpcRequest struct {
    JSONRPC string `json:"jsonrpc"`
    ID      int64  `json:"id,omitempty"`
    Method  string `json:"method"`
    Params  any    `json:"params,omitempty"`
}

type rpcResponse struct {
    JSONRPC string          `json:"jsonrpc"`
    ID      int64           `json:"id,omitempty"`
    Result  json.RawMessage `json:"result,omitempty"`
    Error   *rpcError       `json:"error,omitempty"`
    Method  string          `json:"method,omitempty"` // for notifications
    Params  json.RawMessage `json:"params,omitempty"` // for notifications
}

type rpcError struct {
    Code    int    `json:"code"`
    Message string `json:"message"`
}
```

Messages are written as single-line JSON terminated by `\n`. The reader goroutine reads lines and dispatches:
- Responses (has `id`, no `method`) → routes to the matching `pending` channel
- Notifications with method `session/update` (has `method`, no `id`) → routes to the active session's notification channel

### ACP Message Types (`internal/backend/acp_types.go`)

Typed structs for ACP `session/update` notifications. Updates are discriminated by the `sessionUpdate` field (snake_case), and content blocks are nested objects (`{"type":"text","text":"..."}`), not flat strings:

```go
type SessionNotification struct {
    SessionID string          `json:"sessionId"`
    Update    json.RawMessage `json:"update"`
}

// ContentBlock is the ACP text/image/resource content block.
type ContentBlock struct {
    Type string `json:"type"` // "text" | "image" | ...
    Text string `json:"text"`
}

// AgentMessageChunk: sessionUpdate == "agent_message_chunk"
type AgentMessageChunk struct {
    SessionUpdate string       `json:"sessionUpdate"`
    Content       ContentBlock `json:"content"`
    MessageID     string       `json:"messageId,omitempty"`
}

// ToolCall: sessionUpdate == "tool_call"
type ToolCallNotification struct {
    SessionUpdate string `json:"sessionUpdate"`
    ToolCallID    string `json:"toolCallId"`
    Title         string `json:"title"`
    Kind          string `json:"kind"`
    Status        string `json:"status"` // pending | in_progress | completed | failed
}

// ToolCallUpdate: sessionUpdate == "tool_call_update"
type ToolCallUpdate struct {
    SessionUpdate string `json:"sessionUpdate"`
    ToolCallID    string `json:"toolCallId"`
    Status        string `json:"status"`
}
```

There is **no** `TurnEnd` notification type. Turn completion is signaled by the `session/prompt` response:

```go
// PromptResponse is the result of the session/prompt request.
type PromptResponse struct {
    StopReason string `json:"stopReason"` // end_turn | max_tokens | max_turn_requests | refusal | cancelled
}
```

### KiroEvent Translation

ACP notifications are translated to `streaming.KiroEvent` objects (the same type used by the HTTP backend's event stream parser):

| ACP message | KiroEvent type |
|-----------------|----------------|
| `session/update` → `agent_message_chunk` | `KiroEventText` with text from nested `content.text` |
| `session/update` → `tool_call` (status `in_progress`) | `KiroEventToolUse` |
| `session/update` → `tool_call_update` | progress only — no KiroEvent |
| `session/prompt` response (`stopReason`) | terminal event + channel close |

This reuses the existing OpenAI/Anthropic SSE formatters without modification. `ParseUpdate` discriminates on the `sessionUpdate` field and ignores unknown variants (e.g. `agent_thought_chunk`, `plan`) rather than erroring, so future ACP additions don't break the stream.

### Configuration Changes (`internal/config/config.go`)

New fields added to `Config`:

```go
// ACP backend configuration
BackendMode    string // "http" (default) | "acp"
KiroCLIPath    string // explicit path to kiro-cli binary (optional)
ACPAgent       string // --agent flag value for kiro-cli acp (optional)
```

New environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `BACKEND_MODE` | `http` | Backend to use: `http` or `acp` |
| `KIRO_CLI_PATH` | `` | Explicit path to kiro-cli binary |
| `ACP_AGENT` | `` | Agent name to pass via `--agent` flag |

Validation change: when `BACKEND_MODE=acp`, credential fields (`REFRESH_TOKEN`, `KIRO_CREDS_FILE`, `KIRO_CLI_DB_FILE`) are not required.

### Server Changes (`internal/server/server.go`)

The `Server` struct gains a `backend backend.Backend` field. The `New()` constructor accepts the backend as a dependency. Route handlers call `s.backend.Complete()` instead of `s.httpClient.RequestWithRetry()`.

The existing `httpClient` field is retained for backwards compatibility and used by the HTTP backend internally.

### Startup Changes (`cmd/gateway/main.go`)

Backend selection is wired in `main.go` based on `cfg.BackendMode`:

```
if cfg.BackendMode == "acp" {
    backend = backend.NewACPBackend(cfg)
    // credential validation skipped
} else {
    backend = backend.NewHTTPBackend(authMgr, kiroClient, cfg)
}
```

---

## Data Models

All shapes below conform to Agent Client Protocol v1 (`schema/v1/schema.json`).

### ACP Initialize Request/Response

`protocolVersion` is an **integer** (`ProtocolVersion`, uint16), and capabilities are sent under `clientCapabilities`:

```json
// Request
{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":1,"clientCapabilities":{"fs":{"readTextFile":false,"writeTextFile":false},"terminal":false},"clientInfo":{"name":"go-kiro-gateway","version":"1.0"}}}

// Response — agent echoes the negotiated protocolVersion (kiro-cli 2.8.1 responds with 1)
{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":1,"agentCapabilities":{},"agentInfo":{"name":"kiro-cli"}}}
```

### ACP session/new

`cwd` (absolute) and `mcpServers` are both required; omitting `mcpServers` makes kiro-cli's deserialize fail and the subprocess exit silently:

```json
// Request
{"jsonrpc":"2.0","id":2,"method":"session/new","params":{"cwd":"/abs/path","mcpServers":[]}}

// Response
{"jsonrpc":"2.0","id":2,"result":{"sessionId":"abc-123"}}
```

### ACP session/prompt

Content blocks go in the `prompt` field. The response carries `stopReason` and is the turn-completion signal:

```json
// Request
{"jsonrpc":"2.0","id":3,"method":"session/prompt","params":{"sessionId":"abc-123","prompt":[{"type":"text","text":"user message here"}]}}

// Response (arrives after all updates have streamed)
{"jsonrpc":"2.0","id":3,"result":{"stopReason":"end_turn"}}
```

### ACP session/update (agent → client notification)

Method is `session/update`; updates are discriminated by `sessionUpdate`; content is nested:

```json
// agent_message_chunk
{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"abc-123","update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"Hello"}}}}

// tool_call
{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"abc-123","update":{"sessionUpdate":"tool_call","toolCallId":"call_001","title":"Reading file","kind":"read","status":"in_progress"}}}
```

---

## Error Handling

| Scenario | Behavior |
|----------|----------|
| `kiro-cli` not found at startup | Fatal error with message pointing to installation docs |
| `initialize` handshake timeout | Fatal error at startup |
| `session/new` RPC error | Return HTTP 502 to client |
| `session/prompt` RPC error | Return HTTP 502 to client |
| `session/prompt` response with abnormal `stopReason` (`refusal`, `max_tokens`, `cancelled`) | Map to the appropriate client finish reason; surface as error only when no content was produced |
| Subprocess exits unexpectedly | Log error, return HTTP 503 for all subsequent requests |
| Context cancelled mid-stream | Close notification channel, let client disconnect |

---

## Testing Strategy

- **Unit tests for `jsonrpc.go`**: Test encode/decode of requests, responses, and notifications using in-memory pipes.
- **Unit tests for `acp.go`**: Use a mock `kiro-cli` process (a simple Go binary or pipe-based fake) to test the full initialize → session/new → session/prompt → notifications flow.
- **Unit tests for `http.go`** (HTTP backend wrapper): Verify KiroEvent channel output matches existing streaming tests.
- **Integration tests**: Existing route handler tests pass unchanged since the backend interface is injected. New tests verify the route handlers work with a mock ACP backend.
- **Config tests**: Verify `BACKEND_MODE=acp` skips credential validation and that `BACKEND_MODE=http` (or unset) preserves existing validation behavior.

---

## Sequence Diagram

```
Client          Gateway         ACPBackend        kiro-cli
  │               │                  │                 │
  │ POST /v1/chat │                  │                 │
  │──────────────▶│                  │                 │
  │               │ backend.Complete │                 │
  │               │─────────────────▶│                 │
  │               │                  │ session/new     │
  │               │                  │────────────────▶│
  │               │                  │◀────────────────│
  │               │                  │ session/set_model│
  │               │                  │────────────────▶│
  │               │                  │◀────────────────│
  │               │                  │ session/prompt  │
  │               │                  │────────────────▶│
  │               │                  │ session/update  │  (streamed)
  │               │                  │◀────────────────│
  │  SSE chunk    │  KiroEvent chan   │                 │
  │◀──────────────│◀─────────────────│                 │
  │               │                  │ prompt response │
  │               │                  │  {stopReason}   │
  │               │                  │◀────────────────│
  │  [DONE]       │  chan close       │                 │
  │◀──────────────│◀─────────────────│                 │
```

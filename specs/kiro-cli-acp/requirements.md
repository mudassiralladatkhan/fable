# Requirements: Kiro CLI ACP Backend

## Introduction

Go Kiro Gateway currently proxies OpenAI and Anthropic API calls to the Kiro API (Amazon Q Developer / AWS CodeWhisperer) over HTTP. This feature adds a second backend transport: the locally running Kiro CLI, accessed via the Agent Control Protocol (ACP).

ACP is a JSON-RPC 2.0 protocol over stdin/stdout. When a user has the Kiro CLI installed locally (`kiro-cli`), the gateway can spawn it as a subprocess and communicate with it directly — no cloud credentials, token refresh, or AWS API calls required. This gives users a simpler auth model (they just need to be logged into the Kiro CLI) and an alternative path when direct API access is unavailable.

The two backends — HTTP and ACP — should be selectable via configuration. Both expose the same OpenAI and Anthropic compatible endpoints to upstream clients. The gateway's adapter layer remains unchanged; only the backend that fulfills the request changes.

---

## Requirements

### Requirement 1: ACP Backend Configuration

**User Story:** As a gateway operator, I want to configure the gateway to use a locally running Kiro CLI via ACP, so that I can access Kiro without needing to configure direct API credentials.

#### Acceptance Criteria

1. WHEN `BACKEND_MODE` is set to `acp` THEN the gateway SHALL use the ACP backend for all chat completion requests.
2. WHEN `BACKEND_MODE` is set to `http` or is unset THEN the gateway SHALL use the existing HTTP backend (no behavior change).
3. WHEN `BACKEND_MODE` is `acp` AND `KIRO_CLI_PATH` is not set THEN the gateway SHALL attempt to locate `kiro-cli` on the system PATH.
4. WHEN `KIRO_CLI_PATH` is set THEN the gateway SHALL use that explicit path to spawn the CLI process.
5. IF `BACKEND_MODE` is `acp` AND `kiro-cli` cannot be found or spawned THEN the gateway SHALL fail at startup with a clear error message indicating the CLI is missing.
6. WHEN `BACKEND_MODE` is `acp` THEN credential-related config fields (`REFRESH_TOKEN`, `KIRO_CREDS_FILE`, `KIRO_CLI_DB_FILE`) SHALL be optional (not required for startup validation).

---

### Requirement 2: ACP Process Lifecycle

**User Story:** As a gateway operator, I want the Kiro CLI process to be managed reliably by the gateway, so that it starts up cleanly and is shut down when the gateway exits.

#### Acceptance Criteria

1. WHEN the gateway starts in ACP mode THEN it SHALL spawn `kiro-cli acp` as a subprocess connected via stdin/stdout pipes.
2. WHEN the gateway shuts down THEN it SHALL terminate the `kiro-cli` subprocess gracefully (SIGTERM then SIGKILL after timeout).
3. IF the `kiro-cli` subprocess exits unexpectedly THEN the gateway SHALL log the error and return a 503 response to subsequent requests.
4. WHEN the ACP process is spawned THEN the gateway SHALL perform the JSON-RPC `initialize` handshake before accepting any requests.
5. WHEN `kiro-cli acp --agent <name>` is needed THEN the `ACP_AGENT` config option SHALL allow specifying an agent name to pass to the CLI.

---

### Requirement 3: ACP Session Management

**User Story:** As a user sending requests through the gateway, I want the gateway to manage ACP sessions transparently, so that each request produces correct results without requiring me to manage session state.

#### Acceptance Criteria

1. WHEN a chat completion request arrives THEN the gateway SHALL create a new ACP session via `session/new` for that request.
2. WHEN a session is created THEN the gateway SHALL send the full conversation via `session/prompt`.
3. WHEN the session completes THEN the gateway SHALL not reuse that session for future requests (stateless per request).
4. IF `session/new` fails THEN the gateway SHALL return a 502 error to the client.
5. WHEN multiple concurrent requests arrive THEN each SHALL get its own independent ACP session.

---

### Requirement 4: Streaming Response Support

**User Story:** As a client using streaming mode, I want ACP-backed responses to be streamed in the same SSE format as the HTTP backend, so that I see no difference in behavior.

#### Acceptance Criteria

1. WHEN a client requests streaming (`stream: true`) THEN the gateway SHALL stream `session/update` notifications whose `sessionUpdate` is `agent_message_chunk` as SSE chunks in the appropriate format (OpenAI or Anthropic), extracting text from the nested `content` block.
2. WHEN the `session/prompt` response is received (carrying a `stopReason`) THEN the gateway SHALL finalize the stream and send the `[DONE]` sentinel (OpenAI) or `message_stop` event (Anthropic). The prompt response — not a notification — is the turn-completion signal.
3. WHEN a client requests non-streaming THEN the gateway SHALL collect all `agent_message_chunk` updates until the `session/prompt` response arrives and return a single JSON response.
4. WHEN `session/update` notifications with `sessionUpdate` of `tool_call` or `tool_call_update` arrive THEN the gateway SHALL translate them to the appropriate tool use format for the client.
5. IF the `session/prompt` response returns a JSON-RPC error, OR the `stopReason` indicates an abnormal stop (e.g. `refusal`, `max_tokens`), THEN the gateway SHALL surface it as an appropriate HTTP error or finish reason to the client.

---

### Requirement 5: Model Passthrough in ACP Mode

**User Story:** As a client, I want the model I request to be forwarded to the ACP agent, so that model selection still works when using the ACP backend.

> **Note:** kiro-cli does not implement a `session/set_model` method — sending it crashes the subprocess. kiro-cli exposes model selection only through its `/model <name>` slash command, which is issued as a normal `session/prompt` turn. kiro-cli's model IDs use a dot before the version (e.g. `claude-sonnet-4.6`) while the gateway uses dashes (`claude-sonnet-4-6`).

#### Acceptance Criteria

1. WHEN a client specifies a concrete model in ACP mode THEN the gateway SHALL select it on the session by issuing a `/model <kiro-model-id>` slash command via `session/prompt` before sending the user's prompt.
2. WHEN mapping the requested model to kiro-cli's naming THEN the gateway SHALL convert the gateway model ID to kiro's by joining the final two numeric version segments with a dot (e.g. `claude-sonnet-4-6` → `claude-sonnet-4.6`), leaving single-version and non-numeric names (e.g. `claude-sonnet-4`, `auto`) unchanged.
3. WHEN the requested model is empty or `auto` THEN the gateway SHALL skip model selection and use the session's default model.
4. WHEN the `/model` selection turn reports the model is unavailable (or the command otherwise fails) THEN the gateway SHALL log a warning and proceed with the session's default model rather than failing the request.
5. WHEN the `/v1/models` endpoint is called in ACP mode THEN the gateway SHALL return the same model list as HTTP mode (from cache or fallback), since ACP does not expose a model listing method.

---

### Requirement 6: Observability and Logging

**User Story:** As a gateway operator, I want ACP communication to be visible in logs and debug output, so that I can diagnose problems.

#### Acceptance Criteria

1. WHEN ACP mode is active THEN the startup banner SHALL indicate the backend is ACP and show the resolved `kiro-cli` path.
2. WHEN a JSON-RPC message is sent or received THEN the gateway SHALL log it at DEBUG level (structured, not raw bytes).
3. WHEN `DEBUG_MODE` is `all` THEN the gateway SHALL write ACP request and response payloads to debug log files in the same format used for HTTP debug logs.
4. WHEN the ACP subprocess produces stderr output THEN the gateway SHALL capture it and log it at WARN level.
5. WHEN a request completes in ACP mode THEN the gateway SHALL log the session ID, model, and duration at INFO level.

---

### Requirement 7: ACP v1 Protocol Conformance

**User Story:** As a gateway operator, I want the ACP backend to speak the wire protocol that `kiro-cli` actually implements (Agent Client Protocol v1), so that sessions complete instead of the subprocess exiting silently.

#### Acceptance Criteria

1. WHEN the gateway sends `initialize` THEN `protocolVersion` SHALL be the integer `1` (not a date string) and client capabilities SHALL be sent under the `clientCapabilities` key.
2. WHEN the gateway sends `session/new` THEN it SHALL include both the required `cwd` (absolute path) and `mcpServers` (array, may be empty) fields.
3. WHEN the gateway sends `session/prompt` THEN the content blocks SHALL be carried in the `prompt` field (not `content`).
4. WHEN the gateway receives streaming updates THEN it SHALL listen for the method `session/update` and discriminate updates by the `sessionUpdate` field using snake_case values (`agent_message_chunk`, `tool_call`, `tool_call_update`), reading text from the nested `content` content-block.
5. WHEN `kiro-cli` reports the negotiated protocol version in its `initialize` response THEN the gateway SHALL log it, and IF it is greater than the version the gateway supports THEN the gateway SHALL fail startup with a clear version-mismatch error.

---

### Requirement 8: ACP Session Reuse (Latency)

**User Story:** As a gateway operator, I want the ACP backend to reuse warm `kiro-cli` sessions across requests, so that I avoid the cost of creating a new session (≈3.7s of MCP server initialization) and re-selecting the model on every request.

> **Context:** All ACP sessions live inside the single `kiro-cli` subprocess; "reuse" means reusing a logical session ID, not a process. `session/new` is expensive (it spins up the session's MCP servers); `/model` selection is a full turn; `/clear` is a cheap turn that fully wipes the session's conversation history (verified against kiro-cli 2.8.1). Reuse therefore trades an expensive `session/new` + `/model` for a cheap `/clear`.

#### Acceptance Criteria

1. WHEN the ACP backend completes a request THEN it SHALL return the session to an idle pool for reuse rather than discarding it.
2. WHEN a new request arrives THEN the backend SHALL reuse an idle pooled session whose selected model matches the request's target model, issuing `/clear` to wipe prior context before sending the new prompt.
3. WHEN no idle session matches the target model THEN the backend SHALL create a new session (`session/new` + `/model` as today).
4. WHEN a session is in use for a turn THEN it SHALL be leased exclusively; concurrent requests SHALL NEVER share a session simultaneously.
5. WHEN returning a session to the pool would exceed a configurable maximum number of idle sessions THEN the backend SHALL drop the excess session instead of pooling it.
6. WHEN `/clear` (or the prompt on a reused session) fails or the subprocess has exited THEN the backend SHALL discard that session and fall back to creating a new one, so a poisoned session never breaks a request.
7. WHEN the backend shuts down THEN it SHALL drop all pooled sessions.
8. WHEN session reuse is disabled via configuration (idle pool size of 0) THEN the backend SHALL fall back to the current per-request session behavior (new session each time, no `/clear`).

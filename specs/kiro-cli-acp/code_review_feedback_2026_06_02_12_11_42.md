# Code Review Feedback

## Summary

The implementation is solid and well-structured. The `Backend` interface abstraction is clean, the ACP subprocess lifecycle is handled correctly, and the existing test suite was updated properly. Three blocking issues were found: an import ordering violation that will fail `goimports`/`gofmt` checks, a nil pointer dereference risk when `kiroClient` is nil in HTTP mode, and a missing `io` import that was moved out of order. Several suggestions around error propagation fidelity and test coverage are also noted.

---

## Findings

### `gateway/internal/server/routes_openai.go`

- [x] [BLOCKING] `"io"` import is placed after a non-stdlib import, violating Go import grouping conventions
  - Why: Go requires stdlib imports in their own group before third-party imports. `goimports` will reformat this and could cause CI failures.
  - Fix: Move `"io"` into the stdlib group at the top of the import block, before `backendpkg` and other internal imports.

### `gateway/cmd/gateway/main.go`

- [x] [BLOCKING] `kiroClient` is `nil` when `BackendMode == "acp"`, but `backend.NewHTTPBackend(kiroClient)` in the `else` branch would panic if someone adds a code path that reaches it with `kiroClient == nil`
  - Why: The `var kiroClient client.KiroClient` is only assigned when `BackendMode != "acp"`. The `else` branch calls `backend.NewHTTPBackend(kiroClient)` which stores the nil interface. If `HTTPBackend.Complete` is ever called it will nil-dereference. Currently safe at runtime because the branches are mutually exclusive, but it's a latent footgun.
  - Fix: Initialise `kiroClient` unconditionally, or assert non-nil before constructing `HTTPBackend`. Simplest fix: move `kiroClient := client.NewKiroClient(authMgr, cfg)` out of the `if` block and let it always be constructed (it's cheap). The ACP branch ignores it.

- [x] [SUGGESTION] The `log.Info()` call for ACP startup logs `cfg.KiroCLIPath` which is the configured path, not the resolved path (which may differ when falling back to PATH lookup)
  - Why: If `KIRO_CLI_PATH` is empty and the binary is resolved from PATH, the log shows an empty string.
  - Fix: `NewACPBackend` could return the resolved path, or log it inside `NewACPBackend` itself (which already does `log.Info().Str("cli_path", cliPath).Msg("ACP backend ready")`). The duplicate log in `main.go` at line 110 could simply be removed.

### `gateway/internal/backend/acp.go`

- [x] [SUGGESTION] `Close()` calls `cmd.Wait()` in a goroutine after sending `os.Interrupt`, but `cmd.Wait()` was already called in the subprocess-exit watcher goroutine started at line 96-99. Calling `Wait()` twice on the same `exec.Cmd` returns an error on the second call.
  - Why: `exec.Cmd.Wait()` must only be called once. The second call in `Close()` will return `"wait: no child processes"` but the error is silently discarded (`_ = cmd.Wait()`), so it won't crash — but it's still semantically incorrect and will cause a race on the `done` channel close.
  - Fix: Instead of calling `Wait()` again in `Close()`, select on `b.done` to wait for the subprocess-exit watcher to close it: `select { case <-b.done: case <-time.After(5*time.Second): cmd.Process.Kill() }`.

- [x] [SUGGESTION] `sessionPrompt` sends the full `session/prompt` RPC and waits for a response, but the ACP spec shows `session/prompt` is a fire-and-forget request with no response — the agent streams notifications back asynchronously. If `kiro-cli` never responds to `session/prompt`, `Complete()` will block until context cancellation.
  - Why: Looking at the ACP spec: `session/prompt` triggers streaming `session/notification` messages. The RPC response may be an immediate ack or may not come until `TurnEnd`. This may work correctly but should be verified against actual `kiro-cli` behaviour.
  - Fix: No code change needed now, but add a note in the code documenting this assumption so it's easy to fix if `kiro-cli` doesn't send an ack.

- [x] [NIT] `extractPromptText` returns `""` when no user message is found. The caller (`sessionPrompt`) sends an empty `text` field to `session/prompt`. This is likely harmless but could confuse the CLI.
  - Fix: Consider logging a warning when `extractPromptText` returns empty.

### `gateway/internal/backend/acp_test.go`

- [x] [SUGGESTION] The fake kiro-cli tests (`TestACPBackend_*`) are referenced in tasks.md but the test file only tests helper functions (`extractPromptText`, `resolveKiroCLI`). The full subprocess lifecycle tests (startup, initialize handshake, session flow, concurrent sessions) are not present.
  - Why: Req 2.1–2.5 and Req 3.5 require these to be covered. The `acp_test.go` in the diff doesn't include a fake subprocess helper.
  - Fix: Add integration-style tests using `TestMain` or a helper that spawns a small Go binary (or uses `os.Pipe`) to simulate kiro-cli ACP stdio. This is non-trivial but important for the most critical code path.

### `gateway/internal/backend/http.go`

- [x] [NIT] The goroutine that wraps the event channel (`go func() { defer resp.Body.Close(); ... }`) adds a 64-element buffer. The underlying `ParseKiroStream` channel is also buffered. This double-buffering is harmless but slightly wasteful.
  - Fix: Pass `resp.Body` directly to `ParseKiroStream` and handle body close via `defer` in the goroutine — no wrapper channel needed. Or keep as-is; it's not a correctness issue.

### `gateway/internal/server/routes_openai.go` and `routes_anthropic.go`

- [x] [SUGGESTION] The previous code forwarded the upstream HTTP status code (400, 429, 500) back to the client. The new backend abstraction always returns 502. Clients like Claude Code that inspect error codes for rate-limit detection (429) will no longer receive the correct signal.
  - Why: This is a behaviour change introduced by the abstraction. Req 4.5 says "surface it as an appropriate HTTP error" which implies preserving the upstream code.
  - Fix: Consider adding an `HTTPError` type to the backend package that wraps the status code, so handlers can inspect it: `var httpErr *backend.HTTPError; if errors.As(err, &httpErr) { w.WriteHeader(httpErr.StatusCode) }`. The `HTTPBackend` would return this for non-200 responses.

### `gateway/internal/server/routes_openai_test.go`

- [x] [NIT] `mockStreamingClient` is defined in `routes_openai_test.go` but used in `routes_anthropic_test.go` (same `server_test` package). This is fine in Go but worth a comment noting the shared type.

---

## Positive observations

- The `Backend` interface is minimal and well-scoped — `Complete` + `Close` is exactly the right surface area.
- The JSON-RPC transport layer is clean and well-tested with round-trip pipe tests.
- ACP session lifecycle (new → set_model → prompt → notifications → TurnEnd) maps directly to the spec.
- Config validation bypass for ACP mode is correctly scoped — existing HTTP mode users see zero behaviour change.
- The `sync.Map` for `pending` and `notifs` correctly handles concurrent sessions without explicit locking.
- `extractPromptText` walking backwards to find the last user message is the right approach.

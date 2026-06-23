# Code Review Feedback

## Summary

The change correctly realigns the ACP backend with Agent Client Protocol v1 and directly addresses the root cause of issue #21 (integer `protocolVersion`, required `mcpServers`, `prompt` field, `session/update` method, `sessionUpdate` discriminator, and `stopReason`-driven turn completion). The new concurrent prompt/drain model is sound and race-clean. Findings are limited to error-visibility on the non-streaming path and some missing test coverage — no blockers to the happy-path fix.

## Findings

### gateway/internal/backend/acp.go

- [ ] [SUGGESTION] Non-streaming `session/prompt` errors are no longer surfaced as an HTTP error
  - Why: Previously a `session/prompt` failure returned synchronously from `Complete`, so the handler produced a 502. In the new async model the failure is emitted as an `EventTypeError` event. Streaming consumers (`StreamToOpenAI`/`StreamToAnthropic`) surface it inline, but `CollectFullResponse` (non-streaming) swallows `EventTypeError` and returns the partial result — a client would get a 200 with empty/partial content instead of an error. This is a gap against Requirement 4.5 for the non-streaming path.
  - Fix: Either have `CollectFullResponse` carry the error out to the caller (broader change to shared streaming code), or have the non-streaming server handler check for an error sentinel before writing a 200. At minimum, document the limitation. Worth a short follow-up since it touches shared code beyond this spec.
  - References: Requirement 4.5
  - Status: Deferred to a follow-up by agreement — out of scope for the issue #21 fix since a clean fix touches shared streaming code (`CollectFullResponse`) used by the HTTP backend. Streaming mode (the primary ACP path) already surfaces the error.

- [ ] [SUGGESTION] Context-cancellation can race into the error path instead of a clean close
  - Why: When `ctx` is cancelled, both `case <-ctx.Done()` and `case res = <-promptDone` (with `res.err == context.Canceled`) can become ready. If the select picks `promptDone`, the client receives a `[Gateway error: context canceled]` event rather than the silent `sessionCancel` + clean close the `ctx.Done()` branch provides. Rare and racy, but the two cancellation outcomes are inconsistent.
  - Fix: After `break loop`, treat `errors.Is(res.err, context.Canceled)`/`ctx.Err() != nil` as a clean termination (skip the error event), or check `ctx.Err()` before emitting the prompt error.

- [ ] [NIT] `tool_call` updates are surfaced with `Arguments: "{}"`
  - Why: The ACP `tool_call` notification carries `toolCallId`, `title`, `kind`, and `status` but not the input, so tool calls reach the client name-only with empty arguments. Acceptable as best-effort, but downstream tool-use consumers get no parameters.
  - Fix: None required now. If full tool arguments are needed, they arrive via `tool_call_update`/`rawInput` in the ACP schema and would need accumulation. Note it as a known limitation.

### gateway/internal/backend/acp_test.go

- [x] [SUGGESTION] No `Complete`-level coverage for tool calls, abnormal stop reasons, or the prompt-error path
  - Why: The fake CLI only exercises two `agent_message_chunk` updates followed by `end_turn`. The new branches — `tool_call` → `EventTypeToolCall`, a `session/prompt` JSON-RPC error → `EventTypeError`, and a non-`end_turn` stopReason — are untested end-to-end. A mutation flipping the tool-call status check or dropping the error emit would not be caught.
  - Fix: Add a table-driven or extra fake-CLI mode that (a) emits a `tool_call` update and asserts an `EventTypeToolCall` with the expected `Name`/`ID`, and (b) returns a JSON-RPC error for `session/prompt` and asserts an `EventTypeError` reaches the channel.
  - Resolved: Made the fake CLI `FAKE_KIRO_MODE`-aware and added `TestACPBackend_Complete_EmitsToolCall` (asserts `Name`/`ID`) and `TestACPBackend_Complete_PromptErrorEmitsErrorEvent` (asserts an `EventTypeError` reaches the channel). Both pass under `-race`.

- [x] [SUGGESTION] No test for the protocol-version mismatch fail-fast (Req 7.5)
  - Why: `initialize` now fails startup when the agent negotiates a version greater than `acpProtocolVersion`, but nothing exercises it. This is a deliberate new guard worth locking in.
  - Fix: Add a fake-CLI variant that responds to `initialize` with `protocolVersion: 2` and assert `initialize()`/`NewACPBackend` returns an error.
  - Resolved: Added `TestACPBackend_VersionMismatch_FailsStartup` via a new `startFakeACPBackend(t, mode)` helper that surfaces the handshake error; asserts the error mentions the protocol version.

- [ ] [NIT] `stop_reason` is never asserted end-to-end
  - Why: `Complete` now threads `stopReason` through to the completion log, but no test confirms the `end_turn` response actually drives the clean close (the test would pass even if completion were triggered some other way).
  - Fix: Optional — the existing close test is adequate; an explicit stopReason assertion would strengthen it.

## Positive observations

- The async prompt/drain split is the correct fix and the rationale is well documented in comments (notably why `notifCh` is no longer closed — this removes the pre-existing send-on-closed-channel race rather than just papering over it).
- The post-turn non-blocking drain correctly relies on `dispatchLoop` ordering (updates enqueued before the prompt response), so no trailing updates are lost.
- `ParseUpdate` returning `(nil, nil)` for unhandled variants (e.g. `agent_thought_chunk`, `plan`) is the right call for forward-compatibility and is covered by a test.
- The negotiated-version log + fail-fast is a thoughtful addition beyond the minimum and gives operators a clear upgrade signal.
- Verified locally: `go build`, `go vet`, full `go test ./...`, and `-race -count=3` on the backend package all pass.

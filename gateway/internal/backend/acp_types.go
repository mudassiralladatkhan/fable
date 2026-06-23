// Package backend — ACP session update types (Agent Client Protocol v1).
//
// These structs represent the update payloads delivered inside session/update
// notifications from kiro-cli. Updates are discriminated by the "sessionUpdate"
// field (snake_case), and content is carried in nested content blocks.
// ParseUpdate decodes the discriminator and returns the concrete type.
package backend

import (
	"encoding/json"
	"fmt"
)

// ---------------------------------------------------------------------------
// Content block
// ---------------------------------------------------------------------------

// ContentBlock is an ACP content block (text/image/resource). The gateway only
// consumes the text variant.
type ContentBlock struct {
	Type string `json:"type"` // "text" | "image" | "resource" | ...
	Text string `json:"text"`
}

// ---------------------------------------------------------------------------
// Notification update types
// ---------------------------------------------------------------------------

// AgentMessageChunk is a streaming text chunk from the agent.
// sessionUpdate == "agent_message_chunk".
type AgentMessageChunk struct {
	SessionUpdate string       `json:"sessionUpdate"`
	Content       ContentBlock `json:"content"`
	MessageID     string       `json:"messageId,omitempty"`
}

// ToolCallNotification signals a new tool invocation.
// sessionUpdate == "tool_call".
type ToolCallNotification struct {
	SessionUpdate string `json:"sessionUpdate"`
	ToolCallID    string `json:"toolCallId"`
	Title         string `json:"title"`
	Kind          string `json:"kind"`
	Status        string `json:"status"` // pending | in_progress | completed | failed
}

// ToolCallUpdate carries a progress update for an existing tool call.
// sessionUpdate == "tool_call_update".
type ToolCallUpdate struct {
	SessionUpdate string `json:"sessionUpdate"`
	ToolCallID    string `json:"toolCallId"`
	Status        string `json:"status"`
}

// SessionNotification is the params payload of a session/update message.
type SessionNotification struct {
	SessionID string          `json:"sessionId"`
	Update    json.RawMessage `json:"update"`
}

// PromptResponse is the result of a session/prompt request. Its stopReason —
// not a notification — is the turn-completion signal in ACP.
type PromptResponse struct {
	StopReason string `json:"stopReason"` // end_turn | max_tokens | max_turn_requests | refusal | cancelled
}

// ---------------------------------------------------------------------------
// ParseUpdate
// ---------------------------------------------------------------------------

// updateDiscriminator peeks at the "sessionUpdate" field before full decode.
type updateDiscriminator struct {
	SessionUpdate string `json:"sessionUpdate"`
}

// ParseUpdate decodes the "sessionUpdate" discriminator in raw and returns the
// concrete update type. Unknown or unhandled variants (e.g. agent_thought_chunk,
// plan, usage_update) return (nil, nil) so the stream skips them gracefully
// rather than erroring.
func ParseUpdate(raw json.RawMessage) (any, error) {
	var d updateDiscriminator
	if err := json.Unmarshal(raw, &d); err != nil {
		return nil, fmt.Errorf("acp: decode update discriminator: %w", err)
	}

	switch d.SessionUpdate {
	case "agent_message_chunk":
		var v AgentMessageChunk
		if err := json.Unmarshal(raw, &v); err != nil {
			return nil, fmt.Errorf("acp: decode agent_message_chunk: %w", err)
		}
		return &v, nil

	case "tool_call":
		var v ToolCallNotification
		if err := json.Unmarshal(raw, &v); err != nil {
			return nil, fmt.Errorf("acp: decode tool_call: %w", err)
		}
		return &v, nil

	case "tool_call_update":
		var v ToolCallUpdate
		if err := json.Unmarshal(raw, &v); err != nil {
			return nil, fmt.Errorf("acp: decode tool_call_update: %w", err)
		}
		return &v, nil

	default:
		// Unhandled variant — skip without error.
		return nil, nil
	}
}

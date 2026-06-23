// Package backend — tests for ACP message types and ParseUpdate.
package backend

import (
	"encoding/json"
	"testing"
)

func TestParseUpdate_AgentMessageChunk(t *testing.T) {
	raw := json.RawMessage(`{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"hello world"}}`)
	v, err := ParseUpdate(raw)
	if err != nil {
		t.Fatalf("ParseUpdate error: %v", err)
	}
	chunk, ok := v.(*AgentMessageChunk)
	if !ok {
		t.Fatalf("expected *AgentMessageChunk, got %T", v)
	}
	if chunk.Content.Text != "hello world" {
		t.Errorf("Content.Text = %q, want %q", chunk.Content.Text, "hello world")
	}
}

func TestParseUpdate_ToolCall(t *testing.T) {
	raw := json.RawMessage(`{"sessionUpdate":"tool_call","toolCallId":"call_1","title":"read_file","kind":"read","status":"in_progress"}`)
	v, err := ParseUpdate(raw)
	if err != nil {
		t.Fatalf("ParseUpdate error: %v", err)
	}
	tc, ok := v.(*ToolCallNotification)
	if !ok {
		t.Fatalf("expected *ToolCallNotification, got %T", v)
	}
	if tc.Title != "read_file" {
		t.Errorf("Title = %q, want %q", tc.Title, "read_file")
	}
	if tc.ToolCallID != "call_1" {
		t.Errorf("ToolCallID = %q, want %q", tc.ToolCallID, "call_1")
	}
	if tc.Status != "in_progress" {
		t.Errorf("Status = %q, want %q", tc.Status, "in_progress")
	}
}

func TestParseUpdate_ToolCallUpdate(t *testing.T) {
	raw := json.RawMessage(`{"sessionUpdate":"tool_call_update","toolCallId":"call_1","status":"completed"}`)
	v, err := ParseUpdate(raw)
	if err != nil {
		t.Fatalf("ParseUpdate error: %v", err)
	}
	tcu, ok := v.(*ToolCallUpdate)
	if !ok {
		t.Fatalf("expected *ToolCallUpdate, got %T", v)
	}
	if tcu.Status != "completed" {
		t.Errorf("Status = %q, want %q", tcu.Status, "completed")
	}
}

// Unhandled variants (e.g. agent_thought_chunk, plan) are skipped, not errored,
// so future ACP additions don't break the stream.
func TestParseUpdate_UnhandledVariantSkipped(t *testing.T) {
	raw := json.RawMessage(`{"sessionUpdate":"agent_thought_chunk","content":{"type":"text","text":"thinking"}}`)
	v, err := ParseUpdate(raw)
	if err != nil {
		t.Fatalf("ParseUpdate error: %v", err)
	}
	if v != nil {
		t.Errorf("expected nil for unhandled variant, got %T", v)
	}
}

func TestParseUpdate_InvalidJSON(t *testing.T) {
	raw := json.RawMessage(`{not valid}`)
	_, err := ParseUpdate(raw)
	if err == nil {
		t.Error("expected error for invalid JSON, got nil")
	}
}

func TestParseUpdate_MissingDiscriminatorSkipped(t *testing.T) {
	raw := json.RawMessage(`{"content":{"type":"text","text":"hello"}}`)
	v, err := ParseUpdate(raw)
	if err != nil {
		t.Fatalf("ParseUpdate error: %v", err)
	}
	if v != nil {
		t.Errorf("expected nil when discriminator is missing, got %T", v)
	}
}

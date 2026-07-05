// Package server — Vercel AI Gateway integration.
package server

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/chasedputnam/go-kiro-gateway/gateway/internal/client"
	gwerrors "github.com/chasedputnam/go-kiro-gateway/gateway/internal/errors"
	"github.com/chasedputnam/go-kiro-gateway/gateway/internal/models"
	"github.com/chasedputnam/go-kiro-gateway/gateway/internal/streaming"
)

// handleVercelMessages handles direct Anthropic-formatted requests proxying to Vercel AI Gateway.
func (s *Server) handleVercelMessages(
	w http.ResponseWriter,
	r *http.Request,
	req models.AnthropicMessagesRequest,
	modelID string,
	start time.Time,
) {
	ctx := r.Context()

	// Convert AnthropicMessagesRequest to OpenAI ChatCompletionRequest
	openAIMessages := []models.ChatMessage{}

	// If there's a system prompt, add it as a system message.
	systemPrompt := ""
	if req.System != nil {
		if s, ok := req.System.(string); ok {
			systemPrompt = s
		} else if blocks, ok := req.System.([]any); ok {
			var parts []string
			for _, item := range blocks {
				block, ok := item.(map[string]any)
				if !ok {
					continue
				}
				if block["type"] == "text" {
					if text, ok := block["text"].(string); ok {
						parts = append(parts, text)
					}
				}
			}
			systemPrompt = strings.Join(parts, "\n")
		}
	}
	if systemPrompt != "" {
		openAIMessages = append(openAIMessages, models.ChatMessage{
			Role:    "system",
			Content: systemPrompt,
		})
	}

	// Add other messages.
	for _, msg := range req.Messages {
		var contentStr string
		if s, ok := msg.Content.(string); ok {
			contentStr = s
		} else if blocks, ok := msg.Content.([]any); ok {
			var parts []string
			for _, item := range blocks {
				block, ok := item.(map[string]any)
				if !ok {
					continue
				}
				blockType, _ := block["type"].(string)
				if blockType == "text" {
					if text, ok := block["text"].(string); ok {
						parts = append(parts, text)
					}
				}
			}
			contentStr = strings.Join(parts, "")
		}

		openAIMessages = append(openAIMessages, models.ChatMessage{
			Role:    msg.Role,
			Content: contentStr,
		})
	}

	var maxToks *int
	if req.MaxTokens > 0 {
		mt := req.MaxTokens
		maxToks = &mt
	}

	openAIReq := models.ChatCompletionRequest{
		Model:       modelID,
		Messages:    openAIMessages,
		Stream:      req.Stream,
		Temperature: req.Temperature,
		TopP:        req.TopP,
		MaxTokens:   maxToks,
	}

	reqBody, err := json.Marshal(openAIReq)
	if err != nil {
		log.Error().Err(err).Msg("Vercel: failed to marshal request")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		w.Write(gwerrors.AnthropicErrorResponse("Failed to marshal request", "api_error"))
		return
	}

	vercelURL := s.config.VercelURL
	if vercelURL == "" {
		vercelURL = "https://ai-gateway.vercel.sh/v1/chat/completions"
	} else if !strings.HasSuffix(vercelURL, "/chat/completions") {
		if strings.HasSuffix(vercelURL, "/") {
			vercelURL += "chat/completions"
		} else {
			vercelURL += "/chat/completions"
		}
	}

	httpReq, err := http.NewRequestWithContext(ctx, "POST", vercelURL, bytes.NewReader(reqBody))
	if err != nil {
		log.Error().Err(err).Msg("Vercel: failed to create request")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		w.Write(gwerrors.AnthropicErrorResponse("Failed to create request", "api_error"))
		return
	}

	apiKey := s.config.VercelAPIKey
	if apiKey != "" {
		httpReq.Header.Set("Authorization", "Bearer "+apiKey)
		httpReq.Header.Set("x-api-key", apiKey)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	// Respect proxy/VPN config.
	transport := client.BuildTransport(s.config)
	httpClient := &http.Client{
		Transport: transport,
	}

	log.Info().
		Str("url", vercelURL).
		Str("model", modelID).
		Bool("stream", req.Stream).
		Msg("Forwarding request to Vercel AI Gateway (OpenAI Chat Completions)")

	resp, err := httpClient.Do(httpReq)
	if err != nil {
		log.Error().Err(err).Msg("Vercel API request failed")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadGateway)
		w.Write(gwerrors.AnthropicErrorResponse(err.Error(), "api_error"))
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		// Forward error response directly
		for k, vv := range resp.Header {
			for _, v := range vv {
				w.Header().Add(k, v)
			}
		}
		w.WriteHeader(resp.StatusCode)
		_, _ = io.Copy(w, resp.Body)
		return
	}

	if req.Stream {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		w.WriteHeader(http.StatusOK)

		flusher, ok := w.(http.Flusher)
		if ok {
			flusher.Flush()
		}

		messageID := streaming.GenerateMessageID()
		// Emit message_start
		writeAnthropicSSE(w, flusher, "message_start", map[string]any{
			"type": "message_start",
			"message": map[string]any{
				"id":            messageID,
				"type":          "message",
				"role":          "assistant",
				"content":       []any{},
				"model":         modelID,
				"stop_reason":   nil,
				"stop_sequence": nil,
				"usage": map[string]any{
					"input_tokens":  0,
					"output_tokens": 0,
				},
			},
		})

		// Emit content_block_start
		writeAnthropicSSE(w, flusher, "content_block_start", map[string]any{
			"type":  "content_block_start",
			"index": 0,
			"content_block": map[string]any{
				"type": "text",
				"text": "",
			},
		})

		scanner := bufio.NewScanner(resp.Body)
		var lastFinishReason *string
		completionTokens := 0

		type OpenAIChunk struct {
			ID      string `json:"id"`
			Model   string `json:"model"`
			Choices []struct {
				Delta struct {
					Content string `json:"content"`
				} `json:"delta"`
				FinishReason *string `json:"finish_reason"`
			} `json:"choices"`
			Usage *struct {
				PromptTokens     int `json:"prompt_tokens"`
				CompletionTokens int `json:"completion_tokens"`
			} `json:"usage"`
		}

		for scanner.Scan() {
			line := scanner.Text()
			if !strings.HasPrefix(line, "data: ") {
				continue
			}
			data := strings.TrimPrefix(line, "data: ")
			data = strings.TrimSpace(data)
			if data == "[DONE]" {
				break
			}

			var chunk OpenAIChunk
			if err := json.Unmarshal([]byte(data), &chunk); err != nil {
				continue
			}

			if len(chunk.Choices) > 0 {
				choice := chunk.Choices[0]
				if choice.Delta.Content != "" {
					writeAnthropicSSE(w, flusher, "content_block_delta", map[string]any{
						"type":  "content_block_delta",
						"index": 0,
						"delta": map[string]any{
							"type": "text_delta",
							"text": choice.Delta.Content,
						},
					})
				}
				if choice.FinishReason != nil {
					lastFinishReason = choice.FinishReason
				}
			}

			if chunk.Usage != nil {
				completionTokens = chunk.Usage.CompletionTokens
			}
		}

		// Emit content_block_stop
		writeAnthropicSSE(w, flusher, "content_block_stop", map[string]any{
			"type":  "content_block_stop",
			"index": 0,
		})

		stopReason := "end_turn"
		if lastFinishReason != nil {
			if *lastFinishReason == "length" {
				stopReason = "max_tokens"
			}
		}

		// Emit message_delta
		writeAnthropicSSE(w, flusher, "message_delta", map[string]any{
			"type": "message_delta",
			"delta": map[string]any{
				"stop_reason":   stopReason,
				"stop_sequence": nil,
			},
			"usage": map[string]any{
				"output_tokens": completionTokens,
			},
		})

		// Emit message_stop
		writeAnthropicSSE(w, flusher, "message_stop", map[string]any{
			"type": "message_stop",
		})

	} else {
		// Non-streaming response conversion
		respBody, err := io.ReadAll(resp.Body)
		if err != nil {
			log.Error().Err(err).Msg("Vercel: failed to read response body")
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadGateway)
			w.Write(gwerrors.AnthropicErrorResponse("Failed to read response body from upstream", "api_error"))
			return
		}

		var openAIResp models.ChatCompletionResponse
		if err := json.Unmarshal(respBody, &openAIResp); err != nil {
			log.Error().Err(err).Msg("Vercel: failed to unmarshal response body")
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadGateway)
			w.Write(gwerrors.AnthropicErrorResponse("Failed to parse response from upstream", "api_error"))
			return
		}

		contentBlocks := []map[string]any{}
		responseText := ""
		if len(openAIResp.Choices) > 0 {
			if contentVal, ok := openAIResp.Choices[0].Message["content"]; ok {
				if s, ok := contentVal.(string); ok {
					responseText = s
				}
			}
		}
		contentBlocks = append(contentBlocks, map[string]any{
			"type": "text",
			"text": responseText,
		})

		stopReason := "end_turn"
		if len(openAIResp.Choices) > 0 && openAIResp.Choices[0].FinishReason != nil {
			fr := *openAIResp.Choices[0].FinishReason
			if fr == "length" {
				stopReason = "max_tokens"
			} else if fr == "tool_calls" {
				stopReason = "tool_use"
			}
		}

		anthropicResp := models.AnthropicMessagesResponse{
			ID:         "msg_" + openAIResp.ID,
			Type:       "message",
			Role:       "assistant",
			Content:    contentBlocks,
			Model:      openAIResp.Model,
			StopReason: &stopReason,
			Usage: models.AnthropicUsage{
				InputTokens:  openAIResp.Usage.PromptTokens,
				OutputTokens: openAIResp.Usage.CompletionTokens,
			},
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(anthropicResp)
	}

	duration := time.Since(start)
	log.Info().
		Int("status", resp.StatusCode).
		Str("method", "POST").
		Str("path", "/v1/messages").
		Dur("duration", duration).
		Msg("HTTP response forwarded from Vercel - completed")
}

func writeAnthropicSSE(w http.ResponseWriter, flusher http.Flusher, eventType string, data map[string]any) {
	jsonData, err := json.Marshal(data)
	if err != nil {
		log.Error().Err(err).Str("event_type", eventType).Msg("Failed to marshal Anthropic SSE event")
		return
	}
	w.Write([]byte(fmt.Sprintf("event: %s\ndata: %s\n\n", eventType, jsonData)))
	flusher.Flush()
}

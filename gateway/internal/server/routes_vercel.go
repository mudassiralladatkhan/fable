// Package server — Vercel AI Gateway integration.
package server

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/chasedputnam/go-kiro-gateway/gateway/internal/client"
	gwerrors "github.com/chasedputnam/go-kiro-gateway/gateway/internal/errors"
	"github.com/chasedputnam/go-kiro-gateway/gateway/internal/models"
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

	// Swap model with resolved model.
	req.Model = modelID

	reqBody, err := json.Marshal(req)
	if err != nil {
		log.Error().Err(err).Msg("Vercel: failed to marshal request")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		w.Write(gwerrors.AnthropicErrorResponse("Failed to marshal request", "api_error"))
		return
	}

	vercelURL := s.config.VercelURL
	if vercelURL == "" {
		vercelURL = "https://api.v0.dev/v1/messages"
	} else if !strings.HasSuffix(vercelURL, "/messages") {
		if strings.HasSuffix(vercelURL, "/") {
			vercelURL += "messages"
		} else {
			vercelURL += "/messages"
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
	httpReq.Header.Set("anthropic-version", "2023-06-01")

	// Respect proxy/VPN config.
	transport := client.BuildTransport(s.config)
	httpClient := &http.Client{
		Transport: transport,
	}

	log.Info().
		Str("url", vercelURL).
		Str("model", modelID).
		Bool("stream", req.Stream).
		Msg("Forwarding request to Vercel AI Gateway")

	resp, err := httpClient.Do(httpReq)
	if err != nil {
		log.Error().Err(err).Msg("Vercel API request failed")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadGateway)
		w.Write(gwerrors.AnthropicErrorResponse(err.Error(), "api_error"))
		return
	}
	defer resp.Body.Close()

	// Copy headers.
	for k, vv := range resp.Header {
		for _, v := range vv {
			w.Header().Add(k, v)
		}
	}
	w.WriteHeader(resp.StatusCode)

	if req.Stream {
		flusher, ok := w.(http.Flusher)
		if ok {
			flusher.Flush()
		}
		_, _ = io.Copy(w, resp.Body)
	} else {
		_, _ = io.Copy(w, resp.Body)
	}

	duration := time.Since(start)
	log.Info().
		Int("status", resp.StatusCode).
		Str("method", "POST").
		Str("path", "/v1/messages").
		Dur("duration", duration).
		Msg("HTTP response forwarded from Vercel - completed")
}

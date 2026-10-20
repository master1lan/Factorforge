package modelaccess

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/AceNanako0721/Factorforge/src/factorforge/applications/soxl_jev/config"
	d "github.com/AceNanako0721/Factorforge/src/factorforge/applications/soxl_jev/domain"
)

// chatGenerate calls one OpenAI-compatible chat completion endpoint. The
// operator names the endpoint and the exact model; model output is still
// parsed by the same closed candidate compiler as every other channel.
func (c Client) chatGenerate(parent context.Context, s config.SemanticSettings, id, input string) (d.SemanticGeneration, error) {
	var result d.SemanticGeneration
	if s.APIBaseURL == "" || s.APIKey == "" || s.PromptFile == "" || s.Model == "" || s.MaxOutputBytes <= 0 || s.TimeoutSeconds <= 0 {
		return result, d.Fail("SEMANTIC_CONFIG_REQUIRED", 503)
	}
	instructions, err := os.ReadFile(s.PromptFile)
	if err != nil || len(instructions) == 0 || len(instructions) > s.MaxInputBytes {
		return result, d.Fail("PRIVATE_PROMPT_INVALID", 503)
	}
	var prompt struct {
		Instructions string `json:"instructions"`
	}
	if json.Unmarshal(instructions, &prompt) != nil || strings.TrimSpace(prompt.Instructions) == "" {
		return result, d.Fail("PRIVATE_PROMPT_INVALID", 503)
	}
	body, err := json.Marshal(struct {
		Model          string `json:"model"`
		Temperature    int    `json:"temperature"`
		Stream         bool   `json:"stream"`
		MaxTokens      int    `json:"max_tokens"`
		ResponseFormat struct {
			Type string `json:"type"`
		} `json:"response_format"`
		Messages []struct {
			Role    string `json:"role"`
			Content string `json:"content"`
		} `json:"messages"`
	}{
		Model: s.Model, Temperature: 0, Stream: false, MaxTokens: maxTokens(s.MaxOutputBytes),
		ResponseFormat: struct {
			Type string `json:"type"`
		}{Type: "json_object"},
		Messages: []struct {
			Role    string `json:"role"`
			Content string `json:"content"`
		}{
			{Role: "system", Content: prompt.Instructions + "\n\nRespond with one JSON object only; no prose or code fences."},
			{Role: "user", Content: input},
		},
	})
	if err != nil || len(body) > s.MaxInputBytes {
		return result, d.Fail("SEMANTIC_BUDGET_EXCEEDED", 422)
	}
	// Two attempts at most: a 200 whose body is not one JSON object is a
	// provider-side formatting slip, not a delivery with unknown outcome.
	var content *string
	for attempt := 0; attempt < 2 && content == nil; attempt++ {
		value, err := c.chatAttempt(parent, s, body)
		if err == nil {
			content = value
			break
		}
		if attempt == 1 || !retryable(err) {
			return result, err
		}
	}
	if content == nil {
		return result, d.Fail("PROTOCOL_INVALID", 503)
	}
	digest := sha256.Sum256([]byte(prompt.Instructions))
	return d.SemanticGeneration{
		Provider:    s.Provider,
		AccountID:   s.AccountID,
		Model:       s.Model,
		PromptHash:  hex.EncodeToString(digest[:]),
		Text:        *content,
		CompletedAt: time.Now().UTC(),
	}, nil
}

// chatAttempt performs exactly one HTTP exchange. It never retries internally.
func (c Client) chatAttempt(parent context.Context, s config.SemanticSettings, body []byte) (*string, error) {
	ctx, cancel := context.WithTimeout(parent, time.Duration(s.TimeoutSeconds)*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.APIBaseURL+"/chat/completions", bytes.NewReader(body))
	if err != nil {
		return nil, d.Fail("SEMANTIC_INPUT_INVALID", 422)
	}
	req.Header.Set("Authorization", "Bearer "+s.APIKey)
	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{Timeout: time.Duration(s.TimeoutSeconds) * time.Second}
	response, err := client.Do(req)
	if err != nil {
		return nil, d.Fail("DELIVERY_UNKNOWN", 503)
	}
	defer response.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(response.Body, int64(s.MaxOutputBytes)+1))
	if err != nil || len(raw) > s.MaxOutputBytes {
		return nil, d.Fail("PROTOCOL_INVALID", 503)
	}
	if response.StatusCode != http.StatusOK {
		if response.StatusCode == http.StatusTooManyRequests {
			return nil, d.Fail("RATE_LIMITED", 429)
		}
		return nil, d.Fail("MODEL_ACCESS_FAILED", 503)
	}
	var reply struct {
		Model   string `json:"model"`
		Choices []struct {
			Message struct {
				Content *string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
	}
	// The provider emits fields this client does not model (reasoning content,
	// logprobs, request ids). Only the bounded JSON content below is admitted.
	if json.Unmarshal(raw, &reply) != nil || len(reply.Choices) != 1 || reply.Choices[0].Message.Content == nil {
		return nil, d.Fail("PROTOCOL_INVALID", 503)
	}
	if reply.Model != "" && reply.Model != s.Model {
		return nil, d.Fail("PROTOCOL_INVALID", 503)
	}
	value := *reply.Choices[0].Message.Content
	if strings.TrimSpace(value) == "" || !json.Valid([]byte(value)) {
		return nil, d.Fail("SEMANTIC_OUTPUT_TRUNCATED", 503)
	}
	return &value, nil
}

// retryable distinguishes a provider formatting slip or transient outage from
// a rejected request. Repeating a rejected request cannot help.
func retryable(err error) bool {
	var known *d.Error
	if !errors.As(err, &known) {
		return false
	}
	return known.Code == "SEMANTIC_OUTPUT_TRUNCATED" || known.Code == "PROTOCOL_INVALID" || known.Code == "MODEL_ACCESS_FAILED"
}

// maxTokens keeps generation short enough that a provider's reasoning budget
// cannot consume the whole response allowance.
func maxTokens(outputBytes int) int {
	tokens := outputBytes / 8
	if tokens < 512 {
		return 512
	}
	if tokens > 8000 {
		return 8000
	}
	return tokens
}

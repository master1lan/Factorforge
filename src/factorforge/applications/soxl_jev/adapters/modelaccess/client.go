// Package modelaccess uses only the model application's public JSONL protocol.
package modelaccess

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"github.com/AceNanako0721/Factorforge/src/factorforge/applications/soxl_jev/config"
	d "github.com/AceNanako0721/Factorforge/src/factorforge/applications/soxl_jev/domain"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"
)

type Client struct {
	Root, Config string
	Settings     config.SemanticSettings
}
type boundedOutput struct {
	bytes.Buffer
	limit int
}

func (b *boundedOutput) Write(p []byte) (int, error) {
	if len(p) > b.limit-b.Len() {
		return 0, errors.New("protocol byte limit")
	}
	return b.Buffer.Write(p)
}

// Generate sends exactly once. Startup/stream/cancellation failures never retry.
func (c Client) Generate(parent context.Context, id, input string) (d.SemanticGeneration, error) {
	var result d.SemanticGeneration
	s := c.Settings
	if s.MaxLineBytes == int(^uint(0)>>1) || s.TimeoutSeconds > int64((1<<63-1)/int64(time.Second)) || s.CleanupSeconds > int64((1<<63-1)/int64(time.Second)) {
		return result, d.Fail("SEMANTIC_INPUT_INVALID", 422)
	}
	if !s.Enabled || !regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`).MatchString(id) || strings.TrimSpace(input) == "" || !utf8.ValidString(input) || len(input) > s.MaxInputBytes || s.MaxOutputBytes <= 0 || s.TimeoutSeconds <= 0 {
		return result, d.Fail("SEMANTIC_INPUT_INVALID", 422)
	}
	if s.Channel == "chat" {
		return c.chatGenerate(parent, s, id, input)
	}
	if s.MaxLineBytes <= 0 || s.CleanupSeconds <= 0 {
		return result, d.Fail("SEMANTIC_INPUT_INVALID", 422)
	}
	request := struct {
		V         int    `json:"v"`
		ID        string `json:"id"`
		Op        string `json:"op"`
		Provider  string `json:"provider"`
		AccountID int64  `json:"account_id"`
		Model     string `json:"model"`
		Input     string `json:"input"`
	}{2, id, "generate", s.Provider, s.AccountID, s.Model, input}
	line, err := json.Marshal(request)
	if err != nil || len(line) > s.MaxLineBytes {
		return result, d.Fail("SEMANTIC_BUDGET_EXCEEDED", 422)
	}
	ctx, cancel := context.WithTimeout(parent, time.Duration(s.TimeoutSeconds)*time.Second)
	defer cancel()
	if ctx.Err() != nil {
		return result, d.Fail("MODEL_ACCESS_FAILED", 503)
	}
	cmd := exec.CommandContext(ctx, s.BunPath, filepath.Join(c.Root, "runtime", "model-access-build", "entrypoints", "omp-cli.js"), "serve", "--config", c.Config)
	cmd.Dir = c.Root
	cmd.Stdin = bytes.NewReader(append(line, '\n'))
	output := &boundedOutput{limit: s.MaxLineBytes + 1}
	cmd.Stdout, cmd.Stderr = output, io.Discard
	cmd.Cancel = func() error { return cmd.Process.Signal(os.Interrupt) }
	cmd.WaitDelay = time.Duration(s.CleanupSeconds) * time.Second
	if err = cmd.Start(); err != nil {
		return result, d.Fail("MODEL_ACCESS_FAILED", 503)
	}
	if err = cmd.Wait(); err != nil || ctx.Err() != nil {
		return result, d.Fail("DELIVERY_UNKNOWN", 503)
	}
	raw := output.Bytes()
	if len(raw) > s.MaxLineBytes || !utf8.Valid(raw) || bytes.Count(raw, []byte{'\n'}) != 1 || len(raw) == 0 || raw[len(raw)-1] != '\n' {
		return result, d.Fail("PROTOCOL_INVALID", 503)
	}
	var reply struct {
		V      *int   `json:"v"`
		ID     string `json:"id"`
		OK     *bool  `json:"ok"`
		Result *struct {
			Provider   string              `json:"provider"`
			AccountID  *int64              `json:"account_id"`
			Model      string              `json:"model"`
			PromptHash string              `json:"prompt_hash"`
			Text       *string             `json:"text"`
			Usage      map[string]*float64 `json:"usage"`
		} `json:"result"`
		Error *struct {
			Code            string `json:"code"`
			DeliveryUnknown *bool  `json:"delivery_unknown"`
		} `json:"error"`
	}
	if d.DecodePrivate(raw, &reply) != nil || reply.V == nil || *reply.V != 2 || reply.ID != id || reply.OK == nil {
		return result, d.Fail("PROTOCOL_INVALID", 503)
	}
	var fields map[string]json.RawMessage
	_ = json.Unmarshal(raw, &fields)
	if *reply.OK {
		if _, exists := fields["error"]; exists {
			return result, d.Fail("PROTOCOL_INVALID", 503)
		}
	} else {
		if _, exists := fields["result"]; exists {
			return result, d.Fail("PROTOCOL_INVALID", 503)
		}
	}
	if !*reply.OK {
		if reply.Result != nil || reply.Error == nil || reply.Error.DeliveryUnknown == nil || !regexp.MustCompile(`^[A-Z][A-Z0-9_]+$`).MatchString(reply.Error.Code) {
			return result, d.Fail("PROTOCOL_INVALID", 503)
		}
		if *reply.Error.DeliveryUnknown {
			return result, d.Fail("DELIVERY_UNKNOWN", 503)
		}
		return result, d.Fail("MODEL_ACCESS_FAILED", 503)
	}
	r := reply.Result
	if reply.Error != nil || r == nil || r.Provider != s.Provider || r.AccountID == nil || *r.AccountID != s.AccountID || r.Model != s.Model || !regexp.MustCompile(`^[0-9a-f]{64}$`).MatchString(r.PromptHash) || r.Text == nil || strings.TrimSpace(*r.Text) == "" || len(*r.Text) > s.MaxOutputBytes || r.Usage == nil {
		return result, d.Fail("PROTOCOL_INVALID", 503)
	}
	for key, value := range r.Usage {
		if !d.Has([]string{"input", "output", "totalTokens"}, key) || value == nil || *value < 0 || *value > 9007199254740991 {
			return result, d.Fail("PROTOCOL_INVALID", 503)
		}
	}
	return d.SemanticGeneration{Provider: r.Provider, AccountID: *r.AccountID, Model: r.Model, PromptHash: r.PromptHash, Text: *r.Text, CompletedAt: time.Now().UTC()}, nil
}

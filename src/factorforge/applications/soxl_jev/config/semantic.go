package config

import (
	d "github.com/AceNanako0721/Factorforge/src/factorforge/applications/soxl_jev/domain"
	"github.com/pelletier/go-toml/v2"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"time"
)

type SemanticSettings struct {
	Enabled        bool   `toml:"enabled"`
	Channel        string `toml:"channel"`
	BunPath        string `toml:"bun_path"`
	Provider       string `toml:"provider"`
	AccountID      int64  `toml:"account_id"`
	Model          string `toml:"model"`
	TimeoutSeconds int64  `toml:"timeout_seconds"`
	CleanupSeconds int64  `toml:"cleanup_seconds"`
	MaxEvents      int    `toml:"max_events"`
	MaxQuoteBytes  int    `toml:"max_quote_bytes"`
	MaxInputBytes  int    `toml:"-"`
	MaxOutputBytes int    `toml:"-"`
	MaxLineBytes   int    `toml:"-"`
	// Chat channel credentials come from the canonical model_access block and
	// are never copied into a derived profile.
	APIBaseURL string `toml:"-"`
	APIKey     string `toml:"-"`
	PromptFile string `toml:"-"`
}

// LoadSemantic reads no credential fields and never writes the canonical file.
func LoadSemantic(root, path string) (SemanticSettings, error) {
	var result SemanticSettings
	base, err := filepath.Abs(root)
	if err != nil {
		return result, d.Fail("SEMANTIC_CONFIG_INVALID", 503)
	}
	canonical := filepath.Join(base, "config", "config.toml")
	absolute, err := filepath.Abs(path)
	if err != nil || absolute != canonical {
		return result, d.Fail("SEMANTIC_CONFIG_INVALID", 503)
	}
	info, statErr := os.Lstat(canonical)
	if statErr != nil || !info.Mode().IsRegular() || runtime.GOOS != "windows" && info.Mode().Perm()&0077 != 0 {
		return result, d.Fail("SEMANTIC_CONFIG_INVALID", 503)
	}
	// Windows may expand an ordinary 8.3 path in EvalSymlinks. Inspect actual
	// reparse/link components instead of treating spelling changes as links.
	for cursor := canonical; ; cursor = filepath.Dir(cursor) {
		part, e := os.Lstat(cursor)
		if e != nil || part.Mode()&os.ModeSymlink != 0 {
			return result, d.Fail("SEMANTIC_CONFIG_INVALID", 503)
		}
		if cursor == filepath.Dir(cursor) {
			break
		}
	}
	raw, err := privateFile(canonical, 16<<20)
	if err != nil {
		return result, err
	}
	var c struct {
		Application struct {
			Semantic SemanticSettings `toml:"semantic_extraction"`
		} `toml:"application"`
		ModelAccess struct {
			Enabled        bool   `toml:"enabled"`
			APIBaseURL     string `toml:"api_base_url"`
			APIKey         string `toml:"api_key"`
			TimeoutSeconds int64  `toml:"timeout_seconds"`
			MaxInputBytes  int    `toml:"max_input_bytes"`
			MaxOutputBytes int    `toml:"max_output_bytes"`
			MaxLineBytes   int    `toml:"max_line_bytes"`
			MaxRequests    int64  `toml:"omp_max_requests"`
			Window         int64  `toml:"budget_window_seconds"`
			PromptFile     string `toml:"prompt_file"`
		} `toml:"model_access"`
	}
	if toml.Unmarshal(raw, &c) != nil {
		return result, d.Fail("SEMANTIC_CONFIG_INVALID", 503)
	}
	result = c.Application.Semantic
	m := c.ModelAccess
	if !result.Enabled || !m.Enabled {
		return result, d.Fail("SEMANTIC_DISABLED", 503)
	}
	if result.Channel != "" && result.Channel != "omp" && result.Channel != "chat" {
		return result, d.Fail("SEMANTIC_CONFIG_REQUIRED", 503)
	}
	if result.Channel == "chat" {
		u, err := url.Parse(m.APIBaseURL)
		if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || m.APIKey == "" {
			return result, d.Fail("SEMANTIC_CONFIG_REQUIRED", 503)
		}
		prompt, err := filepath.Abs(filepath.Join(base, m.PromptFile))
		rel, relErr := filepath.Rel(filepath.Join(base, "prompts"), prompt)
		if err != nil || relErr != nil || filepath.IsAbs(rel) || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || rel == ".." || filepath.Base(prompt) == "prompts.example.json" {
			return result, d.Fail("SEMANTIC_CONFIG_REQUIRED", 503)
		}
		if _, err = os.Lstat(prompt); err != nil {
			return result, d.Fail("SEMANTIC_CONFIG_REQUIRED", 503)
		}
		result.APIBaseURL, result.APIKey, result.PromptFile = strings.TrimRight(m.APIBaseURL, "/"), m.APIKey, prompt
	} else if !filepath.IsAbs(result.BunPath) || !regexp.MustCompile(`^[a-z0-9][a-z0-9.-]{0,127}$`).MatchString(result.Provider) || result.AccountID < 1 || result.AccountID > 9007199254740991 || strings.TrimSpace(result.Model) == "" || len([]rune(result.Model)) > 256 || m.TimeoutSeconds <= 0 || m.TimeoutSeconds > 2147483 || result.TimeoutSeconds <= m.TimeoutSeconds || result.TimeoutSeconds > int64((1<<63-1)/int64(time.Second)) || result.CleanupSeconds <= 0 || result.CleanupSeconds > int64((1<<63-1)/int64(time.Second)) || result.MaxEvents <= 0 || result.MaxQuoteBytes <= 0 || m.MaxInputBytes <= 0 || m.MaxOutputBytes <= 0 || m.MaxLineBytes <= 0 || m.MaxRequests < 0 || m.Window < 0 || m.MaxRequests > 0 && m.Window == 0 {
		return result, d.Fail("SEMANTIC_CONFIG_REQUIRED", 503)
	}
	result.MaxInputBytes, result.MaxOutputBytes, result.MaxLineBytes = m.MaxInputBytes, m.MaxOutputBytes, m.MaxLineBytes
	if strings.TrimSpace(m.PromptFile) == "" || m.MaxLineBytes == int(^uint(0)>>1) {
		return result, d.Fail("SEMANTIC_CONFIG_REQUIRED", 503)
	}
	return result, nil
}

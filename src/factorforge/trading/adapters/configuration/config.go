// Package configuration reads the single ignored private TOML file. API and
// execution profiles are mutually exclusive and errors never include values.
package configuration

import (
	"encoding/json"
	d "github.com/AceNanako0721/Factorforge/src/factorforge/trading/domain"
	"github.com/pelletier/go-toml/v2"
	"math"
	"net/url"
	"os"
)

type Config struct {
	Runtime struct {
		Environment string `toml:"environment"`
	} `toml:"runtime"`
	Services struct {
		DatabaseURL          string `toml:"database_url"`
		ExecutionDatabaseURL string `toml:"execution_database_url"`
		ExchangeAPIURL       string `toml:"exchange_api_url"`
		TradingAPIURL        string `toml:"trading_api_url"`
		ProxyURL             string `toml:"proxy_url"`
	} `toml:"services"`
	Credentials struct {
		TradingAPIToken   string `toml:"trading_api_token"`
		ExchangeAPIKey    string `toml:"exchange_api_key"`
		ExchangeAPISecret string `toml:"exchange_api_secret"`
	} `toml:"credentials"`
	Trading struct {
		Adapter                 string         `toml:"adapter"`
		AllowLive               bool           `toml:"allow_live"`
		AccountID               string         `toml:"account_id"`
		PrincipalID             string         `toml:"principal_id"`
		Permissions             []string       `toml:"permissions"`
		StoragePath             string         `toml:"storage_path"`
		AccountCurrency         string         `toml:"account_currency"`
		ReconciliationTolerance string         `toml:"reconciliation_tolerance"`
		AccountPolicy           map[string]any `toml:"account_policy"`
		CostModel               map[string]any `toml:"cost_model"`
		LiveReadiness           map[string]any `toml:"live_readiness"`
		Health                  struct {
			StoragePath    string  `toml:"storage_path"`
			ClockProbeURL  string  `toml:"clock_probe_url"`
			TimeoutSeconds float64 `toml:"timeout_seconds"`
		} `toml:"runtime_health"`
		Signed struct {
			RecvWindow          int            `toml:"recv_window_ms"`
			RequestBudget       int            `toml:"request_budget"`
			PriorityReserve     int            `toml:"priority_request_reserve"`
			BudgetWindowSeconds float64        `toml:"budget_window_seconds"`
			TimeoutSeconds      float64        `toml:"timeout_seconds"`
			PollSeconds         float64        `toml:"poll_seconds"`
			Weights             map[string]int `toml:"request_weights"`
		} `toml:"signed_transport"`
	} `toml:"trading"`
}

func invalid(code string, status int) error { return &d.Error{Code: code, Status: status} }

// validProxy accepts an empty value (direct connection) or an absolute
// http, https or socks5 proxy URL.
func validProxy(value string) bool {
	if value == "" {
		return true
	}
	parsed, err := url.Parse(value)
	if err != nil || parsed.Host == "" {
		return false
	}
	switch parsed.Scheme {
	case "http", "https", "socks5", "socks5h":
		return true
	}
	return false
}
func Load(path string) (Config, error) {
	var config Config
	data, err := os.ReadFile(path)
	if err != nil || toml.Unmarshal(data, &config) != nil {
		return config, invalid("PRIVATE_CONFIGURATION_INVALID", 503)
	}
	if config.Runtime.Environment != "SIM" && config.Runtime.Environment != "LIVE" {
		return config, invalid("ENVIRONMENT_UNSUPPORTED", 422)
	}
	for _, value := range []float64{config.Trading.Health.TimeoutSeconds, config.Trading.Signed.TimeoutSeconds, config.Trading.Signed.PollSeconds, config.Trading.Signed.BudgetWindowSeconds} {
		if math.IsNaN(value) || math.IsInf(value, 0) || value < 0 || value > float64(math.MaxInt64)/float64(1e9) {
			return config, invalid("PRIVATE_CONFIGURATION_INVALID", 503)
		}
	}
	if !validProxy(config.Services.ProxyURL) {
		return config, invalid("PRIVATE_CONFIGURATION_INVALID", 503)
	}
	return config, nil
}
func (c Config) ValidateAPI() error {
	environment := c.Runtime.Environment
	if environment == "SIM" && c.Trading.AllowLive {
		return invalid("LIVE_CAPABILITIES_UNVERIFIED", 423)
	}
	if environment == "LIVE" && len(c.Trading.LiveReadiness) == 0 {
		return invalid("LIVE_CAPABILITIES_UNVERIFIED", 423)
	}
	adapter := "mock"
	if environment == "LIVE" {
		adapter = "binance"
	}
	if c.Trading.Adapter != adapter {
		return invalid("EXECUTION_ADAPTER_UNVERIFIED", 423)
	}
	if c.Services.DatabaseURL == "" || c.Credentials.TradingAPIToken == "" {
		return invalid("PRIVATE_CONFIGURATION_REQUIRED", 503)
	}
	if c.Trading.AccountID == "" || c.Trading.PrincipalID == "" {
		return invalid("ACCOUNT_BINDING_REQUIRED", 503)
	}
	if c.Credentials.ExchangeAPIKey != "" || c.Credentials.ExchangeAPISecret != "" {
		return invalid("EXECUTION_CREDENTIALS_IN_API_PROFILE", 403)
	}
	return nil
}
func (c Config) Principal() d.Principal {
	return d.Principal{PrincipalID: c.Trading.PrincipalID, Environment: c.Runtime.Environment, AccountID: c.Trading.AccountID, Permissions: c.Trading.Permissions}
}
func (c Config) Readiness() (d.Readiness, error) {
	var r d.Readiness
	data, err := json.Marshal(c.Trading.LiveReadiness)
	if err != nil || json.Unmarshal(data, &r) != nil {
		return r, invalid("LIVE_CONFIGURATION_INVALID", 503)
	}
	return r, nil
}

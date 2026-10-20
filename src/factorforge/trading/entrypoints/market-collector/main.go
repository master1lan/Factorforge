package main

import (
	"flag"
	"fmt"
	"github.com/AceNanako0721/Factorforge/src/factorforge/trading/adapters/binance"
	c "github.com/AceNanako0721/Factorforge/src/factorforge/trading/adapters/configuration"
	"github.com/AceNanako0721/Factorforge/src/factorforge/trading/adapters/httptrading"
	d "github.com/AceNanako0721/Factorforge/src/factorforge/trading/domain"
	"github.com/AceNanako0721/Factorforge/src/factorforge/trading/domain/decimal"
	r "github.com/AceNanako0721/Factorforge/src/factorforge/trading/entrypoints/assembly"
	"github.com/AceNanako0721/Factorforge/src/factorforge/trading/workers"
	"time"
)

func main() { r.Exit(run()) }
func run() error {
	profile := flag.String("config", "config/config.toml", "private API profile")
	id := flag.String("run-id", "", "run")
	instrument := flag.String("instrument", "", "registered object")
	interval := flag.String("interval", "", "candle interval")
	lookback := flag.Int("lookback-seconds", 0, "explicit history policy")
	poll := flag.Float64("poll-seconds", 0, "explicit polling policy")
	multiple := flag.String("multiplier", "", "verified multiplier")
	delay := flag.Int("publication-delay-ms", -1, "verified publication delay")
	limit := flag.Int("trade-limit", 0, "trade query limit")
	once := flag.Bool("once", false, "one acquisition")
	simCapabilities := flag.Bool("sim-capabilities", false, "declare simulator protection/short capabilities (REPLAY runs only)")
	simFrames := flag.Bool("sim-frames", false, "submit a simulation frame after each snapshot (SIM with --sim-capabilities only)")
	flag.Parse()
	multiplier, err := decimal.Parse(*multiple)
	if err != nil || multiplier.Sign() <= 0 || *poll <= 0 || *lookback <= 0 || *delay < 0 || *limit < 1 || *limit > 1000 || *id == "" || *instrument == "" || *interval == "" {
		return &d.Error{Code: "COLLECTOR_POLICY_INVALID", Status: 422}
	}
	config, err := c.Load(*profile)
	if err != nil {
		return err
	}
	if err = config.ValidateAPI(); err != nil {
		return err
	}
	if *simFrames && (!*simCapabilities || config.Runtime.Environment != "SIM") {
		return &d.Error{Code: "COLLECTOR_POLICY_INVALID", Status: 422}
	}
	ctx, cancel := r.Context()
	defer cancel()
	market := &binance.Market{Client: r.ClientVia(10*time.Second, config.Services.ProxyURL), Endpoint: config.Services.ExchangeAPIURL, Multipliers: map[string]string{*instrument: *multiple}, PublicationDelay: time.Duration(*delay) * time.Millisecond, SimCapabilities: *simCapabilities}
	client := &httptrading.Client{HTTP: r.Client(15 * time.Second), Endpoint: config.Services.TradingAPIURL, Token: config.Credentials.TradingAPIToken}
	collector := &workers.Collector{Market: market, Client: client, Key: d.RunKey{Environment: config.Runtime.Environment, AccountID: config.Trading.AccountID, RunID: *id}, TradeLimit: *limit, SimFrames: *simFrames}
	for {
		end := time.Now().UTC()
		points, bars, err := collector.Collect(ctx, *instrument, *interval, end.Add(-time.Duration(*lookback)*time.Second), end)
		if err != nil {
			return err
		}
		fmt.Printf("Collected %d points and %d completed candles\n", points, bars)
		if *once || !r.Pause(ctx, time.Duration(*poll*float64(time.Second))) {
			return nil
		}
	}
}

package assembly

import (
	"context"
	"errors"
	"fmt"
	c "github.com/AceNanako0721/Factorforge/src/factorforge/trading/adapters/configuration"
	"github.com/AceNanako0721/Factorforge/src/factorforge/trading/adapters/health"
	"github.com/AceNanako0721/Factorforge/src/factorforge/trading/adapters/postgres"
	"github.com/AceNanako0721/Factorforge/src/factorforge/trading/adapters/sim"
	a "github.com/AceNanako0721/Factorforge/src/factorforge/trading/application"
	d "github.com/AceNanako0721/Factorforge/src/factorforge/trading/domain"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func Context() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
}
func Exit(err error) {
	if err == nil {
		return
	}
	var failure *d.Error
	if errors.As(err, &failure) {
		fmt.Fprintln(os.Stderr, failure.Code)
	} else {
		fmt.Fprintln(os.Stderr, "RUNTIME_UNAVAILABLE")
	}
	os.Exit(1)
}
// Client builds the direct client used behind isolation; it never proxies.
func Client(timeout time.Duration) *http.Client { return clientWithProxy(timeout, "") }

// ClientVia builds an outbound client for external probes and market reads.
// An empty proxyURL keeps the direct connection; otherwise the configured
// proxy is used so hosts unreachable from this network stay readable.
func ClientVia(timeout time.Duration, proxyURL string) *http.Client {
	return clientWithProxy(timeout, proxyURL)
}
func clientWithProxy(timeout time.Duration, proxyURL string) *http.Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	if proxyURL != "" {
		if parsed, err := url.Parse(proxyURL); err == nil {
			transport.Proxy = http.ProxyURL(parsed)
		}
	}
	return &http.Client{Transport: transport, Timeout: timeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
}
func Assemble(ctx context.Context, config c.Config, recover bool) (*postgres.Store, *a.Service, error) {
	if err := config.ValidateAPI(); err != nil {
		return nil, nil, err
	}
	p := config.Principal()
	if d.Validate(p) != nil {
		return nil, nil, &d.Error{Code: "ACCOUNT_BINDING_REQUIRED", Status: 503}
	}
	store, err := postgres.Open(ctx, config.Services.DatabaseURL, config.Runtime.Environment)
	if err != nil {
		return nil, nil, err
	}
	if err = store.VerifyRuntimeRole(ctx); err != nil {
		store.Close()
		return nil, nil, err
	}
	service := &a.Service{Store: store}
	if config.Runtime.Environment == "SIM" {
		service.Simulator = &sim.Broker{}
	}
	settings := config.Trading.Health
	if settings.StoragePath != "" {
		if settings.ClockProbeURL == "" || settings.TimeoutSeconds <= 0 {
			store.Close()
			return nil, nil, &d.Error{Code: "CLOCK_PROBE_POLICY_REQUIRED", Status: 503}
		}
		service.Health = &health.Probe{StoragePath: settings.StoragePath, ClockOffset: health.ClockProbe(ClientVia(time.Duration(settings.TimeoutSeconds*float64(time.Second)), config.Services.ProxyURL), settings.ClockProbeURL, time.Now)}
	}
	if recover {
		key, err := store.BoundRun(ctx, p.AccountID)
		if err != nil {
			store.Close()
			return nil, nil, err
		}
		if key != nil {
			err = store.Transaction(ctx, *key, func(run *d.Aggregate) error {
				run.State = "RECOVERY_CHECK"
				run.VenueReconciledVersion = nil
				run.Version++
				a.Audit(run, "PROCESS_RESTART", p, "startup", a.Response{"state": run.State})
				return nil
			})
			if err != nil {
				store.Close()
				return nil, nil, err
			}
		}
	}
	return store, service, nil
}
func Serve(ctx context.Context, address string, handler http.Handler) error {
	server := &http.Server{Addr: address, Handler: handler, ReadHeaderTimeout: 10 * time.Second}
	done := make(chan error, 1)
	go func() { done <- server.ListenAndServe() }()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return server.Shutdown(shutdown)
	}
}
func Pause(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

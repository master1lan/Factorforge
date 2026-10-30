package trading_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/AceNanako0721/Factorforge/src/factorforge/trading/adapters/postgres"
	d "github.com/AceNanako0721/Factorforge/src/factorforge/trading/domain"
	"github.com/AceNanako0721/Factorforge/src/factorforge/trading/domain/decimal"
	"github.com/jackc/pgx/v5"
)

// Requires native PostgreSQL sockets; run by Claude outside this sandbox.
func TestS115MarketTradeRetentionPrune(t *testing.T) {
	ctx := context.Background()
	server, err := postgres.StartTemporary(ctx, t.TempDir(), nativePostgresBin(t))
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	if err = postgres.Initialize(ctx, server.AdminDSN, "SIM"); err != nil {
		t.Fatal(err)
	}
	conn, err := pgx.Connect(ctx, server.AdminDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	if _, err = conn.Exec(ctx, "INSERT INTO trading_sim.trading_run(account_id,run_id,environment,version,payload) VALUES('acct','run','SIM',0,'{}')"); err != nil {
		t.Fatal(err)
	}
	old := time.Now().UTC().AddDate(0, 0, -40)
	recent := time.Now().UTC().AddDate(0, 0, -1)
	for i, at := range []time.Time{old, old, recent} {
		id := fmt.Sprintf("trade-%d", i)
		payload := fmt.Sprintf(`{"observed_at":%q}`, at.Format(time.RFC3339Nano))
		if _, err = conn.Exec(ctx, "INSERT INTO trading_sim.market_trade(account_id,resource_id,payload,observed_at) VALUES('acct',$1,$2::jsonb,$3)", id, payload, at); err != nil {
			t.Fatal(err)
		}
	}
	store, err := postgres.Open(ctx, server.AdminDSN, "SIM")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	cutoff := time.Now().UTC().AddDate(0, 0, -30)
	if n, err := store.PruneMarketTrades(ctx, cutoff, 1); err != nil || n != 1 {
		t.Fatalf("first batch %d %v", n, err)
	}
	if n, err := store.PruneMarketTrades(ctx, cutoff, 100); err != nil || n != 1 {
		t.Fatalf("second batch %d %v", n, err)
	}
	if n, err := store.PruneMarketTrades(ctx, cutoff, 100); err != nil || n != 0 {
		t.Fatalf("empty batch %d %v", n, err)
	}
	var left int
	if err = conn.QueryRow(ctx, "SELECT count(*) FROM trading_sim.market_trade").Scan(&left); err != nil || left != 1 {
		t.Fatalf("recent row left %d %v", left, err)
	}
	if _, err = store.PruneMarketTrades(ctx, time.Now().UTC().AddDate(0, 0, -1), 10); err == nil {
		t.Fatal("retention floor accepted")
	}
	if _, err = conn.Exec(ctx, "SET ROLE factorforge_sim"); err != nil {
		t.Fatal(err)
	}
	if _, err = conn.Exec(ctx, "DELETE FROM trading_sim.market_trade"); err == nil {
		t.Fatal("app role deleted directly")
	}
	if _, err = conn.Exec(ctx, "SELECT trading_sim.prune_market_trade(now() - interval '40 days', 10)"); err != nil {
		t.Fatalf("app role execute %v", err)
	}
}

// v2.21.0: the ordered projection insert must fill observed_at, otherwise the
// collector's trades violate the NOT NULL retention column and every ingest
// fails with STORE_UNAVAILABLE.
func TestV2210MarketTradeProjectionWritesObservedAt(t *testing.T) {
	ctx := context.Background()
	server, err := postgres.StartTemporary(ctx, t.TempDir(), nativePostgresBin(t))
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	store, err := postgres.Open(ctx, server.SIMDSN, "SIM")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	admin, err := pgx.Connect(ctx, server.AdminDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close(ctx)
	run, _, _, _ := serviceBase(t)
	if err = store.Create(ctx, run); err != nil {
		t.Fatal(err)
	}
	code := run.Specs.Keys()[0]
	spec := run.Specs.Value(code)
	at := time.Now().UTC().Truncate(time.Second)
	price, err := decimal.Parse("1")
	if err != nil {
		t.Fatal(err)
	}
	identity := code + ":fixture-trade-1"
	if err = store.Transaction(ctx, run.RunKey, func(r *d.Aggregate) error {
		r.MarketTrades.Set(identity, &d.MarketTrade{ExternalID: "fixture-trade-1", InstrumentKey: spec.Key, SourceID: "fixture", ObservedAt: at, ReceivedAt: at, AvailableAt: at, Price: price, Quantity: price, SpecVersion: spec.Version})
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	var observed time.Time
	if err = admin.QueryRow(ctx, "SELECT observed_at FROM trading_sim.market_trade WHERE resource_id=$1", identity).Scan(&observed); err != nil {
		t.Fatal("market trade projection missing observed_at", err)
	}
	if !observed.Equal(at) {
		t.Fatalf("observed_at %v", observed)
	}
}

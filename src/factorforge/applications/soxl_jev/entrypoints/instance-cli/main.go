package main

import (
	"context"
	"flag"
	"fmt"
	modelaccess "github.com/AceNanako0721/Factorforge/src/factorforge/applications/soxl_jev/adapters/modelaccess"
	pg "github.com/AceNanako0721/Factorforge/src/factorforge/applications/soxl_jev/adapters/postgres"
	"github.com/AceNanako0721/Factorforge/src/factorforge/applications/soxl_jev/config"
	d "github.com/AceNanako0721/Factorforge/src/factorforge/applications/soxl_jev/domain"
	"github.com/AceNanako0721/Factorforge/src/factorforge/applications/soxl_jev/entrypoints/assembly"
	"github.com/AceNanako0721/Factorforge/src/factorforge/applications/soxl_jev/evidence"
	"github.com/AceNanako0721/Factorforge/src/factorforge/applications/soxl_jev/operations"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"
)

func run() error {
	action := flag.String("action", "", "init-storage, register-provider-budget, grant-reader, grant-pipeline, configure-queue-budget, publish-projection, prepare-evidence, extract-candidates, compile-evidence, prepare-review, render-review, compile-calendar or export-evidence (no trades)")
	source := flag.String("config", "config/config.toml", "Canonical private config for this operator command only")
	version := flag.Int64("expected-version", -1, "Existing read version; -1 only for initial publication")
	proposalInput := flag.String("proposal-input", "", "Private closed JSON evidence and catalog request")
	proposalOutput := flag.String("proposal-output", "", "New private JSON file under this repository runtime")
	proposalBytes := flag.Int("max-proposal-bytes", 0, "Explicit input and output file byte budget")
	semanticInput := flag.String("semantic-input", "", "Optional private semantic artifact for render-review only")
	login := flag.String("login", "", "Existing database login role for grant actions")
	kind := flag.String("kind", "", "Worker kind for grant-pipeline: INGEST, RESEARCH, TRADING, DYNAMIC or DYNAMIC_READ")
	flag.Parse()
	semanticProvided := false
	flag.Visit(func(f *flag.Flag) {
		if f.Name == "semantic-input" {
			semanticProvided = true
		}
	})
	if semanticProvided && (*action != "render-review" || *semanticInput == "") {
		return d.Fail("SEMANTIC_REVIEW_INVALID", 422)
	}
	if *action == "extract-candidates" {
		root, err := os.Getwd()
		if err != nil {
			return d.Fail("SEMANTIC_CONFIG_INVALID", 503)
		}
		path, err := filepath.Abs(*source)
		if err != nil {
			return d.Fail("SEMANTIC_CONFIG_INVALID", 503)
		}
		settings, err := config.LoadSemantic(root, path)
		if err != nil {
			return err
		}
		ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer cancel()
		client := modelaccess.Client{Root: root, Config: path, Settings: settings}
		return operations.ExtractSemanticFile(ctx, client, root, *proposalInput, *proposalOutput, *proposalBytes, evidence.SemanticLimits{MaxEvents: settings.MaxEvents, MaxQuoteBytes: settings.MaxQuoteBytes})
	}
	if d.Has([]string{"prepare-evidence", "compile-evidence", "prepare-review", "render-review", "compile-calendar"}, *action) {
		root, err := os.Getwd()
		if err != nil {
			return d.Fail("PROPOSAL_FILE_INVALID", 422)
		}
		if *action == "compile-evidence" {
			return operations.CompileEvidenceFile(root, *proposalInput, *proposalOutput, *proposalBytes, time.Now().UTC())
		}
		if *action == "compile-calendar" {
			return operations.CompileVenueCalendarFile(root, *proposalInput, *proposalOutput, *proposalBytes, time.Now().UTC())
		}
		if *action == "prepare-review" {
			return operations.PrepareReviewBundleFile(root, *proposalInput, *proposalOutput, *proposalBytes)
		}
		if *action == "render-review" {
			if semanticProvided {
				return operations.RenderReviewBundleWithSemanticFile(root, *proposalInput, *semanticInput, *proposalOutput, *proposalBytes)
			}
			return operations.RenderReviewBundleFile(root, *proposalInput, *proposalOutput, *proposalBytes)
		}
		return operations.PrepareEvidenceFile(root, *proposalInput, *proposalOutput, *proposalBytes)
	}
	if *action == "export-evidence" {
		root, err := os.Getwd()
		if err != nil {
			return d.Fail("PROPOSAL_FILE_INVALID", 422)
		}
		path, err := filepath.Abs(*source)
		if err != nil {
			return d.Fail("INSTANCE_CONFIG_PATH_INVALID", 422)
		}
		profile, err := config.LoadWorker(path, "INGEST")
		if err != nil {
			return err
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(profile.Settings.TimeoutSeconds)*time.Second)
		defer cancel()
		store, err := pg.OpenPipeline(ctx, profile.Access.DatabaseURL, profile.Binding(), "INGEST", profile.Settings.MaxInputBytes)
		if err != nil {
			return err
		}
		defer store.Close()
		return operations.ExportReviewBundleFile(ctx, store, profile.Binding(), root, *proposalInput, *proposalOutput, *proposalBytes)
	}
	if *action != "init-storage" && *action != "publish-projection" && *action != "register-provider-budget" &&
		*action != "grant-reader" && *action != "grant-pipeline" && *action != "configure-queue-budget" {
		return d.Fail("INSTANCE_CLI_ACTION_REQUIRED", 422)
	}
	path, err := filepath.Abs(*source)
	if err != nil {
		return d.Fail("INSTANCE_CONFIG_PATH_INVALID", 422)
	}
	profile, publication, err := config.LoadPublication(path)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(profile.Settings.TimeoutSeconds)*time.Second)
	defer cancel()
	if *action == "register-provider-budget" {
		root, err := os.Getwd()
		if err != nil {
			return d.Fail("PROVIDER_REGISTRATION_INVALID", 422)
		}
		registration, err := config.LoadProviderBudgetRegistration(root, *proposalInput, *proposalBytes)
		if err != nil {
			return err
		}
		if err = pg.ConfigureProviderPool(ctx, publication.DatabaseURL, registration.Policy); err != nil {
			return err
		}
		for _, grant := range registration.Grants {
			if err = pg.GrantProviderAccess(ctx, publication.DatabaseURL, grant); err != nil {
				return err
			}
		}
		return nil
	}
	if *action == "grant-reader" || *action == "grant-pipeline" {
		path, err := filepath.Abs(*source)
		if err != nil {
			return d.Fail("INSTANCE_CONFIG_PATH_INVALID", 422)
		}
		profile, publication, err := config.LoadPublication(path)
		if err != nil {
			return err
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(profile.Settings.TimeoutSeconds)*time.Second)
		defer cancel()
		if *action == "grant-reader" {
			return pg.GrantReader(ctx, publication.DatabaseURL, profile.Binding(), *login)
		}
		return pg.GrantPipeline(ctx, publication.DatabaseURL, profile.Binding(), *login, *kind)
	}
	if *action == "configure-queue-budget" {
		root, err := os.Getwd()
		if err != nil {
			return d.Fail("INSTANCE_CONFIG_PATH_INVALID", 422)
		}
		policy, err := loadQueueBudget(root, *proposalInput, *proposalBytes)
		if err != nil {
			return err
		}
		path, err := filepath.Abs(*source)
		if err != nil {
			return d.Fail("INSTANCE_CONFIG_PATH_INVALID", 422)
		}
		profile, publication, err := config.LoadPublication(path)
		if err != nil {
			return err
		}
		policy.Binding = profile.Binding()
		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(profile.Settings.TimeoutSeconds)*time.Second)
		defer cancel()
		return pg.ConfigureQueueBudget(ctx, publication.DatabaseURL, policy)
	}
	if *action == "init-storage" {
		if err = pg.InitializeProviderControl(ctx, publication.DatabaseURL); err != nil {
			return err
		}
		if err = pg.InitializePipeline(ctx, publication.DatabaseURL, profile.Binding()); err != nil {
			return err
		}
		return pg.Initialize(ctx, publication.DatabaseURL, profile.Binding())
	}
	assets, err := config.LoadPipelineAssets(profile)
	if err != nil {
		return err
	}
	store, err := pg.OpenPipeline(ctx, profile.Access.DatabaseURL, profile.Binding(), "INGEST", profile.Settings.MaxInputBytes)
	if err != nil {
		return err
	}
	defer store.Close()
	ids := []string{}
	for id := range assets.Sources {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	sources := []d.SourceRegistration{}
	for _, id := range ids {
		sources = append(sources, assets.Sources[id])
	}
	next := int64(0)
	var expected *int64
	if *version >= 0 {
		next = *version + 1
		expected = version
	} else if *version != -1 {
		return d.Fail("INSTANCE_VERSION_REQUIRED", 422)
	}
	snapshot, originals, err := store.Projection(ctx, pg.ProjectionOptions{Version: next, SourceVersion: publication.SourceVersion, Stage: profile.Settings.Stage, Sources: sources, FixtureOnly: profile.Mode == "mock", Now: time.Now().UTC(), MaxRecords: publication.MaxRecords, MaxBytes: publication.MaxBytes})
	if err != nil {
		return err
	}
	return pg.PublishPipeline(ctx, publication.DatabaseURL, snapshot, originals, expected)
}
func main() {
	if err := run(); err != nil {
		assembly.PrintError(err)
		os.Exit(1)
	}
	fmt.Println("INSTANCE_OPERATOR_OPERATION_RECORDED")
}

// loadQueueBudget reads one operator budget registration from the private
// runtime tree. The binding comes from the canonical config, never the file.
func loadQueueBudget(root, path string, maxBytes int) (pg.QueueBudget, error) {
	var policy pg.QueueBudget
	if maxBytes <= 0 || path == "" {
		return policy, d.Fail("PIPELINE_BUDGET_REQUIRED", 422)
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return policy, d.Fail("PIPELINE_BUDGET_REQUIRED", 422)
	}
	base, err := filepath.Abs(root)
	if err != nil {
		return policy, d.Fail("PIPELINE_BUDGET_REQUIRED", 422)
	}
	rel, err := filepath.Rel(base, absolute)
	if err != nil || filepath.IsAbs(rel) || !strings.HasPrefix(rel, "runtime"+string(filepath.Separator)) {
		return policy, d.Fail("PIPELINE_BUDGET_REQUIRED", 422)
	}
	info, err := os.Lstat(absolute)
	if err != nil || !info.Mode().IsRegular() || info.Size() > int64(maxBytes) {
		return policy, d.Fail("PIPELINE_BUDGET_REQUIRED", 422)
	}
	raw, err := os.ReadFile(absolute)
	if err != nil || len(raw) > maxBytes {
		return policy, d.Fail("PIPELINE_BUDGET_REQUIRED", 422)
	}
	var file struct {
		Kind          string    `json:"kind"`
		Bucket        string    `json:"bucket"`
		PolicyRef     string    `json:"policy_ref"`
		MaxJobs       int64     `json:"max_jobs"`
		MaxConcurrent int64     `json:"max_concurrent"`
		ValidFrom     time.Time `json:"valid_from"`
		ValidUntil    time.Time `json:"valid_until"`
	}
	if d.DecodePrivate(raw, &file) != nil {
		return policy, d.Fail("PIPELINE_BUDGET_REQUIRED", 422)
	}
	return pg.QueueBudget{Kind: file.Kind, Bucket: file.Bucket, PolicyRef: file.PolicyRef, MaxJobs: file.MaxJobs, MaxConcurrent: file.MaxConcurrent, ValidFrom: file.ValidFrom, ValidUntil: file.ValidUntil}, nil
}

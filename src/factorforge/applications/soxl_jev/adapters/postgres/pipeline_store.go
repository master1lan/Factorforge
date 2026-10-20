package postgres

import (
	"bytes"
	"context"
	"embed"
	"encoding/json"
	"errors"
	d "github.com/AceNanako0721/Factorforge/src/factorforge/applications/soxl_jev/domain"
	dto "github.com/AceNanako0721/Factorforge/src/factorforge/strategy/api/dto"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"io"
	"strings"
	"time"
)

//go:embed migrations/002_pipeline.sql migrations/003_operations.sql migrations/004_search.sql migrations/006_watch_state.sql migrations/007_dynamic_stop.sql migrations/008_dynamic_read.sql
var pipelineFiles embed.FS

type QueueBudget struct {
	Binding                 d.Binding
	Kind, Bucket, PolicyRef string
	MaxJobs, MaxConcurrent  int64
	ValidFrom, ValidUntil   time.Time
}
type PipelineStore struct {
	pool                        *pgxpool.Pool
	binding                     d.Binding
	kind, schema, table, outbox string
	maxBytes                    int
}

func pipelineSchema(b d.Binding) (string, error) {
	if !b.Valid() {
		return "", d.Fail("PIPELINE_BINDING_INVALID", 422)
	}
	return "instance_pipeline_" + strings.ToLower(b.Environment), nil
}
func pipelineRole(b d.Binding, kind string) string {
	return "factorforge_pipeline_" + strings.ToLower(b.Environment) + "_" + strings.ToLower(kind)
}

// InitializePipeline is an explicit administrative provisioning operation.
// No worker or public HTTP API receives this connection or calls this method.
func InitializePipeline(ctx context.Context, dsn string, b d.Binding) error {
	schema, err := pipelineSchema(b)
	if err != nil {
		return err
	}
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return safe(err)
	}
	defer conn.Close(ctx)
	tx, err := conn.Begin(ctx)
	if err != nil {
		return safe(err)
	}
	defer tx.Rollback(ctx)
	for _, file := range []string{"migrations/002_pipeline.sql", "migrations/003_operations.sql", "migrations/004_search.sql", "migrations/006_watch_state.sql", "migrations/007_dynamic_stop.sql", "migrations/008_dynamic_read.sql"} {
		raw, e := pipelineFiles.ReadFile(file)
		if e != nil {
			return d.Fail("MIGRATION_UNAVAILABLE", 503)
		}
		if _, e = tx.Exec(ctx, strings.ReplaceAll(strings.ReplaceAll(string(raw), "__SCHEMA__", schema), "__ENV__", b.Environment)); e != nil {
			return safe(e)
		}
	}
	for _, kind := range []string{"INGEST", "RESEARCH", "TRADING"} {
		role := pipelineRole(b, kind)
		var exists bool
		if err = tx.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname=$1)", role).Scan(&exists); err != nil {
			return safe(err)
		}
		if !exists {
			if _, err = tx.Exec(ctx, "CREATE ROLE "+pgx.Identifier{role}.Sanitize()+" NOLOGIN"); err != nil {
				return safe(err)
			}
		}
		for _, sql := range []string{
			"REVOKE ALL ON ALL TABLES IN SCHEMA " + schema + " FROM " + role,
			"GRANT USAGE ON SCHEMA " + schema + " TO " + role,
			"GRANT SELECT ON " + schema + ".worker_grant," + schema + ".budget," + schema + ".raw_evidence_manifest," + schema + ".routing_receipt TO " + role,
			"GRANT SELECT,INSERT ON " + schema + ".operation_fact TO " + role,
		} {
			if _, err = tx.Exec(ctx, sql); err != nil {
				return safe(err)
			}
		}
		if kind == "INGEST" {
			for _, sql := range []string{
				"GRANT SELECT,INSERT ON " + schema + ".app_research_analysis_job," + schema + ".app_trading_analysis_job TO " + role,
				"GRANT INSERT ON " + schema + ".raw_evidence_manifest," + schema + ".routing_receipt TO " + role,
				"GRANT UPDATE(used_jobs) ON " + schema + ".budget TO " + role,
				"GRANT SELECT,INSERT ON " + schema + ".framework_report TO " + role,
				"GRANT SELECT,INSERT ON " + schema + ".watch_seen TO " + role,
				"GRANT SELECT,INSERT ON " + schema + ".search_state TO " + role,
				"GRANT UPDATE(payload) ON " + schema + ".search_state TO " + role,
			} {
				if _, err = tx.Exec(ctx, sql); err != nil {
					return safe(err)
				}
			}
		} else {
			prefix := strings.ToLower(kind)
			for _, sql := range []string{
				"GRANT SELECT,UPDATE ON " + schema + ".app_" + prefix + "_analysis_job TO " + role,
				"GRANT SELECT,INSERT,UPDATE ON " + schema + "." + prefix + "_submission_outbox TO " + role,
			} {
				if _, err = tx.Exec(ctx, sql); err != nil {
					return safe(err)
				}
			}
		}
	}
	// v2.9.0: the DYNAMIC worker only reads its grant and owns dynamic tables.
	role := pipelineRole(b, "DYNAMIC")
	var exists bool
	if err = tx.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname=$1)", role).Scan(&exists); err != nil {
		return safe(err)
	}
	if !exists {
		if _, err = tx.Exec(ctx, "CREATE ROLE "+pgx.Identifier{role}.Sanitize()+" NOLOGIN"); err != nil {
			return safe(err)
		}
	}
	for _, sql := range []string{
		"REVOKE ALL ON ALL TABLES IN SCHEMA " + schema + " FROM " + role,
		"GRANT USAGE ON SCHEMA " + schema + " TO " + role,
		"GRANT SELECT ON " + schema + ".worker_grant TO " + role,
		"GRANT SELECT,INSERT ON " + schema + ".dynamic_record TO " + role,
		"GRANT SELECT,INSERT,UPDATE ON " + schema + ".dynamic_state TO " + role,
	} {
		if _, err = tx.Exec(ctx, sql); err != nil {
			return safe(err)
		}
	}
	// v2.10.0: DYNAMIC_READ serves the instance read API and never writes.
	role = pipelineRole(b, "DYNAMIC_READ")
	if err = tx.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname=$1)", role).Scan(&exists); err != nil {
		return safe(err)
	}
	if !exists {
		if _, err = tx.Exec(ctx, "CREATE ROLE "+pgx.Identifier{role}.Sanitize()+" NOLOGIN"); err != nil {
			return safe(err)
		}
	}
	for _, sql := range []string{
		"REVOKE ALL ON ALL TABLES IN SCHEMA " + schema + " FROM " + role,
		"GRANT USAGE ON SCHEMA " + schema + " TO " + role,
		"GRANT SELECT ON " + schema + ".worker_grant," + schema + ".dynamic_record," + schema + ".dynamic_state TO " + role,
	} {
		if _, err = tx.Exec(ctx, sql); err != nil {
			return safe(err)
		}
	}
	return safe(tx.Commit(ctx))
}
func GrantPipeline(ctx context.Context, dsn string, b d.Binding, login, kind string) error {
	schema, err := pipelineSchema(b)
	if err != nil {
		return err
	}
	if !d.ValidID(login) || !d.Has([]string{"INGEST", "RESEARCH", "TRADING", "DYNAMIC", "DYNAMIC_READ"}, kind) {
		return d.Fail("PIPELINE_GRANT_INVALID", 422)
	}
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return safe(err)
	}
	defer conn.Close(ctx)
	tx, err := conn.Begin(ctx)
	if err != nil {
		return safe(err)
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, "INSERT INTO "+schema+".worker_grant VALUES($1,$2,$3,$4) ON CONFLICT DO NOTHING", login, b.InstanceID, b.Environment, kind); err != nil {
		return safe(err)
	}
	var instance, env, k string
	if err = tx.QueryRow(ctx, "SELECT instance_id,environment,worker_kind FROM "+schema+".worker_grant WHERE authenticated_role=$1", login).Scan(&instance, &env, &k); err != nil {
		return safe(err)
	}
	if instance != b.InstanceID || env != b.Environment || k != kind {
		return d.Fail("PIPELINE_GRANT_IMMUTABLE", 409)
	}
	if _, err = tx.Exec(ctx, "GRANT "+pgx.Identifier{pipelineRole(b, kind)}.Sanitize()+" TO "+pgx.Identifier{login}.Sanitize()); err != nil {
		return safe(err)
	}
	return safe(tx.Commit(ctx))
}
func ConfigureQueueBudget(ctx context.Context, dsn string, policy QueueBudget) error {
	schema, err := pipelineSchema(policy.Binding)
	if err != nil {
		return err
	}
	if !d.Has([]string{"RESEARCH", policy.Binding.Environment}, policy.Kind) || !d.ValidID(policy.Bucket) || !d.ValidID(policy.PolicyRef) ||
		policy.MaxJobs <= 0 || policy.MaxConcurrent <= 0 || !d.UTC(policy.ValidFrom) || !d.UTC(policy.ValidUntil) || !policy.ValidUntil.After(policy.ValidFrom) {
		return d.Fail("PIPELINE_BUDGET_REQUIRED", 422)
	}
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return safe(err)
	}
	defer conn.Close(ctx)
	_, err = conn.Exec(ctx, "INSERT INTO "+schema+".budget VALUES($1,$2,$3,$4,$5,0,$6,$7,$8) ON CONFLICT DO NOTHING", policy.Binding.InstanceID, policy.Kind, policy.Bucket, policy.MaxJobs, policy.MaxConcurrent, policy.ValidFrom, policy.ValidUntil, policy.PolicyRef)
	return safe(err)
}
func OpenPipeline(ctx context.Context, dsn string, b d.Binding, kind string, maxBytes int) (*PipelineStore, error) {
	schema, err := pipelineSchema(b)
	if err != nil {
		return nil, err
	}
	if !d.Has([]string{"INGEST", "RESEARCH", "TRADING", "DYNAMIC", "DYNAMIC_READ"}, kind) || maxBytes <= 0 {
		return nil, d.Fail("PIPELINE_CONFIGURATION_REQUIRED", 503)
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, safe(err)
	}
	s := &PipelineStore{pool: pool, binding: b, kind: kind, schema: schema, maxBytes: maxBytes}
	if kind != "INGEST" && kind != "DYNAMIC" && kind != "DYNAMIC_READ" {
		s.table = "app_" + strings.ToLower(kind) + "_analysis_job"
		s.outbox = strings.ToLower(kind) + "_submission_outbox"
	}
	if err = s.VerifyRole(ctx); err != nil {
		s.Close()
		return nil, err
	}
	return s, nil
}
func (s *PipelineStore) Close()             { s.pool.Close() }
func (s *PipelineStore) Binding() d.Binding { return s.binding }
func (s *PipelineStore) Kind() string {
	if s.kind == "TRADING" {
		return s.binding.Environment
	}
	return s.kind
}
func (s *PipelineStore) VerifyRole(ctx context.Context) error {
	return s.verifyRole(ctx, s.pool)
}

type pipelineQuerier interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

func (s *PipelineStore) verifyRole(ctx context.Context, q pipelineQuerier) error {
	var allowed bool
	err := q.QueryRow(ctx, `SELECT r.rolcanlogin AND NOT r.rolsuper AND NOT r.rolbypassrls
 AND NOT r.rolcreaterole AND NOT r.rolcreatedb AND NOT r.rolreplication
 AND pg_has_role(session_user,$1,'MEMBER')
 AND NOT EXISTS(SELECT 1 FROM pg_roles p WHERE p.rolname LIKE 'factorforge_pipeline_%'
   AND p.rolname<>$1 AND pg_has_role(session_user,p.oid,'MEMBER'))
 AND NOT EXISTS(SELECT 1 FROM pg_namespace n WHERE n.nspname NOT IN ('pg_catalog','information_schema')
   AND n.nspname NOT LIKE 'pg_toast%' AND n.nspname NOT LIKE 'pg_temp%' AND has_schema_privilege(session_user,n.oid,'CREATE'))
 AND NOT EXISTS(SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
   WHERE n.nspname NOT IN('pg_catalog','information_schema') AND n.nspname NOT LIKE 'pg_toast%'
   AND c.relkind IN('r','p','v','m','f') AND n.nspname<>$2
   AND (has_any_column_privilege(session_user,c.oid,'SELECT,INSERT,UPDATE')
     OR has_table_privilege(session_user,c.oid,'DELETE,TRUNCATE,TRIGGER')))
 FROM pg_roles r WHERE r.rolname=session_user`, pipelineRole(s.binding, s.kind), s.schema).Scan(&allowed)
	if err != nil {
		return safe(err)
	}
	if !allowed {
		return d.Fail("PIPELINE_DATABASE_ROLE_NOT_ISOLATED", 403)
	}
	err = q.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM "+s.schema+".worker_grant WHERE authenticated_role=session_user AND instance_id=$1 AND environment=$2 AND worker_kind=$3)", s.binding.InstanceID, s.binding.Environment, s.kind).Scan(&allowed)
	if err != nil {
		return safe(err)
	}
	if !allowed {
		return d.Fail("PIPELINE_DATABASE_SCOPE_FORBIDDEN", 403)
	}
	// Extra privileges on peer queues or grant tables must not survive a grant.
	tables := []string{"worker_grant", "budget", "raw_evidence_manifest", "routing_receipt", "app_research_analysis_job", "app_trading_analysis_job", "research_submission_outbox", "trading_submission_outbox", "operation_fact", "framework_report", "search_state", "watch_seen"}
	if s.kind == "DYNAMIC_READ" {
		// The read identity sees only its grant and the dynamic tables, read-only.
		for _, table := range append(tables, "dynamic_record", "dynamic_state") {
			var read, write bool
			err = q.QueryRow(ctx, "SELECT has_any_column_privilege(session_user,$1,'SELECT'),has_any_column_privilege(session_user,$1,'INSERT,UPDATE') OR has_table_privilege(session_user,$1,'DELETE,TRUNCATE,TRIGGER')", s.schema+"."+table).Scan(&read, &write)
			if err != nil {
				return safe(err)
			}
			if write || read && !d.Has([]string{"worker_grant", "dynamic_record", "dynamic_state"}, table) {
				return d.Fail("PIPELINE_DATABASE_ROLE_NOT_ISOLATED", 403)
			}
		}
		return nil
	}
	for _, table := range tables {
		var read, write, insert, remove bool
		err = q.QueryRow(ctx, "SELECT has_any_column_privilege(session_user,$1,'SELECT'),has_any_column_privilege(session_user,$1,'UPDATE'),has_any_column_privilege(session_user,$1,'INSERT'),has_table_privilege(session_user,$1,'DELETE,TRUNCATE,TRIGGER')", s.schema+"."+table).Scan(&read, &write, &insert, &remove)
		if err != nil {
			return safe(err)
		}
		shared := d.Has([]string{"worker_grant", "budget", "raw_evidence_manifest", "routing_receipt"}, table)
		readAllowed := shared || table == "operation_fact" || s.kind == "INGEST" && d.Has([]string{"framework_report", "search_state", "watch_seen"}, table) || table == s.table || table == s.outbox || s.kind == "INGEST" && strings.HasPrefix(table, "app_")
		writeAllowed := table == s.table || table == s.outbox || s.kind == "INGEST" && d.Has([]string{"budget", "search_state"}, table)
		insertAllowed := table == "operation_fact" || table == s.outbox || s.kind == "INGEST" && d.Has([]string{"raw_evidence_manifest", "routing_receipt", "app_research_analysis_job", "app_trading_analysis_job", "framework_report", "search_state", "watch_seen"}, table)
		if remove || read && !readAllowed || write && !writeAllowed || insert && !insertAllowed {
			return d.Fail("PIPELINE_DATABASE_ROLE_NOT_ISOLATED", 403)
		}
		if s.kind == "INGEST" && d.Has([]string{"budget", "search_state"}, table) {
			column := "used_jobs"
			if table == "search_state" {
				column = "payload"
			}
			var excessive bool
			err = q.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM pg_attribute a WHERE a.attrelid=$1::regclass AND a.attnum>0 AND NOT a.attisdropped AND a.attname<>$2 AND has_column_privilege(session_user,a.attrelid,a.attnum,'UPDATE'))", s.schema+"."+table, column).Scan(&excessive)
			if err != nil {
				return safe(err)
			}
			if excessive {
				return d.Fail("PIPELINE_DATABASE_ROLE_NOT_ISOLATED", 403)
			}
		}
	}
	return nil
}
func (s *PipelineStore) encode(value any) ([]byte, error) {
	raw, err := json.Marshal(value)
	if err != nil || len(raw) > s.maxBytes {
		return nil, d.Fail("PIPELINE_PAYLOAD_BUDGET_EXCEEDED", 422)
	}
	return raw, nil
}
func (s *PipelineStore) decode(raw []byte, target any) error {
	if len(raw) > s.maxBytes {
		return d.Fail("PIPELINE_PAYLOAD_BUDGET_EXCEEDED", 503)
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if dec.Decode(target) != nil || dec.Decode(new(any)) != io.EOF {
		return d.Fail("PIPELINE_RECORD_INVALID", 503)
	}
	return nil
}
func (s *PipelineStore) Record(ctx context.Context, e d.ExtractedEvidence, r d.RoutingReceipt) error {
	if s.kind != "INGEST" || r.Binding != s.binding || r.EvidenceID != e.Raw.EvidenceID || r.ContentHash != e.Raw.ContentHash || r.ManifestHash != d.Digest(e) {
		return d.Fail("PIPELINE_RECORD_FORBIDDEN", 403)
	}
	if err := s.VerifyRole(ctx); err != nil {
		return err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return safe(err)
	}
	defer tx.Rollback(ctx)
	for _, entry := range []struct {
		table, id string
		value     any
		hash      string
	}{{"raw_evidence_manifest", e.Raw.EvidenceID, e, e.Raw.ContentHash}, {"routing_receipt", r.RoutingID, r, ""}} {
		raw, e := s.encode(entry.value)
		if e != nil {
			return e
		}
		key := "evidence_id"
		if entry.table == "routing_receipt" {
			key = "routing_id"
		}
		var stored []byte
		if entry.table == "raw_evidence_manifest" {
			_, err = tx.Exec(ctx, "INSERT INTO "+s.schema+"."+entry.table+" VALUES($1,$2,$3,$4) ON CONFLICT DO NOTHING", s.binding.InstanceID, entry.id, entry.hash, raw)
		} else {
			_, err = tx.Exec(ctx, "INSERT INTO "+s.schema+"."+entry.table+" VALUES($1,$2,$3) ON CONFLICT DO NOTHING", s.binding.InstanceID, entry.id, raw)
		}
		if err != nil {
			return safe(err)
		}
		if err = tx.QueryRow(ctx, "SELECT payload FROM "+s.schema+"."+entry.table+" WHERE instance_id=$1 AND "+key+"=$2", s.binding.InstanceID, entry.id).Scan(&stored); err != nil {
			return safe(err)
		}
		if !bytes.Equal(raw, stored) {
			return d.Fail("PIPELINE_RECORD_IMMUTABLE", 409)
		}
	}
	return safe(tx.Commit(ctx))
}

func (s *PipelineStore) Evidence(ctx context.Context, id string) (*d.ExtractedEvidence, error) {
	if s.kind != "INGEST" || !d.ValidID(id) {
		return nil, d.Fail("PIPELINE_EVIDENCE_FORBIDDEN", 403)
	}
	if err := s.VerifyRole(ctx); err != nil {
		return nil, err
	}
	var raw []byte
	err := s.pool.QueryRow(ctx, "SELECT payload FROM "+s.schema+".raw_evidence_manifest WHERE instance_id=$1 AND evidence_id=$2", s.binding.InstanceID, id).Scan(&raw)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, safe(err)
	}
	var e d.ExtractedEvidence
	if err = s.decode(raw, &e); err != nil {
		return nil, err
	}
	if e.Raw.EvidenceID != id {
		return nil, d.Fail("PIPELINE_RECORD_BINDING_INVALID", 503)
	}
	return &e, nil
}
func (s *PipelineStore) Routing(ctx context.Context, id string) (*d.RoutingReceipt, error) {
	if s.kind != "INGEST" || !d.ValidID(id) {
		return nil, d.Fail("PIPELINE_ROUTING_FORBIDDEN", 403)
	}
	if err := s.VerifyRole(ctx); err != nil {
		return nil, err
	}
	var raw []byte
	err := s.pool.QueryRow(ctx, "SELECT payload FROM "+s.schema+".routing_receipt WHERE instance_id=$1 AND routing_id=$2", s.binding.InstanceID, id).Scan(&raw)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, safe(err)
	}
	var r d.RoutingReceipt
	if err = s.decode(raw, &r); err != nil {
		return nil, err
	}
	if r.Binding != s.binding || r.RoutingID != id {
		return nil, d.Fail("PIPELINE_RECORD_BINDING_INVALID", 503)
	}
	return &r, nil
}
func (s *PipelineStore) Enqueue(ctx context.Context, job d.PipelineJob, bucket string) error {
	if s.kind != "INGEST" || job.Binding != s.binding || !d.Has([]string{"RESEARCH", s.binding.Environment}, job.QueueKind) ||
		!d.ValidID(job.JobID) || !d.ValidID(bucket) || job.State != "QUEUED" || !d.UTC(job.CreatedAt) || !job.Deadline.After(job.CreatedAt) ||
		job.Request.Binding != s.binding || job.Request.Routing.Binding != s.binding || job.Request.Deadline != job.Deadline ||
		job.QueueKind != "RESEARCH" && job.Request.Routing.Route != "TRADING_CANDIDATE" || job.Candidate != nil || job.Attempt != 0 || job.ClaimedBy != "" || job.LeaseUntil != nil || job.ClaimedAt != nil || job.StartedAt != nil || job.CompletedAt != nil || job.ReceiptRef != nil {
		return d.Fail("PIPELINE_JOB_NOT_ADMITTED", 403)
	}
	if job.Request.VerifyEligibilityWindow() != nil {
		return d.Fail("PIPELINE_JOB_NOT_ADMITTED", 403)
	}
	if err := s.VerifyRole(ctx); err != nil {
		return err
	}
	job.BudgetBucket = bucket
	raw, err := s.encode(job)
	if err != nil {
		return err
	}
	table := "app_research_analysis_job"
	if job.QueueKind != "RESEARCH" {
		table = "app_trading_analysis_job"
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return safe(err)
	}
	defer tx.Rollback(ctx)
	// Serialize duplicate deliveries on their operator budget row. A retry after
	// the job advances must compare immutable input, never its current state.
	var budgetExists bool
	err = tx.QueryRow(ctx, "SELECT true FROM "+s.schema+".budget WHERE instance_id=$1 AND queue_kind=$2 AND bucket=$3 FOR UPDATE", s.binding.InstanceID, job.QueueKind, bucket).Scan(&budgetExists)
	if err != nil {
		return d.Fail("PIPELINE_BUDGET_REQUIRED", 429)
	}
	var stored []byte
	err = tx.QueryRow(ctx, "SELECT payload FROM "+s.schema+"."+table+" WHERE instance_id=$1 AND job_id=$2", s.binding.InstanceID, job.JobID).Scan(&stored)
	if err == nil {
		var current d.PipelineJob
		if err = s.decode(stored, &current); err != nil {
			return err
		}
		if current.Binding != job.Binding || current.JobID != job.JobID || current.QueueKind != job.QueueKind || current.BudgetBucket != bucket ||
			!current.CreatedAt.Equal(job.CreatedAt) || !current.Deadline.Equal(job.Deadline) || d.Digest(current.Request) != d.Digest(job.Request) {
			return d.Fail("PIPELINE_JOB_CONFLICT", 409)
		}
		return nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return safe(err)
	}
	tag, err := tx.Exec(ctx, "UPDATE "+s.schema+".budget SET used_jobs=used_jobs+1 WHERE instance_id=$1 AND queue_kind=$2 AND bucket=$3 AND valid_from<=$4 AND valid_until>$4 AND used_jobs<max_jobs", s.binding.InstanceID, job.QueueKind, bucket, job.CreatedAt)
	if err != nil {
		return safe(err)
	}
	if tag.RowsAffected() != 1 {
		return d.Fail("PIPELINE_BUDGET_EXHAUSTED", 429)
	}
	tag, err = tx.Exec(ctx, "INSERT INTO "+s.schema+"."+table+"(instance_id,job_id,budget_bucket,state,created_at,deadline,payload) VALUES($1,$2,$3,'QUEUED',$4,$5,$6) ON CONFLICT DO NOTHING", s.binding.InstanceID, job.JobID, bucket, job.CreatedAt, job.Deadline, raw)
	if err != nil {
		return safe(err)
	}
	if tag.RowsAffected() != 1 {
		return d.Fail("PIPELINE_JOB_CONFLICT", 409)
	}
	return safe(tx.Commit(ctx))
}

func (s *PipelineStore) Claim(ctx context.Context, worker string, now time.Time, lease time.Duration) (*d.PipelineJob, error) {
	if s.kind == "INGEST" || !d.ValidID(worker) || !d.UTC(now) || lease <= 0 {
		return nil, d.Fail("PIPELINE_CLAIM_FORBIDDEN", 403)
	}
	if err := s.VerifyRole(ctx); err != nil {
		return nil, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, safe(err)
	}
	defer tx.Rollback(ctx)
	// The lock is confined to this physical queue table; research cannot lock the
	// trading table. Provider calls happen after commit, never inside the lock.
	if _, err = tx.Exec(ctx, "LOCK TABLE "+s.schema+"."+s.table+" IN SHARE ROW EXCLUSIVE MODE"); err != nil {
		return nil, safe(err)
	}
	var raw []byte
	var state string
	err = tx.QueryRow(ctx, "SELECT payload,state FROM "+s.schema+"."+s.table+" WHERE instance_id=$1 AND (state='QUEUED' OR state IN ('CLAIMED','RUNNING') AND lease_until<=$2) ORDER BY created_at,job_id LIMIT 1 FOR UPDATE", s.binding.InstanceID, now).Scan(&raw, &state)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, safe(err)
	}
	var job d.PipelineJob
	if err = s.decode(raw, &job); err != nil {
		return nil, err
	}
	if job.Binding != s.binding || job.QueueKind != s.Kind() {
		return nil, d.Fail("PIPELINE_RECORD_BINDING_INVALID", 503)
	}
	job.State = state
	if !now.Before(job.Deadline) || state == "RUNNING" && job.Candidate == nil {
		job.State = "EXPIRED"
		completed := now
		job.CompletedAt = &completed
		job.ReasonCodes = []string{"TASK_EXPIRED"}
		if now.Before(job.Deadline) {
			job.State = "FAILED"
			job.ReasonCodes = []string{"PROVIDER_DELIVERY_UNKNOWN"}
		}
		raw, err = s.encode(job)
		if err != nil {
			return nil, err
		}
		if _, err = tx.Exec(ctx, "UPDATE "+s.schema+"."+s.table+" SET state=$3,payload=$4 WHERE instance_id=$1 AND job_id=$2", s.binding.InstanceID, job.JobID, job.State, raw); err != nil {
			return nil, safe(err)
		}
		return nil, safe(tx.Commit(ctx))
	}
	var maxConcurrent int64
	err = tx.QueryRow(ctx, "SELECT max_concurrent FROM "+s.schema+".budget WHERE instance_id=$1 AND queue_kind=$2 AND bucket=$3 AND valid_from<=$4 AND valid_until>$4", s.binding.InstanceID, s.Kind(), job.BudgetBucket, now).Scan(&maxConcurrent)
	if err != nil {
		if !errors.Is(err, pgx.ErrNoRows) {
			return nil, safe(err)
		}
		job.State = "EXPIRED"
		job.ReasonCodes = []string{"PIPELINE_BUDGET_WINDOW_CLOSED"}
		completed := now
		job.CompletedAt = &completed
		raw, err = s.encode(job)
		if err != nil {
			return nil, err
		}
		if _, err = tx.Exec(ctx, "UPDATE "+s.schema+"."+s.table+" SET state='EXPIRED',payload=$3 WHERE instance_id=$1 AND job_id=$2", s.binding.InstanceID, job.JobID, raw); err != nil {
			return nil, safe(err)
		}
		return nil, safe(tx.Commit(ctx))
	}
	var running int64
	if err = tx.QueryRow(ctx, "SELECT count(*) FROM "+s.schema+"."+s.table+" WHERE instance_id=$1 AND budget_bucket=$2 AND state IN ('CLAIMED','RUNNING') AND lease_until>$3", s.binding.InstanceID, job.BudgetBucket, now).Scan(&running); err != nil {
		return nil, safe(err)
	}
	if running >= maxConcurrent {
		return nil, nil
	}
	until := now.Add(lease)
	if until.After(job.Deadline) {
		until = job.Deadline
	}
	job.State = "CLAIMED"
	if job.ClaimedAt == nil {
		claimed := now
		job.ClaimedAt = &claimed
	}
	job.ClaimedBy = worker
	job.LeaseUntil = &until
	job.Attempt++
	raw, err = s.encode(job)
	if err != nil {
		return nil, err
	}
	if _, err = tx.Exec(ctx, "UPDATE "+s.schema+"."+s.table+" SET state='CLAIMED',claimed_by=$3,lease_until=$4,payload=$5 WHERE instance_id=$1 AND job_id=$2", s.binding.InstanceID, job.JobID, worker, until, raw); err != nil {
		return nil, safe(err)
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, safe(err)
	}
	return &job, nil
}
func (s *PipelineStore) mutate(ctx context.Context, job d.PipelineJob, now time.Time, fn func(*d.PipelineJob, pgx.Tx) error) error {
	if s.kind == "INGEST" || job.Binding != s.binding || job.QueueKind != s.Kind() || !d.UTC(now) {
		return d.Fail("PIPELINE_JOB_FORBIDDEN", 403)
	}
	if err := s.VerifyRole(ctx); err != nil {
		return err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return safe(err)
	}
	defer tx.Rollback(ctx)
	var raw []byte
	err = tx.QueryRow(ctx, "SELECT payload FROM "+s.schema+"."+s.table+" WHERE instance_id=$1 AND job_id=$2 AND claimed_by=$3 AND state IN ('CLAIMED','RUNNING') AND lease_until>$4 FOR UPDATE", s.binding.InstanceID, job.JobID, job.ClaimedBy, now).Scan(&raw)
	if errors.Is(err, pgx.ErrNoRows) {
		return d.Fail("PIPELINE_LEASE_LOST", 409)
	}
	if err != nil {
		return safe(err)
	}
	var current d.PipelineJob
	if err = s.decode(raw, &current); err != nil {
		return err
	}
	if current.Binding != s.binding || current.Attempt != job.Attempt || current.QueueKind != s.Kind() {
		return d.Fail("PIPELINE_LEASE_LOST", 409)
	}
	if err = fn(&current, tx); err != nil {
		return err
	}
	raw, err = s.encode(current)
	if err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, "UPDATE "+s.schema+"."+s.table+" SET state=$3,candidate_present=$4,payload=$5 WHERE instance_id=$1 AND job_id=$2", s.binding.InstanceID, job.JobID, current.State, current.Candidate != nil, raw); err != nil {
		return safe(err)
	}
	return safe(tx.Commit(ctx))
}
func (s *PipelineStore) StartProvider(ctx context.Context, job d.PipelineJob, now time.Time) error {
	return s.mutate(ctx, job, now, func(current *d.PipelineJob, _ pgx.Tx) error {
		if current.State != "CLAIMED" || current.Candidate != nil {
			return d.Fail("PIPELINE_PROVIDER_ALREADY_STARTED", 409)
		}
		current.State = "RUNNING"
		started := now
		current.StartedAt = &started
		return nil
	})
}
func (s *PipelineStore) SaveCandidate(ctx context.Context, job d.PipelineJob, candidate d.AnalysisCandidate, now time.Time) error {
	return s.mutate(ctx, job, now, func(current *d.PipelineJob, _ pgx.Tx) error {
		if current.State != "RUNNING" || candidate.RequestID != current.Request.RequestID || candidate.ManifestHash != current.Request.Routing.ManifestHash || candidate.CompletedAt.After(now) {
			return d.Fail("PIPELINE_CANDIDATE_INVALID", 422)
		}
		if current.Candidate != nil && d.Digest(current.Candidate) != d.Digest(candidate) {
			return d.Fail("PIPELINE_CANDIDATE_IMMUTABLE", 409)
		}
		current.Candidate = &candidate
		return nil
	})
}
func (s *PipelineStore) Complete(ctx context.Context, job d.PipelineJob, state string, reasons []string, now time.Time) error {
	if !d.Has([]string{"ABSTAINED", "EXPIRED", "FAILED", "COMPLETED"}, state) || !d.Codes(reasons) {
		return d.Fail("PIPELINE_COMPLETION_INVALID", 422)
	}
	return s.mutate(ctx, job, now, func(current *d.PipelineJob, _ pgx.Tx) error {
		current.State = state
		completed := now
		current.CompletedAt = &completed
		current.ReasonCodes = reasons
		return nil
	})
}
func (s *PipelineStore) SaveOutbox(ctx context.Context, job d.PipelineJob, row d.SubmissionOutbox, now time.Time) error {
	return s.mutate(ctx, job, now, func(current *d.PipelineJob, tx pgx.Tx) error {
		if current.Request.VerifyEligibilityWindow() != nil || d.Digest(row.EligibilityWindow) != d.Digest(current.Request.EligibilityWindow) || !row.ExpiresAt.Equal(current.Deadline) {
			return d.Fail("PIPELINE_OUTBOX_INVALID", 422)
		}
		if current.Candidate == nil || row.Binding != s.binding || row.JobID != current.JobID || row.QueueKind != s.Kind() || row.DeliveryState != "PENDING" || row.Receipt != nil ||
			row.CandidateHash != d.Digest(current.Candidate) || row.Command.Score.SubmissionID != row.OutboxID || row.Command.Score.ObjectID != current.Request.ObjectID ||
			row.Command.Score.EventID != current.Request.Event.EventID || row.Command.Score.InputManifestHash != current.Request.Routing.ManifestHash ||
			d.Digest(row.Command.Score.Vector) != d.Digest(current.Candidate.Vector) || !now.Before(row.ExpiresAt) || row.ExpiresAt.After(current.Deadline) {
			return d.Fail("PIPELINE_OUTBOX_INVALID", 422)
		}
		command, score := row.Command.Command, row.Command.Score
		r := current.Request
		if command.SchemaVersion != "strategy-2.0" || command.RequestID != row.OutboxID || command.IdempotencyKey != row.OutboxID || command.ExpectedVersion < 0 || !d.Codes([]string{command.Reason}) ||
			score.FactVersion != r.Event.FactVersion || score.ScoreVersion != r.ScoreVersion || score.RevisionKind != r.RevisionKind || d.Digest(score.PreviousScoreID) != d.Digest(r.PreviousScoreID) ||
			score.RubricVersion != r.RubricVersion || score.CalibrationVersion != r.CalibrationVersion || score.ProducerVersion != current.Candidate.ProducerVersion || !score.CompletedAt.Equal(current.Candidate.CompletedAt) ||
			len(score.EvidenceRefs) != 1 || score.EvidenceRefs[0] != r.Evidence.Raw.EvidenceID || !d.ValidID(score.ProducerID) || !d.UTC(row.CreatedAt) || row.CreatedAt.After(now) {
			return d.Fail("PIPELINE_OUTBOX_INVALID", 422)
		}
		raw, err := s.encode(row)
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, "INSERT INTO "+s.schema+"."+s.outbox+" VALUES($1,$2,$3,$4,'PENDING',$5,$6)", s.binding.InstanceID, row.OutboxID, current.JobID, row.CandidateHash, row.ExpiresAt, raw)
		if err != nil {
			return safe(err)
		}
		current.State = "COMPLETED"
		completed := now
		current.CompletedAt = &completed
		current.ReasonCodes = []string{}
		return nil
	})
}
func (s *PipelineStore) Outboxes(ctx context.Context, limit int) ([]d.SubmissionOutbox, error) {
	if s.kind == "INGEST" || limit <= 0 {
		return nil, d.Fail("PIPELINE_OUTBOX_FORBIDDEN", 403)
	}
	if err := s.VerifyRole(ctx); err != nil {
		return nil, err
	}
	rows, err := s.pool.Query(ctx, "SELECT payload FROM "+s.schema+"."+s.outbox+" WHERE instance_id=$1 AND state IN ('PENDING','DELIVERY_UNKNOWN') ORDER BY outbox_id LIMIT $2", s.binding.InstanceID, limit)
	if err != nil {
		return nil, safe(err)
	}
	defer rows.Close()
	result := []d.SubmissionOutbox{}
	for rows.Next() {
		var raw []byte
		if err = rows.Scan(&raw); err != nil {
			return nil, safe(err)
		}
		var row d.SubmissionOutbox
		if err = s.decode(raw, &row); err != nil {
			return nil, err
		}
		if row.Binding != s.binding || row.QueueKind != s.Kind() {
			return nil, d.Fail("PIPELINE_OUTBOX_BINDING_INVALID", 503)
		}
		result = append(result, row)
	}
	return result, safe(rows.Err())
}
func (s *PipelineStore) SetDelivery(ctx context.Context, row d.SubmissionOutbox, state string, receipt *dto.AdmissionReceipt) error {
	if s.kind == "INGEST" || row.Binding != s.binding || row.QueueKind != s.Kind() || !d.Has([]string{"DELIVERY_UNKNOWN", "ACK", "REJECTED", "EXPIRED"}, state) ||
		state == "ACK" && (receipt == nil || receipt.SubmissionID != row.OutboxID) {
		return d.Fail("PIPELINE_DELIVERY_INVALID", 422)
	}
	if err := s.VerifyRole(ctx); err != nil {
		return err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return safe(err)
	}
	defer tx.Rollback(ctx)
	var raw []byte
	if err = tx.QueryRow(ctx, "SELECT payload FROM "+s.schema+"."+s.outbox+" WHERE instance_id=$1 AND outbox_id=$2 FOR UPDATE", s.binding.InstanceID, row.OutboxID).Scan(&raw); err != nil {
		return safe(err)
	}
	var current d.SubmissionOutbox
	if err = s.decode(raw, &current); err != nil {
		return err
	}
	if current.CandidateHash != row.CandidateHash || d.Digest(current.Command) != d.Digest(row.Command) {
		return d.Fail("PIPELINE_OUTBOX_IMMUTABLE", 409)
	}
	if !d.Has([]string{"PENDING", "DELIVERY_UNKNOWN"}, current.DeliveryState) {
		if current.DeliveryState == state && d.Digest(current.Receipt) == d.Digest(receipt) {
			return nil
		}
		return d.Fail("PIPELINE_DELIVERY_IMMUTABLE", 409)
	}
	current.DeliveryState = state
	current.Receipt = receipt
	raw, err = s.encode(current)
	if err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, "UPDATE "+s.schema+"."+s.outbox+" SET state=$3,payload=$4 WHERE instance_id=$1 AND outbox_id=$2", s.binding.InstanceID, row.OutboxID, state, raw); err != nil {
		return safe(err)
	}
	if state == "ACK" {
		var jobRaw []byte
		if err = tx.QueryRow(ctx, "SELECT payload FROM "+s.schema+"."+s.table+" WHERE instance_id=$1 AND job_id=$2 FOR UPDATE", s.binding.InstanceID, current.JobID).Scan(&jobRaw); err != nil {
			return safe(err)
		}
		var job d.PipelineJob
		if err = s.decode(jobRaw, &job); err != nil {
			return err
		}
		if job.Binding != s.binding || job.State != "COMPLETED" || job.JobID != row.JobID || job.Candidate == nil || d.Digest(job.Candidate) != row.CandidateHash {
			return d.Fail("PIPELINE_RECEIPT_BINDING_INVALID", 503)
		}
		ref := receipt.SubmissionID
		job.ReceiptRef = &ref
		jobRaw, err = s.encode(job)
		if err != nil {
			return err
		}
		if _, err = tx.Exec(ctx, "UPDATE "+s.schema+"."+s.table+" SET payload=$3 WHERE instance_id=$1 AND job_id=$2", s.binding.InstanceID, job.JobID, jobRaw); err != nil {
			return safe(err)
		}
	}
	return safe(tx.Commit(ctx))
}

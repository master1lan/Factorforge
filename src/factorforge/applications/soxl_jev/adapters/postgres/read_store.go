// Package postgres provides a bounded, physically separated read projection.
// The connected API login must be read-only, non-superuser and environment
// bound. Publication uses a separate administrative connection; no API holds it.
package postgres

import (
	"bytes"
	"context"
	"embed"
	"encoding/json"
	"errors"
	d "github.com/AceNanako0721/Factorforge/src/factorforge/applications/soxl_jev/domain"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"io"
	"strings"
)

//go:embed migrations/001_read_projection.sql
var files embed.FS

type Store struct {
	pool    *pgxpool.Pool
	binding d.Binding
	schema  string
}

func schema(b d.Binding) (string, error) {
	if !b.Valid() {
		return "", d.Fail("INSTANCE_BINDING_INVALID", 422)
	}
	return "instance_" + strings.ToLower(b.Environment), nil
}
func safe(err error) error {
	if err == nil {
		return nil
	}
	var e *d.Error
	if errors.As(err, &e) {
		return e
	}
	return d.Fail("INSTANCE_STORE_UNAVAILABLE", 503)
}
func Initialize(ctx context.Context, dsn string, b d.Binding) error {
	name, err := schema(b)
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
	raw, err := files.ReadFile("migrations/001_read_projection.sql")
	if err != nil {
		return d.Fail("MIGRATION_UNAVAILABLE", 503)
	}
	sql := strings.ReplaceAll(strings.ReplaceAll(string(raw), "__SCHEMA__", name), "__ENV__", b.Environment)
	if _, err = tx.Exec(ctx, sql); err != nil {
		return safe(err)
	}
	role := "factorforge_instance_" + strings.ToLower(b.Environment) + "_read"
	var exists bool
	if err = tx.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname=$1)", role).Scan(&exists); err != nil {
		return safe(err)
	}
	if !exists {
		if _, err = tx.Exec(ctx, "CREATE ROLE "+pgx.Identifier{role}.Sanitize()+" NOLOGIN"); err != nil {
			return safe(err)
		}
	}
	for _, stmt := range []string{"GRANT USAGE ON SCHEMA " + name + " TO " + role, "GRANT SELECT ON ALL TABLES IN SCHEMA " + name + " TO " + role, "REVOKE INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER ON ALL TABLES IN SCHEMA " + name + " FROM " + role} {
		if _, err = tx.Exec(ctx, stmt); err != nil {
			return safe(err)
		}
	}
	return safe(tx.Commit(ctx))
}
func Open(ctx context.Context, dsn string, b d.Binding) (*Store, error) {
	name, err := schema(b)
	if err != nil {
		return nil, err
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, safe(err)
	}
	s := &Store{pool, b, name}
	if err = s.VerifyRole(ctx); err != nil {
		s.Close()
		return nil, err
	}
	return s, nil
}
func (s *Store) Close()             { s.pool.Close() }
func (s *Store) Binding() d.Binding { return s.binding }
func (s *Store) VerifyRole(ctx context.Context) error {
	role := "factorforge_instance_" + strings.ToLower(s.binding.Environment) + "_read"
	other := "factorforge_instance_live_read"
	if s.binding.Environment == "LIVE" {
		other = "factorforge_instance_sim_read"
	}
	// Check the authenticated session_user, even if SET ROLE was attempted.
	var allowed bool
	err := s.pool.QueryRow(ctx, `SELECT r.rolcanlogin AND NOT r.rolsuper
        AND NOT r.rolbypassrls AND NOT r.rolcreaterole AND NOT r.rolcreatedb AND NOT r.rolreplication
        AND pg_has_role(session_user,$1,'MEMBER')
        AND NOT COALESCE((SELECT pg_has_role(session_user,oid,'MEMBER') FROM pg_roles WHERE rolname=$2),false)
        AND NOT EXISTS(SELECT 1 FROM pg_namespace n WHERE n.nspname NOT IN ('pg_catalog','information_schema','public')
          AND n.nspname NOT LIKE 'pg_toast%' AND n.nspname NOT LIKE 'pg_temp%'
          AND has_schema_privilege(session_user,n.oid,'CREATE'))
        AND NOT EXISTS(SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
          WHERE n.nspname NOT IN ('pg_catalog','information_schema') AND n.nspname NOT LIKE 'pg_toast%'
          AND c.relkind IN ('r','p','v','m','f')
          AND (has_any_column_privilege(session_user,c.oid,'INSERT,UPDATE')
               OR has_table_privilege(session_user,c.oid,'DELETE,TRUNCATE,TRIGGER')
               OR (n.nspname<>$3 AND has_any_column_privilege(session_user,c.oid,'SELECT'))))
        FROM pg_roles r WHERE r.rolname=session_user`, role, other, s.schema).Scan(&allowed)
	if err != nil {
		return safe(err)
	}
	if !allowed {
		return d.Fail("INSTANCE_DATABASE_ROLE_NOT_ISOLATED", 403)
	}
	err = s.pool.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM "+s.schema+".instance_read_grant WHERE authenticated_role=session_user AND instance_id=$1 AND environment=$2)", s.binding.InstanceID, s.binding.Environment).Scan(&allowed)
	if err != nil {
		return safe(err)
	}
	if !allowed {
		return d.Fail("INSTANCE_DATABASE_SCOPE_FORBIDDEN", 403)
	}
	return nil
}
func (s *Store) Read(ctx context.Context, maxBytes int) (d.Snapshot, error) {
	var state d.Snapshot
	if maxBytes < 1 {
		return state, d.Fail("QUERY_POLICY_REQUIRED", 503)
	}
	if err := s.VerifyRole(ctx); err != nil {
		return state, err
	}
	var raw []byte
	var size int
	var version int64
	err := s.pool.QueryRow(ctx, "SELECT version,octet_length(snapshot),CASE WHEN octet_length(snapshot)<=$2 THEN snapshot END FROM "+s.schema+".instance_read_snapshot WHERE instance_id=$1 AND environment=$3", s.binding.InstanceID, maxBytes, s.binding.Environment).Scan(&version, &size, &raw)
	if errors.Is(err, pgx.ErrNoRows) {
		return state, d.Fail("INSTANCE_NOT_RECORDED", 404)
	}
	if err != nil {
		return state, safe(err)
	}
	if size > maxBytes {
		return state, d.Fail("QUERY_RESOURCE_LIMIT", 503)
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if dec.Decode(&state) != nil || dec.Decode(new(any)) != io.EOF {
		return state, d.Fail("INSTANCE_RECORD_INVALID", 503)
	}
	if state.Binding != s.binding || state.Version != version {
		return state, d.Fail("INSTANCE_RECORD_INVALID", 503)
	}
	if err = state.Validate(); err != nil {
		return state, err
	}
	return state, nil
}
func (s *Store) Original(ctx context.Context, id string, version int64, maxBytes int) ([]byte, error) {
	if !d.ValidID(id) || maxBytes < 1 {
		return nil, d.Fail("QUERY_PARAMETER_INVALID", 422)
	}
	if err := s.VerifyRole(ctx); err != nil {
		return nil, err
	}
	var raw []byte
	var size *int
	var current int64
	// Joining the current snapshot rejects a licence/policy revision between the
	// service permission decision and the original read, including revocation.
	err := s.pool.QueryRow(ctx, "SELECT p.version,octet_length(e.content),CASE WHEN octet_length(e.content)<=$3 THEN e.content END FROM "+s.schema+".instance_read_snapshot p LEFT JOIN "+s.schema+".raw_evidence_content e ON e.instance_id=p.instance_id AND e.evidence_id=$2 WHERE p.instance_id=$1 AND p.environment=$4", s.binding.InstanceID, id, maxBytes, s.binding.Environment).Scan(&current, &size, &raw)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, d.Fail("INSTANCE_NOT_RECORDED", 404)
	}
	if err != nil {
		return nil, safe(err)
	}
	if current != version {
		return nil, d.Fail("QUERY_SNAPSHOT_CHANGED", 409)
	}
	if size != nil && *size > maxBytes {
		return nil, d.Fail("QUERY_RESOURCE_LIMIT", 503)
	}
	return raw, nil
}

// Publish is for the source/analysis owner or an explicit administrative import,
// not for the read API. CAS rejects overwrite and licence history rollback.
// It stores no arbitrary JSON dictionaries, provider input, addresses or secrets.
func Publish(ctx context.Context, dsn string, s d.Snapshot, expected *int64) error {
	if err := s.Validate(); err != nil {
		return err
	}
	name, err := schema(s.Binding)
	if err != nil {
		return err
	}
	raw, err := json.Marshal(s)
	if err != nil {
		return d.Fail("INSTANCE_RECORD_INVALID", 503)
	}
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return safe(err)
	}
	defer conn.Close(ctx)
	if expected == nil {
		if s.Version != 0 {
			return d.Fail("INSTANCE_VERSION_CONFLICT", 409)
		}
		tag, err := conn.Exec(ctx, "INSERT INTO "+name+".instance_read_snapshot VALUES($1,$2,$3,$4) ON CONFLICT DO NOTHING", s.Binding.InstanceID, s.Binding.Environment, s.Version, raw)
		if err != nil {
			return safe(err)
		}
		if tag.RowsAffected() != 1 {
			return d.Fail("INSTANCE_VERSION_CONFLICT", 409)
		}
		return nil
	}
	if *expected < 0 || s.Version != *expected+1 {
		return d.Fail("INSTANCE_VERSION_CONFLICT", 409)
	}
	tag, err := conn.Exec(ctx, "UPDATE "+name+".instance_read_snapshot SET version=$3,snapshot=$4 WHERE instance_id=$1 AND environment=$2 AND version=$5", s.Binding.InstanceID, s.Binding.Environment, s.Version, raw, *expected)
	if err != nil {
		return safe(err)
	}
	if tag.RowsAffected() != 1 {
		return d.Fail("INSTANCE_VERSION_CONFLICT", 409)
	}
	return nil
}
func GrantReader(ctx context.Context, dsn string, b d.Binding, login string) error {
	name, err := schema(b)
	if err != nil {
		return err
	}
	if !d.ValidID(login) {
		return d.Fail("INSTANCE_READER_INVALID", 422)
	}
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return safe(err)
	}
	defer conn.Close(ctx)
	_, err = conn.Exec(ctx, "INSERT INTO "+name+".instance_read_grant VALUES($1,$2,$3) ON CONFLICT DO NOTHING", login, b.InstanceID, b.Environment)
	return safe(err)
}

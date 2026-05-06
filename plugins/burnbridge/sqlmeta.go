package main

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/versity/versitygw/backend/meta"
	"github.com/versity/versitygw/s3err"
)

// SqlMeta is a MetadataStorer backed by SQLite.
type SqlMeta struct {
	db *sql.DB
}

type BurnJob struct {
	JobID      string
	Bucket     string
	ObjectName string
	Status     string
	CreatedAt  time.Time
	UpdatedAt  time.Time
}

func NewSqlMeta(dbPath string) (SqlMeta, error) {
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return SqlMeta{}, fmt.Errorf("open sqlite: %w", err)
	}
	if _, err := db.Exec(`PRAGMA journal_mode=WAL;`); err != nil {
		return SqlMeta{}, fmt.Errorf("set WAL mode: %w", err)
	}
	if _, err := db.Exec(`PRAGMA busy_timeout=5000;`); err != nil {
		return SqlMeta{}, fmt.Errorf("set busy_timeout: %w", err)
	}
	db.SetMaxOpenConns(4)
	db.SetMaxIdleConns(4)
	db.SetConnMaxLifetime(30 * time.Minute)

	_, err = db.Exec(`
CREATE TABLE IF NOT EXISTS metadata_entries (
	bucket TEXT NOT NULL,
	object_name TEXT NOT NULL,
	attribute TEXT NOT NULL,
	value BLOB NOT NULL,
	created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
	updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
	PRIMARY KEY (bucket, object_name, attribute)
);
CREATE INDEX IF NOT EXISTS idx_meta_bucket_object ON metadata_entries(bucket, object_name);
CREATE TABLE IF NOT EXISTS burn_jobs (
	job_id TEXT PRIMARY KEY,
	bucket TEXT NOT NULL,
	object_name TEXT NOT NULL,
	status TEXT NOT NULL,
	created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
	updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_burn_jobs_bucket_object ON burn_jobs(bucket, object_name);
`)
	if err != nil {
		return SqlMeta{}, fmt.Errorf("migrate sqlite schema: %w", err)
	}

	return SqlMeta{db: db}, nil
}

func (s SqlMeta) RetrieveAttribute(_ *os.File, bucket, object, attribute string) ([]byte, error) {
	row := s.db.QueryRow(`SELECT value FROM metadata_entries WHERE bucket=? AND object_name=? AND attribute=?`, bucket, object, attribute)
	var value []byte
	if err := row.Scan(&value); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, meta.ErrNoSuchKey
		}
		return nil, fmt.Errorf("retrieve attribute: %w", err)
	}
	return value, nil
}

func (s SqlMeta) StoreAttribute(_ *os.File, bucket, object, attribute string, value []byte) error {
	_, err := s.db.Exec(`
INSERT INTO metadata_entries(bucket, object_name, attribute, value, created_at, updated_at)
VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
ON CONFLICT(bucket, object_name, attribute)
DO UPDATE SET value=excluded.value, updated_at=CURRENT_TIMESTAMP;
`, bucket, object, attribute, value)
	if err != nil {
		return mapSQLError("store attribute", err)
	}
	return nil
}

func (s SqlMeta) DeleteAttribute(bucket, object, attribute string) error {
	res, err := s.db.Exec(`DELETE FROM metadata_entries WHERE bucket=? AND object_name=? AND attribute=?`, bucket, object, attribute)
	if err != nil {
		return mapSQLError("delete attribute", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return meta.ErrNoSuchKey
	}
	return nil
}

func (s SqlMeta) ListAttributes(bucket, object string) ([]string, error) {
	rows, err := s.db.Query(`SELECT attribute FROM metadata_entries WHERE bucket=? AND object_name=?`, bucket, object)
	if err != nil {
		return nil, mapSQLError("list attributes", err)
	}
	defer rows.Close()
	attrs := make([]string, 0)
	for rows.Next() {
		var a string
		if err := rows.Scan(&a); err != nil {
			return nil, fmt.Errorf("scan attribute: %w", err)
		}
		attrs = append(attrs, a)
	}
	return attrs, rows.Err()
}

func (s SqlMeta) DeleteAttributes(bucket, object string) error {
	if _, err := s.db.Exec(`DELETE FROM metadata_entries WHERE bucket=? AND object_name=?`, bucket, object); err != nil {
		return mapSQLError("delete attributes", err)
	}
	if object == "" {
		if _, err := s.db.Exec(`DELETE FROM metadata_entries WHERE bucket=?`, bucket); err != nil {
			return mapSQLError("delete bucket attributes", err)
		}
	}
	return nil
}

func (s SqlMeta) RenameObject(bucket, oldObject, newObject string) error {
	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("begin rename transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	_, err = tx.Exec(`UPDATE metadata_entries SET object_name=?, updated_at=CURRENT_TIMESTAMP WHERE bucket=? AND object_name=?`, newObject, bucket, oldObject)
	if err != nil {
		return mapSQLError("rename object metadata", err)
	}
	_, err = tx.Exec(`UPDATE burn_jobs SET object_name=?, updated_at=CURRENT_TIMESTAMP WHERE bucket=? AND object_name=?`, newObject, bucket, oldObject)
	if err != nil {
		return mapSQLError("rename burn job object reference", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit rename transaction: %w", err)
	}
	return nil
}

func (s SqlMeta) UpsertBurnJob(job BurnJob) error {
	if job.JobID == "" {
		return fmt.Errorf("job id is required")
	}
	if job.Status == "" {
		job.Status = "queued"
	}
	_, err := s.db.Exec(`
INSERT INTO burn_jobs(job_id, bucket, object_name, status, created_at, updated_at)
VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
ON CONFLICT(job_id)
DO UPDATE SET bucket=excluded.bucket, object_name=excluded.object_name, status=excluded.status, updated_at=CURRENT_TIMESTAMP;
`, job.JobID, job.Bucket, job.ObjectName, job.Status)
	if err != nil {
		return mapSQLError("upsert burn job", err)
	}
	return nil
}

func mapSQLError(op string, err error) error {
	if err == nil {
		return nil
	}
	msg := strings.ToLower(err.Error())
	switch {
	case strings.Contains(msg, "database or disk is full"), strings.Contains(msg, "no space left"):
		return s3err.GetAPIError(s3err.ErrNoSpaceLeftOnDevice)
	case strings.Contains(msg, "readonly"), strings.Contains(msg, "read-only"):
		return s3err.GetAPIError(s3err.ErrMethodNotAllowed)
	default:
		return fmt.Errorf("%s: %w", op, err)
	}
}

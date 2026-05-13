// Copyright 2026 Versity Software
// This file is licensed under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package meta

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"
)

const walCheckpointInterval = 60 * time.Second

// applySQLiteFlashPragmas runs industrial flash-oriented PRAGMAs and logs effective values.
func applySQLiteFlashPragmas(sqlDB *sql.DB, log *slog.Logger) {
	if log == nil {
		log = slog.Default()
	}
	stmts := []string{
		"PRAGMA journal_mode=WAL",
		"PRAGMA synchronous=NORMAL",
		"PRAGMA temp_store=MEMORY",
		"PRAGMA mmap_size=268435456",
		"PRAGMA cache_size=-65536",
		"PRAGMA page_size=4096",
		"PRAGMA auto_vacuum=NONE",
		"PRAGMA busy_timeout=5000",
	}
	for _, q := range stmts {
		if _, err := sqlDB.Exec(q); err != nil {
			// page_size / auto_vacuum may fail on existing DB; log and continue
			log.Warn("sqlite PRAGMA exec", "stmt", q, "err", err)
		} else {
			log.Info("sqlite PRAGMA applied", "stmt", q)
		}
	}

	checks := []struct {
		name string
		sql  string
	}{
		{"journal_mode", "PRAGMA journal_mode"},
		{"synchronous", "PRAGMA synchronous"},
		{"temp_store", "PRAGMA temp_store"},
		{"mmap_size", "PRAGMA mmap_size"},
		{"cache_size", "PRAGMA cache_size"},
		{"page_size", "PRAGMA page_size"},
		{"auto_vacuum", "PRAGMA auto_vacuum"},
		{"busy_timeout", "PRAGMA busy_timeout"},
	}
	for _, c := range checks {
		var v any
		if err := sqlDB.QueryRow(c.sql).Scan(&v); err != nil {
			log.Warn("sqlite PRAGMA readback failed", "pragma", c.name, "err", err)
			continue
		}
		log.Info("sqlite PRAGMA effective", "pragma", c.name, "value", fmt.Sprint(v))
	}
}

func startFlashMaintenance(ctx context.Context, sqlDB *sql.DB, log *slog.Logger) {
	if log == nil {
		log = slog.Default()
	}
	t := time.NewTicker(walCheckpointInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			log.Info("sqlite flash maintenance stopped", "reason", ctx.Err())
			return
		case <-t.C:
			if _, err := sqlDB.Exec("PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
				log.Warn("sqlite wal_checkpoint(TRUNCATE) failed", "err", err)
			} else {
				log.Info("sqlite wal_checkpoint(TRUNCATE) completed")
			}
		}
	}
}

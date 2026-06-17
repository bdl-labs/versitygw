// Copyright 2026 Versity Software
// This file is licensed under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

package meta

import (
	"database/sql"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func verifySQLiteIntegrity(sqlDB *sql.DB) (string, error) {
	rows, err := sqlDB.Query("PRAGMA integrity_check")
	if err != nil {
		return "", err
	}
	defer rows.Close()

	var results []string
	for rows.Next() {
		var value string
		if err := rows.Scan(&value); err != nil {
			return "", err
		}
		results = append(results, strings.TrimSpace(value))
	}
	if err := rows.Err(); err != nil {
		return "", err
	}
	if len(results) == 0 {
		return "", fmt.Errorf("empty integrity_check result")
	}
	result := strings.Join(results, "; ")
	if len(results) == 1 && strings.EqualFold(results[0], "ok") {
		return result, nil
	}
	return result, fmt.Errorf("sqlite integrity_check failed: %s", result)
}

func backupSQLiteDatabase(sqlDB *sql.DB, dbPath, reason string, log *slog.Logger) (string, error) {
	target, err := buildSQLiteBackupPath(dbPath, reason)
	if err != nil || target == "" {
		return target, err
	}
	escapedTarget := strings.ReplaceAll(target, "'", "''")
	if _, err := sqlDB.Exec("VACUUM INTO '" + escapedTarget + "'"); err != nil {
		return "", err
	}
	if log != nil {
		log.Info("sqlite safety backup created", "db_path", dbPath, "backup_path", target, "reason", reason)
	}
	return target, nil
}

func copySQLiteFileForDiagnostics(dbPath, reason string, log *slog.Logger) (string, error) {
	target, err := buildSQLiteBackupPath(dbPath, reason)
	if err != nil || target == "" {
		return target, err
	}

	abs, err := filepath.Abs(strings.TrimSpace(dbPath))
	if err != nil {
		return "", err
	}
	src, err := os.Open(abs)
	if err != nil {
		return "", err
	}
	defer src.Close()
	dst, err := os.OpenFile(target, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return "", err
	}
	defer dst.Close()
	if _, err := io.Copy(dst, src); err != nil {
		return "", err
	}
	if err := dst.Sync(); err != nil {
		return "", err
	}
	if log != nil {
		log.Info("sqlite diagnostic copy created", "db_path", abs, "backup_path", target, "reason", reason)
	}
	return target, nil
}

func buildSQLiteBackupPath(dbPath, reason string) (string, error) {
	trimmedPath := strings.TrimSpace(dbPath)
	if trimmedPath == "" {
		return "", nil
	}
	if _, err := os.Stat(trimmedPath); err != nil {
		if os.IsNotExist(err) {
			return "", nil
		}
		return "", err
	}

	abs, err := filepath.Abs(trimmedPath)
	if err != nil {
		return "", err
	}
	backupDir := filepath.Join(filepath.Dir(abs), "sqlite-safety-backups")
	if err := os.MkdirAll(backupDir, 0o755); err != nil {
		return "", err
	}
	base := strings.TrimSuffix(filepath.Base(abs), filepath.Ext(abs))
	return filepath.Join(backupDir, fmt.Sprintf("%s--%s--%s.sqlite3", base, sanitizeSQLiteBackupReason(reason), time.Now().UTC().Format("20060102T150405.000000000Z"))), nil
}

func sanitizeSQLiteBackupReason(reason string) string {
	trimmed := strings.TrimSpace(reason)
	if trimmed == "" {
		return "backup"
	}
	replacer := strings.NewReplacer("\\", "-", "/", "-", ":", "-", "*", "-", "?", "-", "\"", "-", "<", "-", ">", "-", "|", "-", " ", "-")
	return replacer.Replace(trimmed)
}

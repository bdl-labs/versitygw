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
	"fmt"
	"path/filepath"
)

// buildSQLiteDSN returns a SQLite URI suitable for gorm.io/driver/sqlite and mattn/go-sqlite3:
// underscore query parameters set connection PRAGMAs early (WAL, NORMAL sync, cache, busy).
//
// Note: gorm.io/driver/sqlite defaults to modernc.org/sqlite unless built with CGO and the
// mattn driver; both accept this file: URI form.
//
// Values match post-open PRAGMAs in applySQLiteFlashPragmas where applicable (harmless redundancy).
func buildSQLiteDSN(dbPath string) (string, error) {
	abs, err := filepath.Abs(dbPath)
	if err != nil {
		return "", fmt.Errorf("sqlite abs path: %w", err)
	}
	abs = filepath.ToSlash(abs)
	if abs == "" {
		return "", fmt.Errorf("empty sqlite path")
	}
	// Windows: C:/path -> file:/C:/path
	if abs[0] != '/' {
		abs = "/" + abs
	}
	q := "_journal_mode=WAL&_synchronous=NORMAL&_cache_size=-65536&_busy_timeout=5000"
	return "file:" + abs + "?" + q, nil
}

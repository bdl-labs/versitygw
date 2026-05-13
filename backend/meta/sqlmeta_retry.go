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
	"strings"
	"time"
)

func isSQLiteBusy(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "database is locked") || strings.Contains(msg, "busy")
}

// sqliteWithRetry retries fn on transient SQLITE_BUSY with bounded exponential backoff.
func sqliteWithRetry(op string, fn func() error) error {
	const maxAttempts = 12
	backoff := 5 * time.Millisecond
	var last error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		last = fn()
		if last == nil {
			return nil
		}
		if !isSQLiteBusy(last) {
			return last
		}
		time.Sleep(backoff)
		if backoff < 500*time.Millisecond {
			backoff *= 2
		}
	}
	return last
}

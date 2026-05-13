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

package s3log

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"gopkg.in/natefinch/lumberjack.v2"
)

func newFlashFileLogger(path string) (*FileLogger, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("mkdir flash log dir: %w", err)
	}
	lj := &lumberjack.Logger{
		Filename:   path,
		MaxSize:    50, // megabytes
		MaxBackups: 3,
		LocalTime:  true,
	}
	if _, err := fmt.Fprintf(lj, "log starts %v\n", time.Now()); err != nil {
		_ = lj.Close()
		return nil, fmt.Errorf("flash log header: %w", err)
	}
	fl := &FileLogger{logfile: path, w: lj}
	fl.closeFn = func() error { return lj.Close() }
	fl.rotateFn = func() error { return lj.Rotate() }
	return fl, nil
}

// InitFlashFileLogger opens a RAM-backed audit log with lumberjack rotation (50MB x 3 backups).
func InitFlashFileLogger(path string) (AuditLogger, error) {
	return newFlashFileLogger(path)
}

// InitFlashAdminFileLogger is like InitFlashFileLogger for the admin access log.
func InitFlashAdminFileLogger(path string) (AuditLogger, error) {
	fl, err := newFlashFileLogger(path)
	if err != nil {
		return nil, err
	}
	return &AdminFileLogger{FileLogger: *fl}, nil
}

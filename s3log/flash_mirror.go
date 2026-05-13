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
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const flashLogMirrorInterval = 60 * time.Second

// runFlashLogMirror periodically appends new data from shmDir/*.log into dstDir (no per-write fsync).
func runFlashLogMirror(ctx context.Context, shmDir, dstDir string, log *slog.Logger) {
	if log == nil {
		log = slog.Default()
	}
	t := time.NewTicker(flashLogMirrorInterval)
	defer t.Stop()

	var mu sync.Mutex
	offsets := make(map[string]int64)

	copyAll := func() {
		entries, err := os.ReadDir(shmDir)
		if err != nil {
			log.Warn("flash log mirror: read shm log dir", "dir", shmDir, "err", err)
			return
		}
		for _, e := range entries {
			if e.IsDir() {
				continue
			}
			name := e.Name()
			if !strings.HasSuffix(strings.ToLower(name), ".log") {
				continue
			}
			shmPath := filepath.Join(shmDir, name)
			mu.Lock()
			off := offsets[shmPath]
			mu.Unlock()

			n, err := appendCopyLog(shmPath, filepath.Join(dstDir, name), off)
			if err != nil {
				log.Warn("flash log mirror: append copy failed", "src", shmPath, "err", err)
				time.Sleep(2 * time.Second)
				continue
			}
			mu.Lock()
			if n >= 0 {
				offsets[shmPath] = n
			}
			mu.Unlock()
		}
	}

	for {
		select {
		case <-ctx.Done():
			log.Info("flash log mirror stopped", "reason", ctx.Err())
			return
		case <-t.C:
			copyAll()
		}
	}
}

// appendCopyLog reads src from startOff and appends to dst; returns new offset (src size) or -1 on error.
func appendCopyLog(src, dst string, startOff int64) (int64, error) {
	sf, err := os.Open(src)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return -1, err
	}
	defer sf.Close()

	st, err := sf.Stat()
	if err != nil {
		return -1, err
	}
	size := st.Size()
	if size < startOff {
		startOff = 0
	}
	if size == startOff {
		return size, nil
	}
	if _, err := sf.Seek(startOff, io.SeekStart); err != nil {
		return -1, err
	}

	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return -1, err
	}
	df, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return -1, err
	}
	defer df.Close()

	buf := make([]byte, 256*1024)
	remain := size - startOff
	for remain > 0 {
		n := len(buf)
		if int64(n) > remain {
			n = int(remain)
		}
		nr, er := sf.Read(buf[:n])
		if nr > 0 {
			_, ew := df.Write(buf[:nr])
			if ew != nil {
				return -1, ew
			}
			remain -= int64(nr)
		}
		if er != nil {
			if er == io.EOF {
				break
			}
			return -1, er
		}
		if nr == 0 {
			break
		}
	}
	return size, nil
}

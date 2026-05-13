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

// Package sysflash provides optional host hints for flash-backed Linux deployments (e.g. Raspberry Pi + eMMC).
package sysflash

import (
	"bufio"
	"log/slog"
	"os"
	"runtime"
	"strings"
)

// PrintStartupHints logs mount and journald recommendations. Safe no-op on non-Linux.
func PrintStartupHints(log *slog.Logger) {
	if log == nil {
		log = slog.Default()
	}
	log.Info("flash/eMMC tuning hint: configure /etc/systemd/journald.conf - Storage=volatile and RuntimeMaxUse=100M to reduce eMMC journal writes")

	if runtime.GOOS != "linux" {
		return
	}
	f, err := os.Open("/proc/mounts")
	if err != nil {
		log.Warn("could not read /proc/mounts for noatime check", "err", err)
		return
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Text()
		fields := strings.Fields(line)
		if len(fields) < 4 {
			continue
		}
		fstype := fields[2]
		opts := fields[3]
		mp := fields[1]
		if fstype != "ext4" {
			continue
		}
		if !strings.Contains(opts, "noatime") || !strings.Contains(opts, "nodiratime") {
			log.Warn("ext4 mount may increase flash wear: enable noatime,nodiratime where appropriate",
				"mountpoint", mp, "opts", opts)
		}
	}
	if err := sc.Err(); err != nil {
		log.Warn("read /proc/mounts", "err", err)
	}
}

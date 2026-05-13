// Copyright 2023 Versity Software
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
	"crypto/tls"
	"encoding/hex"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/gofiber/fiber/v2"
)

type AuditLogger interface {
	Log(ctx *fiber.Ctx, err error, body []byte, meta LogMeta)
	HangUp() error
	Shutdown() error
}

type LogMeta struct {
	BucketOwner string
	ObjectSize  int64
	Action      string
	HttpStatus  int
}

type LogConfig struct {
	LogFile      string
	WebhookURL   string
	AdminLogFile string

	// FlashEmmcOptimized stores access/admin logs under FlashLogDir (default /dev/shm/app-log)
	// with lumberjack rotation and optional background mirror to FlashMirrorDir.
	FlashEmmcOptimized bool
	FlashLogDir        string
	FlashMirrorDir     string
	MirrorCtx          context.Context
	MirrorLog          *slog.Logger
}

type LogFields struct {
	BucketOwner        string
	Bucket             string
	Time               time.Time
	RemoteIP           string
	Requester          string
	RequestID          string
	Operation          string
	Key                string
	RequestURI         string
	HttpStatus         int
	ErrorCode          string
	BytesSent          int
	ObjectSize         int64
	TotalTime          int64
	TurnAroundTime     int64
	Referer            string
	UserAgent          string
	VersionID          string
	HostID             string
	SignatureVersion   string
	CipherSuite        string
	AuthenticationType string
	HostHeader         string
	TLSVersion         string
	AccessPointARN     string
	AclRequired        string
}

type AdminLogFields struct {
	Time               time.Time
	RemoteIP           string
	Requester          string
	RequestID          string
	Operation          string
	RequestURI         string
	HttpStatus         int
	ErrorCode          string
	BytesSent          int
	TotalTime          int64
	TurnAroundTime     int64
	Referer            string
	UserAgent          string
	SignatureVersion   string
	CipherSuite        string
	AuthenticationType string
	TLSVersion         string
}

type Loggers struct {
	S3Logger    AuditLogger
	AdminLogger AuditLogger

	flashMirrorCancel context.CancelFunc
	flashMirrorWG     sync.WaitGroup
}

// StopFlashLogMirror ends the background RAM to disk log mirror and waits for exit.
func (l *Loggers) StopFlashLogMirror() {
	if l == nil || l.flashMirrorCancel == nil {
		return
	}
	l.flashMirrorCancel()
	l.flashMirrorWG.Wait()
	l.flashMirrorCancel = nil
}

func InitLogger(cfg *LogConfig) (*Loggers, error) {
	if cfg.WebhookURL != "" && cfg.LogFile != "" {
		return nil, fmt.Errorf("there should be specified one of the following: file, webhook")
	}
	loggers := &Loggers{}

	shmDir := cfg.FlashLogDir
	if shmDir == "" {
		shmDir = "/dev/shm/app-log"
	}
	startMirror := cfg.FlashEmmcOptimized && cfg.MirrorCtx != nil && (cfg.LogFile != "" || cfg.AdminLogFile != "")

	switch {
	case cfg.WebhookURL != "":
		fmt.Printf("initializing S3 access logs with '%v' webhook url\n", cfg.WebhookURL)
		l, err := InitWebhookLogger(cfg.WebhookURL)
		if err != nil {
			return nil, err
		}
		loggers.S3Logger = l
	case cfg.LogFile != "":
		if cfg.FlashEmmcOptimized {
			if err := os.MkdirAll(shmDir, 0o755); err != nil {
				return nil, fmt.Errorf("flash log shm dir: %w", err)
			}
			ramPath := filepath.Join(shmDir, filepath.Base(cfg.LogFile))
			fmt.Printf("initializing S3 access logs (flash/RAM) '%v'\n", ramPath)
			l, err := InitFlashFileLogger(ramPath)
			if err != nil {
				return nil, err
			}
			loggers.S3Logger = l
		} else {
			fmt.Printf("initializing S3 access logs with '%v' file\n", cfg.LogFile)
			l, err := InitFileLogger(cfg.LogFile)
			if err != nil {
				return nil, err
			}
			loggers.S3Logger = l
		}
	}

	if cfg.AdminLogFile != "" {
		if cfg.FlashEmmcOptimized {
			if err := os.MkdirAll(shmDir, 0o755); err != nil {
				return nil, fmt.Errorf("flash log shm dir: %w", err)
			}
			ramPath := filepath.Join(shmDir, filepath.Base(cfg.AdminLogFile))
			fmt.Printf("initializing admin access logs (flash/RAM) '%v'\n", ramPath)
			l, err := InitFlashAdminFileLogger(ramPath)
			if err != nil {
				return nil, err
			}
			loggers.AdminLogger = l
		} else {
			fmt.Printf("initializing admin access logs with '%v' file\n", cfg.AdminLogFile)
			l, err := InitAdminFileLogger(cfg.AdminLogFile)
			if err != nil {
				return nil, err
			}
			loggers.AdminLogger = l
		}
	}

	if startMirror {
		dstDir := cfg.FlashMirrorDir
		if dstDir == "" {
			dstDir = "/data/logs"
		}
		if err := os.MkdirAll(dstDir, 0o755); err != nil {
			return nil, fmt.Errorf("flash log mirror dir: %w", err)
		}
		mctx, cancel := context.WithCancel(cfg.MirrorCtx)
		loggers.flashMirrorCancel = cancel
		loggers.flashMirrorWG.Add(1)
		log := cfg.MirrorLog
		if log == nil {
			log = slog.Default()
		}
		go func() {
			defer loggers.flashMirrorWG.Done()
			runFlashLogMirror(mctx, shmDir, dstDir, log)
		}()
	}

	return loggers, nil
}

func genID() string {
	src := rand.New(rand.NewSource(time.Now().UnixNano()))
	b := make([]byte, 8)

	if _, err := src.Read(b); err != nil {
		panic(err)
	}

	return strings.ToUpper(hex.EncodeToString(b))
}

func getTLSVersionName(version uint16) string {
	switch version {
	case tls.VersionTLS10:
		return "TLSv1.0"
	case tls.VersionTLS11:
		return "TLSv1.1"
	case tls.VersionTLS12:
		return "TLSv1.2"
	case tls.VersionTLS13:
		return "TLSv1.3"
	default:
		return ""
	}
}

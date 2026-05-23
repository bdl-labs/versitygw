package main

import (
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/versity/versitygw/archiveconfig"
)

func TestArchiveLogsListAndDownload(t *testing.T) {
	tempDir := mustTempDir(t)
	defer removeTempDir(t, tempDir)
	configPath := filepath.Join(tempDir, "optical-archive.config.json")
	t.Setenv("OPTICAL_ARCHIVE_CONFIG_PATH", configPath)

	app := fiber.New()
	defer func() { _ = app.Shutdown() }()
	app.Get("/__archive/logs", archiveLogsListHandler())
	app.Get("/__archive/logs/download", archiveLogsDownloadHandler())
	app.Post("/__archive/logs/rotate", archiveLogsRotateHandler())

	cfg, _, err := authorizeArchiveConfigRequestForTest(configPath)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	cfg.OpticalArchive.Logging.Gateway.AccessLogPath = filepath.Join(tempDir, "gateway", "gateway-access.log")
	cfg.OpticalArchive.Logging.Gateway.AdminLogPath = filepath.Join(tempDir, "gateway", "gateway-admin.log")
	if err := archiveconfig.Save(configPath, cfg); err != nil {
		t.Fatalf("save config: %v", err)
	}

	if err := os.MkdirAll(filepath.Dir(cfg.OpticalArchive.Logging.Gateway.AccessLogPath), 0o755); err != nil {
		t.Fatalf("mkdir log dir: %v", err)
	}
	content := strings.Repeat("A", 2048)
	if err := os.WriteFile(cfg.OpticalArchive.Logging.Gateway.AccessLogPath, []byte(content), 0o644); err != nil {
		t.Fatalf("write access log: %v", err)
	}

	authHeader := "Basic " + base64.StdEncoding.EncodeToString([]byte("admin:admin123"))

	req := httptest.NewRequest(http.MethodGet, "/__archive/logs", nil)
	req.Header.Set("Authorization", authHeader)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("list logs request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("list logs status=%d", resp.StatusCode)
	}

	var payload struct {
		Items []archiveLogEntry `json:"items"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	_ = resp.Body.Close()
	if len(payload.Items) == 0 {
		t.Fatalf("expected log items")
	}

	var found bool
	for _, item := range payload.Items {
		if item.Source == "gateway" && item.RelativePath == "gateway-access.log" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected gateway-access.log in payload: %#v", payload.Items)
	}

	req = httptest.NewRequest(http.MethodGet, "/__archive/logs/download?source=gateway&path=gateway-access.log", nil)
	req.Header.Set("Authorization", authHeader)
	resp, err = app.Test(req)
	if err != nil {
		t.Fatalf("download log request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("download log status=%d", resp.StatusCode)
	}
	if _, err := io.ReadAll(resp.Body); err != nil {
		t.Fatalf("read download body: %v", err)
	}
	_ = resp.Body.Close()
}

func TestArchiveLogsRotateCreatesArchive(t *testing.T) {
	tempDir := mustTempDir(t)
	defer removeTempDir(t, tempDir)
	configPath := filepath.Join(tempDir, "optical-archive.config.json")
	t.Setenv("OPTICAL_ARCHIVE_CONFIG_PATH", configPath)

	cfg, _, err := authorizeArchiveConfigRequestForTest(configPath)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	cfg.OpticalArchive.Logging.Gateway.AccessLogPath = filepath.Join(tempDir, "gateway", "gateway-access.log")
	cfg.OpticalArchive.Logging.Gateway.AdminLogPath = filepath.Join(tempDir, "gateway", "gateway-admin.log")
	cfg.OpticalArchive.Logging.Gateway.FileSizeMb = 1
	if err := archiveconfig.Save(configPath, cfg); err != nil {
		t.Fatalf("save config: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(cfg.OpticalArchive.Logging.Gateway.AccessLogPath), 0o755); err != nil {
		t.Fatalf("mkdir log dir: %v", err)
	}
	largeContent := strings.Repeat("B", 2*1024*1024)
	if err := os.WriteFile(cfg.OpticalArchive.Logging.Gateway.AccessLogPath, []byte(largeContent), 0o644); err != nil {
		t.Fatalf("write large log: %v", err)
	}

	if err := archiveGatewayLogs(cfg); err != nil {
		t.Fatalf("archive gateway logs: %v", err)
	}

	archiveDir := filepath.Join(filepath.Dir(cfg.OpticalArchive.Logging.Gateway.AccessLogPath), "archive")
	entries, err := os.ReadDir(archiveDir)
	if err != nil {
		t.Fatalf("read archive dir: %v", err)
	}
	if len(entries) == 0 {
		t.Fatalf("expected archived log file")
	}
}

func authorizeArchiveConfigRequestForTest(configPath string) (cfg archiveconfig.File, path string, err error) {
	return archiveconfig.Load(configPath)
}

func mustTempDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "archive-logs-test-*")
	if err != nil {
		t.Fatalf("mkdir temp: %v", err)
	}
	return dir
}

func removeTempDir(t *testing.T, dir string) {
	t.Helper()
	var err error
	for i := 0; i < 10; i++ {
		err = os.RemoveAll(dir)
		if err == nil {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("remove temp dir %s: %v", dir, err)
}

package main

import (
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/versity/versitygw/archiveconfig"
	"github.com/versity/versitygw/s3api"
)

type archiveLogEntry struct {
	Source       string `json:"source"`
	FileName     string `json:"fileName"`
	FullPath     string `json:"fullPath"`
	RelativePath string `json:"relativePath"`
	SizeBytes    int64  `json:"sizeBytes"`
	ModifiedUtc  string `json:"modifiedUtc"`
	Sha256       string `json:"sha256,omitempty"`
}

func archiveLogRouteOptions() []s3api.Option {
	var options []s3api.Option
	options = append(options, archiveRoute(http.MethodGet, "/__archive/logs", archiveLogsListHandler())...)
	options = append(options, archiveRoute(http.MethodGet, "/__archive/logs/download", archiveLogsDownloadHandler())...)
	options = append(options, archiveRoute(http.MethodPost, "/__archive/logs/rotate", archiveLogsRotateHandler())...)
	return options
}

func archiveLogsListHandler() fiber.Handler {
	return func(c *fiber.Ctx) error {
		cfg, _, ok, err := authorizeArchiveConfigRequest(c)
		if err != nil {
			return writeArchiveErrorFrom(c, err, http.StatusInternalServerError)
		}
		if !ok {
			return writeArchiveConfigUnauthorized(c)
		}

		if err := archiveGatewayLogs(cfg); err != nil {
			return writeArchiveError(c, http.StatusInternalServerError, err.Error())
		}

		entries, err := collectArchiveLogEntries(cfg)
		if err != nil {
			return writeArchiveError(c, http.StatusInternalServerError, err.Error())
		}

		return c.JSON(map[string]any{
			"items": entries,
		})
	}
}

func archiveLogsDownloadHandler() fiber.Handler {
	return func(c *fiber.Ctx) error {
		cfg, _, ok, err := authorizeArchiveConfigRequest(c)
		if err != nil {
			return writeArchiveErrorFrom(c, err, http.StatusInternalServerError)
		}
		if !ok {
			return writeArchiveConfigUnauthorized(c)
		}

		source := strings.TrimSpace(c.Query("source"))
		relativePath := strings.TrimSpace(c.Query("path"))
		if source == "" || relativePath == "" {
			return writeArchiveError(c, http.StatusBadRequest, "source and path are required")
		}

		fullPath, err := resolveArchiveLogPath(cfg, source, relativePath)
		if err != nil {
			return writeArchiveError(c, http.StatusBadRequest, err.Error())
		}

		data, err := os.ReadFile(fullPath)
		if err != nil {
			return writeArchiveError(c, http.StatusInternalServerError, err.Error())
		}

		c.Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", filepath.Base(fullPath)))
		return c.Send(data)
	}
}

func archiveLogsRotateHandler() fiber.Handler {
	return func(c *fiber.Ctx) error {
		cfg, _, ok, err := authorizeArchiveConfigRequest(c)
		if err != nil {
			return writeArchiveErrorFrom(c, err, http.StatusInternalServerError)
		}
		if !ok {
			return writeArchiveConfigUnauthorized(c)
		}

		if err := archiveGatewayLogs(cfg); err != nil {
			return writeArchiveError(c, http.StatusInternalServerError, err.Error())
		}

		return c.JSON(map[string]any{
			"rotated": true,
		})
	}
}

func collectArchiveLogEntries(cfg archiveconfig.File) ([]archiveLogEntry, error) {
	var entries []archiveLogEntry

	appendEntries := func(source string, roots []string) error {
		for _, root := range roots {
			root = strings.TrimSpace(root)
			if root == "" {
				continue
			}

			info, err := os.Stat(root)
			if err != nil {
				if os.IsNotExist(err) {
					continue
				}
				return fmt.Errorf("stat log root %s: %w", root, err)
			}

			if info.IsDir() {
				err = filepath.WalkDir(root, func(path string, d os.DirEntry, walkErr error) error {
					if walkErr != nil {
						return walkErr
					}
					if d.IsDir() {
						return nil
					}
					entry, buildErr := buildArchiveLogEntry(source, root, path)
					if buildErr != nil {
						return buildErr
					}
					entries = append(entries, entry)
					return nil
				})
				if err != nil {
					return err
				}
				continue
			}

			entry, err := buildArchiveLogEntry(source, filepath.Dir(root), root)
			if err != nil {
				return err
			}
			entries = append(entries, entry)
		}

		return nil
	}

	if err := appendEntries("gateway", gatewayLogRoots(cfg)); err != nil {
		return nil, err
	}
	if err := appendEntries("recorder", recorderLogRoots(cfg)); err != nil {
		return nil, err
	}

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].ModifiedUtc > entries[j].ModifiedUtc
	})

	return entries, nil
}

func buildArchiveLogEntry(source, baseRoot, fullPath string) (archiveLogEntry, error) {
	info, err := os.Stat(fullPath)
	if err != nil {
		return archiveLogEntry{}, err
	}

	relativePath, err := filepath.Rel(baseRoot, fullPath)
	if err != nil {
		relativePath = filepath.Base(fullPath)
	}

	entry := archiveLogEntry{
		Source:       source,
		FileName:     filepath.Base(fullPath),
		FullPath:     fullPath,
		RelativePath: filepath.ToSlash(relativePath),
		SizeBytes:    info.Size(),
		ModifiedUtc:  info.ModTime().UTC().Format(time.RFC3339),
	}

	if strings.HasSuffix(strings.ToLower(fullPath), ".gz") {
		hash, err := fileSha256(fullPath)
		if err == nil {
			entry.Sha256 = hash
		}
	}

	return entry, nil
}

func resolveArchiveLogPath(cfg archiveconfig.File, source, relativePath string) (string, error) {
	relativePath = filepath.Clean(strings.ReplaceAll(relativePath, "/", string(filepath.Separator)))
	if relativePath == "." || strings.HasPrefix(relativePath, "..") {
		return "", fmt.Errorf("invalid log path")
	}

	var roots []string
	switch strings.ToLower(source) {
	case "gateway":
		roots = gatewayLogRoots(cfg)
	case "recorder":
		roots = recorderLogRoots(cfg)
	default:
		return "", fmt.Errorf("unknown log source %q", source)
	}

	for _, root := range roots {
		root = strings.TrimSpace(root)
		if root == "" {
			continue
		}
		base := root
		if info, err := os.Stat(root); err == nil && !info.IsDir() {
			base = filepath.Dir(root)
		}
		candidate := filepath.Clean(filepath.Join(base, relativePath))
		rel, err := filepath.Rel(base, candidate)
		if err != nil || strings.HasPrefix(rel, "..") {
			continue
		}
		if _, err := os.Stat(candidate); err == nil {
			return candidate, nil
		}
	}

	return "", fmt.Errorf("log file not found")
}

func gatewayLogRoots(cfg archiveconfig.File) []string {
	roots := []string{
		cfg.OpticalArchive.Logging.Gateway.AccessLogPath,
		cfg.OpticalArchive.Logging.Gateway.AdminLogPath,
	}
	for _, path := range []string{cfg.OpticalArchive.Logging.Gateway.AccessLogPath, cfg.OpticalArchive.Logging.Gateway.AdminLogPath} {
		dir := filepath.Join(filepath.Dir(strings.TrimSpace(path)), "archive")
		if strings.TrimSpace(path) != "" {
			roots = append(roots, dir)
		}
	}
	return roots
}

func recorderLogRoots(cfg archiveconfig.File) []string {
	root := strings.TrimSpace(cfg.OpticalArchive.Logging.Recorder.LogDirectory)
	if root == "" {
		return nil
	}
	return []string{root, filepath.Join(root, "archive")}
}

func archiveGatewayLogs(cfg archiveconfig.File) error {
	if err := archiveSingleGatewayLog(
		cfg.OpticalArchive.Logging.Gateway.AccessLogPath,
		cfg.OpticalArchive.Logging.Gateway.FileSizeMb,
		cfg.OpticalArchive.Logging.Gateway.RetentionDays,
		cfg.OpticalArchive.Logging.Gateway.EnableCompression,
	); err != nil {
		return err
	}
	if err := archiveSingleGatewayLog(
		cfg.OpticalArchive.Logging.Gateway.AdminLogPath,
		cfg.OpticalArchive.Logging.Gateway.FileSizeMb,
		cfg.OpticalArchive.Logging.Gateway.RetentionDays,
		cfg.OpticalArchive.Logging.Gateway.EnableCompression,
	); err != nil {
		return err
	}
	return nil
}

func archiveSingleGatewayLog(logPath string, maxSizeMb, retentionDays int, compress bool) error {
	logPath = strings.TrimSpace(logPath)
	if logPath == "" {
		return nil
	}

	info, err := os.Stat(logPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	if maxSizeMb <= 0 {
		maxSizeMb = 64
	}
	limitBytes := int64(maxSizeMb) * 1024 * 1024
	if info.Size() < limitBytes {
		return pruneArchiveDirectory(filepath.Join(filepath.Dir(logPath), "archive"), retentionDays)
	}

	archiveDir := filepath.Join(filepath.Dir(logPath), "archive")
	if err := os.MkdirAll(archiveDir, 0o755); err != nil {
		return err
	}

	stamp := time.Now().UTC().Format("20060102T150405Z")
	archivedName := fmt.Sprintf("%s.%s.log", strings.TrimSuffix(filepath.Base(logPath), filepath.Ext(logPath)), stamp)
	archivedPath := filepath.Join(archiveDir, archivedName)
	if err := copyFile(logPath, archivedPath); err != nil {
		return err
	}

	if compress {
		if err := gzipFileInPlace(archivedPath); err != nil {
			return err
		}
	}

	if err := os.Truncate(logPath, 0); err != nil {
		return err
	}

	return pruneArchiveDirectory(archiveDir, retentionDays)
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.OpenFile(dst, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer out.Close()

	if _, err := io.Copy(out, in); err != nil {
		return err
	}

	return out.Sync()
}

func gzipFileInPlace(path string) error {
	input, err := os.Open(path)
	if err != nil {
		return err
	}

	gzPath := path + ".gz"
	output, err := os.OpenFile(gzPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		_ = input.Close()
		return err
	}

	writer := gzip.NewWriter(output)
	if _, err := io.Copy(writer, input); err != nil {
		_ = input.Close()
		_ = writer.Close()
		_ = output.Close()
		return err
	}
	if err := input.Close(); err != nil {
		_ = writer.Close()
		_ = output.Close()
		return err
	}
	if err := writer.Close(); err != nil {
		_ = output.Close()
		return err
	}
	if err := output.Close(); err != nil {
		return err
	}

	return os.Remove(path)
}

func pruneArchiveDirectory(dir string, retentionDays int) error {
	if retentionDays <= 0 {
		retentionDays = 30
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	expireBefore := time.Now().AddDate(0, 0, -retentionDays)
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		path := filepath.Join(dir, entry.Name())
		info, err := entry.Info()
		if err != nil {
			continue
		}
		if info.ModTime().Before(expireBefore) {
			_ = os.Remove(path)
		}
	}
	return nil
}

func fileSha256(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()

	hasher := sha256.New()
	if _, err := io.Copy(hasher, file); err != nil {
		return "", err
	}
	return hex.EncodeToString(hasher.Sum(nil)), nil
}

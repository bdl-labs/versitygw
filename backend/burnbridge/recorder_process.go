package burnbridge

import (
	"fmt"
	"log/slog"
	"net"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
)

func normalizeRecorderTargetHost(addr string) (string, error) {
	target := strings.TrimSpace(addr)
	if target == "" {
		return "", fmt.Errorf("empty grpc address")
	}

	host, _, err := net.SplitHostPort(target)
	if err != nil {
		if strings.Contains(err.Error(), "missing port in address") {
			host = target
		} else {
			return "", err
		}
	}

	host = strings.TrimSpace(strings.Trim(host, "[]"))
	if host == "" {
		return "", fmt.Errorf("empty grpc host")
	}
	return strings.ToLower(host), nil
}

func isLocalRecorderTarget(addr string) bool {
	host, err := normalizeRecorderTargetHost(addr)
	if err != nil {
		return false
	}

	switch host {
	case "localhost", "127.0.0.1", "::1", "0.0.0.0", "::":
		return true
	}

	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func maybeStartLocalRecorderProcess(opts Options) error {
	if !opts.ManageRecorderProcessLocally {
		return nil
	}
	if !isLocalRecorderTarget(opts.GRPCAddr) {
		return fmt.Errorf("burnbridge: ManageRecorderProcessLocally requires local GRPCAddr, got %q", strings.TrimSpace(opts.GRPCAddr))
	}
	if strings.TrimSpace(opts.RecorderStartCommand) == "" {
		return fmt.Errorf("burnbridge: ManageRecorderProcessLocally enabled but RecorderStartCommand is empty")
	}
	if runtime.GOOS != "linux" {
		slog.Info("burnbridge: local recorder auto-start is enabled but skipped because host is not linux")
		return nil
	}

	command := strings.TrimSpace(opts.RecorderStartCommand)
	workdir := strings.TrimSpace(opts.RecorderWorkingDirectory)
	if workdir == "" {
		workdir = "."
	}
	workdir = filepath.Clean(workdir)

	cmd := exec.Command("/bin/sh", "-lc", command)
	cmd.Dir = workdir
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("burnbridge: start local recorder process: %w", err)
	}

	slog.Info("burnbridge: started local recorder process",
		"pid", cmd.Process.Pid,
		"command", command,
		"workdir", workdir)
	return nil
}

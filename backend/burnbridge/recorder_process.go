package burnbridge

import (
	"fmt"
	"log/slog"
	"net"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"
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

func splitRecorderTarget(addr string) (string, string, error) {
	target := strings.TrimSpace(addr)
	if target == "" {
		return "", "", fmt.Errorf("empty grpc address")
	}
	host, port, err := net.SplitHostPort(target)
	if err != nil {
		return "", "", err
	}
	host = strings.TrimSpace(strings.Trim(host, "[]"))
	port = strings.TrimSpace(port)
	if host == "" || port == "" {
		return "", "", fmt.Errorf("invalid grpc address %q", target)
	}
	return strings.ToLower(host), port, nil
}

func recorderProbeAddress(addr string) (string, error) {
	host, port, err := splitRecorderTarget(addr)
	if err != nil {
		return "", err
	}
	switch host {
	case "0.0.0.0":
		host = "127.0.0.1"
	case "::":
		host = "::1"
	}
	return net.JoinHostPort(host, port), nil
}

func recorderEndpointReachable(addr string, timeout time.Duration) bool {
	probeAddr, err := recorderProbeAddress(addr)
	if err != nil {
		return false
	}
	conn, err := net.DialTimeout("tcp", probeAddr, timeout)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

func waitForRecorderEndpoint(addr string, maxWait time.Duration) error {
	if maxWait <= 0 {
		maxWait = 30 * time.Second
	}
	deadline := time.Now().Add(maxWait)
	for {
		if recorderEndpointReachable(addr, time.Second) {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("burnbridge: recorder gRPC endpoint %q did not become ready within %s", strings.TrimSpace(addr), maxWait)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func maybeStartLocalRecorderProcess(opts Options) error {
	if !opts.ManageRecorderProcessLocally {
		return nil
	}
	if !isLocalRecorderTarget(opts.GRPCAddr) {
		return fmt.Errorf("burnbridge: ManageRecorderProcessLocally requires local GRPCAddr, got %q", strings.TrimSpace(opts.GRPCAddr))
	}
	if recorderEndpointReachable(opts.GRPCAddr, time.Second) {
		slog.Info("burnbridge: local recorder already running; skip auto-start",
			"grpcAddr", strings.TrimSpace(opts.GRPCAddr))
		return nil
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

	waitSeconds := opts.RecorderHealthCheckSeconds
	if waitSeconds <= 0 {
		waitSeconds = 30
	}
	if err := waitForRecorderEndpoint(opts.GRPCAddr, time.Duration(waitSeconds)*time.Second); err != nil {
		return err
	}

	slog.Info("burnbridge: local recorder gRPC endpoint became ready",
		"grpcAddr", strings.TrimSpace(opts.GRPCAddr))
	return nil
}

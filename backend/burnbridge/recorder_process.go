package burnbridge

import (
	"fmt"
	"log/slog"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
)

func maybeStartLocalRecorderProcess(opts Options) error {
	if !opts.ManageRecorderProcessLocally {
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
	return nil
}

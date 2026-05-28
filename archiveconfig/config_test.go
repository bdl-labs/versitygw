package archiveconfig

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadSupportsUtf8Bom(t *testing.T) {
	tempDir := t.TempDir()
	configPath := filepath.Join(tempDir, "optical-archive.config.json")
	content := []byte{0xEF, 0xBB, 0xBF}
	content = append(content, []byte(`{"OpticalArchive":{"Gateway":{"ConfigApiUsername":"admin","ConfigApiPassword":"admin123456"}}}`)...)
	if err := os.WriteFile(configPath, content, 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, resolved, err := Load(configPath)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if resolved != configPath {
		t.Fatalf("resolved path mismatch: got %s want %s", resolved, configPath)
	}
	if cfg.OpticalArchive.Gateway.ConfigApiUsername != "admin" {
		t.Fatalf("unexpected config api username: %q", cfg.OpticalArchive.Gateway.ConfigApiUsername)
	}
}

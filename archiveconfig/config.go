package archiveconfig

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const (
	DefaultConfigPath     = "D:\\BRS\\optical-archive.config.json"
	DefaultConfigUsername = "admin"
	DefaultConfigPassword = "admin123"
)

type File struct {
	OpticalArchive OpticalArchive `json:"OpticalArchive"`
}

type OpticalArchive struct {
	Gateway       Gateway       `json:"Gateway"`
	Recorder      Recorder      `json:"Recorder"`
	Runtime       Runtime       `json:"Runtime"`
	Redundancy    Redundancy    `json:"Redundancy"`
	GatewayInterop GatewayInterop `json:"GatewayInterop"`
}

type Gateway struct {
	ConfigApiUsername    string `json:"ConfigApiUsername"`
	ConfigApiPassword    string `json:"ConfigApiPassword"`
	ConfigFilePath       string `json:"ConfigFilePath"`
	BurnServerConfigPath string `json:"BurnServerConfigPath"`
	GatewayMetadataDbPath string `json:"GatewayMetadataDbPath"`
}

type Recorder struct {
	DriveIndex                 int    `json:"DriveIndex"`
	LayoutDbPath               string `json:"LayoutDbPath"`
	MetadataDbFileNameTemplate string `json:"MetadataDbFileNameTemplate"`
	GrpcChunkSize              int    `json:"GrpcChunkSize"`
	ReadMountPath              string `json:"ReadMountPath"`
}

type Runtime struct {
	SectorSizeBytes            int   `json:"SectorSizeBytes"`
	BlocksPerTransfer          int   `json:"BlocksPerTransfer"`
	SessionCacheCapacityBytes  int64 `json:"SessionCacheCapacityBytes"`
	WriteBufferBytes           int   `json:"WriteBufferBytes"`
}

type Redundancy struct {
	Enabled          bool `json:"Enabled"`
	DataBlockCount   int  `json:"DataBlockCount"`
	ParityBlockCount int  `json:"ParityBlockCount"`
	BlockSizeBytes   int  `json:"BlockSizeBytes"`
}

type GatewayInterop struct {
	GrpcAddr                string `json:"GrpcAddr"`
	ReadMountPath           string `json:"ReadMountPath"`
	GrpcDialTimeoutSeconds  int    `json:"GrpcDialTimeoutSeconds"`
	GrpcReadyTimeoutSeconds int    `json:"GrpcReadyTimeoutSeconds"`
	GrpcPingTimeoutSeconds  int    `json:"GrpcPingTimeoutSeconds"`
}

func DefaultFile(path string) File {
	resolved := strings.TrimSpace(path)
	if resolved == "" {
		resolved = DefaultConfigPath
	}

	return File{
		OpticalArchive: OpticalArchive{
			Gateway: Gateway{
				ConfigApiUsername:    DefaultConfigUsername,
				ConfigApiPassword:    DefaultConfigPassword,
				ConfigFilePath:       resolved,
				BurnServerConfigPath: "D:\\BRS\\primoburner-net\\samples\\BurnServer\\appsettings.json",
				GatewayMetadataDbPath: "D:\\BRS\\versitygw\\burnbridge-meta.db",
			},
			Recorder: Recorder{
				DriveIndex:                 0,
				LayoutDbPath:               "D:\\BRS\\primoburner-net\\samples\\BurnServer\\udf-layout.db",
				MetadataDbFileNameTemplate: "__archive_{bucket}.sqlite3",
				GrpcChunkSize:              1048576,
				ReadMountPath:              "",
			},
			Runtime: Runtime{
				SectorSizeBytes:           2048,
				BlocksPerTransfer:         16,
				SessionCacheCapacityBytes: 536870912,
				WriteBufferBytes:          327680,
			},
			Redundancy: Redundancy{
				Enabled:          true,
				DataBlockCount:   30,
				ParityBlockCount: 2,
				BlockSizeBytes:   2048,
			},
			GatewayInterop: GatewayInterop{
				GrpcAddr:                "127.0.0.1:50051",
				ReadMountPath:           "",
				GrpcDialTimeoutSeconds:  120,
				GrpcReadyTimeoutSeconds: 90,
				GrpcPingTimeoutSeconds:  60,
			},
		},
	}
}

func ResolvePath(configPath string) string {
	if strings.TrimSpace(configPath) == "" {
		if env := strings.TrimSpace(os.Getenv("OPTICAL_ARCHIVE_CONFIG_PATH")); env != "" {
			return env
		}
		if env := strings.TrimSpace(os.Getenv("VGW_BURNBRIDGE_ARCHIVE_CONFIG_PATH")); env != "" {
			return env
		}
		return DefaultConfigPath
	}

	return configPath
}

func Load(configPath string) (File, string, error) {
	resolved := filepath.Clean(ResolvePath(configPath))
	if _, err := os.Stat(resolved); errors.Is(err, os.ErrNotExist) {
		cfg := DefaultFile(resolved)
		if err := Save(resolved, cfg); err != nil {
			return File{}, resolved, err
		}
		return cfg, resolved, nil
	}

	raw, err := os.ReadFile(resolved)
	if err != nil {
		return File{}, resolved, fmt.Errorf("read archive config: %w", err)
	}

	cfg := DefaultFile(resolved)
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return File{}, resolved, fmt.Errorf("parse archive config: %w", err)
	}
	if strings.TrimSpace(cfg.OpticalArchive.Gateway.ConfigFilePath) == "" {
		cfg.OpticalArchive.Gateway.ConfigFilePath = resolved
	}
	if strings.TrimSpace(cfg.OpticalArchive.Gateway.ConfigApiUsername) == "" {
		cfg.OpticalArchive.Gateway.ConfigApiUsername = DefaultConfigUsername
	}
	if strings.TrimSpace(cfg.OpticalArchive.Gateway.ConfigApiPassword) == "" {
		cfg.OpticalArchive.Gateway.ConfigApiPassword = DefaultConfigPassword
	}
	if strings.TrimSpace(cfg.OpticalArchive.Gateway.BurnServerConfigPath) == "" {
		cfg.OpticalArchive.Gateway.BurnServerConfigPath = DefaultFile(resolved).OpticalArchive.Gateway.BurnServerConfigPath
	}
	if strings.TrimSpace(cfg.OpticalArchive.Gateway.GatewayMetadataDbPath) == "" {
		cfg.OpticalArchive.Gateway.GatewayMetadataDbPath = DefaultFile(resolved).OpticalArchive.Gateway.GatewayMetadataDbPath
	}
	if strings.TrimSpace(cfg.OpticalArchive.Recorder.LayoutDbPath) == "" {
		cfg.OpticalArchive.Recorder.LayoutDbPath = DefaultFile(resolved).OpticalArchive.Recorder.LayoutDbPath
	}
	if strings.TrimSpace(cfg.OpticalArchive.Recorder.MetadataDbFileNameTemplate) == "" {
		cfg.OpticalArchive.Recorder.MetadataDbFileNameTemplate = DefaultFile(resolved).OpticalArchive.Recorder.MetadataDbFileNameTemplate
	}
	if strings.TrimSpace(cfg.OpticalArchive.Recorder.ReadMountPath) == "" {
		cfg.OpticalArchive.Recorder.ReadMountPath = DefaultFile(resolved).OpticalArchive.Recorder.ReadMountPath
	}
	if strings.TrimSpace(cfg.OpticalArchive.GatewayInterop.GrpcAddr) == "" {
		cfg.OpticalArchive.GatewayInterop.GrpcAddr = DefaultFile(resolved).OpticalArchive.GatewayInterop.GrpcAddr
	}
	if strings.TrimSpace(cfg.OpticalArchive.GatewayInterop.ReadMountPath) == "" {
		cfg.OpticalArchive.GatewayInterop.ReadMountPath = DefaultFile(resolved).OpticalArchive.GatewayInterop.ReadMountPath
	}

	return cfg, resolved, nil
}

func Save(configPath string, cfg File) error {
	resolved := filepath.Clean(ResolvePath(configPath))
	cfg.OpticalArchive.Gateway.ConfigFilePath = resolved

	if err := os.MkdirAll(filepath.Dir(resolved), 0o755); err != nil {
		return fmt.Errorf("create archive config directory: %w", err)
	}

	raw, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return fmt.Errorf("encode archive config: %w", err)
	}
	raw = append(raw, '\n')

	tmp := resolved + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o600); err != nil {
		return fmt.Errorf("write archive config temp file: %w", err)
	}
	if err := replaceFile(tmp, resolved); err != nil {
		return fmt.Errorf("replace archive config: %w", err)
	}
	return nil
}

func replaceFile(tmpPath, dstPath string) error {
	if err := os.Remove(dstPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return os.Rename(tmpPath, dstPath)
}

func CheckBasicAuth(headerValue string, cfg File) bool {
	if strings.TrimSpace(headerValue) == "" {
		return false
	}
	if !strings.HasPrefix(headerValue, "Basic ") {
		return false
	}

	payload := strings.TrimSpace(strings.TrimPrefix(headerValue, "Basic "))
	decoded, err := base64.StdEncoding.DecodeString(payload)
	if err != nil {
		return false
	}
	parts := strings.SplitN(string(decoded), ":", 2)
	if len(parts) != 2 {
		return false
	}

	expectedUser := cfg.OpticalArchive.Gateway.ConfigApiUsername
	expectedPass := cfg.OpticalArchive.Gateway.ConfigApiPassword
	if strings.TrimSpace(expectedUser) == "" {
		expectedUser = DefaultConfigUsername
	}
	if strings.TrimSpace(expectedPass) == "" {
		expectedPass = DefaultConfigPassword
	}

	return parts[0] == expectedUser && parts[1] == expectedPass
}

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
	Gateway        Gateway        `json:"Gateway"`
	ReadMountPath  string         `json:"ReadMountPath"`
	Recorder       Recorder       `json:"Recorder"`
	Runtime        Runtime        `json:"Runtime"`
	Redundancy     Redundancy     `json:"Redundancy"`
	GatewayInterop GatewayInterop `json:"GatewayInterop"`
	DiscBucketBindings []DiscBucketBinding `json:"DiscBucketBindings"`
}

type Gateway struct {
	ConfigApiUsername     string `json:"ConfigApiUsername"`
	ConfigApiPassword     string `json:"ConfigApiPassword"`
	ConfigFilePath        string `json:"ConfigFilePath"`
	BurnServerConfigPath  string `json:"BurnServerConfigPath"`
	GatewayMetadataDbPath string `json:"GatewayMetadataDbPath"`
}

type Recorder struct {
	DriveIndex                 int    `json:"DriveIndex"`
	LayoutDbPath               string `json:"LayoutDbPath"`
	MetadataDbFileNameTemplate string `json:"MetadataDbFileNameTemplate"`
	GrpcChunkSize              int    `json:"GrpcChunkSize"`
	DiscSerialStrategy         string `json:"DiscSerialStrategy"`
	VolumeLabelStrategy        string `json:"VolumeLabelStrategy"`
	SerialPrefix               string `json:"SerialPrefix"`
	VolumeLabelPrefix          string `json:"VolumeLabelPrefix"`
	GeneratedSerialLength      int    `json:"GeneratedSerialLength"`
	GeneratedVolumeLabelLength int    `json:"GeneratedVolumeLabelLength"`
	AllowCreateBucketBinding   bool   `json:"AllowCreateBucketBinding"`
}

type Runtime struct {
	SectorSizeBytes           int   `json:"SectorSizeBytes"`
	BlocksPerTransfer         int   `json:"BlocksPerTransfer"`
	SessionCacheCapacityBytes int64 `json:"SessionCacheCapacityBytes"`
	WriteBufferBytes          int   `json:"WriteBufferBytes"`
}

type Redundancy struct {
	Enabled          bool `json:"Enabled"`
	DataBlockCount   int  `json:"DataBlockCount"`
	ParityBlockCount int  `json:"ParityBlockCount"`
	BlockSizeBytes   int  `json:"BlockSizeBytes"`
}

type GatewayInterop struct {
	GrpcAddr                string `json:"GrpcAddr"`
	GrpcDialTimeoutSeconds  int    `json:"GrpcDialTimeoutSeconds"`
	GrpcReadyTimeoutSeconds int    `json:"GrpcReadyTimeoutSeconds"`
	GrpcPingTimeoutSeconds  int    `json:"GrpcPingTimeoutSeconds"`
}

type DiscBucketBinding struct {
	ProbeVolumeLabel string `json:"ProbeVolumeLabel"`
	Bucket           string `json:"Bucket"`
	UdfVolumeLabel   string `json:"UdfVolumeLabel"`
}

func DefaultFile(path string) File {
	resolved := strings.TrimSpace(path)
	if resolved == "" {
		resolved = DefaultConfigPath
	}

	return File{
		OpticalArchive: OpticalArchive{
			Gateway: Gateway{
				ConfigApiUsername:     DefaultConfigUsername,
				ConfigApiPassword:     DefaultConfigPassword,
				ConfigFilePath:        resolved,
				BurnServerConfigPath:  "D:\\BRS\\primoburner-net\\samples\\BurnServer\\appsettings.json",
				GatewayMetadataDbPath: "D:\\BRS\\versitygw\\burnbridge-meta.db",
			},
			ReadMountPath: "",
			Recorder: Recorder{
				DriveIndex:                 0,
				LayoutDbPath:               "D:\\BRS\\primoburner-net\\samples\\BurnServer\\udf-layout.db",
				MetadataDbFileNameTemplate: "__archive_{bucket}.sqlite3",
				GrpcChunkSize:              1048576,
				DiscSerialStrategy:         "hash",
				VolumeLabelStrategy:        "serial",
				SerialPrefix:               "OA",
				VolumeLabelPrefix:          "DISC",
				GeneratedSerialLength:      24,
				GeneratedVolumeLabelLength: 24,
				AllowCreateBucketBinding:   true,
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
				GrpcDialTimeoutSeconds:  120,
				GrpcReadyTimeoutSeconds: 90,
				GrpcPingTimeoutSeconds:  60,
			},
			DiscBucketBindings: []DiscBucketBinding{},
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
	defaults := DefaultFile(resolved)
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
		cfg.OpticalArchive.Gateway.GatewayMetadataDbPath = defaults.OpticalArchive.Gateway.GatewayMetadataDbPath
	}
	if strings.TrimSpace(cfg.OpticalArchive.ReadMountPath) == "" {
		cfg.OpticalArchive.ReadMountPath = defaults.OpticalArchive.ReadMountPath
	}
	if strings.TrimSpace(cfg.OpticalArchive.Recorder.LayoutDbPath) == "" {
		cfg.OpticalArchive.Recorder.LayoutDbPath = defaults.OpticalArchive.Recorder.LayoutDbPath
	}
	if strings.TrimSpace(cfg.OpticalArchive.Recorder.MetadataDbFileNameTemplate) == "" {
		cfg.OpticalArchive.Recorder.MetadataDbFileNameTemplate = defaults.OpticalArchive.Recorder.MetadataDbFileNameTemplate
	}
	if strings.TrimSpace(cfg.OpticalArchive.Recorder.DiscSerialStrategy) == "" {
		cfg.OpticalArchive.Recorder.DiscSerialStrategy = defaults.OpticalArchive.Recorder.DiscSerialStrategy
	}
	if strings.TrimSpace(cfg.OpticalArchive.Recorder.VolumeLabelStrategy) == "" {
		cfg.OpticalArchive.Recorder.VolumeLabelStrategy = defaults.OpticalArchive.Recorder.VolumeLabelStrategy
	}
	if strings.TrimSpace(cfg.OpticalArchive.Recorder.SerialPrefix) == "" {
		cfg.OpticalArchive.Recorder.SerialPrefix = defaults.OpticalArchive.Recorder.SerialPrefix
	}
	if strings.TrimSpace(cfg.OpticalArchive.Recorder.VolumeLabelPrefix) == "" {
		cfg.OpticalArchive.Recorder.VolumeLabelPrefix = defaults.OpticalArchive.Recorder.VolumeLabelPrefix
	}
	if cfg.OpticalArchive.Recorder.GeneratedSerialLength <= 0 {
		cfg.OpticalArchive.Recorder.GeneratedSerialLength = defaults.OpticalArchive.Recorder.GeneratedSerialLength
	}
	if cfg.OpticalArchive.Recorder.GeneratedVolumeLabelLength <= 0 {
		cfg.OpticalArchive.Recorder.GeneratedVolumeLabelLength = defaults.OpticalArchive.Recorder.GeneratedVolumeLabelLength
	}
	if strings.TrimSpace(cfg.OpticalArchive.GatewayInterop.GrpcAddr) == "" {
		cfg.OpticalArchive.GatewayInterop.GrpcAddr = defaults.OpticalArchive.GatewayInterop.GrpcAddr
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

func FindDiscBucketBinding(cfg File, probeVolumeLabel string) (DiscBucketBinding, bool) {
	probe := strings.TrimSpace(probeVolumeLabel)
	if probe == "" {
		return DiscBucketBinding{}, false
	}

	for _, binding := range cfg.OpticalArchive.DiscBucketBindings {
		if strings.EqualFold(strings.TrimSpace(binding.ProbeVolumeLabel), probe) {
			return binding, true
		}
	}

	return DiscBucketBinding{}, false
}

func UpsertDiscBucketBinding(cfg *File, probeVolumeLabel, bucket, udfVolumeLabel string) {
	if cfg == nil {
		return
	}

	probe := strings.TrimSpace(probeVolumeLabel)
	bkt := strings.TrimSpace(bucket)
	udf := strings.TrimSpace(udfVolumeLabel)
	if probe == "" || bkt == "" || udf == "" {
		return
	}

	for i := range cfg.OpticalArchive.DiscBucketBindings {
		if strings.EqualFold(strings.TrimSpace(cfg.OpticalArchive.DiscBucketBindings[i].ProbeVolumeLabel), probe) {
			cfg.OpticalArchive.DiscBucketBindings[i].ProbeVolumeLabel = probe
			cfg.OpticalArchive.DiscBucketBindings[i].Bucket = bkt
			cfg.OpticalArchive.DiscBucketBindings[i].UdfVolumeLabel = udf
			return
		}
	}

	cfg.OpticalArchive.DiscBucketBindings = append(cfg.OpticalArchive.DiscBucketBindings, DiscBucketBinding{
		ProbeVolumeLabel: probe,
		Bucket:           bkt,
		UdfVolumeLabel:   udf,
	})
}

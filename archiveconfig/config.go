package archiveconfig

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"unicode"
)

const (
	DefaultConfigPath     = "D:\\BRS\\optical-archive.config.json"
	DefaultConfigUsername = "admin"
	DefaultConfigPassword = "admin123456"
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
	Logging        Logging        `json:"Logging"`
	Upgrade        Upgrade        `json:"Upgrade"`
	LinuxServices  LinuxServices  `json:"LinuxServices"`
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
	MaxReceiveMessageSize      int    `json:"MaxReceiveMessageSize"`
	MaxSendMessageSize         int    `json:"MaxSendMessageSize"`
	Http2InitialConnectionWindowSize int `json:"Http2InitialConnectionWindowSize"`
	Http2InitialStreamWindowSize     int `json:"Http2InitialStreamWindowSize"`
	FinalizeReservePercent     int    `json:"FinalizeReservePercent"`
	FinalizeReserveBytes       int64  `json:"FinalizeReserveBytes"`
	DiscSerialStrategy         string `json:"DiscSerialStrategy"`
	VolumeLabelStrategy        string `json:"VolumeLabelStrategy"`
	SerialPrefix               string `json:"SerialPrefix"`
	VolumeLabelPrefix          string `json:"VolumeLabelPrefix"`
	GeneratedSerialLength      int    `json:"GeneratedSerialLength"`
	GeneratedVolumeLabelLength int    `json:"GeneratedVolumeLabelLength"`
	AllowCreateBucketBinding   bool   `json:"AllowCreateBucketBinding"`
	LicenseFilePath            string `json:"LicenseFilePath"`
}

type Runtime struct {
	SectorSizeBytes           int   `json:"SectorSizeBytes"`
	BlocksPerTransfer         int   `json:"BlocksPerTransfer"`
	SessionCacheCapacityBytes int64 `json:"SessionCacheCapacityBytes"`
	WriteBufferBytes          int   `json:"WriteBufferBytes"`
	WriteBufferSlotCount      int   `json:"WriteBufferSlotCount"`
	GlobalWriteQueueCapacity  int   `json:"GlobalWriteQueueCapacity"`
	ReadQueueCapacity         int   `json:"ReadQueueCapacity"`
	RedundancyReadWindowBlocks int  `json:"RedundancyReadWindowBlocks"`
	PlainReadWindowBlocks      int  `json:"PlainReadWindowBlocks"`
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

type Upgrade struct {
	GatewayStagingDirectory  string `json:"GatewayStagingDirectory"`
	RecorderStagingDirectory string `json:"RecorderStagingDirectory"`
	GatewayApplyCommand      string `json:"GatewayApplyCommand"`
	RecorderApplyCommand     string `json:"RecorderApplyCommand"`
}

type LinuxServices struct {
	ManageRecorderProcessLocally bool   `json:"ManageRecorderProcessLocally"`
	RecorderServiceName          string `json:"RecorderServiceName"`
	RecorderProcessPattern       string `json:"RecorderProcessPattern"`
	RecorderStartCommand         string `json:"RecorderStartCommand"`
	RecorderWorkingDirectory     string `json:"RecorderWorkingDirectory"`
	RecorderHealthCheckSeconds   int    `json:"RecorderHealthCheckSeconds"`
	MountRefreshEnabled          bool   `json:"MountRefreshEnabled"`
	MountRefreshServiceType      string `json:"MountRefreshServiceType"`
	MountRefreshMountPath        string `json:"MountRefreshMountPath"`
	MountRefreshDevice           string `json:"MountRefreshDevice"`
	MountRefreshCommand          string `json:"MountRefreshCommand"`
}

type Logging struct {
	Gateway  GatewayLogging  `json:"Gateway"`
	Recorder RecorderLogging `json:"Recorder"`
}

type GatewayLogging struct {
	AccessLogPath      string `json:"AccessLogPath"`
	AdminLogPath       string `json:"AdminLogPath"`
	FileSizeMb         int    `json:"FileSizeMb"`
	MaxBackups         int    `json:"MaxBackups"`
	RetentionDays      int    `json:"RetentionDays"`
	EnableCompression  bool   `json:"EnableCompression"`
}

type RecorderLogging struct {
	LogDirectory       string `json:"LogDirectory"`
	FileSizeMb         int    `json:"FileSizeMb"`
	RetentionDays      int    `json:"RetentionDays"`
	EnableCompression  bool   `json:"EnableCompression"`
	MinLevel           string `json:"MinLevel"`
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
				GrpcChunkSize:              262144,
				MaxReceiveMessageSize:      262144,
				MaxSendMessageSize:         262144,
				Http2InitialConnectionWindowSize: 1048576,
				Http2InitialStreamWindowSize:     524288,
				FinalizeReservePercent:     3,
				FinalizeReserveBytes:       67108864,
				DiscSerialStrategy:         "hash",
				VolumeLabelStrategy:        "serial",
				SerialPrefix:               "OA",
				VolumeLabelPrefix:          "DISC",
				GeneratedSerialLength:      24,
				GeneratedVolumeLabelLength: 24,
				AllowCreateBucketBinding:   true,
				LicenseFilePath:            "D:\\BRS\\primoburner-net\\samples\\BurnServer\\license.xml",
			},
			Runtime: Runtime{
				SectorSizeBytes:           2048,
				BlocksPerTransfer:         32,
				SessionCacheCapacityBytes: 67108864,
				WriteBufferBytes:          33554432,
				WriteBufferSlotCount:      2,
				GlobalWriteQueueCapacity:  4,
				ReadQueueCapacity:         1,
				RedundancyReadWindowBlocks: 320,
				PlainReadWindowBlocks:      320,
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
			Logging: Logging{
				Gateway: GatewayLogging{
					AccessLogPath:     "D:\\BRS\\versitygw\\logs\\gateway-access.log",
					AdminLogPath:      "D:\\BRS\\versitygw\\logs\\gateway-admin.log",
					FileSizeMb:        64,
					MaxBackups:        16,
					RetentionDays:     30,
					EnableCompression: true,
				},
				Recorder: RecorderLogging{
					LogDirectory:      "D:\\BRS\\primoburner-net\\bin\\logs",
					FileSizeMb:        64,
					RetentionDays:     30,
					EnableCompression: true,
					MinLevel:          "Information",
				},
			},
			Upgrade: Upgrade{
				GatewayStagingDirectory:  "D:\\BRS\\versitygw\\upgrade",
				RecorderStagingDirectory: "D:\\BRS\\primoburner-net\\samples\\BurnServer\\upgrade",
				GatewayApplyCommand:      "",
				RecorderApplyCommand:     "",
			},
			LinuxServices: LinuxServices{
				ManageRecorderProcessLocally: false,
				RecorderServiceName:          "optical-archive-recorder.service",
				RecorderProcessPattern:       "BurnServer.dll",
				RecorderStartCommand:         "",
				RecorderWorkingDirectory:     "",
				RecorderHealthCheckSeconds:   30,
				MountRefreshEnabled:          false,
				MountRefreshServiceType:      "command",
				MountRefreshMountPath:        "",
				MountRefreshDevice:           "",
				MountRefreshCommand:          "",
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
	if cfg.OpticalArchive.Recorder.MaxReceiveMessageSize <= 0 {
		cfg.OpticalArchive.Recorder.MaxReceiveMessageSize = defaults.OpticalArchive.Recorder.MaxReceiveMessageSize
	}
	if cfg.OpticalArchive.Recorder.MaxSendMessageSize <= 0 {
		cfg.OpticalArchive.Recorder.MaxSendMessageSize = defaults.OpticalArchive.Recorder.MaxSendMessageSize
	}
	if cfg.OpticalArchive.Recorder.Http2InitialConnectionWindowSize <= 0 {
		cfg.OpticalArchive.Recorder.Http2InitialConnectionWindowSize = defaults.OpticalArchive.Recorder.Http2InitialConnectionWindowSize
	}
	if cfg.OpticalArchive.Recorder.Http2InitialStreamWindowSize <= 0 {
		cfg.OpticalArchive.Recorder.Http2InitialStreamWindowSize = defaults.OpticalArchive.Recorder.Http2InitialStreamWindowSize
	}
	if cfg.OpticalArchive.Recorder.FinalizeReservePercent < 0 {
		cfg.OpticalArchive.Recorder.FinalizeReservePercent = defaults.OpticalArchive.Recorder.FinalizeReservePercent
	}
	if cfg.OpticalArchive.Recorder.FinalizeReserveBytes < 0 {
		cfg.OpticalArchive.Recorder.FinalizeReserveBytes = defaults.OpticalArchive.Recorder.FinalizeReserveBytes
	}
	if strings.TrimSpace(cfg.OpticalArchive.Recorder.LicenseFilePath) == "" {
		cfg.OpticalArchive.Recorder.LicenseFilePath = defaults.OpticalArchive.Recorder.LicenseFilePath
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
	if strings.TrimSpace(cfg.OpticalArchive.Logging.Gateway.AccessLogPath) == "" {
		cfg.OpticalArchive.Logging.Gateway.AccessLogPath = defaults.OpticalArchive.Logging.Gateway.AccessLogPath
	}
	if strings.TrimSpace(cfg.OpticalArchive.Logging.Gateway.AdminLogPath) == "" {
		cfg.OpticalArchive.Logging.Gateway.AdminLogPath = defaults.OpticalArchive.Logging.Gateway.AdminLogPath
	}
	if cfg.OpticalArchive.Logging.Gateway.FileSizeMb <= 0 {
		cfg.OpticalArchive.Logging.Gateway.FileSizeMb = defaults.OpticalArchive.Logging.Gateway.FileSizeMb
	}
	if cfg.OpticalArchive.Logging.Gateway.MaxBackups <= 0 {
		cfg.OpticalArchive.Logging.Gateway.MaxBackups = defaults.OpticalArchive.Logging.Gateway.MaxBackups
	}
	if cfg.OpticalArchive.Logging.Gateway.RetentionDays <= 0 {
		cfg.OpticalArchive.Logging.Gateway.RetentionDays = defaults.OpticalArchive.Logging.Gateway.RetentionDays
	}
	if strings.TrimSpace(cfg.OpticalArchive.Logging.Recorder.LogDirectory) == "" {
		cfg.OpticalArchive.Logging.Recorder.LogDirectory = defaults.OpticalArchive.Logging.Recorder.LogDirectory
	}
	if cfg.OpticalArchive.Logging.Recorder.FileSizeMb <= 0 {
		cfg.OpticalArchive.Logging.Recorder.FileSizeMb = defaults.OpticalArchive.Logging.Recorder.FileSizeMb
	}
	if cfg.OpticalArchive.Logging.Recorder.RetentionDays <= 0 {
		cfg.OpticalArchive.Logging.Recorder.RetentionDays = defaults.OpticalArchive.Logging.Recorder.RetentionDays
	}
	if strings.TrimSpace(cfg.OpticalArchive.Logging.Recorder.MinLevel) == "" {
		cfg.OpticalArchive.Logging.Recorder.MinLevel = defaults.OpticalArchive.Logging.Recorder.MinLevel
	}
	if strings.TrimSpace(cfg.OpticalArchive.Upgrade.GatewayStagingDirectory) == "" {
		cfg.OpticalArchive.Upgrade.GatewayStagingDirectory = defaults.OpticalArchive.Upgrade.GatewayStagingDirectory
	}
	if strings.TrimSpace(cfg.OpticalArchive.Upgrade.RecorderStagingDirectory) == "" {
		cfg.OpticalArchive.Upgrade.RecorderStagingDirectory = defaults.OpticalArchive.Upgrade.RecorderStagingDirectory
	}
	if strings.TrimSpace(cfg.OpticalArchive.Upgrade.GatewayApplyCommand) == "" {
		cfg.OpticalArchive.Upgrade.GatewayApplyCommand = defaults.OpticalArchive.Upgrade.GatewayApplyCommand
	}
	if strings.TrimSpace(cfg.OpticalArchive.Upgrade.RecorderApplyCommand) == "" {
		cfg.OpticalArchive.Upgrade.RecorderApplyCommand = defaults.OpticalArchive.Upgrade.RecorderApplyCommand
	}
	if cfg.OpticalArchive.LinuxServices.RecorderHealthCheckSeconds <= 0 {
		cfg.OpticalArchive.LinuxServices.RecorderHealthCheckSeconds = defaults.OpticalArchive.LinuxServices.RecorderHealthCheckSeconds
	}
	if strings.TrimSpace(cfg.OpticalArchive.LinuxServices.MountRefreshServiceType) == "" {
		cfg.OpticalArchive.LinuxServices.MountRefreshServiceType = defaults.OpticalArchive.LinuxServices.MountRefreshServiceType
	}
	applyEnvOverrides(&cfg)

	return cfg, resolved, nil
}

func applyEnvOverrides(cfg *File) {
	if cfg == nil {
		return
	}

	applyStringOverride(&cfg.OpticalArchive.Gateway.ConfigApiUsername, "OpticalArchive", "Gateway", "ConfigApiUsername")
	applyStringOverride(&cfg.OpticalArchive.Gateway.ConfigApiPassword, "OpticalArchive", "Gateway", "ConfigApiPassword")
	applyStringOverride(&cfg.OpticalArchive.Gateway.ConfigFilePath, "OpticalArchive", "Gateway", "ConfigFilePath")
	applyStringOverride(&cfg.OpticalArchive.Gateway.BurnServerConfigPath, "OpticalArchive", "Gateway", "BurnServerConfigPath")
	applyStringOverride(&cfg.OpticalArchive.Gateway.GatewayMetadataDbPath, "OpticalArchive", "Gateway", "GatewayMetadataDbPath")

	applyStringOverride(&cfg.OpticalArchive.ReadMountPath, "OpticalArchive", "ReadMountPath")

	applyIntOverride(&cfg.OpticalArchive.Recorder.DriveIndex, "OpticalArchive", "Recorder", "DriveIndex")
	applyStringOverride(&cfg.OpticalArchive.Recorder.LayoutDbPath, "OpticalArchive", "Recorder", "LayoutDbPath")
	applyStringOverride(&cfg.OpticalArchive.Recorder.MetadataDbFileNameTemplate, "OpticalArchive", "Recorder", "MetadataDbFileNameTemplate")
	applyIntOverride(&cfg.OpticalArchive.Recorder.GrpcChunkSize, "OpticalArchive", "Recorder", "GrpcChunkSize")
	applyIntOverride(&cfg.OpticalArchive.Recorder.MaxReceiveMessageSize, "OpticalArchive", "Recorder", "MaxReceiveMessageSize")
	applyIntOverride(&cfg.OpticalArchive.Recorder.MaxSendMessageSize, "OpticalArchive", "Recorder", "MaxSendMessageSize")
	applyIntOverride(&cfg.OpticalArchive.Recorder.Http2InitialConnectionWindowSize, "OpticalArchive", "Recorder", "Http2InitialConnectionWindowSize")
	applyIntOverride(&cfg.OpticalArchive.Recorder.Http2InitialStreamWindowSize, "OpticalArchive", "Recorder", "Http2InitialStreamWindowSize")
	applyIntOverride(&cfg.OpticalArchive.Recorder.FinalizeReservePercent, "OpticalArchive", "Recorder", "FinalizeReservePercent")
	applyInt64Override(&cfg.OpticalArchive.Recorder.FinalizeReserveBytes, "OpticalArchive", "Recorder", "FinalizeReserveBytes")
	applyStringOverride(&cfg.OpticalArchive.Recorder.DiscSerialStrategy, "OpticalArchive", "Recorder", "DiscSerialStrategy")
	applyStringOverride(&cfg.OpticalArchive.Recorder.VolumeLabelStrategy, "OpticalArchive", "Recorder", "VolumeLabelStrategy")
	applyStringOverride(&cfg.OpticalArchive.Recorder.SerialPrefix, "OpticalArchive", "Recorder", "SerialPrefix")
	applyStringOverride(&cfg.OpticalArchive.Recorder.VolumeLabelPrefix, "OpticalArchive", "Recorder", "VolumeLabelPrefix")
	applyIntOverride(&cfg.OpticalArchive.Recorder.GeneratedSerialLength, "OpticalArchive", "Recorder", "GeneratedSerialLength")
	applyIntOverride(&cfg.OpticalArchive.Recorder.GeneratedVolumeLabelLength, "OpticalArchive", "Recorder", "GeneratedVolumeLabelLength")
	applyBoolOverride(&cfg.OpticalArchive.Recorder.AllowCreateBucketBinding, "OpticalArchive", "Recorder", "AllowCreateBucketBinding")
	applyStringOverride(&cfg.OpticalArchive.Recorder.LicenseFilePath, "OpticalArchive", "Recorder", "LicenseFilePath")

	applyIntOverride(&cfg.OpticalArchive.Runtime.SectorSizeBytes, "OpticalArchive", "Runtime", "SectorSizeBytes")
	applyIntOverride(&cfg.OpticalArchive.Runtime.BlocksPerTransfer, "OpticalArchive", "Runtime", "BlocksPerTransfer")
	applyInt64Override(&cfg.OpticalArchive.Runtime.SessionCacheCapacityBytes, "OpticalArchive", "Runtime", "SessionCacheCapacityBytes")
	applyIntOverride(&cfg.OpticalArchive.Runtime.WriteBufferBytes, "OpticalArchive", "Runtime", "WriteBufferBytes")
	applyIntOverride(&cfg.OpticalArchive.Runtime.WriteBufferSlotCount, "OpticalArchive", "Runtime", "WriteBufferSlotCount")
	applyIntOverride(&cfg.OpticalArchive.Runtime.GlobalWriteQueueCapacity, "OpticalArchive", "Runtime", "GlobalWriteQueueCapacity")
	applyIntOverride(&cfg.OpticalArchive.Runtime.ReadQueueCapacity, "OpticalArchive", "Runtime", "ReadQueueCapacity")
	applyIntOverride(&cfg.OpticalArchive.Runtime.RedundancyReadWindowBlocks, "OpticalArchive", "Runtime", "RedundancyReadWindowBlocks")
	applyIntOverride(&cfg.OpticalArchive.Runtime.PlainReadWindowBlocks, "OpticalArchive", "Runtime", "PlainReadWindowBlocks")

	applyBoolOverride(&cfg.OpticalArchive.Redundancy.Enabled, "OpticalArchive", "Redundancy", "Enabled")
	applyIntOverride(&cfg.OpticalArchive.Redundancy.DataBlockCount, "OpticalArchive", "Redundancy", "DataBlockCount")
	applyIntOverride(&cfg.OpticalArchive.Redundancy.ParityBlockCount, "OpticalArchive", "Redundancy", "ParityBlockCount")
	applyIntOverride(&cfg.OpticalArchive.Redundancy.BlockSizeBytes, "OpticalArchive", "Redundancy", "BlockSizeBytes")

	applyStringOverride(&cfg.OpticalArchive.GatewayInterop.GrpcAddr, "OpticalArchive", "GatewayInterop", "GrpcAddr")
	applyIntOverride(&cfg.OpticalArchive.GatewayInterop.GrpcDialTimeoutSeconds, "OpticalArchive", "GatewayInterop", "GrpcDialTimeoutSeconds")
	applyIntOverride(&cfg.OpticalArchive.GatewayInterop.GrpcReadyTimeoutSeconds, "OpticalArchive", "GatewayInterop", "GrpcReadyTimeoutSeconds")
	applyIntOverride(&cfg.OpticalArchive.GatewayInterop.GrpcPingTimeoutSeconds, "OpticalArchive", "GatewayInterop", "GrpcPingTimeoutSeconds")

	applyStringOverride(&cfg.OpticalArchive.Logging.Gateway.AccessLogPath, "OpticalArchive", "Logging", "Gateway", "AccessLogPath")
	applyStringOverride(&cfg.OpticalArchive.Logging.Gateway.AdminLogPath, "OpticalArchive", "Logging", "Gateway", "AdminLogPath")
	applyIntOverride(&cfg.OpticalArchive.Logging.Gateway.FileSizeMb, "OpticalArchive", "Logging", "Gateway", "FileSizeMb")
	applyIntOverride(&cfg.OpticalArchive.Logging.Gateway.MaxBackups, "OpticalArchive", "Logging", "Gateway", "MaxBackups")
	applyIntOverride(&cfg.OpticalArchive.Logging.Gateway.RetentionDays, "OpticalArchive", "Logging", "Gateway", "RetentionDays")
	applyBoolOverride(&cfg.OpticalArchive.Logging.Gateway.EnableCompression, "OpticalArchive", "Logging", "Gateway", "EnableCompression")

	applyStringOverride(&cfg.OpticalArchive.Logging.Recorder.LogDirectory, "OpticalArchive", "Logging", "Recorder", "LogDirectory")
	applyIntOverride(&cfg.OpticalArchive.Logging.Recorder.FileSizeMb, "OpticalArchive", "Logging", "Recorder", "FileSizeMb")
	applyIntOverride(&cfg.OpticalArchive.Logging.Recorder.RetentionDays, "OpticalArchive", "Logging", "Recorder", "RetentionDays")
	applyBoolOverride(&cfg.OpticalArchive.Logging.Recorder.EnableCompression, "OpticalArchive", "Logging", "Recorder", "EnableCompression")
	applyStringOverride(&cfg.OpticalArchive.Logging.Recorder.MinLevel, "OpticalArchive", "Logging", "Recorder", "MinLevel")

	applyStringOverride(&cfg.OpticalArchive.Upgrade.GatewayStagingDirectory, "OpticalArchive", "Upgrade", "GatewayStagingDirectory")
	applyStringOverride(&cfg.OpticalArchive.Upgrade.RecorderStagingDirectory, "OpticalArchive", "Upgrade", "RecorderStagingDirectory")
	applyStringOverride(&cfg.OpticalArchive.Upgrade.GatewayApplyCommand, "OpticalArchive", "Upgrade", "GatewayApplyCommand")
	applyStringOverride(&cfg.OpticalArchive.Upgrade.RecorderApplyCommand, "OpticalArchive", "Upgrade", "RecorderApplyCommand")

	applyBoolOverride(&cfg.OpticalArchive.LinuxServices.ManageRecorderProcessLocally, "OpticalArchive", "LinuxServices", "ManageRecorderProcessLocally")
	applyStringOverride(&cfg.OpticalArchive.LinuxServices.RecorderServiceName, "OpticalArchive", "LinuxServices", "RecorderServiceName")
	applyStringOverride(&cfg.OpticalArchive.LinuxServices.RecorderProcessPattern, "OpticalArchive", "LinuxServices", "RecorderProcessPattern")
	applyStringOverride(&cfg.OpticalArchive.LinuxServices.RecorderStartCommand, "OpticalArchive", "LinuxServices", "RecorderStartCommand")
	applyStringOverride(&cfg.OpticalArchive.LinuxServices.RecorderWorkingDirectory, "OpticalArchive", "LinuxServices", "RecorderWorkingDirectory")
	applyIntOverride(&cfg.OpticalArchive.LinuxServices.RecorderHealthCheckSeconds, "OpticalArchive", "LinuxServices", "RecorderHealthCheckSeconds")
	applyBoolOverride(&cfg.OpticalArchive.LinuxServices.MountRefreshEnabled, "OpticalArchive", "LinuxServices", "MountRefreshEnabled")
	applyStringOverride(&cfg.OpticalArchive.LinuxServices.MountRefreshServiceType, "OpticalArchive", "LinuxServices", "MountRefreshServiceType")
	applyStringOverride(&cfg.OpticalArchive.LinuxServices.MountRefreshMountPath, "OpticalArchive", "LinuxServices", "MountRefreshMountPath")
	applyStringOverride(&cfg.OpticalArchive.LinuxServices.MountRefreshDevice, "OpticalArchive", "LinuxServices", "MountRefreshDevice")
	applyStringOverride(&cfg.OpticalArchive.LinuxServices.MountRefreshCommand, "OpticalArchive", "LinuxServices", "MountRefreshCommand")
}

func applyStringOverride(target *string, parts ...string) {
	if target == nil {
		return
	}
	if raw, ok := lookupEnvOverride(parts...); ok {
		*target = strings.TrimSpace(raw)
	}
}

func applyIntOverride(target *int, parts ...string) {
	if target == nil {
		return
	}
	if raw, ok := lookupEnvOverride(parts...); ok {
		if value, err := strconv.Atoi(strings.TrimSpace(raw)); err == nil {
			*target = value
		}
	}
}

func applyInt64Override(target *int64, parts ...string) {
	if target == nil {
		return
	}
	if raw, ok := lookupEnvOverride(parts...); ok {
		if value, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 64); err == nil {
			*target = value
		}
	}
}

func applyBoolOverride(target *bool, parts ...string) {
	if target == nil {
		return
	}
	if raw, ok := lookupEnvOverride(parts...); ok {
		if value, err := strconv.ParseBool(strings.TrimSpace(raw)); err == nil {
			*target = value
		}
	}
}

func lookupEnvOverride(parts ...string) (string, bool) {
	for _, key := range envOverrideKeys(parts...) {
		if value, ok := os.LookupEnv(key); ok {
			return value, true
		}
	}
	return "", false
}

func envOverrideKeys(parts ...string) []string {
	if len(parts) == 0 {
		return nil
	}

	dotnetStyle := strings.Join(parts, "__")
	snakeParts := make([]string, 0, len(parts))
	for _, part := range parts {
		snakeParts = append(snakeParts, strings.ToUpper(camelToSnake(part)))
	}

	return []string{
		dotnetStyle,
		strings.Join(snakeParts, "__"),
	}
}

func camelToSnake(in string) string {
	if in == "" {
		return ""
	}

	var out []rune
	for i, r := range in {
		if i > 0 && unicode.IsUpper(r) {
			prev := rune(in[i-1])
			if unicode.IsLower(prev) || unicode.IsDigit(prev) {
				out = append(out, '_')
			}
		}
		out = append(out, r)
	}
	return string(out)
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

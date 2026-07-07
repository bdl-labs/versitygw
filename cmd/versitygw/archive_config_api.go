package main

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/gofiber/fiber/v3"
	"github.com/versity/versitygw/archiveconfig"
	burnbridgev1 "github.com/versity/versitygw/backend/burnbridge/proto"
	"github.com/versity/versitygw/s3api"
)

func archiveConfigRouteOptions() []s3api.Option {
	var options []s3api.Option
	options = append(options, archiveRoute(http.MethodGet, "/__archive/config", archiveConfigGetHandler())...)
	options = append(options, archiveRoute(http.MethodPut, "/__archive/config", archiveConfigPutHandler())...)
	options = append(options, archiveRoute(http.MethodPost, "/__archive/config/refresh", archiveConfigRefreshHandler())...)
	return options
}

func archiveConfigGetHandler() fiber.Handler {
	return func(c fiber.Ctx) error {
		cfg, path, ok, err := authorizeArchiveConfigRequest(c)
		if err != nil {
			return writeArchiveErrorFrom(c, err, http.StatusInternalServerError)
		}
		if !ok {
			return writeArchiveConfigUnauthorized(c)
		}

		response := map[string]any{
			"path":   path,
			"groups": archiveConfigGroups(cfg),
			"config": cfg,
		}
		return c.JSON(response)
	}
}

func archiveConfigPutHandler() fiber.Handler {
	return func(c fiber.Ctx) error {
		current, path, ok, err := authorizeArchiveConfigRequest(c)
		if err != nil {
			return writeArchiveErrorFrom(c, err, http.StatusInternalServerError)
		}
		if !ok {
			return writeArchiveConfigUnauthorized(c)
		}

		var next archiveconfig.File
		if err := json.Unmarshal(c.Body(), &next); err != nil {
			return writeArchiveError(c, http.StatusBadRequest, "invalid archive config payload")
		}

		mergeArchiveConfigDefaults(&next, current, path)
		if err := archiveconfig.Save(path, next); err != nil {
			return writeArchiveError(c, http.StatusInternalServerError, err.Error())
		}

		applyResult, applyErr := applyArchiveConfigRuntimeOptions(next)
		return c.JSON(map[string]any{
			"saved":        true,
			"path":         path,
			"groups":       archiveConfigGroups(next),
			"config":       next,
			"runtimeApply": archiveConfigRuntimeApplyPayload(applyResult, applyErr),
		})
	}
}

func archiveConfigRefreshHandler() fiber.Handler {
	return func(c fiber.Ctx) error {
		cfg, path, ok, err := authorizeArchiveConfigRequest(c)
		if err != nil {
			return writeArchiveErrorFrom(c, err, http.StatusInternalServerError)
		}
		if !ok {
			return writeArchiveConfigUnauthorized(c)
		}

		result, applyErr := applyArchiveConfigRuntimeOptions(cfg)
		if applyErr != nil {
			return writeArchiveError(c, http.StatusBadGateway, applyErr.Error())
		}

		return c.JSON(map[string]any{
			"saved":        true,
			"path":         path,
			"groups":       archiveConfigGroups(cfg),
			"config":       cfg,
			"runtimeApply": archiveConfigRuntimeApplyPayload(result, nil),
		})
	}
}

func authorizeArchiveConfigRequest(c fiber.Ctx) (archiveconfig.File, string, bool, error) {
	cfg, path, err := archiveconfig.Load("")
	if err != nil {
		return archiveconfig.File{}, "", false, fiber.NewError(http.StatusInternalServerError, err.Error())
	}

	header := strings.TrimSpace(c.Get("Authorization"))
	return cfg, path, archiveconfig.CheckBasicAuth(header, cfg), nil
}

func writeArchiveConfigUnauthorized(c fiber.Ctx) error {
	c.Set("WWW-Authenticate", `Basic realm="optical-archive-config"`)
	return writeArchiveError(c, http.StatusUnauthorized, "archive config authentication required")
}

func mergeArchiveConfigDefaults(next *archiveconfig.File, current archiveconfig.File, path string) {
	defaults := archiveconfig.DefaultFile(path)
	if strings.TrimSpace(next.OpticalArchive.Gateway.ConfigApiUsername) == "" {
		next.OpticalArchive.Gateway.ConfigApiUsername = current.OpticalArchive.Gateway.ConfigApiUsername
		if strings.TrimSpace(next.OpticalArchive.Gateway.ConfigApiUsername) == "" {
			next.OpticalArchive.Gateway.ConfigApiUsername = defaults.OpticalArchive.Gateway.ConfigApiUsername
		}
	}
	if strings.TrimSpace(next.OpticalArchive.Gateway.ConfigApiPassword) == "" {
		next.OpticalArchive.Gateway.ConfigApiPassword = current.OpticalArchive.Gateway.ConfigApiPassword
		if strings.TrimSpace(next.OpticalArchive.Gateway.ConfigApiPassword) == "" {
			next.OpticalArchive.Gateway.ConfigApiPassword = defaults.OpticalArchive.Gateway.ConfigApiPassword
		}
	}
	if strings.TrimSpace(next.OpticalArchive.Gateway.ConfigFilePath) == "" {
		next.OpticalArchive.Gateway.ConfigFilePath = path
	}
	if strings.TrimSpace(next.OpticalArchive.Gateway.BurnServerConfigPath) == "" {
		next.OpticalArchive.Gateway.BurnServerConfigPath = current.OpticalArchive.Gateway.BurnServerConfigPath
	}
	if strings.TrimSpace(next.OpticalArchive.Gateway.GatewayMetadataDbPath) == "" {
		next.OpticalArchive.Gateway.GatewayMetadataDbPath = current.OpticalArchive.Gateway.GatewayMetadataDbPath
	}
	if strings.TrimSpace(next.OpticalArchive.ReadMountPath) == "" {
		next.OpticalArchive.ReadMountPath = current.OpticalArchive.ReadMountPath
	}
	if strings.TrimSpace(next.OpticalArchive.Recorder.LayoutDbPath) == "" {
		next.OpticalArchive.Recorder.LayoutDbPath = current.OpticalArchive.Recorder.LayoutDbPath
	}
	if strings.TrimSpace(next.OpticalArchive.Recorder.MetadataDbFileNameTemplate) == "" {
		next.OpticalArchive.Recorder.MetadataDbFileNameTemplate = current.OpticalArchive.Recorder.MetadataDbFileNameTemplate
	}
	if strings.TrimSpace(next.OpticalArchive.Recorder.LicenseFilePath) == "" {
		next.OpticalArchive.Recorder.LicenseFilePath = current.OpticalArchive.Recorder.LicenseFilePath
	}
	if next.OpticalArchive.Recorder.GrpcChunkSize <= 0 {
		next.OpticalArchive.Recorder.GrpcChunkSize = current.OpticalArchive.Recorder.GrpcChunkSize
	}
	if next.OpticalArchive.Recorder.MaxReceiveMessageSize <= 0 {
		next.OpticalArchive.Recorder.MaxReceiveMessageSize = current.OpticalArchive.Recorder.MaxReceiveMessageSize
	}
	if next.OpticalArchive.Recorder.MaxSendMessageSize <= 0 {
		next.OpticalArchive.Recorder.MaxSendMessageSize = current.OpticalArchive.Recorder.MaxSendMessageSize
	}
	if next.OpticalArchive.Recorder.Http2InitialConnectionWindowSize <= 0 {
		next.OpticalArchive.Recorder.Http2InitialConnectionWindowSize = current.OpticalArchive.Recorder.Http2InitialConnectionWindowSize
	}
	if next.OpticalArchive.Recorder.Http2InitialStreamWindowSize <= 0 {
		next.OpticalArchive.Recorder.Http2InitialStreamWindowSize = current.OpticalArchive.Recorder.Http2InitialStreamWindowSize
	}
	if next.OpticalArchive.Recorder.FinalizeReservePercent < 0 {
		next.OpticalArchive.Recorder.FinalizeReservePercent = current.OpticalArchive.Recorder.FinalizeReservePercent
	}
	if next.OpticalArchive.Recorder.FinalizeReserveBytes < 0 {
		next.OpticalArchive.Recorder.FinalizeReserveBytes = current.OpticalArchive.Recorder.FinalizeReserveBytes
	}
	if next.OpticalArchive.Recorder.UncommittedUploadJobTimeoutSeconds <= 0 {
		next.OpticalArchive.Recorder.UncommittedUploadJobTimeoutSeconds = current.OpticalArchive.Recorder.UncommittedUploadJobTimeoutSeconds
	}
	if next.OpticalArchive.Recorder.UncommittedUploadJobScanSeconds <= 0 {
		next.OpticalArchive.Recorder.UncommittedUploadJobScanSeconds = current.OpticalArchive.Recorder.UncommittedUploadJobScanSeconds
	}
	if strings.TrimSpace(next.OpticalArchive.Recorder.PostFinalizeMediaRecoveryMode) == "" {
		next.OpticalArchive.Recorder.PostFinalizeMediaRecoveryMode = current.OpticalArchive.Recorder.PostFinalizeMediaRecoveryMode
	}
	if strings.TrimSpace(next.OpticalArchive.Recorder.CdWriteSpeedX) == "" {
		next.OpticalArchive.Recorder.CdWriteSpeedX = current.OpticalArchive.Recorder.CdWriteSpeedX
	}
	if strings.TrimSpace(next.OpticalArchive.Recorder.DvdWriteSpeedX) == "" {
		next.OpticalArchive.Recorder.DvdWriteSpeedX = current.OpticalArchive.Recorder.DvdWriteSpeedX
	}
	if strings.TrimSpace(next.OpticalArchive.Recorder.BdWriteSpeedX) == "" {
		next.OpticalArchive.Recorder.BdWriteSpeedX = current.OpticalArchive.Recorder.BdWriteSpeedX
	}
	if strings.TrimSpace(next.OpticalArchive.Recorder.DiscSerialStrategy) == "" {
		next.OpticalArchive.Recorder.DiscSerialStrategy = current.OpticalArchive.Recorder.DiscSerialStrategy
	}
	if strings.TrimSpace(next.OpticalArchive.Recorder.VolumeLabelStrategy) == "" {
		next.OpticalArchive.Recorder.VolumeLabelStrategy = current.OpticalArchive.Recorder.VolumeLabelStrategy
	}
	if strings.TrimSpace(next.OpticalArchive.Recorder.SerialPrefix) == "" {
		next.OpticalArchive.Recorder.SerialPrefix = current.OpticalArchive.Recorder.SerialPrefix
	}
	if strings.TrimSpace(next.OpticalArchive.Recorder.VolumeLabelPrefix) == "" {
		next.OpticalArchive.Recorder.VolumeLabelPrefix = current.OpticalArchive.Recorder.VolumeLabelPrefix
	}
	if next.OpticalArchive.Recorder.GeneratedSerialLength <= 0 {
		next.OpticalArchive.Recorder.GeneratedSerialLength = current.OpticalArchive.Recorder.GeneratedSerialLength
	}
	if next.OpticalArchive.Recorder.GeneratedVolumeLabelLength <= 0 {
		next.OpticalArchive.Recorder.GeneratedVolumeLabelLength = current.OpticalArchive.Recorder.GeneratedVolumeLabelLength
	}
	if next.OpticalArchive.Recorder.AnchorCopies <= 0 {
		next.OpticalArchive.Recorder.AnchorCopies = current.OpticalArchive.Recorder.AnchorCopies
	}
	if next.OpticalArchive.Recorder.AnchorScanBlocks <= 0 {
		next.OpticalArchive.Recorder.AnchorScanBlocks = current.OpticalArchive.Recorder.AnchorScanBlocks
	}
	if next.OpticalArchive.Recorder.AnchorScanReadBatchBlocks <= 0 {
		next.OpticalArchive.Recorder.AnchorScanReadBatchBlocks = current.OpticalArchive.Recorder.AnchorScanReadBatchBlocks
	}
	if next.OpticalArchive.Recorder.AnchorScanMaxConsecutiveUnreadableBlocks <= 0 {
		next.OpticalArchive.Recorder.AnchorScanMaxConsecutiveUnreadableBlocks = current.OpticalArchive.Recorder.AnchorScanMaxConsecutiveUnreadableBlocks
	}
	if next.OpticalArchive.Runtime.SectorSizeBytes <= 0 {
		next.OpticalArchive.Runtime.SectorSizeBytes = current.OpticalArchive.Runtime.SectorSizeBytes
	}
	if next.OpticalArchive.Runtime.BlocksPerTransfer <= 0 {
		next.OpticalArchive.Runtime.BlocksPerTransfer = current.OpticalArchive.Runtime.BlocksPerTransfer
	}
	if next.OpticalArchive.Runtime.ReadBlocksPerTransfer < 0 {
		next.OpticalArchive.Runtime.ReadBlocksPerTransfer = current.OpticalArchive.Runtime.ReadBlocksPerTransfer
	}
	if next.OpticalArchive.Runtime.SessionCacheCapacityBytes <= 0 {
		next.OpticalArchive.Runtime.SessionCacheCapacityBytes = current.OpticalArchive.Runtime.SessionCacheCapacityBytes
	}
	if next.OpticalArchive.Runtime.WriteBufferBytes < 0 {
		next.OpticalArchive.Runtime.WriteBufferBytes = current.OpticalArchive.Runtime.WriteBufferBytes
	}
	if next.OpticalArchive.Runtime.WriteBufferSlotCount <= 0 {
		next.OpticalArchive.Runtime.WriteBufferSlotCount = current.OpticalArchive.Runtime.WriteBufferSlotCount
	}
	if next.OpticalArchive.Runtime.GlobalWriteQueueCapacity <= 0 {
		next.OpticalArchive.Runtime.GlobalWriteQueueCapacity = current.OpticalArchive.Runtime.GlobalWriteQueueCapacity
	}
	if next.OpticalArchive.Runtime.ReadQueueCapacity <= 0 {
		next.OpticalArchive.Runtime.ReadQueueCapacity = current.OpticalArchive.Runtime.ReadQueueCapacity
	}
	if next.OpticalArchive.Runtime.RedundancyReadWindowBlocks <= 0 {
		next.OpticalArchive.Runtime.RedundancyReadWindowBlocks = current.OpticalArchive.Runtime.RedundancyReadWindowBlocks
	}
	if next.OpticalArchive.Runtime.PlainReadWindowBlocks <= 0 {
		next.OpticalArchive.Runtime.PlainReadWindowBlocks = current.OpticalArchive.Runtime.PlainReadWindowBlocks
	}
	if next.OpticalArchive.Runtime.ReadOutputBufferBytes <= 0 {
		next.OpticalArchive.Runtime.ReadOutputBufferBytes = current.OpticalArchive.Runtime.ReadOutputBufferBytes
	}
	if next.OpticalArchive.Runtime.ReadOutputBufferSlotCount <= 0 {
		next.OpticalArchive.Runtime.ReadOutputBufferSlotCount = current.OpticalArchive.Runtime.ReadOutputBufferSlotCount
	}
	if next.OpticalArchive.Redundancy.DataBlockCount <= 0 {
		next.OpticalArchive.Redundancy.DataBlockCount = current.OpticalArchive.Redundancy.DataBlockCount
	}
	if next.OpticalArchive.Redundancy.ParityBlockCount <= 0 {
		next.OpticalArchive.Redundancy.ParityBlockCount = current.OpticalArchive.Redundancy.ParityBlockCount
	}
	if next.OpticalArchive.Redundancy.BlockSizeBytes <= 0 {
		next.OpticalArchive.Redundancy.BlockSizeBytes = current.OpticalArchive.Redundancy.BlockSizeBytes
	}
	if strings.TrimSpace(next.OpticalArchive.GatewayInterop.GrpcAddr) == "" {
		next.OpticalArchive.GatewayInterop.GrpcAddr = current.OpticalArchive.GatewayInterop.GrpcAddr
	}
	if next.OpticalArchive.GatewayInterop.GrpcDialTimeoutSeconds <= 0 {
		next.OpticalArchive.GatewayInterop.GrpcDialTimeoutSeconds = current.OpticalArchive.GatewayInterop.GrpcDialTimeoutSeconds
	}
	if next.OpticalArchive.GatewayInterop.GrpcReadyTimeoutSeconds <= 0 {
		next.OpticalArchive.GatewayInterop.GrpcReadyTimeoutSeconds = current.OpticalArchive.GatewayInterop.GrpcReadyTimeoutSeconds
	}
	if next.OpticalArchive.GatewayInterop.GrpcPingTimeoutSeconds <= 0 {
		next.OpticalArchive.GatewayInterop.GrpcPingTimeoutSeconds = current.OpticalArchive.GatewayInterop.GrpcPingTimeoutSeconds
	}
	if strings.TrimSpace(next.OpticalArchive.Logging.Gateway.AccessLogPath) == "" {
		next.OpticalArchive.Logging.Gateway.AccessLogPath = current.OpticalArchive.Logging.Gateway.AccessLogPath
	}
	if strings.TrimSpace(next.OpticalArchive.Logging.Gateway.AdminLogPath) == "" {
		next.OpticalArchive.Logging.Gateway.AdminLogPath = current.OpticalArchive.Logging.Gateway.AdminLogPath
	}
	if next.OpticalArchive.Logging.Gateway.FileSizeMb <= 0 {
		next.OpticalArchive.Logging.Gateway.FileSizeMb = current.OpticalArchive.Logging.Gateway.FileSizeMb
	}
	if next.OpticalArchive.Logging.Gateway.MaxBackups <= 0 {
		next.OpticalArchive.Logging.Gateway.MaxBackups = current.OpticalArchive.Logging.Gateway.MaxBackups
	}
	if next.OpticalArchive.Logging.Gateway.RetentionDays <= 0 {
		next.OpticalArchive.Logging.Gateway.RetentionDays = current.OpticalArchive.Logging.Gateway.RetentionDays
	}
	if strings.TrimSpace(next.OpticalArchive.Logging.Recorder.LogDirectory) == "" {
		next.OpticalArchive.Logging.Recorder.LogDirectory = current.OpticalArchive.Logging.Recorder.LogDirectory
	}
	if next.OpticalArchive.Logging.Recorder.FileSizeMb <= 0 {
		next.OpticalArchive.Logging.Recorder.FileSizeMb = current.OpticalArchive.Logging.Recorder.FileSizeMb
	}
	if next.OpticalArchive.Logging.Recorder.RetentionDays <= 0 {
		next.OpticalArchive.Logging.Recorder.RetentionDays = current.OpticalArchive.Logging.Recorder.RetentionDays
	}
	if strings.TrimSpace(next.OpticalArchive.Logging.Recorder.MinLevel) == "" {
		next.OpticalArchive.Logging.Recorder.MinLevel = current.OpticalArchive.Logging.Recorder.MinLevel
	}
	if strings.TrimSpace(next.OpticalArchive.Upgrade.GatewayStagingDirectory) == "" {
		next.OpticalArchive.Upgrade.GatewayStagingDirectory = current.OpticalArchive.Upgrade.GatewayStagingDirectory
	}
	if strings.TrimSpace(next.OpticalArchive.Upgrade.RecorderStagingDirectory) == "" {
		next.OpticalArchive.Upgrade.RecorderStagingDirectory = current.OpticalArchive.Upgrade.RecorderStagingDirectory
	}
	if strings.TrimSpace(next.OpticalArchive.Upgrade.GatewayApplyCommand) == "" {
		next.OpticalArchive.Upgrade.GatewayApplyCommand = current.OpticalArchive.Upgrade.GatewayApplyCommand
	}
	if strings.TrimSpace(next.OpticalArchive.Upgrade.RecorderApplyCommand) == "" {
		next.OpticalArchive.Upgrade.RecorderApplyCommand = current.OpticalArchive.Upgrade.RecorderApplyCommand
	}
	if next.OpticalArchive.LinuxServices.RecorderHealthCheckSeconds <= 0 {
		next.OpticalArchive.LinuxServices.RecorderHealthCheckSeconds = current.OpticalArchive.LinuxServices.RecorderHealthCheckSeconds
	}
	if strings.TrimSpace(next.OpticalArchive.LinuxServices.RecorderServiceName) == "" {
		next.OpticalArchive.LinuxServices.RecorderServiceName = current.OpticalArchive.LinuxServices.RecorderServiceName
	}
	if strings.TrimSpace(next.OpticalArchive.LinuxServices.RecorderProcessPattern) == "" {
		next.OpticalArchive.LinuxServices.RecorderProcessPattern = current.OpticalArchive.LinuxServices.RecorderProcessPattern
	}
	if strings.TrimSpace(next.OpticalArchive.LinuxServices.MountRefreshServiceType) == "" {
		next.OpticalArchive.LinuxServices.MountRefreshServiceType = current.OpticalArchive.LinuxServices.MountRefreshServiceType
	}
	if strings.TrimSpace(next.OpticalArchive.GpioMediaMonitor.PinNumberingScheme) == "" {
		next.OpticalArchive.GpioMediaMonitor.PinNumberingScheme = current.OpticalArchive.GpioMediaMonitor.PinNumberingScheme
	}
	if strings.TrimSpace(next.OpticalArchive.GpioMediaMonitor.DiscInsertedLevel) == "" {
		next.OpticalArchive.GpioMediaMonitor.DiscInsertedLevel = current.OpticalArchive.GpioMediaMonitor.DiscInsertedLevel
	}
	if strings.TrimSpace(next.OpticalArchive.GpioMediaMonitor.TrayOpenLevel) == "" {
		next.OpticalArchive.GpioMediaMonitor.TrayOpenLevel = current.OpticalArchive.GpioMediaMonitor.TrayOpenLevel
	}
	if strings.TrimSpace(next.OpticalArchive.GpioMediaMonitor.EjectActiveLevel) == "" {
		next.OpticalArchive.GpioMediaMonitor.EjectActiveLevel = current.OpticalArchive.GpioMediaMonitor.EjectActiveLevel
	}
	if next.OpticalArchive.GpioMediaMonitor.EjectPulseMilliseconds <= 0 {
		next.OpticalArchive.GpioMediaMonitor.EjectPulseMilliseconds = current.OpticalArchive.GpioMediaMonitor.EjectPulseMilliseconds
		if next.OpticalArchive.GpioMediaMonitor.EjectPulseMilliseconds <= 0 {
			next.OpticalArchive.GpioMediaMonitor.EjectPulseMilliseconds = defaults.OpticalArchive.GpioMediaMonitor.EjectPulseMilliseconds
		}
	}
	if next.OpticalArchive.GpioMediaMonitor.TraySettleMilliseconds <= 0 {
		next.OpticalArchive.GpioMediaMonitor.TraySettleMilliseconds = current.OpticalArchive.GpioMediaMonitor.TraySettleMilliseconds
		if next.OpticalArchive.GpioMediaMonitor.TraySettleMilliseconds <= 0 {
			next.OpticalArchive.GpioMediaMonitor.TraySettleMilliseconds = defaults.OpticalArchive.GpioMediaMonitor.TraySettleMilliseconds
		}
	}
	if next.OpticalArchive.GpioMediaMonitor.CloseTrayFallbackWaitMilliseconds <= 0 {
		next.OpticalArchive.GpioMediaMonitor.CloseTrayFallbackWaitMilliseconds = current.OpticalArchive.GpioMediaMonitor.CloseTrayFallbackWaitMilliseconds
		if next.OpticalArchive.GpioMediaMonitor.CloseTrayFallbackWaitMilliseconds <= 0 {
			next.OpticalArchive.GpioMediaMonitor.CloseTrayFallbackWaitMilliseconds = defaults.OpticalArchive.GpioMediaMonitor.CloseTrayFallbackWaitMilliseconds
		}
	}
	if next.OpticalArchive.GpioMediaMonitor.DebounceMilliseconds <= 0 {
		next.OpticalArchive.GpioMediaMonitor.DebounceMilliseconds = current.OpticalArchive.GpioMediaMonitor.DebounceMilliseconds
		if next.OpticalArchive.GpioMediaMonitor.DebounceMilliseconds <= 0 {
			next.OpticalArchive.GpioMediaMonitor.DebounceMilliseconds = defaults.OpticalArchive.GpioMediaMonitor.DebounceMilliseconds
		}
	}
	if next.OpticalArchive.GpioMediaMonitor.InsertSettleMilliseconds <= 0 {
		next.OpticalArchive.GpioMediaMonitor.InsertSettleMilliseconds = current.OpticalArchive.GpioMediaMonitor.InsertSettleMilliseconds
		if next.OpticalArchive.GpioMediaMonitor.InsertSettleMilliseconds <= 0 {
			next.OpticalArchive.GpioMediaMonitor.InsertSettleMilliseconds = defaults.OpticalArchive.GpioMediaMonitor.InsertSettleMilliseconds
		}
	}
	if next.OpticalArchive.GpioMediaMonitor.DiscInfoReadyTimeoutMilliseconds <= 0 {
		next.OpticalArchive.GpioMediaMonitor.DiscInfoReadyTimeoutMilliseconds = current.OpticalArchive.GpioMediaMonitor.DiscInfoReadyTimeoutMilliseconds
		if next.OpticalArchive.GpioMediaMonitor.DiscInfoReadyTimeoutMilliseconds <= 0 {
			next.OpticalArchive.GpioMediaMonitor.DiscInfoReadyTimeoutMilliseconds = defaults.OpticalArchive.GpioMediaMonitor.DiscInfoReadyTimeoutMilliseconds
		}
	}
	if next.OpticalArchive.GpioMediaMonitor.DiscInfoRetryDelayMilliseconds <= 0 {
		next.OpticalArchive.GpioMediaMonitor.DiscInfoRetryDelayMilliseconds = current.OpticalArchive.GpioMediaMonitor.DiscInfoRetryDelayMilliseconds
		if next.OpticalArchive.GpioMediaMonitor.DiscInfoRetryDelayMilliseconds <= 0 {
			next.OpticalArchive.GpioMediaMonitor.DiscInfoRetryDelayMilliseconds = defaults.OpticalArchive.GpioMediaMonitor.DiscInfoRetryDelayMilliseconds
		}
	}
}

func archiveConfigGroups(cfg archiveconfig.File) map[string]any {
	return map[string]any{
		"Gateway": map[string]any{
			"ConfigApiUsername":     cfg.OpticalArchive.Gateway.ConfigApiUsername,
			"ConfigApiPassword":     cfg.OpticalArchive.Gateway.ConfigApiPassword,
			"ConfigFilePath":        cfg.OpticalArchive.Gateway.ConfigFilePath,
			"BurnServerConfigPath":  cfg.OpticalArchive.Gateway.BurnServerConfigPath,
			"GatewayMetadataDbPath": cfg.OpticalArchive.Gateway.GatewayMetadataDbPath,
		},
		"Shared": map[string]any{
			"ReadMountPath": cfg.OpticalArchive.ReadMountPath,
		},
		"Recorder": map[string]any{
			"DriveIndex":                               cfg.OpticalArchive.Recorder.DriveIndex,
			"LayoutDbPath":                             cfg.OpticalArchive.Recorder.LayoutDbPath,
			"MetadataDbFileNameTemplate":               cfg.OpticalArchive.Recorder.MetadataDbFileNameTemplate,
			"LicenseFilePath":                          cfg.OpticalArchive.Recorder.LicenseFilePath,
			"GrpcChunkSize":                            cfg.OpticalArchive.Recorder.GrpcChunkSize,
			"MaxReceiveMessageSize":                    cfg.OpticalArchive.Recorder.MaxReceiveMessageSize,
			"MaxSendMessageSize":                       cfg.OpticalArchive.Recorder.MaxSendMessageSize,
			"Http2InitialConnectionWindowSize":         cfg.OpticalArchive.Recorder.Http2InitialConnectionWindowSize,
			"Http2InitialStreamWindowSize":             cfg.OpticalArchive.Recorder.Http2InitialStreamWindowSize,
			"FinalizeReservePercent":                   cfg.OpticalArchive.Recorder.FinalizeReservePercent,
			"FinalizeReserveBytes":                     cfg.OpticalArchive.Recorder.FinalizeReserveBytes,
			"UncommittedUploadJobTimeoutSeconds":       cfg.OpticalArchive.Recorder.UncommittedUploadJobTimeoutSeconds,
			"UncommittedUploadJobScanSeconds":          cfg.OpticalArchive.Recorder.UncommittedUploadJobScanSeconds,
			"PostFinalizeMediaRecoveryMode":            cfg.OpticalArchive.Recorder.PostFinalizeMediaRecoveryMode,
			"DisableExplicitSpeedControl":              cfg.OpticalArchive.Recorder.DisableExplicitSpeedControl,
			"CdWriteSpeedX":                            cfg.OpticalArchive.Recorder.CdWriteSpeedX,
			"DvdWriteSpeedX":                           cfg.OpticalArchive.Recorder.DvdWriteSpeedX,
			"BdWriteSpeedX":                            cfg.OpticalArchive.Recorder.BdWriteSpeedX,
			"DiscSerialStrategy":                       cfg.OpticalArchive.Recorder.DiscSerialStrategy,
			"VolumeLabelStrategy":                      cfg.OpticalArchive.Recorder.VolumeLabelStrategy,
			"SerialPrefix":                             cfg.OpticalArchive.Recorder.SerialPrefix,
			"VolumeLabelPrefix":                        cfg.OpticalArchive.Recorder.VolumeLabelPrefix,
			"GeneratedSerialLength":                    cfg.OpticalArchive.Recorder.GeneratedSerialLength,
			"GeneratedVolumeLabelLength":               cfg.OpticalArchive.Recorder.GeneratedVolumeLabelLength,
			"AllowCreateBucketBinding":                 cfg.OpticalArchive.Recorder.AllowCreateBucketBinding,
			"AnchorEnabled":                            cfg.OpticalArchive.Recorder.AnchorEnabled,
			"AnchorRecoveryEnabled":                    cfg.OpticalArchive.Recorder.AnchorRecoveryEnabled,
			"AnchorCopies":                             cfg.OpticalArchive.Recorder.AnchorCopies,
			"AnchorScanBlocks":                         cfg.OpticalArchive.Recorder.AnchorScanBlocks,
			"AnchorScanReadBatchBlocks":                cfg.OpticalArchive.Recorder.AnchorScanReadBatchBlocks,
			"AnchorScanMaxConsecutiveUnreadableBlocks": cfg.OpticalArchive.Recorder.AnchorScanMaxConsecutiveUnreadableBlocks,
			"HiddenUdfLayoutEnabled":                   cfg.OpticalArchive.Recorder.HiddenUdfLayoutEnabled,
		},
		"Runtime": map[string]any{
			"SectorSizeBytes":            cfg.OpticalArchive.Runtime.SectorSizeBytes,
			"BlocksPerTransfer":          cfg.OpticalArchive.Runtime.BlocksPerTransfer,
			"ReadBlocksPerTransfer":      cfg.OpticalArchive.Runtime.ReadBlocksPerTransfer,
			"SessionCacheCapacityBytes":  cfg.OpticalArchive.Runtime.SessionCacheCapacityBytes,
			"WriteBufferBytes":           cfg.OpticalArchive.Runtime.WriteBufferBytes,
			"WriteBufferSlotCount":       cfg.OpticalArchive.Runtime.WriteBufferSlotCount,
			"GlobalWriteQueueCapacity":   cfg.OpticalArchive.Runtime.GlobalWriteQueueCapacity,
			"ReadQueueCapacity":          cfg.OpticalArchive.Runtime.ReadQueueCapacity,
			"RedundancyReadWindowBlocks": cfg.OpticalArchive.Runtime.RedundancyReadWindowBlocks,
			"PlainReadWindowBlocks":      cfg.OpticalArchive.Runtime.PlainReadWindowBlocks,
			"ReadOutputBufferBytes":      cfg.OpticalArchive.Runtime.ReadOutputBufferBytes,
			"ReadOutputBufferSlotCount":  cfg.OpticalArchive.Runtime.ReadOutputBufferSlotCount,
		},
		"Redundancy": map[string]any{
			"Enabled":          cfg.OpticalArchive.Redundancy.Enabled,
			"DataBlockCount":   cfg.OpticalArchive.Redundancy.DataBlockCount,
			"ParityBlockCount": cfg.OpticalArchive.Redundancy.ParityBlockCount,
			"BlockSizeBytes":   cfg.OpticalArchive.Redundancy.BlockSizeBytes,
		},
		"GatewayInterop": map[string]any{
			"GrpcAddr":                cfg.OpticalArchive.GatewayInterop.GrpcAddr,
			"GrpcDialTimeoutSeconds":  cfg.OpticalArchive.GatewayInterop.GrpcDialTimeoutSeconds,
			"GrpcReadyTimeoutSeconds": cfg.OpticalArchive.GatewayInterop.GrpcReadyTimeoutSeconds,
			"GrpcPingTimeoutSeconds":  cfg.OpticalArchive.GatewayInterop.GrpcPingTimeoutSeconds,
		},
		"LoggingGateway": map[string]any{
			"AccessLogPath":     cfg.OpticalArchive.Logging.Gateway.AccessLogPath,
			"AdminLogPath":      cfg.OpticalArchive.Logging.Gateway.AdminLogPath,
			"FileSizeMb":        cfg.OpticalArchive.Logging.Gateway.FileSizeMb,
			"MaxBackups":        cfg.OpticalArchive.Logging.Gateway.MaxBackups,
			"RetentionDays":     cfg.OpticalArchive.Logging.Gateway.RetentionDays,
			"EnableCompression": cfg.OpticalArchive.Logging.Gateway.EnableCompression,
		},
		"LoggingRecorder": map[string]any{
			"LogDirectory":      cfg.OpticalArchive.Logging.Recorder.LogDirectory,
			"FileSizeMb":        cfg.OpticalArchive.Logging.Recorder.FileSizeMb,
			"RetentionDays":     cfg.OpticalArchive.Logging.Recorder.RetentionDays,
			"EnableCompression": cfg.OpticalArchive.Logging.Recorder.EnableCompression,
			"MinLevel":          cfg.OpticalArchive.Logging.Recorder.MinLevel,
		},
		"Upgrade": map[string]any{
			"GatewayStagingDirectory":  cfg.OpticalArchive.Upgrade.GatewayStagingDirectory,
			"RecorderStagingDirectory": cfg.OpticalArchive.Upgrade.RecorderStagingDirectory,
			"GatewayApplyCommand":      cfg.OpticalArchive.Upgrade.GatewayApplyCommand,
			"RecorderApplyCommand":     cfg.OpticalArchive.Upgrade.RecorderApplyCommand,
		},
		"LinuxServices": map[string]any{
			"ManageRecorderProcessLocally": cfg.OpticalArchive.LinuxServices.ManageRecorderProcessLocally,
			"RecorderServiceName":          cfg.OpticalArchive.LinuxServices.RecorderServiceName,
			"RecorderProcessPattern":       cfg.OpticalArchive.LinuxServices.RecorderProcessPattern,
			"RecorderStartCommand":         cfg.OpticalArchive.LinuxServices.RecorderStartCommand,
			"RecorderWorkingDirectory":     cfg.OpticalArchive.LinuxServices.RecorderWorkingDirectory,
			"RecorderHealthCheckSeconds":   cfg.OpticalArchive.LinuxServices.RecorderHealthCheckSeconds,
			"MountRefreshEnabled":          cfg.OpticalArchive.LinuxServices.MountRefreshEnabled,
			"MountRefreshServiceType":      cfg.OpticalArchive.LinuxServices.MountRefreshServiceType,
			"MountRefreshMountPath":        cfg.OpticalArchive.LinuxServices.MountRefreshMountPath,
			"MountRefreshDevice":           cfg.OpticalArchive.LinuxServices.MountRefreshDevice,
			"MountRefreshCommand":          cfg.OpticalArchive.LinuxServices.MountRefreshCommand,
		},
		"GpioMediaMonitor": map[string]any{
			"Enabled":                           cfg.OpticalArchive.GpioMediaMonitor.Enabled,
			"ChipNumber":                        cfg.OpticalArchive.GpioMediaMonitor.ChipNumber,
			"PinNumberingScheme":                cfg.OpticalArchive.GpioMediaMonitor.PinNumberingScheme,
			"DiscInPin":                         cfg.OpticalArchive.GpioMediaMonitor.DiscInPin,
			"TrayInPin":                         cfg.OpticalArchive.GpioMediaMonitor.TrayInPin,
			"EjectPin":                          cfg.OpticalArchive.GpioMediaMonitor.EjectPin,
			"EjectControlEnabled":               cfg.OpticalArchive.GpioMediaMonitor.EjectControlEnabled,
			"EjectActiveLevel":                  cfg.OpticalArchive.GpioMediaMonitor.EjectActiveLevel,
			"EjectPulseMilliseconds":            cfg.OpticalArchive.GpioMediaMonitor.EjectPulseMilliseconds,
			"TraySettleMilliseconds":            cfg.OpticalArchive.GpioMediaMonitor.TraySettleMilliseconds,
			"CloseTrayFallbackWaitMilliseconds": cfg.OpticalArchive.GpioMediaMonitor.CloseTrayFallbackWaitMilliseconds,
			"DiscInsertedLevel":                 cfg.OpticalArchive.GpioMediaMonitor.DiscInsertedLevel,
			"TrayOpenLevel":                     cfg.OpticalArchive.GpioMediaMonitor.TrayOpenLevel,
			"DebounceMilliseconds":              cfg.OpticalArchive.GpioMediaMonitor.DebounceMilliseconds,
			"InsertSettleMilliseconds":          cfg.OpticalArchive.GpioMediaMonitor.InsertSettleMilliseconds,
			"DiscInfoReadyTimeoutMilliseconds":  cfg.OpticalArchive.GpioMediaMonitor.DiscInfoReadyTimeoutMilliseconds,
			"DiscInfoRetryDelayMilliseconds":    cfg.OpticalArchive.GpioMediaMonitor.DiscInfoRetryDelayMilliseconds,
			"RequireTrayClosedForInsert":        cfg.OpticalArchive.GpioMediaMonitor.RequireTrayClosedForInsert,
			"UseDiscInForPresence":              cfg.OpticalArchive.GpioMediaMonitor.UseDiscInForPresence,
			"ProcessInitialState":               cfg.OpticalArchive.GpioMediaMonitor.ProcessInitialState,
		},
	}
}

func applyArchiveConfigRuntimeOptions(cfg archiveconfig.File) (map[string]any, error) {
	client, conn, err := openRecorderClient(cfg)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	resp, err := client.ConfigureRuntimeOptions(ctx, &burnbridgev1.ConfigureRuntimeOptionsRequest{
		SetHiddenUdfLayoutEnabled: true,
		HiddenUdfLayoutEnabled:    cfg.OpticalArchive.Recorder.HiddenUdfLayoutEnabled,
	})
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"applied":                   true,
		"hiddenUdfLayoutEnabled":    resp.GetHiddenUdfLayoutEnabled(),
		"hiddenUdfLayoutOverridden": resp.GetHiddenUdfLayoutOverridden(),
		"hiddenUdfLayoutSource":     resp.GetHiddenUdfLayoutSource(),
		"message":                   resp.GetMessage(),
		"restartRequiredFields": []string{
			"Recorder.AnchorEnabled",
			"Recorder.AnchorRecoveryEnabled",
			"Recorder.AnchorCopies",
			"Recorder.AnchorScanBlocks",
			"Recorder.AnchorScanReadBatchBlocks",
			"Recorder.AnchorScanMaxConsecutiveUnreadableBlocks",
			"Runtime.*",
			"GatewayInterop.*",
		},
	}, nil
}

func archiveConfigRuntimeApplyPayload(result map[string]any, err error) map[string]any {
	if err != nil {
		return map[string]any{
			"applied": false,
			"error":   err.Error(),
		}
	}
	if result == nil {
		return map[string]any{"applied": false}
	}
	return result
}

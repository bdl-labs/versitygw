package main

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/gofiber/fiber/v2"
	"github.com/versity/versitygw/archiveconfig"
	"github.com/versity/versitygw/s3api"
)

func archiveConfigRouteOptions() []s3api.Option {
	return []s3api.Option{
		s3api.WithRoute(http.MethodGet, "/__archive/config", archiveConfigGetHandler()),
		s3api.WithRoute(http.MethodPut, "/__archive/config", archiveConfigPutHandler()),
	}
}

func archiveConfigGetHandler() fiber.Handler {
	return func(c *fiber.Ctx) error {
		cfg, path, ok, err := authorizeArchiveConfigRequest(c)
		if err != nil {
			return err
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
	return func(c *fiber.Ctx) error {
		current, path, ok, err := authorizeArchiveConfigRequest(c)
		if err != nil {
			return err
		}
		if !ok {
			return writeArchiveConfigUnauthorized(c)
		}

		var next archiveconfig.File
		if err := json.Unmarshal(c.Body(), &next); err != nil {
			return fiber.NewError(http.StatusBadRequest, "invalid archive config payload")
		}

		mergeArchiveConfigDefaults(&next, current, path)
		if err := archiveconfig.Save(path, next); err != nil {
			return fiber.NewError(http.StatusInternalServerError, err.Error())
		}

		return c.JSON(map[string]any{
			"saved":  true,
			"path":   path,
			"groups": archiveConfigGroups(next),
			"config": next,
		})
	}
}

func authorizeArchiveConfigRequest(c *fiber.Ctx) (archiveconfig.File, string, bool, error) {
	cfg, path, err := archiveconfig.Load("")
	if err != nil {
		return archiveconfig.File{}, "", false, fiber.NewError(http.StatusInternalServerError, err.Error())
	}

	header := strings.TrimSpace(c.Get("Authorization"))
	return cfg, path, archiveconfig.CheckBasicAuth(header, cfg), nil
}

func writeArchiveConfigUnauthorized(c *fiber.Ctx) error {
	c.Set("WWW-Authenticate", `Basic realm="optical-archive-config"`)
	return fiber.NewError(http.StatusUnauthorized, "archive config authentication required")
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
	if next.OpticalArchive.Recorder.GrpcChunkSize <= 0 {
		next.OpticalArchive.Recorder.GrpcChunkSize = current.OpticalArchive.Recorder.GrpcChunkSize
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
	if next.OpticalArchive.Runtime.SectorSizeBytes <= 0 {
		next.OpticalArchive.Runtime.SectorSizeBytes = current.OpticalArchive.Runtime.SectorSizeBytes
	}
	if next.OpticalArchive.Runtime.BlocksPerTransfer <= 0 {
		next.OpticalArchive.Runtime.BlocksPerTransfer = current.OpticalArchive.Runtime.BlocksPerTransfer
	}
	if next.OpticalArchive.Runtime.SessionCacheCapacityBytes <= 0 {
		next.OpticalArchive.Runtime.SessionCacheCapacityBytes = current.OpticalArchive.Runtime.SessionCacheCapacityBytes
	}
	if next.OpticalArchive.Runtime.WriteBufferBytes < 0 {
		next.OpticalArchive.Runtime.WriteBufferBytes = current.OpticalArchive.Runtime.WriteBufferBytes
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
			"DriveIndex":                 cfg.OpticalArchive.Recorder.DriveIndex,
			"LayoutDbPath":               cfg.OpticalArchive.Recorder.LayoutDbPath,
			"MetadataDbFileNameTemplate": cfg.OpticalArchive.Recorder.MetadataDbFileNameTemplate,
			"GrpcChunkSize":              cfg.OpticalArchive.Recorder.GrpcChunkSize,
			"DiscSerialStrategy":         cfg.OpticalArchive.Recorder.DiscSerialStrategy,
			"VolumeLabelStrategy":        cfg.OpticalArchive.Recorder.VolumeLabelStrategy,
			"SerialPrefix":               cfg.OpticalArchive.Recorder.SerialPrefix,
			"VolumeLabelPrefix":          cfg.OpticalArchive.Recorder.VolumeLabelPrefix,
			"GeneratedSerialLength":      cfg.OpticalArchive.Recorder.GeneratedSerialLength,
			"GeneratedVolumeLabelLength": cfg.OpticalArchive.Recorder.GeneratedVolumeLabelLength,
			"AllowCreateBucketBinding":   cfg.OpticalArchive.Recorder.AllowCreateBucketBinding,
		},
		"Runtime": map[string]any{
			"SectorSizeBytes":           cfg.OpticalArchive.Runtime.SectorSizeBytes,
			"BlocksPerTransfer":         cfg.OpticalArchive.Runtime.BlocksPerTransfer,
			"SessionCacheCapacityBytes": cfg.OpticalArchive.Runtime.SessionCacheCapacityBytes,
			"WriteBufferBytes":          cfg.OpticalArchive.Runtime.WriteBufferBytes,
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
	}
}

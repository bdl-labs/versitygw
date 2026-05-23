package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/versity/versitygw/archiveconfig"
	burnbridgev1 "github.com/versity/versitygw/backend/burnbridge/proto"
	"github.com/versity/versitygw/s3api"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type archiveVersionResponse struct {
	Gateway  map[string]any `json:"gateway"`
	Recorder map[string]any `json:"recorder,omitempty"`
}

func archiveAdminRouteOptions() []s3api.Option {
	return []s3api.Option{
		s3api.WithRoute(http.MethodGet, "/__archive/version", archiveVersionHandler()),
		s3api.WithRoute(http.MethodPost, "/__archive/license", archiveLicenseUploadHandler()),
		s3api.WithRoute(http.MethodPost, "/__archive/upgrade/upload", archiveUpgradeUploadHandler()),
		s3api.WithRoute(http.MethodPost, "/__archive/upgrade/apply", archiveUpgradeApplyHandler()),
	}
}

func archiveVersionHandler() fiber.Handler {
	return func(c *fiber.Ctx) error {
		cfg, _, ok, err := authorizeArchiveConfigRequest(c)
		if err != nil {
			return err
		}
		if !ok {
			return writeArchiveConfigUnauthorized(c)
		}

		response := archiveVersionResponse{
			Gateway: map[string]any{
				"serviceName":  "VersityGW",
				"serviceVersion": Version,
				"build":        Build,
				"buildTime":    BuildTime,
				"goVersion":    runtime.Version(),
				"osArch":       runtime.GOOS + "/" + runtime.GOARCH,
			},
		}

		recorder, recErr := fetchRecorderVersion(cfg)
		if recErr != nil {
			response.Recorder = map[string]any{
				"status": "error",
				"error":  recErr.Error(),
			}
		} else {
			response.Recorder = recorder
		}
		return c.JSON(response)
	}
}

func archiveLicenseUploadHandler() fiber.Handler {
	return func(c *fiber.Ctx) error {
		cfg, path, ok, err := authorizeArchiveConfigRequest(c)
		if err != nil {
			return err
		}
		if !ok {
			return writeArchiveConfigUnauthorized(c)
		}

		uploaded, err := c.FormFile("file")
		if err != nil {
			return fiber.NewError(http.StatusBadRequest, "license file is required")
		}

		content, err := readUploadedFile(uploaded)
		if err != nil {
			return fiber.NewError(http.StatusBadRequest, err.Error())
		}
		reloadNow := strings.EqualFold(strings.TrimSpace(c.FormValue("reloadNow")), "true")

		result, err := pushRecorderLicense(cfg, uploaded.Filename, content, reloadNow)
		if err != nil {
			return fiber.NewError(http.StatusBadGateway, err.Error())
		}

		licenseFilePath, _ := result["licenseFilePath"].(string)
		if strings.TrimSpace(licenseFilePath) != "" &&
			!strings.EqualFold(cfg.OpticalArchive.Recorder.LicenseFilePath, licenseFilePath) {
			cfg.OpticalArchive.Recorder.LicenseFilePath = licenseFilePath
			if saveErr := archiveconfig.Save(path, cfg); saveErr != nil {
				return fiber.NewError(http.StatusInternalServerError, saveErr.Error())
			}
		}

		return c.JSON(result)
	}
}

func archiveUpgradeUploadHandler() fiber.Handler {
	return func(c *fiber.Ctx) error {
		cfg, _, ok, err := authorizeArchiveConfigRequest(c)
		if err != nil {
			return err
		}
		if !ok {
			return writeArchiveConfigUnauthorized(c)
		}

		target := strings.ToLower(strings.TrimSpace(c.FormValue("target")))
		if target == "" {
			target = "gateway"
		}
		uploaded, err := c.FormFile("file")
		if err != nil {
			return fiber.NewError(http.StatusBadRequest, "upgrade file is required")
		}
		content, err := readUploadedFile(uploaded)
		if err != nil {
			return fiber.NewError(http.StatusBadRequest, err.Error())
		}

		switch target {
		case "gateway":
			payload, err := stageGatewayUpgrade(cfg, uploaded.Filename, content)
			if err != nil {
				return fiber.NewError(http.StatusInternalServerError, err.Error())
			}
			return c.JSON(payload)
		case "recorder":
			payload, err := stageRecorderUpgrade(cfg, uploaded.Filename, content)
			if err != nil {
				return fiber.NewError(http.StatusBadGateway, err.Error())
			}
			return c.JSON(payload)
		default:
			return fiber.NewError(http.StatusBadRequest, "target must be gateway or recorder")
		}
	}
}

func archiveUpgradeApplyHandler() fiber.Handler {
	return func(c *fiber.Ctx) error {
		cfg, _, ok, err := authorizeArchiveConfigRequest(c)
		if err != nil {
			return err
		}
		if !ok {
			return writeArchiveConfigUnauthorized(c)
		}

		target := strings.ToLower(strings.TrimSpace(c.FormValue("target")))
		if target == "" {
			target = "gateway"
		}
		fileName := strings.TrimSpace(c.FormValue("fileName"))
		if fileName == "" {
			return fiber.NewError(http.StatusBadRequest, "fileName is required")
		}

		switch target {
		case "gateway":
			payload, err := applyGatewayUpgrade(cfg, fileName)
			if err != nil {
				return fiber.NewError(http.StatusInternalServerError, err.Error())
			}
			return c.JSON(payload)
		case "recorder":
			payload, err := applyRecorderUpgrade(cfg, fileName)
			if err != nil {
				return fiber.NewError(http.StatusBadGateway, err.Error())
			}
			return c.JSON(payload)
		default:
			return fiber.NewError(http.StatusBadRequest, "target must be gateway or recorder")
		}
	}
}

func readUploadedFile(uploaded *multipart.FileHeader) ([]byte, error) {
	file, err := uploaded.Open()
	if err != nil {
		return nil, err
	}
	defer file.Close()
	return io.ReadAll(file)
}

func sha256Hex(content []byte) string {
	sum := sha256.Sum256(content)
	return hex.EncodeToString(sum[:])
}

func ensureDirectory(path string) error {
	return os.MkdirAll(path, 0o755)
}

func stageGatewayUpgrade(cfg archiveconfig.File, fileName string, content []byte) (map[string]any, error) {
	root := strings.TrimSpace(cfg.OpticalArchive.Upgrade.GatewayStagingDirectory)
	if root == "" {
		return nil, fmt.Errorf("OpticalArchive.Upgrade.GatewayStagingDirectory is not configured")
	}
	if err := ensureDirectory(root); err != nil {
		return nil, err
	}
	safeName := filepath.Base(strings.TrimSpace(fileName))
	if safeName == "" {
		safeName = "gateway-upgrade.bin"
	}
	path := filepath.Join(root, safeName)
	if err := os.WriteFile(path, content, 0o644); err != nil {
		return nil, err
	}
	return map[string]any{
		"status":        "uploaded",
		"target":        "gateway",
		"storedPath":    path,
		"bytesReceived": len(content),
		"sha256":        sha256Hex(content),
	}, nil
}

func applyGatewayUpgrade(cfg archiveconfig.File, fileName string) (map[string]any, error) {
	command := strings.TrimSpace(cfg.OpticalArchive.Upgrade.GatewayApplyCommand)
	if command == "" {
		return nil, fmt.Errorf("OpticalArchive.Upgrade.GatewayApplyCommand is not configured")
	}
	root := strings.TrimSpace(cfg.OpticalArchive.Upgrade.GatewayStagingDirectory)
	stagedPath := filepath.Join(root, filepath.Base(strings.TrimSpace(fileName)))
	if _, err := os.Stat(stagedPath); err != nil {
		return nil, err
	}
	resolved := strings.ReplaceAll(command, "{file}", stagedPath)
	resolved = strings.ReplaceAll(resolved, "{dir}", filepath.Dir(stagedPath))
	output, exitCode, err := runShellCommand(resolved, filepath.Dir(stagedPath))
	return map[string]any{
		"status":     map[bool]string{true: "applied", false: "failed"}[err == nil && exitCode == 0],
		"target":     "gateway",
		"command":    resolved,
		"stagedPath": stagedPath,
		"exitCode":   exitCode,
		"output":     strings.TrimSpace(output),
	}, err
}

func stageRecorderUpgrade(cfg archiveconfig.File, fileName string, content []byte) (map[string]any, error) {
	client, conn, err := openRecorderClient(cfg)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	stream, err := client.UploadUpgradePackage(ctx)
	if err != nil {
		return nil, err
	}

	const chunkSize = 1 << 20
	for offset := 0; offset < len(content); offset += chunkSize {
		end := offset + chunkSize
		if end > len(content) {
			end = len(content)
		}
		if err := stream.Send(&burnbridgev1.UploadUpgradePackageChunk{
			FileName: fileName,
			Data:     content[offset:end],
			Eof:      end == len(content),
		}); err != nil {
			return nil, err
		}
	}
	resp, err := stream.CloseAndRecv()
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"status":        resp.GetStatus(),
		"target":        "recorder",
		"storedPath":    resp.GetStoredPath(),
		"bytesReceived": resp.GetBytesReceived(),
		"sha256":        resp.GetSha256(),
		"message":       resp.GetMessage(),
	}, nil
}

func applyRecorderUpgrade(cfg archiveconfig.File, fileName string) (map[string]any, error) {
	client, conn, err := openRecorderClient(cfg)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	resp, err := client.ApplyUpgrade(ctx, &burnbridgev1.ApplyUpgradeRequest{
		StagedFileName: filepath.Base(strings.TrimSpace(fileName)),
	})
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"status":     resp.GetStatus(),
		"target":     "recorder",
		"command":    resp.GetCommand(),
		"stagedPath": resp.GetStagedPath(),
		"exitCode":   resp.GetExitCode(),
		"output":     resp.GetOutput(),
		"message":    resp.GetMessage(),
	}, nil
}

func pushRecorderLicense(cfg archiveconfig.File, fileName string, content []byte, reloadNow bool) (map[string]any, error) {
	client, conn, err := openRecorderClient(cfg)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	resp, err := client.UpdateLicense(ctx, &burnbridgev1.UpdateLicenseRequest{
		FileName:  fileName,
		Content:   content,
		ReloadNow: reloadNow,
	})
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"status":          resp.GetStatus(),
		"message":         resp.GetMessage(),
		"licenseFilePath": resp.GetLicenseFilePath(),
		"sha256":          resp.GetSha256(),
	}, nil
}

func fetchRecorderVersion(cfg archiveconfig.File) (map[string]any, error) {
	client, conn, err := openRecorderClient(cfg)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	resp, err := client.GetVersion(ctx, &burnbridgev1.GetVersionRequest{})
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"status":              "ok",
		"serviceName":         resp.GetServiceName(),
		"serviceVersion":      resp.GetServiceVersion(),
		"assemblyVersion":     resp.GetAssemblyVersion(),
		"runtimeVersion":      resp.GetRuntimeVersion(),
		"buildConfiguration":  resp.GetBuildConfiguration(),
		"processArchitecture": resp.GetProcessArchitecture(),
		"osDescription":       resp.GetOsDescription(),
		"baseDirectory":       resp.GetBaseDirectory(),
		"licenseFilePath":     resp.GetLicenseFilePath(),
	}, nil
}

func openRecorderClient(cfg archiveconfig.File) (burnbridgev1.BurnBridgeClient, *grpc.ClientConn, error) {
	addr := strings.TrimSpace(cfg.OpticalArchive.GatewayInterop.GrpcAddr)
	if addr == "" {
		return nil, nil, fmt.Errorf("OpticalArchive.GatewayInterop.GrpcAddr is not configured")
	}
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, err
	}
	return burnbridgev1.NewBurnBridgeClient(conn), conn, nil
}

func runShellCommand(command string, workdir string) (string, int, error) {
	var cmd *exec.Cmd
	if runtime.GOOS == "windows" {
		cmd = exec.Command("cmd.exe", "/c", command)
	} else {
		cmd = exec.Command("/bin/sh", "-lc", command)
	}
	cmd.Dir = workdir
	output, err := cmd.CombinedOutput()
	exitCode := 0
	if cmd.ProcessState != nil {
		exitCode = cmd.ProcessState.ExitCode()
	}
	if err != nil {
		return string(output), exitCode, err
	}
	return string(output), exitCode, nil
}

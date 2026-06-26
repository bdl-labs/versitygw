package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/urfave/cli/v2"
	"github.com/versity/versitygw/archiveconfig"
	"github.com/versity/versitygw/backend/burnbridge"
	"github.com/versity/versitygw/s3api/utils"
)

var (
	burnbridgeDBPath               string
	burnbridgeGRPCAddr             string
	burnbridgeGRPCUseTLS           bool
	burnbridgeGRPCCAFile           string
	burnbridgeGRPCServerName       string
	burnbridgeGRPCInsecureSkipTLS  bool
	burnbridgeGRPCSkipPing         bool
	burnbridgeUDFVolumeLabel       string
	burnbridgeGRPCChunkSize        int
	burnbridgeGRPCDialTimeout      time.Duration
	burnbridgeGRPCReadyTimeout     time.Duration
	burnbridgeGRPCPingTimeout      time.Duration
	burnbridgeGRPCCancelJobTimeout time.Duration
	burnbridgePutObjectTimeout     time.Duration
	// Recorder-side S3 pull (RegisterS3ObjectPullSource)
	burnbridgeRecorderS3Endpoint        string
	burnbridgeRecorderS3Region          string
	burnbridgeRecorderS3AccessKey       string
	burnbridgeRecorderS3SecretKey       string
	burnbridgeRecorderS3SessionToken    string
	burnbridgeRecorderS3ForcePathStyle  bool
	burnbridgeRecorderS3PresignedGetURL string
)

func burnbridgeCommand() *cli.Command {
	return &cli.Command{
		Name:        "burnbridge",
		Usage:       "burnbridge storage backend",
		Description: "Runs gateway with built-in burnbridge backend",
		Action:      runBurnbridge,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "db-path",
				Usage:       "sqlite database file path for burnbridge metadata",
				EnvVars:     []string{"VGW_BURNBRIDGE_DB_PATH"},
				Destination: &burnbridgeDBPath,
				Value:       "./burnbridge-meta.db",
			},
			&cli.StringFlag{
				Name:        "grpc-addr",
				Usage:       "BurnBridge gRPC server address (host:port)",
				EnvVars:     []string{"VGW_BURNBRIDGE_GRPC_ADDR"},
				Destination: &burnbridgeGRPCAddr,
				Value:       "127.0.0.1:50051",
			},
			&cli.BoolFlag{
				Name:        "grpc-tls",
				Usage:       "use TLS for gRPC (system roots or --grpc-ca)",
				EnvVars:     []string{"VGW_BURNBRIDGE_GRPC_TLS"},
				Destination: &burnbridgeGRPCUseTLS,
			},
			&cli.StringFlag{
				Name:        "grpc-ca",
				Usage:       "PEM file of trusted CAs for gRPC TLS",
				EnvVars:     []string{"VGW_BURNBRIDGE_GRPC_CA"},
				Destination: &burnbridgeGRPCCAFile,
			},
			&cli.StringFlag{
				Name:        "grpc-server-name",
				Usage:       "TLS server name (SNI) override",
				EnvVars:     []string{"VGW_BURNBRIDGE_GRPC_SERVER_NAME"},
				Destination: &burnbridgeGRPCServerName,
			},
			&cli.BoolFlag{
				Name:        "grpc-insecure-skip-verify",
				Usage:       "skip TLS certificate verification (dev only)",
				EnvVars:     []string{"VGW_BURNBRIDGE_GRPC_INSECURE_SKIP_VERIFY"},
				Destination: &burnbridgeGRPCInsecureSkipTLS,
			},
			&cli.BoolFlag{
				Name:        "grpc-skip-ping",
				Usage:       "do not send GetJobStatus probe at startup",
				EnvVars:     []string{"VGW_BURNBRIDGE_GRPC_SKIP_PING"},
				Destination: &burnbridgeGRPCSkipPing,
			},
			&cli.StringFlag{
				Name:        "udf-volume-label",
				Usage:       "UDF volume label passed to CommitJob",
				EnvVars:     []string{"VGW_BURNBRIDGE_UDF_VOLUME_LABEL"},
				Destination: &burnbridgeUDFVolumeLabel,
			},
			&cli.IntFlag{
				Name:        "grpc-chunk-size",
				Usage:       "max size in bytes per UploadObjectChunk message",
				EnvVars:     []string{"VGW_BURNBRIDGE_GRPC_CHUNK_SIZE"},
				Destination: &burnbridgeGRPCChunkSize,
				Value:       64 * 1024,
			},
			&cli.DurationFlag{
				Name:        "grpc-dial-timeout",
				Usage:       "deadline for initial connection setup (dial + ready wait + startup ping unless --grpc-skip-ping)",
				EnvVars:     []string{"VGW_BURNBRIDGE_GRPC_DIAL_TIMEOUT"},
				Destination: &burnbridgeGRPCDialTimeout,
				Value:       120 * time.Second,
			},
			&cli.DurationFlag{
				Name:        "grpc-ready-timeout",
				Usage:       "max wait for gRPC channel to become Ready after NewClient (within grpc-dial-timeout)",
				EnvVars:     []string{"VGW_BURNBRIDGE_GRPC_READY_TIMEOUT"},
				Destination: &burnbridgeGRPCReadyTimeout,
				Value:       90 * time.Second,
			},
			&cli.DurationFlag{
				Name:        "grpc-ping-timeout",
				Usage:       "deadline for startup GetJobStatus probe",
				EnvVars:     []string{"VGW_BURNBRIDGE_GRPC_PING_TIMEOUT"},
				Destination: &burnbridgeGRPCPingTimeout,
				Value:       60 * time.Second,
			},
			&cli.DurationFlag{
				Name:        "grpc-cancel-job-timeout",
				Usage:       "deadline for best-effort CancelJob after failed PutObject",
				EnvVars:     []string{"VGW_BURNBRIDGE_GRPC_CANCEL_JOB_TIMEOUT"},
				Destination: &burnbridgeGRPCCancelJobTimeout,
				Value:       5 * time.Second,
			},
			&cli.DurationFlag{
				Name:        "put-object-timeout",
				Usage:       "extra deadline for PutObject (CreateJob+upload+CommitJob+meta); 0 = only use gateway request context (set a large value for slow optical burn, e.g. 8h)",
				EnvVars:     []string{"VGW_BURNBRIDGE_PUT_OBJECT_TIMEOUT"},
				Destination: &burnbridgePutObjectTimeout,
				Value:       0,
			},
			&cli.StringFlag{
				Name:        "recorder-s3-endpoint",
				Usage:       "if set, after CreateJob gateway sends RegisterS3ObjectPullSource with this S3-compatible base URL (scheme://host[:port]) so the recorder can GetObject directly",
				EnvVars:     []string{"VGW_BURNBRIDGE_RECORDER_S3_ENDPOINT"},
				Destination: &burnbridgeRecorderS3Endpoint,
			},
			&cli.StringFlag{
				Name:        "recorder-s3-region",
				Usage:       "AWS region for SigV4 when recorder pulls from recorder-s3-endpoint (optional for some gateways)",
				EnvVars:     []string{"VGW_BURNBRIDGE_RECORDER_S3_REGION"},
				Destination: &burnbridgeRecorderS3Region,
			},
			&cli.StringFlag{
				Name:        "recorder-s3-access-key",
				Usage:       "access key id for recorder S3 GetObject (optional if using only presigned URL)",
				EnvVars:     []string{"VGW_BURNBRIDGE_RECORDER_S3_ACCESS_KEY"},
				Destination: &burnbridgeRecorderS3AccessKey,
			},
			&cli.StringFlag{
				Name:        "recorder-s3-secret-key",
				Usage:       "secret access key for recorder S3 GetObject",
				EnvVars:     []string{"VGW_BURNBRIDGE_RECORDER_S3_SECRET_KEY"},
				Destination: &burnbridgeRecorderS3SecretKey,
			},
			&cli.StringFlag{
				Name:        "recorder-s3-session-token",
				Usage:       "optional STS session token for recorder S3 pull",
				EnvVars:     []string{"VGW_BURNBRIDGE_RECORDER_S3_SESSION_TOKEN"},
				Destination: &burnbridgeRecorderS3SessionToken,
			},
			&cli.BoolFlag{
				Name:        "recorder-s3-path-style",
				Usage:       "use path-style addressing for recorder S3 GetObject (http://host/bucket/key)",
				EnvVars:     []string{"VGW_BURNBRIDGE_RECORDER_S3_PATH_STYLE"},
				Destination: &burnbridgeRecorderS3ForcePathStyle,
			},
			&cli.StringFlag{
				Name:        "recorder-s3-presigned-url",
				Usage:       "optional presigned GET URL for this deployment (same for all objects unless empty); recorder may use instead of SigV4",
				EnvVars:     []string{"VGW_BURNBRIDGE_RECORDER_S3_PRESIGNED_URL"},
				Destination: &burnbridgeRecorderS3PresignedGetURL,
			},
		},
	}
}

func runBurnbridge(ctx *cli.Context) error {
	utils.SetBucketNameValidationRelaxedUppercase(true)

	dbPath := burnbridgeDBPath
	if dbPath == "" {
		dbPath = "./burnbridge-meta.db"
	}
	dir := filepath.Dir(dbPath)
	if dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("create db directory: %w", err)
		}
	}

	grpcAddr := burnbridgeGRPCAddr
	if grpcAddr == "" {
		grpcAddr = "127.0.0.1:50051"
	}

	opts := burnbridge.Options{
		DBPath:                 dbPath,
		GRPCAddr:               grpcAddr,
		GRPCUseTLS:             burnbridgeGRPCUseTLS,
		GRPCCAFile:             burnbridgeGRPCCAFile,
		GRPCServerName:         burnbridgeGRPCServerName,
		GRPCInsecureSkipVerify: burnbridgeGRPCInsecureSkipTLS,
		GRPCSkipPing:           burnbridgeGRPCSkipPing,
		UDFVolumeLabel:         burnbridgeUDFVolumeLabel,
		ChunkSize:              burnbridgeGRPCChunkSize,
		DialTimeout:            burnbridgeGRPCDialTimeout,
		GRPCReadyTimeout:       burnbridgeGRPCReadyTimeout,
		PingTimeout:            burnbridgeGRPCPingTimeout,
		CancelJobTimeout:       burnbridgeGRPCCancelJobTimeout,
		PutObjectTimeout:       burnbridgePutObjectTimeout,

		RecorderS3Endpoint:        burnbridgeRecorderS3Endpoint,
		RecorderS3Region:          burnbridgeRecorderS3Region,
		RecorderS3AccessKey:       burnbridgeRecorderS3AccessKey,
		RecorderS3SecretKey:       burnbridgeRecorderS3SecretKey,
		RecorderS3SessionToken:    burnbridgeRecorderS3SessionToken,
		RecorderS3ForcePathStyle:  burnbridgeRecorderS3ForcePathStyle,
		RecorderS3PresignedGetURL: burnbridgeRecorderS3PresignedGetURL,
		AllowCreateBucketBinding:  true,
	}

	if flashEmmcOptimized {
		opts.SQLiteMaintCtx = ctx.Context
	}
	applyBurnBridgeEnvOverrides(&opts)
	if cfg, _, err := archiveconfig.Load(""); err == nil {
		if cfg.OpticalArchive.Recorder.GrpcChunkSize > 0 {
			opts.ChunkSize = cfg.OpticalArchive.Recorder.GrpcChunkSize
		}
		opts.AllowCreateBucketBinding = cfg.OpticalArchive.Recorder.AllowCreateBucketBinding
		opts.ManageRecorderProcessLocally = cfg.OpticalArchive.LinuxServices.ManageRecorderProcessLocally
		opts.RecorderServiceName = cfg.OpticalArchive.LinuxServices.RecorderServiceName
		opts.RecorderProcessPattern = cfg.OpticalArchive.LinuxServices.RecorderProcessPattern
		opts.RecorderStartCommand = cfg.OpticalArchive.LinuxServices.RecorderStartCommand
		opts.RecorderWorkingDirectory = cfg.OpticalArchive.LinuxServices.RecorderWorkingDirectory
		opts.RecorderHealthCheckSeconds = cfg.OpticalArchive.LinuxServices.RecorderHealthCheckSeconds
	}

	be, err := burnbridge.New(opts)
	if err != nil {
		return fmt.Errorf("init burnbridge backend: %w", err)
	}
	return runGateway(ctx.Context, be)
}

func applyBurnBridgeEnvOverrides(opts *burnbridge.Options) {
	if opts == nil {
		return
	}
	overrideBoolEnv(&opts.ManageRecorderProcessLocally, "VGW_BURNBRIDGE_MANAGE_RECORDER_PROCESS_LOCALLY")
	overrideStringEnv(&opts.RecorderServiceName, "VGW_BURNBRIDGE_RECORDER_SERVICE_NAME")
	overrideStringEnv(&opts.RecorderProcessPattern, "VGW_BURNBRIDGE_RECORDER_PROCESS_PATTERN")
	overrideStringEnv(&opts.RecorderStartCommand, "VGW_BURNBRIDGE_RECORDER_START_COMMAND")
	overrideStringEnv(&opts.RecorderWorkingDirectory, "VGW_BURNBRIDGE_RECORDER_WORKING_DIRECTORY")
	overrideIntEnv(&opts.RecorderHealthCheckSeconds, "VGW_BURNBRIDGE_RECORDER_HEALTH_CHECK_SECONDS")
}

func overrideStringEnv(target *string, key string) {
	if target == nil {
		return
	}
	if value, ok := os.LookupEnv(key); ok {
		*target = strings.TrimSpace(value)
	}
}

func overrideBoolEnv(target *bool, key string) {
	if target == nil {
		return
	}
	if value, ok := os.LookupEnv(key); ok {
		if parsed, err := strconv.ParseBool(strings.TrimSpace(value)); err == nil {
			*target = parsed
		}
	}
}

func overrideIntEnv(target *int, key string) {
	if target == nil {
		return
	}
	if value, ok := os.LookupEnv(key); ok {
		if parsed, err := strconv.Atoi(strings.TrimSpace(value)); err == nil {
			*target = parsed
		}
	}
}

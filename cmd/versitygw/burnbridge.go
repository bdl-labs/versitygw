package main

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/urfave/cli/v2"
	"github.com/versity/versitygw/backend/burnbridge"
)

var (
	burnbridgeDBPath              string
	burnbridgeGRPCAddr            string
	burnbridgeGRPCUseTLS          bool
	burnbridgeGRPCCAFile          string
	burnbridgeGRPCServerName      string
	burnbridgeGRPCInsecureSkipTLS bool
	burnbridgeGRPCSkipPing        bool
	burnbridgeUDFVolumeLabel      string
	burnbridgeGRPCChunkSize       int
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
				Value:       1 << 20,
			},
		},
	}
}

func runBurnbridge(ctx *cli.Context) error {
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
	}

	be, err := burnbridge.New(opts)
	if err != nil {
		return fmt.Errorf("init burnbridge backend: %w", err)
	}
	return runGateway(ctx.Context, be)
}

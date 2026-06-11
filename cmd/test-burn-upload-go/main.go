package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

const (
	defaultConfigPath             = `D:\BRS\Publisher\config\optical-archive.config.json`
	defaultDataDir                = `D:\testdata`
	defaultBucket                 = "archive-test"
	defaultEndpoint               = "http://127.0.0.1:7070"
	defaultRegion                 = "us-east-1"
	defaultAccessKey              = "drive"
	defaultSecretKey              = "drive123456"
	defaultReadTimeout            = 900
	defaultConnectTimeout         = 60
	defaultFinalizeTimeout        = 3600
	defaultBucketReadyTimeout     = 120
	defaultBucketReadyPollSeconds = 2
	defaultMemorySampleInterval   = 2
)

type mode string

const (
	modeHelp           mode = "help"
	modeFullFlow       mode = "full-flow"
	modeDownload       mode = "download"
	modeRemoteDownload mode = "remote-download"
	modeListObjects    mode = "listobjects"
	modeDiscInfo       mode = "discinfo"
	modeFinalize       mode = "finalize"
	modeCloseDisc      mode = "closedisc"
	modeHeadObject     mode = "headobject"
	modeGetObject      mode = "getobject"
	modeInterruptRetry mode = "interrupt-retry"
)

type cliOptions struct {
	mode               mode
	dataDir            string
	bucket             string
	awsProfile         string
	configPath         string
	skipUpload         bool
	skipFinalize       bool
	skipBucketCreate   bool
	skipMD5Verify      bool
	remoteOnly         bool
	listObjectsOnly    bool
	headObjectOnly     bool
	discInfoOnly       bool
	finalizeOnly       bool
	closeDiscOnly      bool
	singleObjectKey    string
	singleObjectOutput string
	interruptRetryOnly bool
	failAfterBytes     int64
}

func main() {
	opts, err := parseInvocation(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, "ERROR:", err)
		fmt.Fprintln(os.Stderr)
		printUsage(os.Stderr)
		os.Exit(1)
	}

	if opts.mode == modeHelp {
		printUsage(os.Stdout)
		return
	}

	runner, err := newRunner(opts)
	if err != nil {
		fmt.Fprintln(os.Stderr, "ERROR:", err)
		os.Exit(1)
	}

	if err := runner.run(); err != nil {
		fmt.Fprintln(os.Stderr, "ERROR:", err)
		os.Exit(1)
	}
}

func parseInvocation(args []string) (cliOptions, error) {
	if len(args) == 0 {
		return parseFullFlow(nil), nil
	}

	switch normalizeArg(args[0]) {
	case "help", "--help", "/?":
		return cliOptions{mode: modeHelp}, nil
	case "download":
		return parseDownload(args[1:]), nil
	case "download-nomd5":
		opts := parseDownload(args[1:])
		opts.skipMD5Verify = true
		return opts, nil
	case "remote-download":
		return parseRemoteDownload(args[1:], false), nil
	case "remote-download-nomd5":
		return parseRemoteDownload(args[1:], true), nil
	case "listobjects":
		return parseBucketOnly(args[1:], modeListObjects), nil
	case "discinfo":
		return parseBucketOnly(args[1:], modeDiscInfo), nil
	case "finalize":
		return parseBucketOnly(args[1:], modeFinalize), nil
	case "closedisc":
		return parseBucketOnly(args[1:], modeCloseDisc), nil
	case "headobject":
		return parseHeadObject(args[1:])
	case "getobject":
		return parseGetObject(args[1:], false)
	case "getobject-nomd5":
		return parseGetObject(args[1:], true)
	case "interrupt-retry":
		return parseInterruptRetry(args[1:])
	default:
		return parseFullFlow(args), nil
	}
}

func parseFullFlow(args []string) cliOptions {
	dataDir := positionalOrDefault(args, 0, defaultDataDir)
	bucket := positionalOrDefault(args, 1, defaultBucket)
	profile, configPath := parseProfileConfig(args, 2, 3)
	return cliOptions{
		mode:             modeFullFlow,
		dataDir:          dataDir,
		bucket:           bucket,
		awsProfile:       profile,
		configPath:       resolveConfigPath(configPath),
		skipBucketCreate: false,
	}
}

func parseDownload(args []string) cliOptions {
	dataDir := positionalOrDefault(args, 0, defaultDataDir)
	bucket := positionalOrDefault(args, 1, defaultBucket)
	profile, configPath := parseProfileConfig(args, 2, 3)
	skipMD5 := len(args) > 4 && strings.EqualFold(strings.TrimSpace(args[4]), "nomd5")
	return cliOptions{
		mode:             modeDownload,
		dataDir:          dataDir,
		bucket:           bucket,
		awsProfile:       profile,
		configPath:       resolveConfigPath(configPath),
		skipUpload:       true,
		skipFinalize:     true,
		skipBucketCreate: true,
		skipMD5Verify:    skipMD5,
	}
}

func parseRemoteDownload(args []string, skipMD5 bool) cliOptions {
	bucket := positionalOrDefault(args, 0, defaultBucket)
	profile, configPath := parseProfileConfig(args, 1, 2)
	return cliOptions{
		mode:             modeRemoteDownload,
		dataDir:          ".",
		bucket:           bucket,
		awsProfile:       profile,
		configPath:       resolveConfigPath(configPath),
		skipUpload:       true,
		skipFinalize:     true,
		skipBucketCreate: true,
		skipMD5Verify:    skipMD5,
		remoteOnly:       true,
	}
}

func parseBucketOnly(args []string, m mode) cliOptions {
	bucket := positionalOrDefault(args, 0, defaultBucket)
	profile, configPath := parseProfileConfig(args, 1, 2)

	opts := cliOptions{
		mode:             m,
		dataDir:          ".",
		bucket:           bucket,
		awsProfile:       profile,
		configPath:       resolveConfigPath(configPath),
		skipUpload:       true,
		skipBucketCreate: true,
	}

	switch m {
	case modeListObjects:
		opts.skipFinalize = true
		opts.listObjectsOnly = true
	case modeDiscInfo:
		opts.skipFinalize = true
		opts.discInfoOnly = true
	case modeFinalize:
		opts.finalizeOnly = true
	case modeCloseDisc:
		opts.closeDiscOnly = true
	}

	return opts
}

func parseHeadObject(args []string) (cliOptions, error) {
	bucket := positionalOrDefault(args, 0, defaultBucket)
	key := positionalOrDefault(args, 1, "")
	if strings.TrimSpace(key) == "" {
		return cliOptions{}, errors.New("object key is required")
	}

	profile, configPath := parseProfileConfig(args, 2, 3)
	return cliOptions{
		mode:             modeHeadObject,
		dataDir:          ".",
		bucket:           bucket,
		awsProfile:       profile,
		configPath:       resolveConfigPath(configPath),
		skipUpload:       true,
		skipFinalize:     true,
		skipBucketCreate: true,
		headObjectOnly:   true,
		singleObjectKey:  key,
	}, nil
}

func parseGetObject(args []string, skipMD5 bool) (cliOptions, error) {
	bucket := positionalOrDefault(args, 0, defaultBucket)
	key := positionalOrDefault(args, 1, "")
	if strings.TrimSpace(key) == "" {
		return cliOptions{}, errors.New("object key is required")
	}

	output := positionalOrDefault(args, 2, "")
	profile, configPath := parseProfileConfig(args, 3, 4)
	return cliOptions{
		mode:               modeGetObject,
		dataDir:            ".",
		bucket:             bucket,
		awsProfile:         profile,
		configPath:         resolveConfigPath(configPath),
		skipUpload:         true,
		skipFinalize:       true,
		skipBucketCreate:   true,
		skipMD5Verify:      skipMD5,
		singleObjectKey:    key,
		singleObjectOutput: output,
	}, nil
}

func parseInterruptRetry(args []string) (cliOptions, error) {
	dataFile := positionalOrDefault(args, 0, "")
	if strings.TrimSpace(dataFile) == "" {
		return cliOptions{}, errors.New("data file is required")
	}

	bucket := positionalOrDefault(args, 1, defaultBucket)
	failAfterMiBRaw := positionalOrDefault(args, 2, "16")
	failAfterMiB, err := strconv.ParseInt(strings.TrimSpace(failAfterMiBRaw), 10, 64)
	if err != nil || failAfterMiB <= 0 {
		return cliOptions{}, fmt.Errorf("invalid failAfterMiB: %s", failAfterMiBRaw)
	}

	profile, configPath := parseProfileConfig(args, 3, 4)
	return cliOptions{
		mode:               modeInterruptRetry,
		dataDir:            dataFile,
		bucket:             bucket,
		awsProfile:         profile,
		configPath:         resolveConfigPath(configPath),
		interruptRetryOnly: true,
		failAfterBytes:     failAfterMiB * 1024 * 1024,
	}, nil
}

func positionalOrDefault(args []string, index int, fallback string) string {
	if len(args) <= index {
		return fallback
	}

	value := strings.TrimSpace(args[index])
	if value == "" {
		return fallback
	}

	return value
}

func parseProfileConfig(args []string, profileIndex, configIndex int) (string, string) {
	var profile string
	if len(args) > profileIndex {
		profile = strings.TrimSpace(args[profileIndex])
	}

	var configPath string
	if profile != "" && strings.EqualFold(filepath.Ext(profile), ".json") {
		configPath = profile
		profile = ""
	}

	if len(args) > configIndex {
		configPath = strings.TrimSpace(args[configIndex])
	}

	return profile, configPath
}

func resolveConfigPath(configPath string) string {
	configPath = strings.TrimSpace(configPath)
	if configPath != "" {
		return configPath
	}

	if env := strings.TrimSpace(os.Getenv("OPTICAL_ARCHIVE_CONFIG_PATH")); env != "" {
		return env
	}

	return defaultConfigPath
}

func normalizeArg(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}

func printUsage(stream *os.File) {
	lines := []string{
		"Usage:",
		"  test-burn-upload-go.exe [DataDir] [Bucket] [AwsProfile] [ConfigPath]",
		"  test-burn-upload-go.exe download [DataDir] [Bucket] [AwsProfile] [ConfigPath] [nomd5]",
		"  test-burn-upload-go.exe download-nomd5 [DataDir] [Bucket] [AwsProfile] [ConfigPath]",
		"  test-burn-upload-go.exe remote-download [Bucket] [AwsProfile] [ConfigPath]",
		"  test-burn-upload-go.exe remote-download-nomd5 [Bucket] [AwsProfile] [ConfigPath]",
		"  test-burn-upload-go.exe listobjects [Bucket] [AwsProfile] [ConfigPath]",
		"  test-burn-upload-go.exe discinfo [Bucket] [AwsProfile] [ConfigPath]",
		"  test-burn-upload-go.exe finalize [Bucket] [AwsProfile] [ConfigPath]",
		"  test-burn-upload-go.exe closedisc [Bucket] [AwsProfile] [ConfigPath]",
		"  test-burn-upload-go.exe headobject [Bucket] [ObjectKey] [AwsProfile] [ConfigPath]",
		"  test-burn-upload-go.exe getobject [Bucket] [ObjectKey] [OutputPath] [AwsProfile] [ConfigPath]",
		"  test-burn-upload-go.exe getobject-nomd5 [Bucket] [ObjectKey] [OutputPath] [AwsProfile] [ConfigPath]",
		"  test-burn-upload-go.exe interrupt-retry [DataFile] [Bucket] [FailAfterMiB] [AwsProfile] [ConfigPath]",
		"",
		"Modes:",
		"  full-flow",
		"    Upload local files, validate ListObjects and HeadObject, call FinalizeLayout, then download.",
		"",
		"  download",
		"    Skip upload, skip finalize, skip bucket creation, use local directory as expected file list.",
		"",
		"  download-nomd5",
		"    Same as download, but skip downloaded file MD5 verification.",
		"",
		"  remote-download",
		"    Build expected object list from remote ListObjects only. No local data directory required.",
		"",
		"  remote-download-nomd5",
		"    Same as remote-download, but skip MD5 verification.",
		"",
		"  listobjects",
		"    List all objects in the bucket and print key and size.",
		"",
		"  discinfo",
		"    Generate a burnbridge control key for disc-info and print the returned JSON fields.",
		"",
		"  finalize",
		"    Generate a burnbridge control key for finalize-layout and print the returned JSON fields.",
		"",
		"  closedisc",
		"    Generate a burnbridge control key for close-disc and request disc close/finalize on recorder.",
		"",
		"  headobject",
		"    Print one object's metadata without downloading the object body.",
		"",
		"  getobject",
		"    Download one object, validate size, and optionally print MD5.",
		"",
		"  getobject-nomd5",
		"    Download one object and validate size only.",
		"",
		"  interrupt-retry",
		"    Simulate mid-stream disconnect, retry the same key, finalize, and download verify.",
		"",
		"Examples:",
		`  D:\BRS\Publisher\scripts\test-burn-upload-go.exe D:\testdata e60102350000000024d104e2`,
		`  D:\BRS\Publisher\scripts\test-burn-upload-go.exe download D:\testdata e60102350000000024d104e2`,
		`  D:\BRS\Publisher\scripts\test-burn-upload-go.exe remote-download e60102350000000024d104e2`,
		`  D:\BRS\Publisher\scripts\test-burn-upload-go.exe discinfo e60102350000000024d104e2`,
		`  D:\BRS\Publisher\scripts\test-burn-upload-go.exe headobject e60102350000000024d104e2 docs/chat-export.md`,
		`  D:\BRS\Publisher\scripts\test-burn-upload-go.exe getobject-nomd5 e60102350000000024d104e2 docs/chat-export.md`,
		`  D:\BRS\Publisher\scripts\test-burn-upload-go.exe interrupt-retry D:\testdata\sample.bin e60102350000000024d104e2 16`,
	}

	for _, line := range lines {
		fmt.Fprintln(stream, line)
	}
}

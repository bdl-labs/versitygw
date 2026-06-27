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
	defaultConfigFileName         = "optical-archive.config.json"
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
	defaultFinalizeFreeThreshold  = int64(1024 * 1024 * 1024)
)

type mode string

const (
	modeHelp           mode = "help"
	modeFullFlow       mode = "full-flow"
	modeMixedFlow      mode = "mixed"
	modeDownload       mode = "download"
	modeRemoteDownload mode = "remote-download"
	modeMultipartFlow  mode = "multipart"
	modePutObjectFlow  mode = "putobject"
	modeSmallBatchFlow mode = "small-batch"
	modeListObjects    mode = "listobjects"
	modeDriveInfo      mode = "driveinfo"
	modeDiscInfo       mode = "discinfo"
	modeFinalize       mode = "finalize"
	modeCloseDisc      mode = "closedisc"
	modeMediaRemoved   mode = "media-removed"
	modeMediaInserted  mode = "media-inserted"
	modeTrayOpen       mode = "tray-open"
	modeTrayClose      mode = "tray-close"
	modeHeadObject     mode = "headobject"
	modeGetObject      mode = "getobject"
	modeInterruptRetry mode = "interrupt-retry"
	modeMultipartRetry mode = "multipart-interrupt-retry"
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
	driveInfoOnly      bool
	headObjectOnly     bool
	discInfoOnly       bool
	finalizeOnly       bool
	closeDiscOnly      bool
	mediaRemovedOnly   bool
	mediaInsertedOnly  bool
	trayOpenOnly       bool
	trayCloseOnly      bool
	singleObjectKey    string
	singleObjectOutput string
	interruptRetryOnly bool
	multipartRetryOnly bool
	multipartPartBytes int64
	multipartThreshold int64
	smallFileCount     int
	smallFileBytes     int64
	smallConcurrency   int
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
	case "download", "dl":
		return parseDownload(args[1:]), nil
	case "download-nomd5", "dl-nomd5":
		opts := parseDownload(args[1:])
		opts.skipMD5Verify = true
		return opts, nil
	case "remote-download", "rdl":
		return parseRemoteDownload(args[1:], false), nil
	case "remote-download-nomd5", "rdl-nomd5":
		return parseRemoteDownload(args[1:], true), nil
	case "multipart", "mp":
		return parseMultipartFlow(args[1:], true)
	case "multipart-md5", "mp-md5":
		return parseMultipartFlow(args[1:], false)
	case "multipart-nomd5", "mp-nomd5":
		return parseMultipartFlow(args[1:], true)
	case "putobject", "put", "po":
		return parsePutObjectFlow(args[1:], true)
	case "putobject-md5", "put-md5", "po-md5":
		return parsePutObjectFlow(args[1:], false)
	case "small-batch", "smallbatch", "sb":
		return parseSmallBatchFlow(args[1:], true)
	case "small-batch-md5", "smallbatch-md5", "sb-md5":
		return parseSmallBatchFlow(args[1:], false)
	case "mixed", "mix":
		return parseMixedFlow(args[1:], true)
	case "mixed-md5", "mix-md5":
		return parseMixedFlow(args[1:], false)
	case "mixed-nomd5", "mix-nomd5":
		return parseMixedFlow(args[1:], true)
	case "listobjects", "ls", "list":
		return parseBucketOnly(args[1:], modeListObjects), nil
	case "driveinfo", "drive-info", "drive":
		return parseBucketOnly(args[1:], modeDriveInfo), nil
	case "discinfo", "disc-info", "disc":
		return parseBucketOnly(args[1:], modeDiscInfo), nil
	case "finalize", "final":
		return parseBucketOnly(args[1:], modeFinalize), nil
	case "closedisc", "close-disc":
		return parseBucketOnly(args[1:], modeCloseDisc), nil
	case "media-removed", "mediaremoved", "unmount", "removed":
		return parseBucketOnly(args[1:], modeMediaRemoved), nil
	case "media-inserted", "mediainserted", "mount", "inserted":
		return parseBucketOnly(args[1:], modeMediaInserted), nil
	case "tray-open", "trayopen", "open-tray", "opentray", "eject", "open":
		return parseBucketOnly(args[1:], modeTrayOpen), nil
	case "tray-close", "trayclose", "close-tray", "closetray", "close":
		return parseBucketOnly(args[1:], modeTrayClose), nil
	case "headobject":
		return parseHeadObject(args[1:])
	case "head":
		return parseShortHeadObject(args[1:])
	case "getobject":
		return parseGetObject(args[1:], false)
	case "get":
		return parseShortGetObject(args[1:], false)
	case "getobject-nomd5":
		return parseGetObject(args[1:], true)
	case "get-nomd5":
		return parseShortGetObject(args[1:], true)
	case "interrupt-retry":
		return parseInterruptRetry(args[1:])
	case "multipart-interrupt-retry":
		return parseMultipartInterruptRetry(args[1:])
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
		skipMD5Verify:    true,
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

func parseMultipartFlow(args []string, skipMD5 bool) (cliOptions, error) {
	dataFile := positionalOrDefault(args, 0, "")
	if strings.TrimSpace(dataFile) == "" {
		return cliOptions{}, errors.New("data file is required")
	}

	bucket := positionalOrDefault(args, 1, defaultBucket)
	partSizeMiBRaw := positionalOrDefault(args, 2, "32")
	partSizeMiB, err := strconv.ParseInt(strings.TrimSpace(partSizeMiBRaw), 10, 64)
	if err != nil || partSizeMiB <= 0 {
		return cliOptions{}, fmt.Errorf("invalid partSizeMiB: %s", partSizeMiBRaw)
	}

	profile, configPath := parseProfileConfig(args, 3, 4)
	return cliOptions{
		mode:               modeMultipartFlow,
		dataDir:            dataFile,
		bucket:             bucket,
		awsProfile:         profile,
		configPath:         resolveConfigPath(configPath),
		multipartPartBytes: partSizeMiB * 1024 * 1024,
		skipMD5Verify:      skipMD5,
	}, nil
}

func parsePutObjectFlow(args []string, skipMD5 bool) (cliOptions, error) {
	dataFile := positionalOrDefault(args, 0, "")
	if strings.TrimSpace(dataFile) == "" {
		return cliOptions{}, errors.New("data file is required")
	}

	bucket := positionalOrDefault(args, 1, defaultBucket)
	profile, configPath := parseProfileConfig(args, 2, 3)
	return cliOptions{
		mode:          modePutObjectFlow,
		dataDir:       dataFile,
		bucket:        bucket,
		awsProfile:    profile,
		configPath:    resolveConfigPath(configPath),
		skipMD5Verify: skipMD5,
	}, nil
}

func parseSmallBatchFlow(args []string, skipMD5 bool) (cliOptions, error) {
	dataDir := positionalOrDefault(args, 0, filepath.Join(defaultDataDir, "small-batch"))
	bucket := positionalOrDefault(args, 1, defaultBucket)
	countRaw := positionalOrDefault(args, 2, "256")
	count, err := strconv.Atoi(strings.TrimSpace(countRaw))
	if err != nil || count <= 0 {
		return cliOptions{}, fmt.Errorf("invalid fileCount: %s", countRaw)
	}
	sizeRaw := positionalOrDefault(args, 3, "2048")
	size, err := strconv.ParseInt(strings.TrimSpace(sizeRaw), 10, 64)
	if err != nil || size <= 0 {
		return cliOptions{}, fmt.Errorf("invalid fileSizeBytes: %s", sizeRaw)
	}
	concurrencyRaw := positionalOrDefault(args, 4, "32")
	concurrency, err := strconv.Atoi(strings.TrimSpace(concurrencyRaw))
	if err != nil || concurrency <= 0 {
		return cliOptions{}, fmt.Errorf("invalid concurrency: %s", concurrencyRaw)
	}
	profile, configPath := parseProfileConfig(args, 5, 6)
	return cliOptions{
		mode:             modeSmallBatchFlow,
		dataDir:          dataDir,
		bucket:           bucket,
		awsProfile:       profile,
		configPath:       resolveConfigPath(configPath),
		skipBucketCreate: false,
		skipMD5Verify:    skipMD5,
		smallFileCount:   count,
		smallFileBytes:   size,
		smallConcurrency: concurrency,
	}, nil
}

func parseMixedFlow(args []string, skipMD5 bool) (cliOptions, error) {
	dataDir := positionalOrDefault(args, 0, defaultDataDir)
	bucket := positionalOrDefault(args, 1, defaultBucket)

	thresholdMiBRaw := positionalOrDefault(args, 2, "32")
	thresholdMiB, err := strconv.ParseInt(strings.TrimSpace(thresholdMiBRaw), 10, 64)
	if err != nil || thresholdMiB <= 0 {
		return cliOptions{}, fmt.Errorf("invalid multipartThresholdMiB: %s", thresholdMiBRaw)
	}

	partSizeMiBRaw := positionalOrDefault(args, 3, "32")
	partSizeMiB, err := strconv.ParseInt(strings.TrimSpace(partSizeMiBRaw), 10, 64)
	if err != nil || partSizeMiB <= 0 {
		return cliOptions{}, fmt.Errorf("invalid partSizeMiB: %s", partSizeMiBRaw)
	}

	profile, configPath := parseProfileConfig(args, 4, 5)
	return cliOptions{
		mode:               modeMixedFlow,
		dataDir:            dataDir,
		bucket:             bucket,
		awsProfile:         profile,
		configPath:         resolveConfigPath(configPath),
		skipBucketCreate:   false,
		skipMD5Verify:      skipMD5,
		multipartPartBytes: partSizeMiB * 1024 * 1024,
		multipartThreshold: thresholdMiB * 1024 * 1024,
	}, nil
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
	case modeDriveInfo:
		opts.skipFinalize = true
		opts.driveInfoOnly = true
	case modeDiscInfo:
		opts.skipFinalize = true
		opts.discInfoOnly = true
	case modeFinalize:
		opts.finalizeOnly = true
	case modeCloseDisc:
		opts.closeDiscOnly = true
	case modeMediaRemoved:
		opts.mediaRemovedOnly = true
	case modeMediaInserted:
		opts.mediaInsertedOnly = true
	case modeTrayOpen:
		opts.trayOpenOnly = true
	case modeTrayClose:
		opts.trayCloseOnly = true
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

func parseShortHeadObject(args []string) (cliOptions, error) {
	key := positionalOrDefault(args, 0, "")
	if strings.TrimSpace(key) == "" {
		return cliOptions{}, errors.New("object key is required")
	}

	profile, configPath := parseProfileConfig(args, 1, 2)
	return cliOptions{
		mode:             modeHeadObject,
		dataDir:          ".",
		bucket:           defaultBucket,
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

func parseShortGetObject(args []string, skipMD5 bool) (cliOptions, error) {
	key := positionalOrDefault(args, 0, "")
	if strings.TrimSpace(key) == "" {
		return cliOptions{}, errors.New("object key is required")
	}

	output := positionalOrDefault(args, 1, "")
	profile, configPath := parseProfileConfig(args, 2, 3)
	return cliOptions{
		mode:               modeGetObject,
		dataDir:            ".",
		bucket:             defaultBucket,
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

func parseMultipartInterruptRetry(args []string) (cliOptions, error) {
	dataFile := positionalOrDefault(args, 0, "")
	if strings.TrimSpace(dataFile) == "" {
		return cliOptions{}, errors.New("data file is required")
	}

	bucket := positionalOrDefault(args, 1, defaultBucket)
	partSizeMiBRaw := positionalOrDefault(args, 2, "32")
	partSizeMiB, err := strconv.ParseInt(strings.TrimSpace(partSizeMiBRaw), 10, 64)
	if err != nil || partSizeMiB <= 0 {
		return cliOptions{}, fmt.Errorf("invalid partSizeMiB: %s", partSizeMiBRaw)
	}

	failAfterMiBRaw := positionalOrDefault(args, 3, "16")
	failAfterMiB, err := strconv.ParseInt(strings.TrimSpace(failAfterMiBRaw), 10, 64)
	if err != nil || failAfterMiB <= 0 {
		return cliOptions{}, fmt.Errorf("invalid failAfterMiB: %s", failAfterMiBRaw)
	}

	profile, configPath := parseProfileConfig(args, 4, 5)
	return cliOptions{
		mode:               modeMultipartRetry,
		dataDir:            dataFile,
		bucket:             bucket,
		awsProfile:         profile,
		configPath:         resolveConfigPath(configPath),
		multipartRetryOnly: true,
		multipartPartBytes: partSizeMiB * 1024 * 1024,
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

	if executablePath, err := os.Executable(); err == nil {
		siblingConfigPath := filepath.Join(filepath.Dir(executablePath), defaultConfigFileName)
		if _, statErr := os.Stat(siblingConfigPath); statErr == nil {
			return siblingConfigPath
		}
	}

	return defaultConfigPath
}

func normalizeArg(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}

func printUsage(stream *os.File) {
	lines := []string{
		"Usage:",
		"  test-burn-upload-go.exe ls",
		"  test-burn-upload-go.exe drive",
		"  test-burn-upload-go.exe disc",
		"  test-burn-upload-go.exe open",
		"  test-burn-upload-go.exe close",
		"  test-burn-upload-go.exe mount",
		"  test-burn-upload-go.exe unmount",
		"  test-burn-upload-go.exe get [ObjectKey] [OutputPath]",
		"",
		"Compatibility usage:",
		"  test-burn-upload-go.exe [DataDir] [Bucket] [AwsProfile] [ConfigPath]",
		"  test-burn-upload-go.exe mixed [DataDir] [Bucket] [MultipartThresholdMiB] [PartSizeMiB] [AwsProfile] [ConfigPath]",
		"  test-burn-upload-go.exe mixed-md5 [DataDir] [Bucket] [MultipartThresholdMiB] [PartSizeMiB] [AwsProfile] [ConfigPath]",
		"  test-burn-upload-go.exe mixed-nomd5 [DataDir] [Bucket] [MultipartThresholdMiB] [PartSizeMiB] [AwsProfile] [ConfigPath]",
		"  test-burn-upload-go.exe putobject [DataFile] [Bucket] [AwsProfile] [ConfigPath]",
		"  test-burn-upload-go.exe putobject-md5 [DataFile] [Bucket] [AwsProfile] [ConfigPath]",
		"  test-burn-upload-go.exe small-batch [DataDir] [Bucket] [FileCount] [FileSizeBytes] [Concurrency] [AwsProfile] [ConfigPath]",
		"  test-burn-upload-go.exe small-batch-md5 [DataDir] [Bucket] [FileCount] [FileSizeBytes] [Concurrency] [AwsProfile] [ConfigPath]",
		"  test-burn-upload-go.exe multipart [DataFile] [Bucket] [PartSizeMiB] [AwsProfile] [ConfigPath]",
		"  test-burn-upload-go.exe multipart-md5 [DataFile] [Bucket] [PartSizeMiB] [AwsProfile] [ConfigPath]",
		"  test-burn-upload-go.exe multipart-nomd5 [DataFile] [Bucket] [PartSizeMiB] [AwsProfile] [ConfigPath]",
		"  test-burn-upload-go.exe download [DataDir] [Bucket] [AwsProfile] [ConfigPath] [nomd5]",
		"  test-burn-upload-go.exe download-nomd5 [DataDir] [Bucket] [AwsProfile] [ConfigPath]",
		"  test-burn-upload-go.exe remote-download [Bucket] [AwsProfile] [ConfigPath]",
		"  test-burn-upload-go.exe remote-download-nomd5 [Bucket] [AwsProfile] [ConfigPath]",
		"  test-burn-upload-go.exe listobjects [Bucket] [AwsProfile] [ConfigPath]",
		"  test-burn-upload-go.exe driveinfo [ControlBucket] [AwsProfile] [ConfigPath]",
		"  test-burn-upload-go.exe discinfo [Bucket] [AwsProfile] [ConfigPath]",
		"  test-burn-upload-go.exe finalize [Bucket] [AwsProfile] [ConfigPath]",
		"  test-burn-upload-go.exe closedisc [Bucket] [AwsProfile] [ConfigPath]",
		"  test-burn-upload-go.exe media-removed [Bucket] [AwsProfile] [ConfigPath]",
		"  test-burn-upload-go.exe media-inserted [Bucket] [AwsProfile] [ConfigPath]",
		"  test-burn-upload-go.exe tray-open [Bucket] [AwsProfile] [ConfigPath]",
		"  test-burn-upload-go.exe tray-close [Bucket] [AwsProfile] [ConfigPath]",
		"  test-burn-upload-go.exe headobject [Bucket] [ObjectKey] [AwsProfile] [ConfigPath]",
		"  test-burn-upload-go.exe getobject [Bucket] [ObjectKey] [OutputPath] [AwsProfile] [ConfigPath]",
		"  test-burn-upload-go.exe getobject-nomd5 [Bucket] [ObjectKey] [OutputPath] [AwsProfile] [ConfigPath]",
		"  test-burn-upload-go.exe interrupt-retry [DataFile] [Bucket] [FailAfterMiB] [AwsProfile] [ConfigPath]",
		"  test-burn-upload-go.exe multipart-interrupt-retry [DataFile] [Bucket] [PartSizeMiB] [FailAfterMiB] [AwsProfile] [ConfigPath]",
		"",
		"Short aliases:",
		"  ls=listobjects, drive=driveinfo, disc=discinfo, open=tray-open, close=tray-close",
		"  mount=media-inserted, unmount=media-removed, dl=download, rdl=remote-download, put=putobject, po=putobject, sb=small-batch, mp=multipart, mix=mixed",
		"",
		"Config resolution:",
		"  1. explicit ConfigPath argument",
		"  2. OPTICAL_ARCHIVE_CONFIG_PATH",
		"  3. optical-archive.config.json next to this exe",
		"  4. D:\\BRS\\Publisher\\config\\optical-archive.config.json",
		"",
		"Modes:",
		"  full-flow",
		"    Upload local files, validate ListObjects and HeadObject, call FinalizeLayout, then download. MD5 is skipped by default.",
		"",
		"  mixed",
		"    Upload one directory serially with size-based strategy: large files use multipart, small files use PutObject. MD5 is skipped by default.",
		"",
		"  mixed-md5",
		"    Same as mixed, but verify downloaded files with MD5.",
		"",
		"  mixed-nomd5",
		"    Same as mixed, but skip downloaded file MD5 verification.",
		"",
		"  putobject",
		"    Upload one local file through a single S3 PutObject request, finalize, then download size-check.",
		"",
		"  putobject-md5",
		"    Same as putobject, but verify the downloaded file with MD5.",
		"",
		"  small-batch",
		"    Generate many small files, upload them concurrently with PutObject, finalize, then download verify. MD5 is skipped by default.",
		"",
		"  small-batch-md5",
		"    Same as small-batch, but verify downloaded files with MD5.",
		"",
		"  multipart",
		"    Upload one local file through standard S3 multipart APIs, complete it, finalize, then download size-check. MD5 is skipped by default.",
		"",
		"  multipart-md5",
		"    Same as multipart, but verify the downloaded file with MD5.",
		"",
		"  multipart-nomd5",
		"    Same as multipart, but skip downloaded file MD5 verification.",
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
		"  driveinfo",
		"    Generate a burnbridge control key for drive-info and print the virtual control bucket.",
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
		"  media-removed",
		"    Generate a burnbridge control key for media-removed and notify recorder that media was removed.",
		"",
		"  media-inserted",
		"    Generate a burnbridge control key for media-inserted and notify recorder that media was inserted.",
		"",
		"  tray-open",
		"    Generate a burnbridge control key for tray-open and request the recorder tray to open.",
		"",
		"  tray-close",
		"    Generate a burnbridge control key for tray-close and request the recorder tray to close.",
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
		"  multipart-interrupt-retry",
		"    Upload part 1 normally, fail part 2 mid-stream, retry the same part, then complete and verify.",
		"",
		"Examples:",
		`  .\test-burn-upload-go.exe ls`,
		`  .\test-burn-upload-go.exe drive`,
		`  .\test-burn-upload-go.exe open`,
		`  .\test-burn-upload-go.exe close`,
		`  .\test-burn-upload-go.exe put D:\testdata\large.bin archive-test`,
		`  .\test-burn-upload-go.exe sb D:\testdata\small-batch archive-test 256 2048 32`,
		`  .\test-burn-upload-go.exe mix D:\BRS\Publisher\temp\mixed-batch archive-test 32 32`,
		`  .\test-burn-upload-go.exe mp D:\testdata\10.zip archive-test 32`,
		`  .\test-burn-upload-go.exe rdl`,
		`  .\test-burn-upload-go.exe get docs/chat-export.md`,
	}

	for _, line := range lines {
		fmt.Fprintln(stream, line)
	}
}

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
	defaultConfigPath             = `D:\BRS\publisher\config\optical-archive.config.json`
	defaultDataDir                = `D:\testdata`
	defaultBucket                 = ""
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

const (
	bucketModeDisc = "disc"
	bucketModeTime = "time"
)

type mode string

const (
	modeHelp            mode = "help"
	modeFullFlow        mode = "full-flow"
	modeMixedFlow       mode = "mixed"
	modeDownload        mode = "download"
	modeRemoteDownload  mode = "remote-download"
	modeMultipartFlow   mode = "multipart"
	modePutObjectFlow   mode = "putobject"
	modeSmallBatchFlow  mode = "small-batch"
	modeVarSmallBatch   mode = "var-small-batch"
	modeSmallPackFlow   mode = "small-pack"
	modeListObjects     mode = "listobjects"
	modeDriveInfo       mode = "driveinfo"
	modeDiscInfo        mode = "discinfo"
	modeDbVersions      mode = "db-versions"
	modeDbRestore       mode = "db-restore"
	modeDbUseVersion    mode = "db-use-version"
	modeAnchorStatus    mode = "anchor-status"
	modeEncrypt         mode = "encrypt"
	modeFinalize        mode = "finalize"
	modeCloseDisc       mode = "closedisc"
	modeCloseDiscForce  mode = "close-disc-force"
	modeMediaRemoved    mode = "media-removed"
	modeMediaInserted   mode = "media-inserted"
	modeTrayOpen        mode = "tray-open"
	modeTrayClose       mode = "tray-close"
	modeHeadObject      mode = "headobject"
	modeGetObject       mode = "getobject"
	modeDeleteObject    mode = "deleteobject"
	modeInterruptRetry  mode = "interrupt-retry"
	modeMultipartRetry  mode = "multipart-interrupt-retry"
	modeMultipartResume mode = "multipart-resume"
)

type cliOptions struct {
	mode               mode
	dataDir            string
	bucket             string
	bucketMode         string
	awsProfile         string
	configPath         string
	skipUpload         bool
	skipFinalize       bool
	skipBucketCreate   bool
	skipMD5Verify      bool
	remoteOnly         bool
	listObjectsOnly    bool
	driveInfoOnly      bool
	dbVersionsOnly     bool
	dbRestoreOnly      bool
	dbUseVersionOnly   bool
	anchorStatusOnly   bool
	encryptOnly        bool
	headObjectOnly     bool
	discInfoOnly       bool
	finalizeOnly       bool
	closeDiscOnly      bool
	mediaRemovedOnly   bool
	mediaInsertedOnly  bool
	trayOpenOnly       bool
	trayCloseOnly      bool
	deleteObjectOnly   bool
	singleObjectKey    string
	singleObjectOutput string
	resumeUploadID     string
	resumeStartPart    int32
	interruptRetryOnly bool
	multipartRetryOnly bool
	multipartPartBytes int64
	multipartThreshold int64
	smallFileCount     int
	smallFileBytes     int64
	smallConcurrency   int
	variableSmallFiles bool
	failAfterBytes     int64
	controlArgument    string
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

func parseInvocation(args []string) (opts cliOptions, err error) {
	bucketMode := bucketModeDisc
	args, bucketMode, err = extractBucketModeArgs(args)
	if err != nil {
		return cliOptions{}, err
	}
	defer func() {
		if err == nil && opts.bucketMode == "" {
			opts.bucketMode = bucketMode
		}
	}()

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
	case "var-small-batch", "varsmallbatch", "vsb":
		return parseVarSmallBatchFlow(args[1:], true)
	case "var-small-batch-md5", "varsmallbatch-md5", "vsb-md5":
		return parseVarSmallBatchFlow(args[1:], false)
	case "small-pack", "smallpack", "sp":
		return parseSmallPackFlow(args[1:], true)
	case "small-pack-md5", "smallpack-md5", "sp-md5":
		return parseSmallPackFlow(args[1:], false)
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
	case "db-versions", "dbversions", "metadata-db-versions", "metadata-versions", "db":
		return parseBucketOnly(args[1:], modeDbVersions), nil
	case "db-restore", "dbrestore", "restore":
		return parseControlArgument(args[1:], modeDbRestore)
	case "db-use-version", "dbuse", "use-db", "use-version":
		return parseControlArgument(args[1:], modeDbUseVersion)
	case "anchor-status", "anchor":
		return parseBucketOnly(args[1:], modeAnchorStatus), nil
	case "encrypt", "encryption":
		return parseControlArgumentOptional(args[1:], modeEncrypt), nil
	case "finalize", "final":
		return parseBucketOnly(args[1:], modeFinalize), nil
	case "closedisc", "close-disc":
		return parseBucketOnly(args[1:], modeCloseDisc), nil
	case "closedisc-force", "close-disc-force", "close-force", "force-close", "force-closedisc":
		return parseBucketOnly(args[1:], modeCloseDiscForce), nil
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
		return parseFlexibleGetObject(args[1:], false)
	case "get":
		return parseShortGetObject(args[1:], false)
	case "getobject-nomd5", "get-nomd5", "getnomd5":
		return parseShortGetObject(args[1:], true)
	case "deleteobject", "delete-object", "delete", "del", "rm":
		return parseDeleteObject(args[1:])
	case "interrupt-retry":
		return parseInterruptRetry(args[1:])
	case "multipart-interrupt-retry", "mp-retry", "multipart-retry":
		return parseMultipartInterruptRetry(args[1:])
	case "multipart-resume", "mp-resume":
		return parseMultipartResume(args[1:])
	default:
		return parseFullFlow(args), nil
	}
}

func extractBucketModeArgs(args []string) ([]string, string, error) {
	if len(args) == 0 {
		return args, bucketModeDisc, nil
	}

	mode := bucketModeDisc
	cleaned := make([]string, 0, len(args))
	for _, raw := range args {
		arg := strings.TrimSpace(raw)
		normalized := strings.ToLower(arg)
		switch {
		case normalized == "--bucket-disc" || normalized == "--bucket-mode=disc" || normalized == "--bucket=disc":
			mode = bucketModeDisc
		case normalized == "--bucket-time" || normalized == "--time-bucket" || normalized == "--bucket-mode=time" || normalized == "--bucket=time":
			mode = bucketModeTime
		case strings.HasPrefix(normalized, "--bucket-mode=") || strings.HasPrefix(normalized, "--bucket="):
			return nil, "", fmt.Errorf("unsupported bucket mode argument: %s", raw)
		default:
			cleaned = append(cleaned, raw)
		}
	}

	return cleaned, mode, nil
}

func parseFullFlow(args []string) cliOptions {
	dataDir := positionalOrDefault(args, 0, defaultDataDir)
	profile, configPath := parseProfileConfig(args, 1, 2)
	return cliOptions{
		mode:             modeFullFlow,
		dataDir:          dataDir,
		bucket:           defaultBucket,
		awsProfile:       profile,
		configPath:       resolveConfigPath(configPath),
		skipBucketCreate: false,
		skipMD5Verify:    true,
	}
}

func parseDownload(args []string) cliOptions {
	dataDir := positionalOrDefault(args, 0, defaultDataDir)
	profile, configPath := parseProfileConfig(args, 1, 2)
	skipMD5 := len(args) > 3 && strings.EqualFold(strings.TrimSpace(args[3]), "nomd5")
	return cliOptions{
		mode:             modeDownload,
		dataDir:          dataDir,
		bucket:           defaultBucket,
		awsProfile:       profile,
		configPath:       resolveConfigPath(configPath),
		skipUpload:       true,
		skipFinalize:     true,
		skipBucketCreate: true,
		skipMD5Verify:    skipMD5,
	}
}

func parseRemoteDownload(args []string, skipMD5 bool) cliOptions {
	profile, configPath := parseProfileConfig(args, 0, 1)
	return cliOptions{
		mode:             modeRemoteDownload,
		dataDir:          ".",
		bucket:           defaultBucket,
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

	partSizeMiBRaw := positionalOrDefault(args, 1, "32")
	partSizeMiB, err := strconv.ParseInt(strings.TrimSpace(partSizeMiBRaw), 10, 64)
	if err != nil || partSizeMiB <= 0 {
		return cliOptions{}, fmt.Errorf("invalid partSizeMiB: %s", partSizeMiBRaw)
	}

	profile, configPath := parseProfileConfig(args, 2, 3)
	return cliOptions{
		mode:               modeMultipartFlow,
		dataDir:            dataFile,
		bucket:             defaultBucket,
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

	profile, configPath := parseProfileConfig(args, 1, 2)
	return cliOptions{
		mode:          modePutObjectFlow,
		dataDir:       dataFile,
		bucket:        defaultBucket,
		awsProfile:    profile,
		configPath:    resolveConfigPath(configPath),
		skipMD5Verify: skipMD5,
	}, nil
}

func parseMultipartResume(args []string) (cliOptions, error) {
	dataFile := positionalOrDefault(args, 0, "")
	if strings.TrimSpace(dataFile) == "" {
		return cliOptions{}, errors.New("data file is required")
	}

	objectKey := positionalOrDefault(args, 1, "")
	if strings.TrimSpace(objectKey) == "" {
		return cliOptions{}, errors.New("object key is required")
	}
	uploadID := positionalOrDefault(args, 2, "")
	if strings.TrimSpace(uploadID) == "" {
		return cliOptions{}, errors.New("upload id is required")
	}

	startPartRaw := positionalOrDefault(args, 3, "0")
	startPart, err := strconv.ParseInt(strings.TrimSpace(startPartRaw), 10, 32)
	if err != nil || startPart < 0 {
		return cliOptions{}, fmt.Errorf("invalid startPartNumber: %s", startPartRaw)
	}

	partSizeMiBRaw := positionalOrDefault(args, 4, "32")
	partSizeMiB, err := strconv.ParseInt(strings.TrimSpace(partSizeMiBRaw), 10, 64)
	if err != nil || partSizeMiB <= 0 {
		return cliOptions{}, fmt.Errorf("invalid partSizeMiB: %s", partSizeMiBRaw)
	}

	profile, configPath := parseProfileConfig(args, 5, 6)
	return cliOptions{
		mode:               modeMultipartResume,
		dataDir:            dataFile,
		bucket:             defaultBucket,
		awsProfile:         profile,
		configPath:         resolveConfigPath(configPath),
		skipBucketCreate:   true,
		skipFinalize:       true,
		skipMD5Verify:      true,
		singleObjectKey:    strings.TrimSpace(objectKey),
		resumeUploadID:     strings.TrimSpace(uploadID),
		resumeStartPart:    int32(startPart),
		multipartPartBytes: partSizeMiB * 1024 * 1024,
	}, nil
}

func parseSmallBatchFlow(args []string, skipMD5 bool) (cliOptions, error) {
	dataDir := positionalOrDefault(args, 0, filepath.Join(defaultDataDir, "small-batch"))
	countRaw := positionalOrDefault(args, 1, "256")
	count, err := strconv.Atoi(strings.TrimSpace(countRaw))
	if err != nil || count <= 0 {
		return cliOptions{}, fmt.Errorf("invalid fileCount: %s", countRaw)
	}
	sizeRaw := positionalOrDefault(args, 2, "2048")
	size, err := strconv.ParseInt(strings.TrimSpace(sizeRaw), 10, 64)
	if err != nil || size <= 0 {
		return cliOptions{}, fmt.Errorf("invalid fileSizeBytes: %s", sizeRaw)
	}
	concurrencyRaw := positionalOrDefault(args, 3, "32")
	concurrency, err := strconv.Atoi(strings.TrimSpace(concurrencyRaw))
	if err != nil || concurrency <= 0 {
		return cliOptions{}, fmt.Errorf("invalid concurrency: %s", concurrencyRaw)
	}
	profile, configPath := parseProfileConfig(args, 4, 5)
	return cliOptions{
		mode:             modeSmallBatchFlow,
		dataDir:          dataDir,
		bucket:           defaultBucket,
		awsProfile:       profile,
		configPath:       resolveConfigPath(configPath),
		skipBucketCreate: false,
		skipMD5Verify:    skipMD5,
		smallFileCount:   count,
		smallFileBytes:   size,
		smallConcurrency: concurrency,
	}, nil
}

func parseVarSmallBatchFlow(args []string, skipMD5 bool) (cliOptions, error) {
	if len(args) == 0 {
		args = []string{filepath.Join(defaultDataDir, "var-small-batch"), "10000", "2048", "32"}
	}
	if len(args) < 3 {
		args = append(args, "10000")
	}
	if len(args) < 4 {
		args = append(args, "2048")
	}
	if len(args) < 5 {
		args = append(args, "32")
	}
	opts, err := parseSmallBatchFlow(args, skipMD5)
	if err != nil {
		return cliOptions{}, err
	}
	opts.mode = modeVarSmallBatch
	opts.variableSmallFiles = true
	if opts.smallFileCount <= 0 {
		opts.smallFileCount = 10000
	}
	if opts.smallFileBytes <= 0 {
		opts.smallFileBytes = 2048
	}
	return opts, nil
}

func parseSmallPackFlow(args []string, skipMD5 bool) (cliOptions, error) {
	opts, err := parseSmallBatchFlow(args, skipMD5)
	if err != nil {
		return cliOptions{}, err
	}
	opts.mode = modeSmallPackFlow
	return opts, nil
}

func parseMixedFlow(args []string, skipMD5 bool) (cliOptions, error) {
	dataDir := positionalOrDefault(args, 0, defaultDataDir)

	thresholdMiBRaw := positionalOrDefault(args, 1, "32")
	thresholdMiB, err := strconv.ParseInt(strings.TrimSpace(thresholdMiBRaw), 10, 64)
	if err != nil || thresholdMiB <= 0 {
		return cliOptions{}, fmt.Errorf("invalid multipartThresholdMiB: %s", thresholdMiBRaw)
	}

	partSizeMiBRaw := positionalOrDefault(args, 2, "32")
	partSizeMiB, err := strconv.ParseInt(strings.TrimSpace(partSizeMiBRaw), 10, 64)
	if err != nil || partSizeMiB <= 0 {
		return cliOptions{}, fmt.Errorf("invalid partSizeMiB: %s", partSizeMiBRaw)
	}

	profile, configPath := parseProfileConfig(args, 3, 4)
	return cliOptions{
		mode:               modeMixedFlow,
		dataDir:            dataDir,
		bucket:             defaultBucket,
		awsProfile:         profile,
		configPath:         resolveConfigPath(configPath),
		skipBucketCreate:   false,
		skipMD5Verify:      skipMD5,
		multipartPartBytes: partSizeMiB * 1024 * 1024,
		multipartThreshold: thresholdMiB * 1024 * 1024,
	}, nil
}

func parseBucketOnly(args []string, m mode) cliOptions {
	opts := cliOptions{
		mode:             m,
		dataDir:          ".",
		bucket:           defaultBucket,
		configPath:       resolveConfigPath(""),
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
	case modeDbVersions:
		opts.skipFinalize = true
		opts.dbVersionsOnly = true
	case modeDbRestore:
		opts.skipFinalize = true
		opts.dbRestoreOnly = true
	case modeDbUseVersion:
		opts.skipFinalize = true
		opts.dbUseVersionOnly = true
	case modeAnchorStatus:
		opts.skipFinalize = true
		opts.anchorStatusOnly = true
	case modeEncrypt:
		opts.skipFinalize = true
		opts.encryptOnly = true
	case modeFinalize:
		opts.finalizeOnly = true
	case modeCloseDisc:
		opts.closeDiscOnly = true
	case modeCloseDiscForce:
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

func parseControlArgument(args []string, m mode) (cliOptions, error) {
	if strings.TrimSpace(positionalOrDefault(args, 0, "")) == "" {
		return cliOptions{}, fmt.Errorf("%s argument is required", m)
	}
	opts := parseBucketOnly(nil, m)
	opts.controlArgument = strings.TrimSpace(args[0])
	return opts, nil
}

func parseControlArgumentOptional(args []string, m mode) cliOptions {
	opts := parseBucketOnly(nil, m)
	opts.controlArgument = strings.TrimSpace(positionalOrDefault(args, 0, "status"))
	return opts
}

func parseHeadObject(args []string) (cliOptions, error) {
	return parseShortHeadObject(args)
}

func parseFlexibleGetObject(args []string, skipMD5 bool) (cliOptions, error) {
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

func parseDeleteObject(args []string) (cliOptions, error) {
	key := positionalOrDefault(args, 0, "")
	if strings.TrimSpace(key) == "" {
		return cliOptions{}, errors.New("object key is required")
	}

	profile, configPath := parseProfileConfig(args, 1, 2)
	return cliOptions{
		mode:             modeDeleteObject,
		dataDir:          ".",
		bucket:           defaultBucket,
		awsProfile:       profile,
		configPath:       resolveConfigPath(configPath),
		skipUpload:       true,
		skipFinalize:     true,
		skipBucketCreate: true,
		deleteObjectOnly: true,
		singleObjectKey:  key,
	}, nil
}

func parseInterruptRetry(args []string) (cliOptions, error) {
	dataFile := positionalOrDefault(args, 0, "")
	if strings.TrimSpace(dataFile) == "" {
		return cliOptions{}, errors.New("data file is required")
	}

	failAfterMiBRaw := positionalOrDefault(args, 1, "16")
	failAfterMiB, err := strconv.ParseInt(strings.TrimSpace(failAfterMiBRaw), 10, 64)
	if err != nil || failAfterMiB <= 0 {
		return cliOptions{}, fmt.Errorf("invalid failAfterMiB: %s", failAfterMiBRaw)
	}

	profile, configPath := parseProfileConfig(args, 2, 3)
	return cliOptions{
		mode:               modeInterruptRetry,
		dataDir:            dataFile,
		bucket:             defaultBucket,
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

	partSizeMiBRaw := positionalOrDefault(args, 1, "32")
	partSizeMiB, err := strconv.ParseInt(strings.TrimSpace(partSizeMiBRaw), 10, 64)
	if err != nil || partSizeMiB <= 0 {
		return cliOptions{}, fmt.Errorf("invalid partSizeMiB: %s", partSizeMiBRaw)
	}

	failAfterMiBRaw := positionalOrDefault(args, 2, "16")
	failAfterMiB, err := strconv.ParseInt(strings.TrimSpace(failAfterMiBRaw), 10, 64)
	if err != nil || failAfterMiB <= 0 {
		return cliOptions{}, fmt.Errorf("invalid failAfterMiB: %s", failAfterMiBRaw)
	}

	profile, configPath := parseProfileConfig(args, 3, 4)
	return cliOptions{
		mode:               modeMultipartRetry,
		dataDir:            dataFile,
		bucket:             defaultBucket,
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

	if executablePath, err := os.Executable(); err == nil {
		siblingConfigPath := filepath.Join(filepath.Dir(executablePath), defaultConfigFileName)
		if _, statErr := os.Stat(siblingConfigPath); statErr == nil {
			return siblingConfigPath
		}
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
		"  test-burn-upload-go.exe <command> [args]",
		"",
		"Buckets:",
		"  Data commands automatically use the active data bucket when one exists.",
		"  Blank-disc uploads default to --bucket-disc: use disc-info.data.discSerialNumberHex as the data bucket.",
		"  Use --bucket-time to create a timestamp data bucket for a new blank-disc test run.",
		"  Control commands automatically use the drive-serial control bucket.",
		"  Bucket arguments are intentionally not supported.",
		"",
		"Data commands:",
		"  ls                                  List objects in the active data bucket.",
		"  head <ObjectKey>                    Show one object's metadata.",
		"  get <ObjectKey> [OutputPath]         Download one object and print MD5.",
		"  get-nomd5 <ObjectKey> [OutputPath]   Download one object without MD5.",
		"  delete <ObjectKey>                   Delete one object metadata entry.",
		"  del <ObjectKey>                      Alias of delete.",
		"  rm <ObjectKey>                       Alias of delete.",
		"  dl [DataDir]                         Download and verify objects from local file list.",
		"  dl-nomd5 [DataDir]                   Download from local file list without MD5.",
		"  rdl                                 Download all remote objects.",
		"  rdl-nomd5                           Download all remote objects without MD5.",
		"",
		"Control commands:",
		"  drive                               Read drive info.",
		"  disc                                Read disc info.",
		"  anchor                              Read BRS Anchor status.",
		"  db                                  List metadata DB versions.",
		"  restore <Generation>                Restore metadata DB generation.",
		"  use-db <Generation>                 Verify/use metadata DB generation view.",
		"  encrypt [on|off|status]             Set or read hidden-UDF runtime mode.",
		"  finalize                            Run FinalizeLayout.",
		"  closedisc                           Close disc after safe staged flush.",
		"  close-force                         Force close disc after best-effort staged flush.",
		"  mount                               Notify recorder media inserted.",
		"  unmount                             Notify recorder media removed.",
		"  open                                Open recorder tray.",
		"  close                               Close recorder tray.",
		"",
		"Write and regression commands:",
		"  put <DataFile>                       PutObject upload, finalize, then download size-check.",
		"  put-md5 <DataFile>                   PutObject upload with MD5 verification.",
		"  mp <DataFile> [PartSizeMiB]          Multipart upload, finalize, then download size-check.",
		"  mp-md5 <DataFile> [PartSizeMiB]      Multipart upload with MD5 verification.",
		"  mix <DataDir> [ThresholdMiB] [PartSizeMiB]",
		"                                      Mixed upload: small files use PutObject, large files use multipart.",
		"  sb <DataDir> [FileCount] [FileSizeBytes] [Concurrency]",
		"                                      Generate fixed-size small files and test upload/download.",
		"  vsb <DataDir> [FileCount] [MaxFileSizeBytes] [Concurrency]",
		"                                      Generate variable-size small files and test upload/download.",
		"  sp <DataDir> [FileCount] [FileSizeBytes] [Concurrency]",
		"                                      Generate small files and test pack upload/download.",
		"  interrupt-retry <DataFile> [FailAfterMiB]",
		"                                      Simulate interrupted PutObject and retry.",
		"  mp-retry <DataFile> [PartSizeMiB] [FailAfterMiB]",
		"                                      Simulate interrupted UploadPart and retry.",
		"  mp-resume <DataFile> <ObjectKey> <UploadId> [StartPart] [PartSizeMiB]",
		"                                      Resume an existing multipart upload.",
		"",
		"Config resolution:",
		"  1. optical-archive.config.json next to this executable.",
		"  2. OPTICAL_ARCHIVE_CONFIG_PATH.",
		"  3. D:\\BRS\\publisher\\config\\optical-archive.config.json.",
		"",
		"Examples:",
		`  .\test-burn-upload-go.exe ls`,
		`  .\test-burn-upload-go.exe disc`,
		`  .\test-burn-upload-go.exe get docs/readme.txt D:\verify\readme.txt`,
		`  .\test-burn-upload-go.exe get-nomd5 10.zip D:\verify\10.zip`,
		`  .\test-burn-upload-go.exe delete tmp/old-object.bin`,
		`  .\test-burn-upload-go.exe put D:\testdata\large.bin`,
		`  .\test-burn-upload-go.exe mp D:\testdata\10.zip 32`,
		`  .\test-burn-upload-go.exe mix D:\testdata 32 32`,
		`  .\test-burn-upload-go.exe open`,
		`  .\test-burn-upload-go.exe close`,
	}

	for _, line := range lines {
		fmt.Fprintln(stream, line)
	}
}

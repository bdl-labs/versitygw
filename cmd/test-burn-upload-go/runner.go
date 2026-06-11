package main

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsv4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/google/uuid"
)

type toolConfig struct {
	OpticalArchive struct {
		Gateway struct {
			S3Endpoint string `json:"S3Endpoint"`
			S3Region   string `json:"S3Region"`
		} `json:"Gateway"`
		Testing struct {
			S3Endpoint         string `json:"S3Endpoint"`
			S3Region           string `json:"S3Region"`
			S3AccessKeyID      string `json:"S3AccessKeyId"`
			S3SecretAccessKey  string `json:"S3SecretAccessKey"`
			AWSAccessKeyID     string `json:"AwsAccessKeyId"`
			AWSSecretAccessKey string `json:"AwsSecretAccessKey"`
		} `json:"Testing"`
	} `json:"OpticalArchive"`
}

type resolvedConfig struct {
	configPath            string
	endpoint              string
	region                string
	accessKey             string
	secretKey             string
	publisherRoot         string
	logRoot               string
	testRunRoot           string
	verifyDownloadRoot    string
	runtimeGatewayLogDir  string
	runtimeRecorderLogDir string
	awsReadTimeout        time.Duration
	awsConnectTimeout     time.Duration
	finalizeReadTimeout   time.Duration
}

type runner struct {
	opts              cliOptions
	cfg               resolvedConfig
	client            *s3.Client
	runRoot           string
	runLogPath        string
	memoryCSVPath     string
	metricsCSVPath    string
	memorySummaryPath string
	summaryJSONPath   string
	logger            *runLogger
	sampler           *memorySampler
}

type runLogger struct {
	mu   sync.Mutex
	path string
}

type fileSnapshot struct {
	Root  string
	Items []sourceItem
	Map   map[string]sourceItem
}

type sourceItem struct {
	RelativePath string
	FullPath     string
	Size         int64
	MD5          string
}

type fileMetric struct {
	RelativePath         string
	FullName             string
	SizeBytes            int64
	SizeMiB              float64
	ExpectedMD5          string
	UploadSeconds        float64
	UploadMiBPerSecond   float64
	DownloadSeconds      float64
	DownloadMiBPerSecond float64
	ActualMD5            string
	Status               string
	Notes                string
}

type runSummary struct {
	Status                  string  `json:"Status"`
	Bucket                  string  `json:"Bucket"`
	RequestedBucket         string  `json:"RequestedBucket"`
	DataDirectory           string  `json:"DataDirectory"`
	RemoteOnly              bool    `json:"RemoteOnly"`
	FileCount               int     `json:"FileCount"`
	TotalBytes              int64   `json:"TotalBytes"`
	TotalMiB                float64 `json:"TotalMiB"`
	UploadSeconds           float64 `json:"UploadSeconds"`
	UploadMiBPerSecond      float64 `json:"UploadMiBPerSecond"`
	FinalizeLayoutSeconds   float64 `json:"FinalizeLayoutSeconds"`
	DownloadSeconds         float64 `json:"DownloadSeconds"`
	DownloadMiBPerSecond    float64 `json:"DownloadMiBPerSecond"`
	TotalTestSeconds        float64 `json:"TotalTestSeconds"`
	RunRoot                 string  `json:"RunRoot"`
	MetricsCSVPath          string  `json:"MetricsCsvPath"`
	MemoryCSVPath           string  `json:"MemoryCsvPath"`
	MemorySummaryPath       string  `json:"MemorySummaryPath"`
	FinalizeResponsePath    string  `json:"FinalizeResponsePath"`
	VerifyDownloadDirectory string  `json:"VerifyDownloadDirectory"`
	RecorderLogDirectory    string  `json:"RecorderLogDirectory"`
	GatewayLogDirectory     string  `json:"GatewayLogDirectory"`
}

type discInfoData struct {
	BlockSizeBytes           int64  `json:"blockSizeBytes"`
	Bucket                   string `json:"bucket"`
	DiscSerialNumberHex      string `json:"discSerialNumberHex"`
	DiscStatusName           string `json:"discStatusName"`
	FinalizeReserveBytes     int64  `json:"finalizeReserveBytes"`
	FreeBlocks               int64  `json:"freeBlocks"`
	FreeCapacityBytes        int64  `json:"freeCapacityBytes"`
	LayoutCompletedAtUTC     string `json:"layoutCompletedAtUtc"`
	LayoutStatus             string `json:"layoutStatus"`
	MediaType                string `json:"mediaType"`
	RecordableCapacityBlocks int64  `json:"recordableCapacityBlocks"`
	SessionIsFinalized       bool   `json:"sessionIsFinalized"`
	TotalBlocks              int64  `json:"totalBlocks"`
	TotalCapacityBytes       int64  `json:"totalCapacityBytes"`
	UpdatedAt                string `json:"updatedAt"`
	UsedCapacityBytes        int64  `json:"usedCapacityBytes"`
	VolumeLabel              string `json:"volumeLabel"`
	WritableCapacityBytes    int64  `json:"writableCapacityBytes"`
	WritableState            string `json:"writableState"`
}

type discInfoDocument struct {
	Action      string       `json:"action"`
	APIVersion  string       `json:"apiVersion"`
	Bucket      string       `json:"bucket"`
	Data        discInfoData `json:"data"`
	OK          bool         `json:"ok"`
	RequestID   string       `json:"requestId"`
	RequestTime int64        `json:"requestTime"`
}

type interruptRetrySummary struct {
	Status                         string  `json:"status"`
	Bucket                         string  `json:"bucket"`
	ObjectKey                      string  `json:"objectKey"`
	FilePath                       string  `json:"filePath"`
	FileSizeBytes                  int64   `json:"fileSizeBytes"`
	FileMD5                        string  `json:"fileMd5"`
	FailAfterBytes                 int64   `json:"failAfterBytes"`
	FailAfterMiB                   float64 `json:"failAfterMiB"`
	InterruptedUploadSeconds       float64 `json:"interruptedUploadSeconds"`
	InterruptedUploadError         string  `json:"interruptedUploadError"`
	VisibleAfterFailure            bool    `json:"visibleAfterFailure"`
	RetryUploadSeconds             float64 `json:"retryUploadSeconds"`
	FinalizeLayoutSeconds          float64 `json:"finalizeLayoutSeconds"`
	DownloadSeconds                float64 `json:"downloadSeconds"`
	DownloadMD5                    string  `json:"downloadMd5"`
	BeforeUsedCapacityBytes        int64   `json:"beforeUsedCapacityBytes"`
	AfterFailureUsedCapacityBytes  int64   `json:"afterFailureUsedCapacityBytes"`
	AfterFinalizeUsedCapacityBytes int64   `json:"afterFinalizeUsedCapacityBytes"`
	DeltaAfterFailureBytes         int64   `json:"deltaAfterFailureBytes"`
	DeltaRetryAndFinalizeBytes     int64   `json:"deltaRetryAndFinalizeBytes"`
	DeltaTotalBytes                int64   `json:"deltaTotalBytes"`
	RunRoot                        string  `json:"runRoot"`
	BeforeDiscInfoPath             string  `json:"beforeDiscInfoPath"`
	AfterFailureDiscInfoPath       string  `json:"afterFailureDiscInfoPath"`
	AfterFinalizeDiscInfoPath      string  `json:"afterFinalizeDiscInfoPath"`
	FinalizeResponsePath           string  `json:"finalizeResponsePath"`
	DownloadPath                   string  `json:"downloadPath"`
	MemoryCSVPath                  string  `json:"memoryCsvPath"`
	MemorySummaryPath              string  `json:"memorySummaryPath"`
}

type memorySampler struct {
	scriptPath string
	cmd        *exec.Cmd
}

type memoryAggregate struct {
	name          string
	maxWorkingSet uint64
	maxPrivate    uint64
	maxPaged      uint64
	firstSeen     string
	lastSeen      string
}

type failingReader struct {
	source     io.Reader
	failAfter  int64
	readBytes  int64
	failErr    error
	failedOnce bool
}

var errSimulatedDisconnect = errors.New("simulated upload disconnect")

func newRunner(opts cliOptions) (*runner, error) {
	if opts.interruptRetryOnly {
		info, err := os.Stat(opts.dataDir)
		if err != nil || info.IsDir() {
			return nil, fmt.Errorf("data file not found: %s", opts.dataDir)
		}
	}
	if !opts.interruptRetryOnly && !opts.remoteOnly && !opts.listObjectsOnly && !opts.discInfoOnly && !opts.finalizeOnly && !opts.closeDiscOnly && !opts.headObjectOnly && strings.TrimSpace(opts.singleObjectKey) == "" {
		info, err := os.Stat(opts.dataDir)
		if err != nil || !info.IsDir() {
			return nil, fmt.Errorf("data directory not found: %s", opts.dataDir)
		}
	}

	cfg, err := loadResolvedConfig(opts)
	if err != nil {
		return nil, err
	}

	runRoot, err := buildRunRoot(cfg.testRunRoot, opts.bucket)
	if err != nil {
		return nil, err
	}

	if err := ensureDir(runRoot); err != nil {
		return nil, err
	}

	client, err := newS3Client(cfg, opts.awsProfile)
	if err != nil {
		return nil, err
	}

	r := &runner{
		opts:              opts,
		cfg:               cfg,
		client:            client,
		runRoot:           runRoot,
		runLogPath:        filepath.Join(runRoot, "test-run.log"),
		memoryCSVPath:     filepath.Join(runRoot, "memory-samples.csv"),
		metricsCSVPath:    filepath.Join(runRoot, "file-metrics.csv"),
		memorySummaryPath: filepath.Join(runRoot, "memory-summary.txt"),
		summaryJSONPath:   filepath.Join(runRoot, "summary.json"),
	}
	r.logger = &runLogger{path: r.runLogPath}
	return r, nil
}

func (r *runner) run() (err error) {
	metrics := map[string]*fileMetric{}
	resultStatus := "Failed"
	testStart := time.Now()
	finalizeDuration := 0.0
	finalizeOutputPath := filepath.Join(r.runRoot, "finalize-layout-response.json")
	verifyDir := filepath.Join(r.cfg.verifyDownloadRoot, r.opts.bucket)

	defer func() {
		_ = exportFileMetrics(r.metricsCSVPath, metrics)
		if r.sampler != nil {
			_ = r.sampler.stop()
		}
		_ = r.writeMemorySummary()
		_ = copyRuntimeLogs(r.runRoot, r.cfg.runtimeGatewayLogDir, r.cfg.runtimeRecorderLogDir)
		r.logf("Result: %s", resultStatus)
		r.logf("")
		r.logf("Done.")
	}()

	r.logf("Using config path: %s", r.opts.configPath)
	r.logf("Endpoint: %s", r.cfg.endpoint)
	r.logf("Region: %s", r.cfg.region)
	r.logf("Bucket: %s", r.opts.bucket)
	r.logf("DataDir: %s", r.opts.dataDir)
	r.logf("RunRoot: %s", r.runRoot)
	r.logf("SkipUpload: %t", r.opts.skipUpload)
	r.logf("SkipFinalize: %t", r.opts.skipFinalize)
	r.logf("SkipBucketCreate: %t", r.opts.skipBucketCreate)
	r.logf("SkipMd5Verify: %t", r.opts.skipMD5Verify)
	r.logf("RemoteOnly: %t", r.opts.remoteOnly)
	r.logf("ListObjectsOnly: %t", r.opts.listObjectsOnly)
	r.logf("HeadObjectOnly: %t", r.opts.headObjectOnly)
	r.logf("DiscInfoOnly: %t", r.opts.discInfoOnly)
	r.logf("FinalizeOnly: %t", r.opts.finalizeOnly)
	r.logf("CloseDiscOnly: %t", r.opts.closeDiscOnly)
	r.logf("InterruptRetryOnly: %t", r.opts.interruptRetryOnly)
	if strings.TrimSpace(r.opts.singleObjectKey) != "" {
		r.logf("SingleObjectKey: %s", r.opts.singleObjectKey)
	}
	if strings.TrimSpace(r.opts.singleObjectOutput) != "" {
		r.logf("SingleObjectOutputPath: %s", r.opts.singleObjectOutput)
	}
	if r.opts.failAfterBytes > 0 {
		r.logf("FailAfterBytes: %d", r.opts.failAfterBytes)
	}
	if strings.TrimSpace(r.opts.awsProfile) != "" {
		r.logf("Profile: %s", r.opts.awsProfile)
	}

	if sampler, samplerErr := startMemorySampler(r.memoryCSVPath, defaultMemorySampleInterval); samplerErr == nil {
		r.sampler = sampler
		r.logf("Memory sampler started: %s", r.memoryCSVPath)
	} else {
		r.logf("Memory sampler disabled: %v", samplerErr)
	}

	r.logf("[1/8] Checking gateway connectivity...")
	if _, err = r.listBuckets(defaultReadTimeout); err != nil {
		return err
	}

	activeBucket, err := r.ensureActiveBucket()
	if err != nil {
		return err
	}
	r.logf("Active bucket: %s", activeBucket)

	switch {
	case r.opts.listObjectsOnly:
		err = r.runListObjectsOnly(activeBucket)
	case r.opts.discInfoOnly:
		err = r.runDiscInfoOnly(activeBucket)
	case r.opts.finalizeOnly:
		err = r.runFinalizeOnly(activeBucket)
	case r.opts.closeDiscOnly:
		err = r.runCloseDiscOnly(activeBucket)
	case r.opts.headObjectOnly:
		err = r.runHeadObjectOnly(activeBucket)
	case r.opts.interruptRetryOnly:
		err = r.runInterruptRetry(activeBucket)
	case strings.TrimSpace(r.opts.singleObjectKey) != "":
		err = r.runSingleObjectDownload(activeBucket)
	default:
		var summary *runSummary
		summary, finalizeDuration, verifyDir, err = r.runFullFlow(activeBucket, metrics, finalizeOutputPath, testStart)
		if summary != nil {
			if marshalErr := writeJSONFile(r.summaryJSONPath, summary); marshalErr != nil && err == nil {
				err = marshalErr
			}
		}
	}
	if err != nil {
		return err
	}

	if len(metrics) > 0 {
		totalBytes := int64(0)
		uploadSeconds := 0.0
		downloadSeconds := 0.0
		for _, metric := range metrics {
			totalBytes += metric.SizeBytes
			uploadSeconds += metric.UploadSeconds
			downloadSeconds += metric.DownloadSeconds
		}

		totalSeconds := roundSeconds(time.Since(testStart))
		summary := &runSummary{
			Status:                  "Success",
			Bucket:                  activeBucket,
			RequestedBucket:         r.opts.bucket,
			DataDirectory:           r.opts.dataDir,
			RemoteOnly:              r.opts.remoteOnly,
			FileCount:               len(metrics),
			TotalBytes:              totalBytes,
			TotalMiB:                roundFloat(float64(totalBytes)/(1024*1024), 3),
			UploadSeconds:           roundFloat(uploadSeconds, 3),
			UploadMiBPerSecond:      rateMiBPerSecond(totalBytes, uploadSeconds),
			FinalizeLayoutSeconds:   roundFloat(finalizeDuration, 3),
			DownloadSeconds:         roundFloat(downloadSeconds, 3),
			DownloadMiBPerSecond:    rateMiBPerSecond(totalBytes, downloadSeconds),
			TotalTestSeconds:        totalSeconds,
			RunRoot:                 r.runRoot,
			MetricsCSVPath:          r.metricsCSVPath,
			MemoryCSVPath:           r.memoryCSVPath,
			MemorySummaryPath:       r.memorySummaryPath,
			FinalizeResponsePath:    finalizeOutputPath,
			VerifyDownloadDirectory: verifyDir,
			RecorderLogDirectory:    r.cfg.runtimeRecorderLogDir,
			GatewayLogDirectory:     r.cfg.runtimeGatewayLogDir,
		}
		if err := writeJSONFile(r.summaryJSONPath, summary); err != nil {
			return err
		}

		r.logf("")
		r.logf("Run summary:")
		r.logf("  files            : %d", summary.FileCount)
		r.logf("  total size       : %.3f MiB", summary.TotalMiB)
		r.logf("  upload total     : %.3fs @ %.3f MiB/s", summary.UploadSeconds, summary.UploadMiBPerSecond)
		r.logf("  finalize total   : %.3fs", summary.FinalizeLayoutSeconds)
		r.logf("  download total   : %.3fs @ %.3f MiB/s", summary.DownloadSeconds, summary.DownloadMiBPerSecond)
		r.logf("  total test time  : %.3fs", summary.TotalTestSeconds)
		r.logf("  verify download  : %s", summary.VerifyDownloadDirectory)
		r.logf("  per-file metrics : %s", r.metricsCSVPath)
		r.logf("  memory samples   : %s", r.memoryCSVPath)
		r.logf("  summary json     : %s", r.summaryJSONPath)
	}

	resultStatus = "Success"
	return nil
}

func (r *runner) ensureActiveBucket() (string, error) {
	r.logf("[2/8] Ensuring bucket s3://%s exists...", r.opts.bucket)
	activeBucket := r.opts.bucket

	exists, err := r.bucketExists(activeBucket)
	if err != nil {
		return "", err
	}
	if exists {
		r.logf("Requested bucket already exists: %s", activeBucket)
		return activeBucket, nil
	}

	createdRequestedBucket := false
	if !r.opts.skipBucketCreate && !r.opts.skipUpload {
		if err := r.createBucket(activeBucket); err != nil {
			r.logf("Create bucket request failed for '%s': %v", activeBucket, err)
		} else {
			createdRequestedBucket = true
			r.logf("Create bucket requested: %s", activeBucket)
		}
	}

	if createdRequestedBucket {
		if ready, readyErr := r.ensureBucketReady(activeBucket, defaultBucketReadyTimeout); readyErr != nil {
			return "", readyErr
		} else if ready {
			r.logf("Requested bucket became ready: %s", activeBucket)
			return activeBucket, nil
		}
	}

	resolvedBucket, resolution, visibleBuckets, err := r.resolveActiveBucket(activeBucket, defaultBucketReadyTimeout)
	if err != nil {
		return "", err
	}
	if resolvedBucket == "" {
		if len(visibleBuckets) > 0 {
			r.logf("Visible buckets after wait: %s", strings.Join(visibleBuckets, ", "))
		}
		return "", fmt.Errorf("bucket '%s' is unavailable and no active disc bucket could be resolved", r.opts.bucket)
	}

	if resolvedBucket != r.opts.bucket {
		r.logf("Requested bucket '%s' is unavailable; falling back to mounted disc bucket '%s'.", r.opts.bucket, resolvedBucket)
	} else if resolution == "requested" {
		r.logf("Requested bucket became ready: %s", resolvedBucket)
	}
	return resolvedBucket, nil
}

func (r *runner) runListObjectsOnly(bucket string) error {
	r.logf("[3/3] Listing objects...")
	items, err := r.listAllObjects(bucket)
	if err != nil {
		return err
	}
	r.logf("Object count: %d", len(items))
	for _, item := range items {
		r.logf("  %s | size=%d bytes", item.RelativePath, item.Size)
	}
	return nil
}

func (r *runner) runDiscInfoOnly(bucket string) error {
	r.logf("[3/3] Reading DiscInfo...")
	discInfoPath := filepath.Join(r.runRoot, "discinfo.json")
	controlKey, raw, err := r.downloadControlObject(bucket, "disc-info", discInfoPath)
	if err != nil {
		return err
	}
	r.logf("DiscInfo control key: %s", controlKey)
	r.logf("DiscInfo saved to: %s", discInfoPath)
	r.writeControlJSONLog(raw)
	return nil
}

func (r *runner) runFinalizeOnly(bucket string) error {
	r.logf("[3/3] Triggering FinalizeLayout...")
	outputPath := filepath.Join(r.runRoot, "finalize-layout-response.json")
	start := time.Now()
	controlKey, raw, err := r.downloadControlObject(bucket, "finalize-layout", outputPath)
	if err != nil {
		return err
	}
	r.logf("FinalizeLayout control key: %s", controlKey)
	r.logf("FinalizeLayout response saved to: %s", outputPath)
	r.logf("FinalizeLayout elapsed: %.3fs", roundSeconds(time.Since(start)))
	r.writeControlJSONLog(raw)
	return nil
}

func (r *runner) runCloseDiscOnly(bucket string) error {
	r.logf("[3/3] Triggering CloseDisc...")
	outputPath := filepath.Join(r.runRoot, "close-disc-response.json")
	start := time.Now()
	controlKey, raw, err := r.downloadControlObject(bucket, "close-disc", outputPath)
	if err != nil {
		return err
	}
	r.logf("CloseDisc control key: %s", controlKey)
	r.logf("CloseDisc response saved to: %s", outputPath)
	r.logf("CloseDisc elapsed: %.3fs", roundSeconds(time.Since(start)))
	r.writeControlJSONLog(raw)
	return nil
}

func (r *runner) runHeadObjectOnly(bucket string) error {
	r.logf("[3/3] Reading object metadata...")
	head, err := r.headObject(bucket, r.opts.singleObjectKey, defaultReadTimeout)
	if err != nil {
		return err
	}

	r.logf("  object       : %s", r.opts.singleObjectKey)
	r.logf("  size         : %d", derefInt64(head.ContentLength))
	if head.ETag != nil {
		r.logf("  etag         : %s", strings.TrimSpace(*head.ETag))
	}
	if head.LastModified != nil {
		r.logf("  lastModified : %s", head.LastModified.Format(time.RFC3339))
	}
	if head.ContentType != nil {
		r.logf("  contentType  : %s", strings.TrimSpace(*head.ContentType))
	}
	if len(head.Metadata) > 0 {
		keys := make([]string, 0, len(head.Metadata))
		for key := range head.Metadata {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		r.logf("  metadataKeys : %d", len(keys))
		for _, key := range keys {
			r.logf("    %s=%s", key, head.Metadata[key])
		}
	}
	if head.StorageClass != "" {
		r.logf("  storageClass : %s", head.StorageClass)
	}
	if head.ChecksumCRC32 != nil {
		r.logf("  checksumCrc32 : %s", *head.ChecksumCRC32)
	}
	if head.ChecksumCRC32C != nil {
		r.logf("  checksumCrc32c: %s", *head.ChecksumCRC32C)
	}
	if head.ChecksumSHA1 != nil {
		r.logf("  checksumSha1 : %s", *head.ChecksumSHA1)
	}
	if head.ChecksumSHA256 != nil {
		r.logf("  checksumSha256 : %s", *head.ChecksumSHA256)
	}
	return nil
}

func (r *runner) runSingleObjectDownload(bucket string) error {
	r.logf("[3/5] Checking object metadata...")
	head, err := r.headObject(bucket, r.opts.singleObjectKey, defaultReadTimeout)
	if err != nil {
		return err
	}
	r.logf("  %s | size=%d bytes", r.opts.singleObjectKey, derefInt64(head.ContentLength))

	targetPath := resolveSingleObjectOutputPath(r.cfg.verifyDownloadRoot, bucket, r.opts.singleObjectKey, r.opts.singleObjectOutput)
	if err := ensureDir(filepath.Dir(targetPath)); err != nil {
		return err
	}

	r.logf("[4/5] Downloading single object...")
	start := time.Now()
	if err := r.downloadObjectToFile(bucket, r.opts.singleObjectKey, targetPath, defaultReadTimeout); err != nil {
		return err
	}
	seconds := roundSeconds(time.Since(start))

	info, err := os.Stat(targetPath)
	if err != nil {
		return fmt.Errorf("downloaded file missing: %s", r.opts.singleObjectKey)
	}
	if info.Size() != derefInt64(head.ContentLength) {
		return fmt.Errorf("downloaded size mismatch for %s", r.opts.singleObjectKey)
	}

	md5Value := ""
	if !r.opts.skipMD5Verify {
		md5Value, err = computeMD5(targetPath)
		if err != nil {
			return err
		}
	}

	rate := rateMiBPerSecond(info.Size(), seconds)
	r.logf("[5/5] Result")
	r.logf("  object       : %s", r.opts.singleObjectKey)
	r.logf("  output       : %s", targetPath)
	r.logf("  size         : %d bytes", info.Size())
	r.logf("  download     : %.3fs", seconds)
	r.logf("  rate         : %.3f MiB/s", rate)
	if !r.opts.skipMD5Verify {
		r.logf("  md5          : %s", md5Value)
	}
	return nil
}

func (r *runner) runInterruptRetry(bucket string) error {
	source, err := buildSingleFileSource(r.opts.dataDir)
	if err != nil {
		return err
	}
	objectKey := source.RelativePath
	if strings.TrimSpace(objectKey) == "" {
		objectKey = filepath.Base(source.FullPath)
	}

	beforeDiscInfoPath := filepath.Join(r.runRoot, "discinfo-before.json")
	afterFailureDiscInfoPath := filepath.Join(r.runRoot, "discinfo-after-failure.json")
	afterFinalizeDiscInfoPath := filepath.Join(r.runRoot, "discinfo-after-finalize.json")
	finalizeOutputPath := filepath.Join(r.runRoot, "finalize-layout-response.json")
	downloadPath := resolveSingleObjectOutputPath(r.cfg.verifyDownloadRoot, bucket, objectKey, "")

	r.logf("[3/8] Preparing interrupt-retry source file...")
	r.logf("  objectKey     : %s", objectKey)
	r.logf("  filePath      : %s", source.FullPath)
	r.logf("  fileSizeBytes : %d", source.Size)
	r.logf("  fileMd5       : %s", source.MD5)

	r.logf("[4/8] Reading baseline DiscInfo...")
	beforeInfo, err := r.fetchDiscInfo(bucket, beforeDiscInfoPath)
	if err != nil {
		return err
	}
	r.logf("  usedCapacityBytes(before) : %d", beforeInfo.Data.UsedCapacityBytes)

	r.logf("[5/8] Uploading with simulated disconnect...")
	interruptedStart := time.Now()
	interruptedErr := r.putObjectWithSimulatedDisconnect(bucket, objectKey, source.FullPath, r.opts.failAfterBytes)
	interruptedSeconds := roundSeconds(time.Since(interruptedStart))
	if interruptedErr == nil {
		return fmt.Errorf("simulated interrupted upload unexpectedly succeeded")
	}
	r.logf("  interruptedUploadSeconds : %.3fs", interruptedSeconds)
	r.logf("  interruptedUploadError   : %v", interruptedErr)

	time.Sleep(3 * time.Second)

	r.logf("[6/8] Reading DiscInfo after interrupted upload...")
	afterFailureInfo, err := r.fetchDiscInfo(bucket, afterFailureDiscInfoPath)
	if err != nil {
		return err
	}
	visibleAfterFailure, visibleErr := r.objectExists(bucket, objectKey)
	if visibleErr != nil {
		return visibleErr
	}
	r.logf("  usedCapacityBytes(afterFailure) : %d", afterFailureInfo.Data.UsedCapacityBytes)
	r.logf("  deltaAfterFailureBytes          : %d", afterFailureInfo.Data.UsedCapacityBytes-beforeInfo.Data.UsedCapacityBytes)
	r.logf("  visibleAfterFailure            : %t", visibleAfterFailure)

	r.logf("[7/8] Retrying full upload for same key...")
	retryStart := time.Now()
	if err := r.putObject(bucket, objectKey, source.FullPath); err != nil {
		return err
	}
	retrySeconds := roundSeconds(time.Since(retryStart))
	r.logf("  retryUploadSeconds : %.3fs", retrySeconds)

	r.logf("[7.5/8] Triggering FinalizeLayout...")
	finalizeStart := time.Now()
	controlKey, finalizeRaw, err := r.downloadControlObject(bucket, "finalize-layout", finalizeOutputPath)
	if err != nil {
		return err
	}
	finalizeSeconds := roundSeconds(time.Since(finalizeStart))
	r.logf("  finalize control key : %s", controlKey)
	r.logf("  finalize elapsed     : %.3fs", finalizeSeconds)
	r.writeFinalizeSummary(finalizeRaw, bucket)

	r.logf("[8/8] Downloading retried object and verifying MD5...")
	if err := ensureDir(filepath.Dir(downloadPath)); err != nil {
		return err
	}
	downloadStart := time.Now()
	if err := r.downloadObjectToFile(bucket, objectKey, downloadPath, defaultReadTimeout); err != nil {
		return err
	}
	downloadSeconds := roundSeconds(time.Since(downloadStart))
	downloadMD5, err := computeMD5(downloadPath)
	if err != nil {
		return err
	}
	if downloadMD5 != source.MD5 {
		return fmt.Errorf("downloaded MD5 mismatch for %s", objectKey)
	}
	r.logf("  downloadSeconds : %.3fs", downloadSeconds)
	r.logf("  downloadMd5     : %s", downloadMD5)

	r.logf("[8.5/8] Reading DiscInfo after finalize...")
	afterFinalizeInfo, err := r.fetchDiscInfo(bucket, afterFinalizeDiscInfoPath)
	if err != nil {
		return err
	}
	deltaAfterFailure := afterFailureInfo.Data.UsedCapacityBytes - beforeInfo.Data.UsedCapacityBytes
	deltaRetryAndFinalize := afterFinalizeInfo.Data.UsedCapacityBytes - afterFailureInfo.Data.UsedCapacityBytes
	deltaTotal := afterFinalizeInfo.Data.UsedCapacityBytes - beforeInfo.Data.UsedCapacityBytes
	r.logf("  usedCapacityBytes(afterFinalize) : %d", afterFinalizeInfo.Data.UsedCapacityBytes)
	r.logf("  deltaRetryAndFinalizeBytes       : %d", deltaRetryAndFinalize)
	r.logf("  deltaTotalBytes                  : %d", deltaTotal)

	summary := &interruptRetrySummary{
		Status:                         "Success",
		Bucket:                         bucket,
		ObjectKey:                      objectKey,
		FilePath:                       source.FullPath,
		FileSizeBytes:                  source.Size,
		FileMD5:                        source.MD5,
		FailAfterBytes:                 r.opts.failAfterBytes,
		FailAfterMiB:                   roundFloat(float64(r.opts.failAfterBytes)/(1024*1024), 3),
		InterruptedUploadSeconds:       interruptedSeconds,
		InterruptedUploadError:         interruptedErr.Error(),
		VisibleAfterFailure:            visibleAfterFailure,
		RetryUploadSeconds:             retrySeconds,
		FinalizeLayoutSeconds:          finalizeSeconds,
		DownloadSeconds:                downloadSeconds,
		DownloadMD5:                    downloadMD5,
		BeforeUsedCapacityBytes:        beforeInfo.Data.UsedCapacityBytes,
		AfterFailureUsedCapacityBytes:  afterFailureInfo.Data.UsedCapacityBytes,
		AfterFinalizeUsedCapacityBytes: afterFinalizeInfo.Data.UsedCapacityBytes,
		DeltaAfterFailureBytes:         deltaAfterFailure,
		DeltaRetryAndFinalizeBytes:     deltaRetryAndFinalize,
		DeltaTotalBytes:                deltaTotal,
		RunRoot:                        r.runRoot,
		BeforeDiscInfoPath:             beforeDiscInfoPath,
		AfterFailureDiscInfoPath:       afterFailureDiscInfoPath,
		AfterFinalizeDiscInfoPath:      afterFinalizeDiscInfoPath,
		FinalizeResponsePath:           finalizeOutputPath,
		DownloadPath:                   downloadPath,
		MemoryCSVPath:                  r.memoryCSVPath,
		MemorySummaryPath:              r.memorySummaryPath,
	}
	if err := writeJSONFile(r.summaryJSONPath, summary); err != nil {
		return err
	}

	r.logf("")
	r.logf("Interrupt-retry summary:")
	r.logf("  file size bytes             : %d", summary.FileSizeBytes)
	r.logf("  fail after bytes            : %d", summary.FailAfterBytes)
	r.logf("  before used bytes           : %d", summary.BeforeUsedCapacityBytes)
	r.logf("  after failure used bytes    : %d", summary.AfterFailureUsedCapacityBytes)
	r.logf("  after finalize used bytes   : %d", summary.AfterFinalizeUsedCapacityBytes)
	r.logf("  delta after failure bytes   : %d", summary.DeltaAfterFailureBytes)
	r.logf("  delta retry+finalize bytes  : %d", summary.DeltaRetryAndFinalizeBytes)
	r.logf("  total delta bytes           : %d", summary.DeltaTotalBytes)
	r.logf("  visible after failure       : %t", summary.VisibleAfterFailure)
	r.logf("  summary json                : %s", r.summaryJSONPath)
	return nil
}

func (r *runner) runFullFlow(bucket string, metrics map[string]*fileMetric, finalizeOutputPath string, testStart time.Time) (*runSummary, float64, string, error) {
	var snapshot fileSnapshot
	var err error
	if r.opts.remoteOnly {
		r.logf("[3/8] Building remote object snapshot from ListObjects...")
		snapshot, err = r.getRemoteObjectMap(bucket)
	} else {
		if r.opts.skipMD5Verify {
			r.logf("[3/8] Building local file snapshot without MD5...")
		} else {
			r.logf("[3/8] Building local file snapshot with MD5...")
		}
		snapshot, err = getLocalFileMap(r.opts.dataDir, !r.opts.skipMD5Verify)
	}
	if err != nil {
		return nil, 0, "", err
	}

	totalSourceBytes := int64(0)
	for _, item := range snapshot.Items {
		totalSourceBytes += item.Size
		metrics[item.RelativePath] = &fileMetric{
			RelativePath: item.RelativePath,
			FullName:     item.FullPath,
			SizeBytes:    item.Size,
			SizeMiB:      roundFloat(float64(item.Size)/(1024*1024), 3),
			ExpectedMD5:  item.MD5,
			Status:       "Pending",
		}
	}

	if r.opts.remoteOnly {
		r.logf("Remote object count: %d", len(snapshot.Items))
		r.logf("Remote total size: %s MiB", formatSizeMiB(totalSourceBytes))
	} else {
		r.logf("Local file count: %d", len(snapshot.Items))
		r.logf("Local total size: %s MiB", formatSizeMiB(totalSourceBytes))
	}

	if r.opts.remoteOnly {
		r.logf("[4/8] Uploading directory %s ... skipped (remote-only mode)", r.opts.dataDir)
	} else if r.opts.skipUpload {
		r.logf("[4/8] Uploading directory %s ... skipped", r.opts.dataDir)
	} else {
		r.logf("[4/8] Uploading directory %s ...", r.opts.dataDir)
		for _, item := range snapshot.Items {
			start := time.Now()
			if err := r.putObject(bucket, item.RelativePath, item.FullPath); err != nil {
				return nil, 0, "", err
			}
			seconds := roundSeconds(time.Since(start))
			rate := rateMiBPerSecond(item.Size, seconds)
			metric := metrics[item.RelativePath]
			metric.UploadSeconds = seconds
			metric.UploadMiBPerSecond = rate
			metric.Status = "Uploaded"
			r.logf("  upload %s | size=%s MiB | elapsed=%.3fs | rate=%.3f MiB/s", item.RelativePath, formatSizeMiB(item.Size), seconds, rate)
		}
	}

	r.logf("[5/8] Validating ListObjects metadata view...")
	remoteItems, err := r.listAllObjects(bucket)
	if err != nil {
		return nil, 0, "", err
	}
	if r.opts.remoteOnly {
		r.logf("Expected object count: %d", len(snapshot.Items))
	} else {
		r.logf("Local file count : %d", len(snapshot.Items))
	}
	r.logf("Remote object count: %d", len(remoteItems))
	if len(remoteItems) < len(snapshot.Items) {
		if r.opts.remoteOnly {
			return nil, 0, "", errors.New("ListObjects returned fewer objects than remote snapshot")
		}
		return nil, 0, "", errors.New("ListObjects returned fewer objects than uploaded set")
	}

	remoteByKey := make(map[string]sourceItem, len(remoteItems))
	for _, item := range remoteItems {
		remoteByKey[item.RelativePath] = item
	}
	for _, item := range snapshot.Items {
		if _, ok := remoteByKey[item.RelativePath]; !ok {
			exists, existsErr := r.objectExists(bucket, item.RelativePath)
			if existsErr != nil {
				return nil, 0, "", existsErr
			}
			if exists {
				r.logf("  ListObjects key compare mismatch tolerated; HeadObject succeeded for %s", item.RelativePath)
				continue
			}
			return nil, 0, "", fmt.Errorf("uploaded object missing from ListObjects and HeadObject: %s", item.RelativePath)
		}
	}

	r.logf("[6/8] Validating HeadObject size metadata...")
	for _, item := range snapshot.Items {
		head, err := r.headObject(bucket, item.RelativePath, defaultReadTimeout)
		if err != nil {
			return nil, 0, "", err
		}
		remoteSize := derefInt64(head.ContentLength)
		if r.opts.remoteOnly {
			r.logf("  %s : expected=%d remote=%d", item.RelativePath, item.Size, remoteSize)
		} else {
			r.logf("  %s : local=%d remote=%d", item.RelativePath, item.Size, remoteSize)
		}
		if item.Size != remoteSize {
			return nil, 0, "", fmt.Errorf("HeadObject size mismatch for %s", item.RelativePath)
		}
	}

	finalizeDuration := 0.0
	if r.opts.skipFinalize {
		r.logf("[7/8] Triggering FinalizeLayout ... skipped")
	} else {
		r.logf("[7/8] Triggering FinalizeLayout ...")
		start := time.Now()
		controlKey, raw, err := r.downloadControlObject(bucket, "finalize-layout", finalizeOutputPath)
		if err != nil {
			return nil, 0, "", err
		}
		finalizeDuration = roundSeconds(time.Since(start))
		r.logf("FinalizeLayout control key: %s", controlKey)
		r.logf("FinalizeLayout response saved to: %s", finalizeOutputPath)
		r.logf("FinalizeLayout elapsed: %.3fs", finalizeDuration)
		r.logf("FinalizeLayout summary:")
		r.writeFinalizeSummary(raw, bucket)
	}

	bucketVerifyDir := filepath.Join(r.cfg.verifyDownloadRoot, bucket)
	if err := cleanDir(bucketVerifyDir); err != nil {
		return nil, 0, "", err
	}
	if r.opts.skipMD5Verify {
		r.logf("[8/8] Downloading uploaded objects without MD5 verification...")
	} else {
		r.logf("[8/8] Downloading uploaded objects and verifying MD5...")
	}

	keys := make([]string, 0, len(snapshot.Map))
	for key := range snapshot.Map {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	for _, key := range keys {
		expected := snapshot.Map[key]
		targetPath := filepath.Join(bucketVerifyDir, filepath.FromSlash(key))
		if err := ensureDir(filepath.Dir(targetPath)); err != nil {
			return nil, 0, "", err
		}
		start := time.Now()
		if err := r.downloadObjectToFile(bucket, key, targetPath, defaultReadTimeout); err != nil {
			return nil, 0, "", err
		}
		seconds := roundSeconds(time.Since(start))

		info, err := os.Stat(targetPath)
		if err != nil {
			return nil, 0, "", fmt.Errorf("downloaded file missing: %s", key)
		}
		if info.Size() != expected.Size {
			return nil, 0, "", fmt.Errorf("downloaded size mismatch for %s", key)
		}

		actualMD5 := ""
		if !r.opts.skipMD5Verify && !r.opts.remoteOnly {
			actualMD5, err = computeMD5(targetPath)
			if err != nil {
				return nil, 0, "", err
			}
			if actualMD5 != expected.MD5 {
				return nil, 0, "", fmt.Errorf("downloaded MD5 mismatch for %s", key)
			}
		}

		rate := rateMiBPerSecond(expected.Size, seconds)
		metric := metrics[key]
		metric.DownloadSeconds = seconds
		metric.DownloadMiBPerSecond = rate
		metric.ActualMD5 = actualMD5
		metric.Status = "Verified"

		if r.opts.skipMD5Verify || r.opts.remoteOnly {
			r.logf("  downloaded %s | size=%s MiB | download=%.3fs | rate=%.3f MiB/s", key, formatSizeMiB(expected.Size), seconds, rate)
		} else {
			r.logf("  verified %s | size=%s MiB | download=%.3fs | rate=%.3f MiB/s | md5=%s", key, formatSizeMiB(expected.Size), seconds, rate, actualMD5)
		}
	}

	totalTestSeconds := roundSeconds(time.Since(testStart))
	uploadSeconds := 0.0
	downloadSeconds := 0.0
	for _, metric := range metrics {
		uploadSeconds += metric.UploadSeconds
		downloadSeconds += metric.DownloadSeconds
	}

	summary := &runSummary{
		Status:                  "Success",
		Bucket:                  bucket,
		RequestedBucket:         r.opts.bucket,
		DataDirectory:           r.opts.dataDir,
		RemoteOnly:              r.opts.remoteOnly,
		FileCount:               len(snapshot.Items),
		TotalBytes:              totalSourceBytes,
		TotalMiB:                roundFloat(float64(totalSourceBytes)/(1024*1024), 3),
		UploadSeconds:           roundFloat(uploadSeconds, 3),
		UploadMiBPerSecond:      rateMiBPerSecond(totalSourceBytes, uploadSeconds),
		FinalizeLayoutSeconds:   roundFloat(finalizeDuration, 3),
		DownloadSeconds:         roundFloat(downloadSeconds, 3),
		DownloadMiBPerSecond:    rateMiBPerSecond(totalSourceBytes, downloadSeconds),
		TotalTestSeconds:        totalTestSeconds,
		RunRoot:                 r.runRoot,
		MetricsCSVPath:          r.metricsCSVPath,
		MemoryCSVPath:           r.memoryCSVPath,
		MemorySummaryPath:       r.memorySummaryPath,
		FinalizeResponsePath:    finalizeOutputPath,
		VerifyDownloadDirectory: bucketVerifyDir,
		RecorderLogDirectory:    r.cfg.runtimeRecorderLogDir,
		GatewayLogDirectory:     r.cfg.runtimeGatewayLogDir,
	}
	return summary, finalizeDuration, bucketVerifyDir, nil
}

func loadResolvedConfig(opts cliOptions) (resolvedConfig, error) {
	cfgPath := resolveConfigPath(opts.configPath)
	resolvedCfgPath := cfgPath
	if abs, err := filepath.Abs(cfgPath); err == nil {
		resolvedCfgPath = abs
	}

	raw := toolConfig{}
	if content, err := os.ReadFile(resolvedCfgPath); err == nil {
		content = trimUTF8BOM(content)
		if err := json.Unmarshal(content, &raw); err != nil {
			return resolvedConfig{}, fmt.Errorf("parse config %s: %w", resolvedCfgPath, err)
		}
	}

	configDir := filepath.Dir(resolvedCfgPath)
	publisherRoot := filepath.Dir(configDir)
	logRoot := filepath.Join(publisherRoot, "logs")
	testRunRoot := filepath.Join(logRoot, "test-runs")
	verifyDownloadRoot := filepath.Join(publisherRoot, "verify-download")

	if err := ensureDir(testRunRoot); err != nil {
		return resolvedConfig{}, err
	}
	if err := ensureDir(verifyDownloadRoot); err != nil {
		return resolvedConfig{}, err
	}

	endpoint := strings.TrimSpace(os.Getenv("AWS_ENDPOINT_URL"))
	if endpoint == "" {
		endpoint = strings.TrimSpace(raw.OpticalArchive.Testing.S3Endpoint)
	}
	if endpoint == "" {
		endpoint = strings.TrimSpace(raw.OpticalArchive.Gateway.S3Endpoint)
	}
	if endpoint == "" {
		endpoint = defaultEndpoint
	}

	region := strings.TrimSpace(os.Getenv("AWS_REGION"))
	if region == "" {
		region = strings.TrimSpace(raw.OpticalArchive.Testing.S3Region)
	}
	if region == "" {
		region = strings.TrimSpace(raw.OpticalArchive.Gateway.S3Region)
	}
	if region == "" {
		region = defaultRegion
	}

	accessKey := strings.TrimSpace(os.Getenv("AWS_ACCESS_KEY_ID"))
	if accessKey == "" {
		accessKey = strings.TrimSpace(raw.OpticalArchive.Testing.S3AccessKeyID)
	}
	if accessKey == "" {
		accessKey = strings.TrimSpace(raw.OpticalArchive.Testing.AWSAccessKeyID)
	}
	if accessKey == "" {
		accessKey = defaultAccessKey
	}

	secretKey := strings.TrimSpace(os.Getenv("AWS_SECRET_ACCESS_KEY"))
	if secretKey == "" {
		secretKey = strings.TrimSpace(raw.OpticalArchive.Testing.S3SecretAccessKey)
	}
	if secretKey == "" {
		secretKey = strings.TrimSpace(raw.OpticalArchive.Testing.AWSSecretAccessKey)
	}
	if secretKey == "" {
		secretKey = defaultSecretKey
	}

	readTimeoutSeconds := parseEnvInt("AWS_CLI_READ_TIMEOUT_SECONDS", defaultReadTimeout)
	connectTimeoutSeconds := parseEnvInt("AWS_CLI_CONNECT_TIMEOUT_SECONDS", defaultConnectTimeout)

	return resolvedConfig{
		configPath:            resolvedCfgPath,
		endpoint:              endpoint,
		region:                region,
		accessKey:             accessKey,
		secretKey:             secretKey,
		publisherRoot:         publisherRoot,
		logRoot:               logRoot,
		testRunRoot:           testRunRoot,
		verifyDownloadRoot:    verifyDownloadRoot,
		runtimeGatewayLogDir:  filepath.Join(logRoot, "gateway"),
		runtimeRecorderLogDir: filepath.Join(logRoot, "recorder"),
		awsReadTimeout:        time.Duration(readTimeoutSeconds) * time.Second,
		awsConnectTimeout:     time.Duration(connectTimeoutSeconds) * time.Second,
		finalizeReadTimeout:   time.Duration(defaultFinalizeTimeout) * time.Second,
	}, nil
}

func newS3Client(cfg resolvedConfig, profile string) (*s3.Client, error) {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.DialContext = (&net.Dialer{
		Timeout:   cfg.awsConnectTimeout,
		KeepAlive: 30 * time.Second,
	}).DialContext
	transport.MaxIdleConns = 64
	transport.MaxIdleConnsPerHost = 64
	transport.IdleConnTimeout = 90 * time.Second

	loadOptions := []func(*awsconfig.LoadOptions) error{
		awsconfig.WithRegion(cfg.region),
		awsconfig.WithHTTPClient(&http.Client{Transport: transport}),
		awsconfig.WithRetryMaxAttempts(1),
	}

	if strings.TrimSpace(profile) != "" {
		loadOptions = append(loadOptions, awsconfig.WithSharedConfigProfile(profile))
	} else {
		loadOptions = append(loadOptions,
			awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(cfg.accessKey, cfg.secretKey, "")))
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(context.Background(), loadOptions...)
	if err != nil {
		return nil, err
	}
	// For local burnbridge HTTP testing we keep request checksums opt-in only.
	// This allows true streaming/unseekable bodies so interrupt-retry can fail mid-upload
	// instead of being rejected by the AWS SDK before any bytes are sent.
	awsCfg.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
	awsCfg.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenRequired

	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.BaseEndpoint = &cfg.endpoint
		o.UsePathStyle = true
		o.DisableLogOutputChecksumValidationSkipped = true
	})
	return client, nil
}

func buildRunRoot(root, bucket string) (string, error) {
	timestamp := time.Now().Format("20060102-150405-000")
	safeBucket := sanitizeBucketForPath(bucket)
	return filepath.Join(root, fmt.Sprintf("%s-%s-%s", timestamp, uuid.NewString()[:8], safeBucket)), nil
}

func sanitizeBucketForPath(bucket string) string {
	var builder strings.Builder
	for _, ch := range bucket {
		switch {
		case ch >= 'a' && ch <= 'z':
			builder.WriteRune(ch)
		case ch >= 'A' && ch <= 'Z':
			builder.WriteRune(ch)
		case ch >= '0' && ch <= '9':
			builder.WriteRune(ch)
		case ch == '.' || ch == '_' || ch == '-':
			builder.WriteRune(ch)
		default:
			builder.WriteByte('_')
		}
	}
	value := strings.Trim(builder.String(), "_")
	if value == "" {
		return "bucket"
	}
	return value
}

func (r *runner) listBuckets(timeoutSeconds int) ([]types.Bucket, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeoutSeconds)*time.Second)
	defer cancel()

	out, err := r.client.ListBuckets(ctx, &s3.ListBucketsInput{})
	if err != nil {
		return nil, err
	}
	return out.Buckets, nil
}

func (r *runner) bucketExists(bucket string) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), r.cfg.awsConnectTimeout)
	defer cancel()
	_, err := r.client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: &bucket})
	if err == nil {
		return true, nil
	}
	if isNotFoundLikeError(err) {
		return false, nil
	}
	return false, err
}

func (r *runner) createBucket(bucket string) error {
	ctx, cancel := context.WithTimeout(context.Background(), r.cfg.awsReadTimeout)
	defer cancel()
	input := &s3.CreateBucketInput{Bucket: &bucket}
	if region := strings.TrimSpace(r.cfg.region); region != "" && !strings.EqualFold(region, "us-east-1") {
		input.CreateBucketConfiguration = &types.CreateBucketConfiguration{
			LocationConstraint: types.BucketLocationConstraint(region),
		}
	}
	_, err := r.client.CreateBucket(ctx, input)
	return err
}

func (r *runner) resolveActiveBucket(requestedBucket string, timeoutSeconds int) (string, string, []string, error) {
	deadline := time.Now().Add(time.Duration(timeoutSeconds) * time.Second)
	var lastBuckets []string
	for time.Now().Before(deadline) {
		exists, err := r.bucketExists(requestedBucket)
		if err != nil {
			return "", "", nil, err
		}
		if exists {
			return requestedBucket, "requested", lastBuckets, nil
		}

		buckets, err := r.listBuckets(defaultReadTimeout)
		if err != nil {
			return "", "", nil, err
		}
		lastBuckets = lastBuckets[:0]
		for _, bucket := range buckets {
			name := strings.TrimSpace(awsString(bucket.Name))
			if name != "" {
				lastBuckets = append(lastBuckets, name)
			}
		}
		if len(lastBuckets) == 1 {
			return lastBuckets[0], "single-visible", lastBuckets, nil
		}

		time.Sleep(defaultBucketReadyPollSeconds * time.Second)
	}
	return "", "unavailable", lastBuckets, nil
}

func (r *runner) ensureBucketReady(bucket string, timeoutSeconds int) (bool, error) {
	deadline := time.Now().Add(time.Duration(timeoutSeconds) * time.Second)
	for time.Now().Before(deadline) {
		exists, err := r.bucketExists(bucket)
		if err != nil {
			return false, err
		}
		if exists {
			return true, nil
		}
		time.Sleep(defaultBucketReadyPollSeconds * time.Second)
	}
	return false, nil
}

func getLocalFileMap(root string, computeHashes bool) (fileSnapshot, error) {
	resolvedRoot, err := filepath.Abs(root)
	if err != nil {
		return fileSnapshot{}, err
	}

	items := make([]sourceItem, 0)
	err = filepath.WalkDir(resolvedRoot, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}

		info, err := entry.Info()
		if err != nil {
			return err
		}

		key, err := getRelativeS3Key(resolvedRoot, path)
		if err != nil {
			return err
		}
		item := sourceItem{
			RelativePath: key,
			FullPath:     path,
			Size:         info.Size(),
		}
		if computeHashes {
			item.MD5, err = computeMD5(path)
			if err != nil {
				return err
			}
		}
		items = append(items, item)
		return nil
	})
	if err != nil {
		return fileSnapshot{}, err
	}

	sort.Slice(items, func(i, j int) bool { return items[i].FullPath < items[j].FullPath })
	itemMap := make(map[string]sourceItem, len(items))
	for _, item := range items {
		itemMap[item.RelativePath] = item
	}
	return fileSnapshot{Root: resolvedRoot, Items: items, Map: itemMap}, nil
}

func buildSingleFileSource(path string) (sourceItem, error) {
	fullPath, err := filepath.Abs(path)
	if err != nil {
		return sourceItem{}, err
	}
	info, err := os.Stat(fullPath)
	if err != nil {
		return sourceItem{}, err
	}
	if info.IsDir() {
		return sourceItem{}, fmt.Errorf("expected file path, got directory: %s", fullPath)
	}
	md5Value, err := computeMD5(fullPath)
	if err != nil {
		return sourceItem{}, err
	}
	return sourceItem{
		RelativePath: filepath.Base(fullPath),
		FullPath:     fullPath,
		Size:         info.Size(),
		MD5:          md5Value,
	}, nil
}

func (r *runner) getRemoteObjectMap(bucket string) (fileSnapshot, error) {
	items, err := r.listAllObjects(bucket)
	if err != nil {
		return fileSnapshot{}, err
	}
	itemMap := make(map[string]sourceItem, len(items))
	for _, item := range items {
		itemMap[item.RelativePath] = item
	}
	return fileSnapshot{Items: items, Map: itemMap}, nil
}

func (r *runner) listAllObjects(bucket string) ([]sourceItem, error) {
	ctx, cancel := context.WithTimeout(context.Background(), r.cfg.awsReadTimeout)
	defer cancel()

	paginator := s3.NewListObjectsV2Paginator(r.client, &s3.ListObjectsV2Input{Bucket: &bucket})
	items := make([]sourceItem, 0)
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		for _, object := range page.Contents {
			key := strings.TrimSpace(awsString(object.Key))
			if key == "" {
				continue
			}
			items = append(items, sourceItem{
				RelativePath: key,
				Size:         derefInt64(object.Size),
			})
		}
	}
	sort.Slice(items, func(i, j int) bool { return items[i].RelativePath < items[j].RelativePath })
	return items, nil
}

func (r *runner) fetchDiscInfo(bucket, outputPath string) (*discInfoDocument, error) {
	_, raw, err := r.downloadControlObject(bucket, "disc-info", outputPath)
	if err != nil {
		return nil, err
	}
	var doc discInfoDocument
	if err := json.Unmarshal(trimUTF8BOM(raw), &doc); err != nil {
		return nil, err
	}
	return &doc, nil
}

func resolveSingleObjectOutputPath(verifyRoot, bucket, objectKey, outputPath string) string {
	if strings.TrimSpace(outputPath) != "" {
		return outputPath
	}
	return filepath.Join(verifyRoot, bucket, filepath.FromSlash(objectKey))
}

func (r *runner) putObject(bucket, key, fullPath string) error {
	file, err := os.Open(fullPath)
	if err != nil {
		return err
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), r.cfg.awsReadTimeout)
	defer cancel()
	_, err = r.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        &bucket,
		Key:           &key,
		Body:          file,
		ContentLength: int64Ptr(info.Size()),
	})
	return err
}

func (r *runner) putObjectWithSimulatedDisconnect(bucket, key, fullPath string, failAfterBytes int64) error {
	file, err := os.Open(fullPath)
	if err != nil {
		return err
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		return err
	}

	reader := &failingReader{
		source:    file,
		failAfter: failAfterBytes,
		failErr:   errSimulatedDisconnect,
	}

	ctx, cancel := context.WithTimeout(context.Background(), r.cfg.awsReadTimeout)
	defer cancel()
	_, err = r.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        &bucket,
		Key:           &key,
		Body:          reader,
		ContentLength: int64Ptr(info.Size()),
	}, func(o *s3.Options) {
		o.APIOptions = append(o.APIOptions, awsv4.SwapComputePayloadSHA256ForUnsignedPayloadMiddleware)
		o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
	})
	return err
}

func (r *runner) headObject(bucket, key string, timeoutSeconds int) (*s3.HeadObjectOutput, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeoutSeconds)*time.Second)
	defer cancel()
	return r.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: &bucket,
		Key:    &key,
	})
}

func (r *runner) objectExists(bucket, key string) (bool, error) {
	_, err := r.headObject(bucket, key, defaultReadTimeout)
	if err == nil {
		return true, nil
	}
	if isNotFoundLikeError(err) {
		return false, nil
	}
	return false, nil
}

func (r *runner) downloadObjectToFile(bucket, key, outputPath string, timeoutSeconds int) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeoutSeconds)*time.Second)
	defer cancel()

	resp, err := r.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &bucket,
		Key:    &key,
	})
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	file, err := os.Create(outputPath)
	if err != nil {
		return err
	}
	defer file.Close()

	if _, err := io.Copy(file, resp.Body); err != nil {
		return err
	}
	return nil
}

func (r *runner) downloadControlObject(bucket, action, outputPath string) (string, []byte, error) {
	controlKey := fmt.Sprintf(".__bbctl__/v1/%s/%d/%s", action, time.Now().UTC().UnixMilli(), uuid.NewString())
	if err := r.downloadObjectToFile(bucket, controlKey, outputPath, defaultFinalizeTimeout); err != nil {
		return "", nil, err
	}
	raw, err := os.ReadFile(outputPath)
	if err != nil {
		return "", nil, err
	}
	return controlKey, raw, nil
}

func (r *runner) writeControlJSONLog(raw []byte) {
	payload, err := parseJSONDocument(raw)
	if err != nil {
		r.logf("%s", strings.TrimSpace(string(raw)))
		return
	}

	keys := make([]string, 0, len(payload))
	for key := range payload {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	for _, key := range keys {
		value := payload[key]
		if key == "data" || key == "error" {
			object, ok := value.(map[string]any)
			if !ok || len(object) == 0 {
				continue
			}
			r.logf("  %s:", key)
			innerKeys := make([]string, 0, len(object))
			for innerKey := range object {
				innerKeys = append(innerKeys, innerKey)
			}
			sort.Strings(innerKeys)
			for _, innerKey := range innerKeys {
				r.logf("    %s : %v", innerKey, object[innerKey])
			}
			continue
		}
		r.logf("  %s : %v", key, value)
	}
}

func (r *runner) writeFinalizeSummary(raw []byte, fallbackBucket string) {
	doc, err := parseJSONDocument(raw)
	if err != nil {
		r.logf("%s", strings.TrimSpace(string(raw)))
		return
	}

	data, _ := doc["data"].(map[string]any)
	errorMap, _ := doc["error"].(map[string]any)

	status := firstNonEmpty(anyString(data["recorderStatus"]), anyString(data["status"]), "<unknown>")
	message := firstNonEmpty(anyString(data["recorderMessage"]), anyString(data["message"]), anyString(errorMap["message"]))
	bucket := firstNonEmpty(anyString(doc["bucket"]), fallbackBucket)
	grpcOk := firstNonEmpty(anyString(data["grpcOk"]), anyString(doc["ok"]), "<unknown>")
	completedAt := firstNonEmpty(anyString(data["completedAtUtc"]), anyString(data["completedAt"]))
	closeDisc := firstNonEmpty(anyString(data["closeDisc"]), "false")
	grpcCode := firstNonEmpty(anyString(data["grpcCode"]), anyString(errorMap["code"]))

	r.logf("  bucket       : %s", bucket)
	r.logf("  status       : %s", status)
	r.logf("  message      : %s", message)
	r.logf("  grpcOk       : %s", grpcOk)
	if grpcCode != "" {
		r.logf("  grpcCode     : %s", grpcCode)
	}
	r.logf("  closeDisc    : %s", closeDisc)
	if completedAt != "" {
		r.logf("  completedAt  : %s", completedAt)
	}
}

func (r *runner) writeMemorySummary() error {
	rows, err := readCSVRows(r.memoryCSVPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	if len(rows) <= 1 {
		return nil
	}

	aggregates := map[string]*memoryAggregate{}
	for _, row := range rows[1:] {
		if len(row) < 8 {
			continue
		}
		name := strings.TrimSpace(row[1])
		if name == "" {
			continue
		}
		entry := aggregates[name]
		if entry == nil {
			entry = &memoryAggregate{name: name, firstSeen: row[0]}
			aggregates[name] = entry
		}
		entry.lastSeen = row[0]
		entry.maxWorkingSet = maxUint64(entry.maxWorkingSet, parseUint64(row[3]))
		entry.maxPrivate = maxUint64(entry.maxPrivate, parseUint64(row[4]))
		entry.maxPaged = maxUint64(entry.maxPaged, parseUint64(row[5]))
	}

	names := make([]string, 0, len(aggregates))
	for name := range aggregates {
		names = append(names, name)
	}
	sort.Strings(names)

	lines := []string{
		"Memory usage summary",
		"====================",
	}
	for _, name := range names {
		entry := aggregates[name]
		lines = append(lines, fmt.Sprintf("%s: peak working set=%s, peak private=%s, peak paged=%s, first=%s, last=%s",
			entry.name,
			formatProcessMemoryValue(entry.maxWorkingSet),
			formatProcessMemoryValue(entry.maxPrivate),
			formatProcessMemoryValue(entry.maxPaged),
			entry.firstSeen,
			entry.lastSeen))
	}

	if err := os.WriteFile(r.memorySummaryPath, []byte(strings.Join(lines, "\n")+"\n"), 0o644); err != nil {
		return err
	}
	for _, line := range lines {
		r.logf("%s", line)
	}
	return nil
}

func (r *runner) logf(format string, args ...any) {
	r.logger.Printf(format, args...)
}

func (l *runLogger) Printf(format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()

	message := fmt.Sprintf(format, args...)
	fmt.Println(message)
	if strings.TrimSpace(l.path) == "" {
		return
	}
	timestamp := time.Now().Format("2006-01-02 15:04:05.000")
	line := fmt.Sprintf("[%s] %s\n", timestamp, message)
	_ = os.MkdirAll(filepath.Dir(l.path), 0o755)
	file, err := os.OpenFile(l.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return
	}
	defer file.Close()
	_, _ = file.WriteString(line)
}

func startMemorySampler(csvPath string, intervalSeconds int) (*memorySampler, error) {
	if err := ensureDir(filepath.Dir(csvPath)); err != nil {
		return nil, err
	}
	header := "TimestampUtc,ProcessName,Id,WorkingSetBytes,PrivateBytes,PagedBytes,HandleCount,ThreadCount\n"
	if err := os.WriteFile(csvPath, []byte(header), 0o644); err != nil {
		return nil, err
	}

	if runtimeGOOS() != "windows" {
		return nil, errors.New("memory sampler currently supports Windows only")
	}

	tempScript := filepath.Join(os.TempDir(), fmt.Sprintf("oa-memory-sampler-%s.ps1", uuid.NewString()))
	script := `[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)][string]$Path,
    [Parameter(Mandatory = $true)][int]$IntervalSeconds
)
$ErrorActionPreference = 'Continue'
while ($true) {
    $timestamp = [DateTime]::UtcNow.ToString('o')
    $processes = Get-Process BurnServer,versitygw -ErrorAction SilentlyContinue
    foreach ($proc in @($processes)) {
        $line = '{0},{1},{2},{3},{4},{5},{6},{7}' -f $timestamp,$proc.ProcessName,$proc.Id,$proc.WorkingSet64,$proc.PrivateMemorySize64,$proc.PagedMemorySize64,$proc.HandleCount,$proc.Threads.Count
        [System.IO.File]::AppendAllText($Path, $line + [Environment]::NewLine)
    }
    Start-Sleep -Seconds $IntervalSeconds
}`
	if err := os.WriteFile(tempScript, []byte(script), 0o644); err != nil {
		return nil, err
	}

	cmd := exec.Command("powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", tempScript, "-Path", csvPath, "-IntervalSeconds", strconv.Itoa(intervalSeconds))
	if err := cmd.Start(); err != nil {
		_ = os.Remove(tempScript)
		return nil, err
	}
	return &memorySampler{scriptPath: tempScript, cmd: cmd}, nil
}

func (s *memorySampler) stop() error {
	if s == nil || s.cmd == nil || s.cmd.Process == nil {
		return nil
	}
	_ = s.cmd.Process.Kill()
	_, _ = s.cmd.Process.Wait()
	if s.scriptPath != "" {
		_ = os.Remove(s.scriptPath)
	}
	return nil
}

func exportFileMetrics(path string, metrics map[string]*fileMetric) error {
	if strings.TrimSpace(path) == "" {
		return nil
	}
	if err := ensureDir(filepath.Dir(path)); err != nil {
		return err
	}

	keys := make([]string, 0, len(metrics))
	for key := range metrics {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()
	header := []string{
		"RelativePath",
		"FullName",
		"SizeBytes",
		"SizeMiB",
		"ExpectedMD5",
		"UploadSeconds",
		"UploadMiBPerSecond",
		"DownloadSeconds",
		"DownloadMiBPerSecond",
		"ActualMD5",
		"Status",
		"Notes",
	}
	if err := writer.Write(header); err != nil {
		return err
	}

	for _, key := range keys {
		metric := metrics[key]
		row := []string{
			metric.RelativePath,
			metric.FullName,
			strconv.FormatInt(metric.SizeBytes, 10),
			strconv.FormatFloat(metric.SizeMiB, 'f', 3, 64),
			metric.ExpectedMD5,
			strconv.FormatFloat(metric.UploadSeconds, 'f', 3, 64),
			strconv.FormatFloat(metric.UploadMiBPerSecond, 'f', 3, 64),
			strconv.FormatFloat(metric.DownloadSeconds, 'f', 3, 64),
			strconv.FormatFloat(metric.DownloadMiBPerSecond, 'f', 3, 64),
			metric.ActualMD5,
			metric.Status,
			metric.Notes,
		}
		if err := writer.Write(row); err != nil {
			return err
		}
	}
	return writer.Error()
}

func readCSVRows(path string) ([][]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	reader := csv.NewReader(file)
	reader.FieldsPerRecord = -1
	return reader.ReadAll()
}

func copyRuntimeLogs(runRoot, gatewayDir, recorderDir string) error {
	pairs := []struct {
		source string
		target string
	}{
		{source: gatewayDir, target: filepath.Join(runRoot, "gateway-logs")},
		{source: recorderDir, target: filepath.Join(runRoot, "recorder-logs")},
	}

	for _, pair := range pairs {
		if strings.TrimSpace(pair.source) == "" {
			continue
		}
		entries, err := os.ReadDir(pair.source)
		if err != nil {
			continue
		}
		if err := ensureDir(pair.target); err != nil {
			return err
		}
		for _, entry := range entries {
			if entry.IsDir() {
				continue
			}
			sourcePath := filepath.Join(pair.source, entry.Name())
			targetPath := filepath.Join(pair.target, entry.Name())
			if err := copyFile(sourcePath, targetPath); err != nil {
				return err
			}
		}
	}
	return nil
}

func copyFile(sourcePath, targetPath string) error {
	source, err := os.Open(sourcePath)
	if err != nil {
		return err
	}
	defer source.Close()

	target, err := os.Create(targetPath)
	if err != nil {
		return err
	}
	defer target.Close()

	if _, err := io.Copy(target, source); err != nil {
		return err
	}
	return nil
}

func writeJSONFile(path string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(data, '\n'), 0o644)
}

func trimUTF8BOM(content []byte) []byte {
	if len(content) >= 3 && content[0] == 0xEF && content[1] == 0xBB && content[2] == 0xBF {
		return content[3:]
	}
	return content
}

func cleanDir(path string) error {
	if err := os.RemoveAll(path); err != nil {
		return err
	}
	return ensureDir(path)
}

func ensureDir(path string) error {
	return os.MkdirAll(path, 0o755)
}

func getRelativeS3Key(root, fullPath string) (string, error) {
	rel, err := filepath.Rel(root, fullPath)
	if err != nil {
		return "", err
	}
	return filepath.ToSlash(rel), nil
}

func computeMD5(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()

	hash := md5.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func formatSizeMiB(bytes int64) string {
	return fmt.Sprintf("%.2f", float64(bytes)/(1024*1024))
}

func rateMiBPerSecond(bytes int64, seconds float64) float64 {
	if seconds <= 0 {
		return 0
	}
	return roundFloat((float64(bytes)/(1024*1024))/seconds, 3)
}

func roundSeconds(duration time.Duration) float64 {
	return roundFloat(duration.Seconds(), 3)
}

func roundFloat(value float64, digits int) float64 {
	format := "%." + strconv.Itoa(digits) + "f"
	rounded, _ := strconv.ParseFloat(fmt.Sprintf(format, value), 64)
	return rounded
}

func parseEnvInt(name string, fallback int) int {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func parseUint64(value string) uint64 {
	parsed, _ := strconv.ParseUint(strings.TrimSpace(value), 10, 64)
	return parsed
}

func formatProcessMemoryValue(value uint64) string {
	if value == 0 {
		return "n/a"
	}
	return fmt.Sprintf("%.2f MiB", float64(value)/(1024*1024))
}

func anyString(value any) string {
	if value == nil {
		return ""
	}

	switch typed := value.(type) {
	case string:
		return strings.TrimSpace(typed)
	case bool:
		return strconv.FormatBool(typed)
	case float64:
		if typed == float64(int64(typed)) {
			return strconv.FormatInt(int64(typed), 10)
		}
		return strconv.FormatFloat(typed, 'f', -1, 64)
	case json.Number:
		return typed.String()
	default:
		return strings.TrimSpace(fmt.Sprintf("%v", value))
	}
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func isNotFoundLikeError(err error) bool {
	if err == nil {
		return false
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		code := strings.TrimSpace(apiErr.ErrorCode())
		switch code {
		case "NotFound", "NoSuchBucket", "NoSuchKey", "404":
			return true
		}
	}
	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		return false
	}
	text := strings.ToLower(err.Error())
	return strings.Contains(text, "not found") || strings.Contains(text, "status code: 404")
}

func awsString(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func maxUint64(current, next uint64) uint64 {
	if next > current {
		return next
	}
	return current
}

func runtimeGOOS() string {
	return runtime.GOOS
}

func derefInt64(value *int64) int64 {
	if value == nil {
		return 0
	}
	return *value
}

func int64Ptr(value int64) *int64 {
	return &value
}

func (r *failingReader) Read(p []byte) (int, error) {
	if r.failedOnce {
		return 0, r.failErr
	}
	if r.failAfter <= 0 {
		r.failedOnce = true
		return 0, r.failErr
	}

	remaining := r.failAfter - r.readBytes
	if remaining <= 0 {
		r.failedOnce = true
		return 0, r.failErr
	}
	if int64(len(p)) > remaining {
		p = p[:remaining]
	}

	n, err := r.source.Read(p)
	r.readBytes += int64(n)
	if err != nil {
		return n, err
	}
	if r.readBytes >= r.failAfter {
		r.failedOnce = true
		return n, r.failErr
	}
	return n, nil
}

func parseJSONDocument(raw []byte) (map[string]any, error) {
	var payload map[string]any
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&payload); err != nil {
		return nil, err
	}
	return payload, nil
}

// Copyright 2026 Versity Software
// This file is licensed under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package burnbridge

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"io/fs"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing/fstest"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/versity/versitygw/archiveconfig"
	"github.com/versity/versitygw/auth"
	"github.com/versity/versitygw/backend"
	burnbridgev1 "github.com/versity/versitygw/backend/burnbridge/proto"
	meta "github.com/versity/versitygw/backend/meta"
	"github.com/versity/versitygw/s3err"
	"github.com/versity/versitygw/s3response"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Options struct {
	DBPath string

	// ReadMountPath, if non-empty, is the filesystem root where burned objects appear as {ReadMountPath}/{bucket}/{key}
	// after the optical volume has been finalized and mounted (or refreshed). Until then, files may be missing while
	// SQLite metadata already exists: HeadObject still succeeds using metadata; GetObject streams bytes via gRPC ReadObject
	// while the file is missing from the read mount, or returns 503 if the recorder does not implement ReadObject.
	ReadMountPath string

	GRPCAddr               string
	GRPCUseTLS             bool
	GRPCCAFile             string
	GRPCServerName         string
	GRPCInsecureSkipVerify bool
	GRPCSkipPing           bool

	UDFVolumeLabel string
	ChunkSize      int

	// DialTimeout caps the whole dial+ready-wait (+ optional ping) phase in New.
	DialTimeout time.Duration
	// GRPCReadyTimeout is how long to wait for the client conn to reach Ready after NewClient.
	GRPCReadyTimeout time.Duration
	// PingTimeout is the deadline for the startup GetJobStatus probe.
	PingTimeout time.Duration
	// CancelJobTimeout is used for best-effort CancelJob after a failed PutObject.
	CancelJobTimeout time.Duration
	// PutObjectTimeout, if > 0, wraps the incoming PutObject context with an additional deadline
	// for CreateJob, streaming upload, CommitJob, and SQLite metadata writes (slow optical burn).
	// If zero, only the gateway/request context limits the call.
	PutObjectTimeout time.Duration

	// SQLiteMaintCtx enables periodic WAL checkpoint for flash deployments; cancelled when context ends.
	SQLiteMaintCtx context.Context

	// RecorderS3Pull: when EndpointURL is non-empty, after each CreateJob the gateway calls
	// RegisterS3ObjectPullSource so the recorder can GetObject from that S3-compatible endpoint.
	RecorderS3Endpoint        string
	RecorderS3Region          string
	RecorderS3AccessKey       string
	RecorderS3SecretKey       string
	RecorderS3SessionToken    string
	RecorderS3ForcePathStyle  bool
	RecorderS3PresignedGetURL string // optional; if set, recorder may prefer GET to this URL

	AllowCreateBucketBinding bool
	ManageRecorderProcessLocally bool
	RecorderServiceName          string
	RecorderProcessPattern       string
	RecorderStartCommand         string
	RecorderWorkingDirectory     string
	RecorderHealthCheckSeconds   int
}

// objectLockIndex maps (bucket,key) to a fixed shard; see objectLockShards in constants.go.
func objectLockIndex(bucket, key string) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(bucket))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(key))
	return int(h.Sum32()) % objectLockShards
}

// unlockOnCloseReadCloser releases an object lock when the S3 body is fully read/closed so PutObject cannot
// overlap an in-flight GetObject stream for the same key.
type unlockOnCloseReadCloser struct {
	r      io.ReadCloser
	unlock func()
}

func (u *unlockOnCloseReadCloser) Read(p []byte) (int, error) {
	if u.r == nil {
		return 0, io.EOF
	}
	return u.r.Read(p)
}

func (u *unlockOnCloseReadCloser) Close() error {
	var err error
	if u.r != nil {
		err = u.r.Close()
		u.r = nil
	}
	if u.unlock != nil {
		u.unlock()
		u.unlock = nil
	}
	return err
}

type BurnBridge struct {
	backend.BackendUnsupported
	meta             meta.SqlMeta
	grpc             burnbridgev1.BurnBridgeClient
	grpcConn         *grpc.ClientConn
	chunkSize        int
	udfLabel         string
	readMount        string
	cancelJobTimeout time.Duration
	putObjectTimeout time.Duration
	// putSerialMu ensures at most one PutObject (CreateJob…CommitJob) runs at a time across all keys,
	// matching a single recorder / drive that cannot burn two objects concurrently.
	putSerialMu sync.Mutex
	objectLocks [objectLockShards]sync.Mutex
	// putQueueSem limits total in-flight+waiting PutObject tasks. Requests above capacity block
	// until earlier tasks complete, so task issuance pressure stays bounded.
	putQueueSem chan struct{}

	recorderS3Endpoint        string
	recorderS3Region          string
	recorderS3AccessKey       string
	recorderS3SecretKey       string
	recorderS3SessionToken    string
	recorderS3PathStyle       bool
	recorderS3PresignedGetURL string

	// activeBucket is the S3 bucket name for this session, derived from the disc volume label at New().
	activeBucket   string
	volumeLabelRaw string
	allowBucketBinding bool
}

var _ backend.Backend = &BurnBridge{}

const (
	// emptyQuotedMD5 is the S3-style quoted ETag for zero-length payload.
	emptyQuotedMD5 = "\"d41d8cd98f00b204e9800998ecf8427e\""

	// burnbridgeDefaultContentType is the only Content-Type returned for committed objects (no per-object metadata in SQLite).
	burnbridgeDefaultContentType = "application/octet-stream"

	discInfoContentType = "application/json"

	// objectLockShards serializes GetObject (and per-key coordination with Put) for (bucket,key).
	objectLockShards = 256

	// burnbridgeUploadMaxDataPerFrame caps gRPC UploadObjectChunk.Data size (typical 4MiB recv limit).
	burnbridgeUploadMaxDataPerFrame = 3 << 20

	defaultChunkSizeBytes = 1 << 20

	listDefaultMaxKeys   int32 = 1000
	defaultPutQueueLimit       = 512
)

// burnbridgeWORMNoDelete is returned for delete operations on WORM optical media.
var burnbridgeWORMNoDelete = s3err.APIError{
	Code:           "MethodNotAllowed",
	Description:    "BurnBridge optical media is WORM: object deletion is not supported.",
	HTTPStatusCode: http.StatusMethodNotAllowed,
}

var burnbridgeMultipartUnsupported = s3err.APIError{
	Code:           "NotImplemented",
	Description:    "BurnBridge does not support multipart upload APIs.",
	HTTPStatusCode: http.StatusNotImplemented,
}

func isS3BucketAlnum(b byte) bool {
	return (b >= 'a' && b <= 'z') || (b >= '0' && b <= '9')
}

// sanitizeS3BucketFromVolumeLabel maps a UDF/optical volume label to a DNS-compliant S3 bucket name.
func sanitizeS3BucketFromVolumeLabel(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", fmt.Errorf("empty volume label")
	}
	var b strings.Builder
	prevHyphen := false
	for _, r := range strings.ToLower(raw) {
		switch {
		case r >= 'a' && r <= 'z' || r >= '0' && r <= '9':
			b.WriteRune(r)
			prevHyphen = false
		case r == '-' || r == '_' || r == ' ' || r == '.':
			if b.Len() > 0 && !prevHyphen {
				b.WriteByte('-')
				prevHyphen = true
			}
		}
	}
	s := strings.Trim(b.String(), "-")
	for strings.Contains(s, "--") {
		s = strings.ReplaceAll(s, "--", "-")
	}
	if len(s) > 63 {
		s = s[:63]
		s = strings.TrimRight(s, "-")
	}
	if len(s) < 3 {
		return "", fmt.Errorf("volume label %q maps to a name shorter than 3 characters", raw)
	}
	if !isS3BucketAlnum(s[0]) || !isS3BucketAlnum(s[len(s)-1]) {
		return "", fmt.Errorf("S3 bucket name must start and end with a letter or digit: %q", s)
	}
	return s, nil
}

func volumeLabelFromBucketName(bucket string) string {
	return strings.TrimSpace(strings.ToUpper(bucket))
}

func probeRecorderDiscAtStartup(ctx context.Context, client burnbridgev1.BurnBridgeClient) (bucket string, rawVolume string, readyResp *burnbridgev1.TestUnitReadyResponse, err error) {
	resp, err := client.TestUnitReady(ctx, &burnbridgev1.TestUnitReadyRequest{})
	if err != nil {
		if isGRPCUnimplemented(err) {
			return "", "", nil, fmt.Errorf("burnbridge: recorder must implement TestUnitReady and return volume_label when ready (got Unimplemented)")
		}
		return "", "", nil, fmt.Errorf("burnbridge TestUnitReady at startup: %w", err)
	}
	if !resp.GetReady() {
		return "", "", resp, nil
	}
	raw := strings.TrimSpace(resp.GetVolumeLabel())
	if raw == "" {
		return "", "", nil, fmt.Errorf("burnbridge: TestUnitReady returned ready but empty volume_label (recorder must set disc volume label)")
	}
	bucket, err = sanitizeS3BucketFromVolumeLabel(raw)
	if err != nil {
		return "", "", nil, fmt.Errorf("burnbridge: %w", err)
	}
	return bucket, raw, resp, nil
}

func loadDiscBucketBinding(rawVolume string) (archiveconfig.DiscBucketBinding, bool) {
	cfg, _, err := archiveconfig.Load("")
	if err != nil {
		return archiveconfig.DiscBucketBinding{}, false
	}
	return archiveconfig.FindDiscBucketBinding(cfg, rawVolume)
}

func persistDiscBucketBinding(rawVolume, bucket, udfVolumeLabel string) error {
	cfg, path, err := archiveconfig.Load("")
	if err != nil {
		return err
	}
	archiveconfig.UpsertDiscBucketBinding(&cfg, rawVolume, bucket, udfVolumeLabel)
	return archiveconfig.Save(path, cfg)
}

func discInfoDocFromProto(s3Bucket string, resp *burnbridgev1.TestUnitReadyResponse) *meta.BurnbridgeDiscInfoDocument {
	if resp == nil || !resp.GetReady() {
		return nil
	}
	return &meta.BurnbridgeDiscInfoDocument{
		Bucket:             s3Bucket,
		VolumeLabel:        strings.TrimSpace(resp.GetVolumeLabel()),
		UpdatedAt:          time.Now().UTC().Format(time.RFC3339Nano),
		TotalCapacityBytes: resp.GetTotalCapacityBytes(),
		FreeCapacityBytes:  resp.GetFreeCapacityBytes(),
		MediaType:          strings.TrimSpace(resp.GetMediaType()),
	}
}

func parseReadyReason(message string) (reasonCode string, reasonDetail string) {
	msg := strings.TrimSpace(message)
	if msg == "" {
		return "Unknown", "recorder reports not ready"
	}
	parts := strings.SplitN(msg, ":", 2)
	if len(parts) < 2 {
		return "Unknown", msg
	}
	code := strings.TrimSpace(parts[0])
	detail := strings.TrimSpace(parts[1])
	if code == "" {
		code = "Unknown"
	}
	if detail == "" {
		detail = "recorder reports not ready"
	}
	return code, detail
}

func normalizeOpts(o *Options) {
	if o.ChunkSize <= 0 {
		o.ChunkSize = defaultChunkSizeBytes
	}
	if o.DialTimeout <= 0 {
		o.DialTimeout = 45 * time.Second
	}
	if o.GRPCReadyTimeout <= 0 {
		o.GRPCReadyTimeout = 30 * time.Second
	}
	if o.PingTimeout <= 0 {
		o.PingTimeout = 10 * time.Second
	}
	if o.CancelJobTimeout <= 0 {
		o.CancelJobTimeout = 5 * time.Second
	}
}

// New constructs a BurnBridge backend using SQL metadata and a BurnBridge gRPC endpoint.
func New(opts Options) (*BurnBridge, error) {
	if opts.DBPath == "" {
		return nil, fmt.Errorf("burnbridge: db path required")
	}
	if opts.GRPCAddr == "" {
		return nil, fmt.Errorf("burnbridge: grpc address required (e.g. 127.0.0.1:50051)")
	}
	normalizeOpts(&opts)
	if err := maybeStartLocalRecorderProcess(opts); err != nil {
		return nil, err
	}

	var metaOpts []meta.SqlMetaOption
	if opts.SQLiteMaintCtx != nil {
		metaOpts = append(metaOpts, meta.WithFlashMaintenance(opts.SQLiteMaintCtx, slog.Default()))
	}
	metaStore, err := meta.NewSqlMeta(opts.DBPath, metaOpts...)
	if err != nil {
		return nil, err
	}

	dialCtx, cancel := context.WithTimeout(context.Background(), opts.DialTimeout)
	defer cancel()

	conn, err := dialBurnBridgeGRPC(dialCtx, opts.GRPCReadyTimeout, opts.GRPCAddr, opts.GRPCUseTLS, opts.GRPCCAFile, opts.GRPCServerName, opts.GRPCInsecureSkipVerify)
	if err != nil {
		return nil, err
	}

	client := burnbridgev1.NewBurnBridgeClient(conn)

	readyCtx, readyCancel := context.WithTimeout(dialCtx, opts.PingTimeout)
	activeBucket, rawVol, turResp, err := probeRecorderDiscAtStartup(readyCtx, client)
	readyCancel()
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	if binding, ok := loadDiscBucketBinding(rawVol); ok {
		if strings.TrimSpace(binding.Bucket) != "" {
			activeBucket = strings.TrimSpace(binding.Bucket)
		}
		if strings.TrimSpace(opts.UDFVolumeLabel) == "" && strings.TrimSpace(binding.UdfVolumeLabel) != "" {
			opts.UDFVolumeLabel = strings.TrimSpace(binding.UdfVolumeLabel)
		}
	}
	if activeBucket != "" {
		if doc := discInfoDocFromProto(activeBucket, turResp); doc != nil {
			if perr := metaStore.StoreBurnbridgeDiscInfo(doc); perr != nil {
				_ = conn.Close()
				return nil, fmt.Errorf("burnbridge: persist disc info: %w", perr)
			}
		}
		slog.Info("burnbridge: startup TestUnitReady OK; volume label mapped to S3 bucket",
			"volumeLabel", rawVol, "bucket", activeBucket)
	} else {
		msg := ""
		if turResp != nil {
			msg = strings.TrimSpace(turResp.GetMessage())
		}
		reasonCode, reasonDetail := parseReadyReason(msg)
		slog.Warn("burnbridge: startup recorder not ready; starting in degraded mode with empty bucket list",
			"reason_code", reasonCode, "reason_detail", reasonDetail)
	}

	if !opts.GRPCSkipPing {
		pingCtx, pingCancel := context.WithTimeout(dialCtx, opts.PingTimeout)
		pingErr := grpcConnectivityPing(pingCtx, client)
		pingCancel()
		if pingErr != nil {
			_ = conn.Close()
			return nil, fmt.Errorf("burnbridge grpc ping: %w", pingErr)
		}
	}

	readMount := strings.TrimSpace(sharedReadMountPath())
	if readMount != "" {
		readMount = filepath.Clean(readMount)
	}

	udfLabel := strings.TrimSpace(opts.UDFVolumeLabel)
	if udfLabel == "" {
		udfLabel = strings.TrimSpace(rawVol)
	}

	return &BurnBridge{
		meta:             metaStore,
		grpc:             client,
		grpcConn:         conn,
		chunkSize:        opts.ChunkSize,
		udfLabel:         udfLabel,
		readMount:        readMount,
		cancelJobTimeout: opts.CancelJobTimeout,
		putObjectTimeout: opts.PutObjectTimeout,

		activeBucket:   activeBucket,
		volumeLabelRaw: rawVol,
		allowBucketBinding: opts.AllowCreateBucketBinding,

		recorderS3Endpoint:        strings.TrimSpace(opts.RecorderS3Endpoint),
		recorderS3Region:          strings.TrimSpace(opts.RecorderS3Region),
		recorderS3AccessKey:       opts.RecorderS3AccessKey,
		recorderS3SecretKey:       opts.RecorderS3SecretKey,
		recorderS3SessionToken:    opts.RecorderS3SessionToken,
		recorderS3PathStyle:       opts.RecorderS3ForcePathStyle,
		recorderS3PresignedGetURL: strings.TrimSpace(opts.RecorderS3PresignedGetURL),
		putQueueSem:               make(chan struct{}, defaultPutQueueLimit),
	}, nil
}

func sharedReadMountPath() string {
	cfg, _, err := archiveconfig.Load("")
	if err != nil {
		return ""
	}
	return strings.TrimSpace(cfg.OpticalArchive.ReadMountPath)
}

func (b *BurnBridge) requireRecorderReady(ctx context.Context) error {
	resp, err := b.grpc.TestUnitReady(ctx, &burnbridgev1.TestUnitReadyRequest{})
	if err != nil {
		if isGRPCUnimplemented(err) {
			slog.Warn("burnbridge: TestUnitReady unimplemented on recorder; treating unit as ready (upgrade recorder to enforce readiness)")
			return nil
		}
		return fmt.Errorf("burnbridge TestUnitReady: %w", err)
	}
	if !resp.GetReady() {
		reasonCode, reasonDetail := parseReadyReason(resp.GetMessage())
		slog.Warn("burnbridge: recorder not ready",
			"reason_code", reasonCode,
			"reason_detail", reasonDetail)
		return s3err.APIError{
			Code:           "BurnbridgeUnitNotReady",
			Description:    fmt.Sprintf("%s: %s", reasonCode, reasonDetail),
			HTTPStatusCode: http.StatusServiceUnavailable,
		}
	}
	if doc := discInfoDocFromProto(b.activeBucket, resp); doc != nil {
		if err := b.meta.StoreBurnbridgeDiscInfo(doc); err != nil {
			return fmt.Errorf("burnbridge: persist disc info: %w", err)
		}
	}
	return nil
}

func (b *BurnBridge) burnbridgeBucketExists(name string) bool {
	return name != "" && name == b.activeBucket
}

func (b *BurnBridge) createBucketBindingAllowed() bool {
	return b.allowBucketBinding
}

func (b *BurnBridge) canRebindActiveBucket(target string) bool {
	if strings.TrimSpace(target) == "" {
		return false
	}
	if strings.EqualFold(strings.TrimSpace(target), strings.TrimSpace(b.activeBucket)) {
		return true
	}
	committed, err := b.meta.ListCommittedObjects(b.activeBucket)
	if err != nil {
		return false
	}
	return len(committed) == 0
}

func (b *BurnBridge) bindActiveBucket(bucket, volumeLabel string) error {
	if strings.TrimSpace(bucket) == "" {
		return fmt.Errorf("burnbridge: empty bucket binding")
	}
	if strings.TrimSpace(volumeLabel) == "" {
		return fmt.Errorf("burnbridge: empty volume label binding")
	}

	originalProbe := strings.TrimSpace(b.volumeLabelRaw)
	b.activeBucket = bucket
	b.volumeLabelRaw = volumeLabel
	b.udfLabel = volumeLabel
	if originalProbe != "" {
		if err := persistDiscBucketBinding(originalProbe, bucket, volumeLabel); err != nil {
			return err
		}
	}
	return b.meta.StoreBurnbridgeDiscInfo(&meta.BurnbridgeDiscInfoDocument{
		Bucket:             bucket,
		VolumeLabel:        volumeLabel,
		UpdatedAt:          time.Now().UTC().Format(time.RFC3339Nano),
		TotalCapacityBytes: 0,
		FreeCapacityBytes:  0,
		MediaType:          "uninitialized",
	})
}

// ------------------------------
// Bucket APIs
// ------------------------------

func (b *BurnBridge) ListBuckets(context.Context, s3response.ListBucketsInput) (s3response.ListAllMyBucketsResult, error) {
	name := b.activeBucket
	if strings.TrimSpace(name) == "" {
		return s3response.ListAllMyBucketsResult{
			Buckets: s3response.ListAllMyBucketsList{Bucket: []s3response.ListAllMyBucketsEntry{}},
		}, nil
	}
	return s3response.ListAllMyBucketsResult{
		Buckets: s3response.ListAllMyBucketsList{
			Bucket: []s3response.ListAllMyBucketsEntry{{Name: name, CreationDate: time.Now()}},
		},
	}, nil
}

func (b *BurnBridge) CreateBucket(_ context.Context, input *s3.CreateBucketInput, _ []byte) error {
	if input == nil || input.Bucket == nil {
		return s3err.GetAPIError(s3err.ErrInvalidBucketName)
	}
	if !b.createBucketBindingAllowed() {
		return s3err.APIError{
			Code:           "MethodNotAllowed",
			Description:    "BurnBridge create bucket binding is disabled by configuration.",
			HTTPStatusCode: http.StatusMethodNotAllowed,
		}
	}

	requestedBucket := strings.TrimSpace(*input.Bucket)
	if requestedBucket == "" {
		return s3err.GetAPIError(s3err.ErrInvalidBucketName)
	}

	targetVolumeLabel := volumeLabelFromBucketName(requestedBucket)
	sanitizedBucket, err := sanitizeS3BucketFromVolumeLabel(targetVolumeLabel)
	if err != nil || !strings.EqualFold(sanitizedBucket, requestedBucket) {
		return s3err.GetAPIError(s3err.ErrInvalidBucketName)
	}

	if strings.TrimSpace(b.activeBucket) == "" {
		return b.bindActiveBucket(requestedBucket, targetVolumeLabel)
	}
	if strings.EqualFold(b.activeBucket, requestedBucket) {
		return s3err.GetAPIError(s3err.ErrBucketAlreadyOwnedByYou)
	}
	if !b.canRebindActiveBucket(requestedBucket) {
		return s3err.GetAPIError(s3err.ErrBucketAlreadyExists)
	}

	return b.bindActiveBucket(requestedBucket, targetVolumeLabel)
}

// HeadBucket confirms the bucket exists and the recorder reports the optical unit ready for this bucket.
func (b *BurnBridge) HeadBucket(ctx context.Context, input *s3.HeadBucketInput) (*s3.HeadBucketOutput, error) {
	if input == nil || input.Bucket == nil {
		return nil, fmt.Errorf("bucket required")
	}
	bucket := *input.Bucket
	if !b.burnbridgeBucketExists(bucket) {
		return nil, s3err.GetAPIError(s3err.ErrNoSuchBucket)
	}
	if err := b.requireRecorderReady(ctx); err != nil {
		return nil, err
	}
	return &s3.HeadBucketOutput{}, nil
}

// DeleteBucket is rejected: the active bucket is tied to loaded optical media (WORM session).
func (b *BurnBridge) DeleteBucket(_ context.Context, bucket string) error {
	if !b.burnbridgeBucketExists(bucket) {
		return s3err.GetAPIError(s3err.ErrNoSuchBucket)
	}
	return burnbridgeWORMNoDelete
}

// GetBucketOwnershipControls returns a stable default to keep WebUI compatibility.
func (b *BurnBridge) GetBucketOwnershipControls(_ context.Context, bucket string) (types.ObjectOwnership, error) {
	if !b.burnbridgeBucketExists(bucket) {
		return types.ObjectOwnershipBucketOwnerEnforced, s3err.GetAPIError(s3err.ErrNoSuchBucket)
	}
	return types.ObjectOwnershipBucketOwnerEnforced, nil
}

// GetBucketVersioning returns an empty (unconfigured) versioning state.
func (b *BurnBridge) GetBucketVersioning(_ context.Context, bucket string) (s3response.GetBucketVersioningOutput, error) {
	if !b.burnbridgeBucketExists(bucket) {
		return s3response.GetBucketVersioningOutput{}, s3err.GetAPIError(s3err.ErrNoSuchBucket)
	}
	return s3response.GetBucketVersioningOutput{}, nil
}

// GetBucketAcl provides a minimal ACL view for auth middleware compatibility.
func (b *BurnBridge) GetBucketAcl(_ context.Context, input *s3.GetBucketAclInput) ([]byte, error) {
	if input == nil || input.Bucket == nil || !b.burnbridgeBucketExists(*input.Bucket) {
		return nil, s3err.GetAPIError(s3err.ErrNoSuchBucket)
	}
	acl := auth.ACL{
		Owner: b.activeBucket,
		Grantees: []auth.Grantee{
			{
				Permission: auth.PermissionFullControl,
				Access:     b.activeBucket,
				Type:       types.TypeCanonicalUser,
			},
		},
	}
	return json.Marshal(acl)
}

// GetBucketTagging returns an empty tag set for compatibility (no backend tag persistence).
func (b *BurnBridge) GetBucketTagging(_ context.Context, bucket string) (map[string]string, error) {
	if !b.burnbridgeBucketExists(bucket) {
		return nil, s3err.GetAPIError(s3err.ErrNoSuchBucket)
	}
	return map[string]string{}, nil
}

// GetBucketPolicy reports no bucket policy for burnbridge buckets.
func (b *BurnBridge) GetBucketPolicy(_ context.Context, bucket string) ([]byte, error) {
	if !b.burnbridgeBucketExists(bucket) {
		return nil, s3err.GetAPIError(s3err.ErrNoSuchBucket)
	}
	return nil, s3err.GetAPIError(s3err.ErrNoSuchBucketPolicy)
}

// GetBucketCors returns no per-bucket CORS config so gateway fallback can apply.
func (b *BurnBridge) GetBucketCors(_ context.Context, bucket string) ([]byte, error) {
	if !b.burnbridgeBucketExists(bucket) {
		return nil, s3err.GetAPIError(s3err.ErrNoSuchBucket)
	}
	return nil, s3err.GetAPIError(s3err.ErrNoSuchCORSConfiguration)
}

// GetObjectLockConfiguration returns "not configured" for burnbridge buckets.
func (b *BurnBridge) GetObjectLockConfiguration(_ context.Context, bucket string) ([]byte, error) {
	if !b.burnbridgeBucketExists(bucket) {
		return nil, s3err.GetAPIError(s3err.ErrNoSuchBucket)
	}
	return nil, s3err.GetAPIError(s3err.ErrObjectLockConfigurationNotFound)
}

// ListMultipartUploads returns an empty list for WebUI compatibility.
// BurnBridge still does not support multipart upload write path APIs.
func (b *BurnBridge) ListMultipartUploads(_ context.Context, input *s3.ListMultipartUploadsInput) (s3response.ListMultipartUploadsResult, error) {
	if input == nil || input.Bucket == nil {
		return s3response.ListMultipartUploadsResult{}, s3err.GetAPIError(s3err.ErrInvalidBucketName)
	}
	bucket := *input.Bucket
	if !b.burnbridgeBucketExists(bucket) {
		return s3response.ListMultipartUploadsResult{}, s3err.GetAPIError(s3err.ErrNoSuchBucket)
	}

	delimiter := ""
	if input.Delimiter != nil {
		delimiter = *input.Delimiter
	}
	prefix := ""
	if input.Prefix != nil {
		prefix = *input.Prefix
	}
	maxUploads := int(listDefaultMaxKeys)
	if input.MaxUploads != nil {
		maxUploads = int(*input.MaxUploads)
	}

	return s3response.ListMultipartUploadsResult{
		Bucket:         bucket,
		Delimiter:      delimiter,
		Prefix:         prefix,
		MaxUploads:     maxUploads,
		Uploads:        []s3response.Upload{},
		CommonPrefixes: []s3response.CommonPrefix{},
	}, nil
}

func (b *BurnBridge) String() string { return "BurnBridge" }

// Close releases gRPC resources.
func (b *BurnBridge) Close() error {
	if b.grpcConn == nil {
		return nil
	}
	err := b.grpcConn.Close()
	b.grpcConn = nil
	return err
}

// Shutdown implements backend.Backend: closes the BurnBridge gRPC connection.
func (b *BurnBridge) Shutdown() {
	_ = b.Close()
}

// ------------------------------
// Shared metadata helpers
// ------------------------------

func quotedMD5Bytes(p []byte) string {
	sum := md5.Sum(p)
	return `"` + hex.EncodeToString(sum[:]) + `"`
}

func discInfoLastModifiedFromJSON(raw []byte) time.Time {
	var d meta.BurnbridgeDiscInfoDocument
	if err := json.Unmarshal(raw, &d); err != nil {
		return time.Now().UTC()
	}
	if strings.TrimSpace(d.UpdatedAt) == "" {
		return time.Now().UTC()
	}
	if t, err := time.Parse(time.RFC3339Nano, d.UpdatedAt); err == nil {
		return t.UTC()
	}
	if t, err := time.Parse(time.RFC3339, d.UpdatedAt); err == nil {
		return t.UTC()
	}
	return time.Now().UTC()
}

func finalizeLayoutLastModifiedFromJSON(raw []byte) time.Time {
	var d meta.BurnbridgeFinalizeLayoutDocument
	if err := json.Unmarshal(raw, &d); err != nil {
		return time.Now().UTC()
	}
	if strings.TrimSpace(d.CompletedAtUtc) == "" {
		return time.Now().UTC()
	}
	if t, err := time.Parse(time.RFC3339Nano, d.CompletedAtUtc); err == nil {
		return t.UTC()
	}
	if t, err := time.Parse(time.RFC3339, d.CompletedAtUtc); err == nil {
		return t.UTC()
	}
	return time.Now().UTC()
}

func buildFinalizeLayoutResultJSON(bucket string, closeDisc bool, resp *burnbridgev1.FinalizeLayoutResponse, grpcErr error) ([]byte, error) {
	doc := meta.BurnbridgeFinalizeLayoutDocument{
		Bucket:         bucket,
		CloseDisc:      closeDisc,
		CompletedAtUtc: time.Now().UTC().Format(time.RFC3339Nano),
	}
	if grpcErr != nil {
		doc.GrpcOK = false
		if st, ok := status.FromError(grpcErr); ok {
			doc.GrpcCode = st.Code().String()
			doc.GrpcDetails = st.Message()
		} else {
			doc.Error = grpcErr.Error()
		}
	} else {
		doc.GrpcOK = true
		doc.GrpcCode = codes.OK.String()
		if resp != nil {
			doc.RecorderStatus = resp.GetStatus()
			doc.RecorderMessage = resp.GetMessage()
		}
	}
	return json.Marshal(doc)
}

// invokeFinalizeLayoutAgainstRecorder persists a JSON transcript (success or RPC error) for object key FinalizeLayout.
func (b *BurnBridge) invokeFinalizeLayoutAgainstRecorder(ctx context.Context, bucket string, closeDisc bool) ([]byte, error) {
	b.putSerialMu.Lock()
	defer b.putSerialMu.Unlock()

	recCtx := ctx
	if recCtx == nil {
		recCtx = context.Background()
	}
	resp, grpcErr := b.grpc.FinalizeLayout(recCtx, &burnbridgev1.FinalizeLayoutRequest{
		Bucket:         bucket,
		UdfVolumeLabel: b.udfLabel,
		CloseDisc:      closeDisc,
	})

	payload, mErr := buildFinalizeLayoutResultJSON(bucket, closeDisc, resp, grpcErr)
	if mErr != nil {
		return nil, fmt.Errorf("burnbridge finalize layout json: %w", mErr)
	}
	if err := b.meta.StoreBurnbridgeFinalizeLayoutJSON(bucket, payload); err != nil {
		return nil, err
	}
	if grpcErr != nil {
		slog.Warn("burnbridge: FinalizeLayout gRPC reported failure (transcript stored for GET)",
			"bucket", bucket, "grpc_err", grpcErr)
	}
	return payload, nil
}

func shouldReuseFinalizeLayoutTranscript(bucket string, raw []byte) bool {
	var doc meta.BurnbridgeFinalizeLayoutDocument
	if err := json.Unmarshal(raw, &doc); err != nil {
		return false
	}
	if !strings.EqualFold(strings.TrimSpace(doc.Bucket), strings.TrimSpace(bucket)) {
		return false
	}
	return doc.GrpcOK && strings.EqualFold(strings.TrimSpace(doc.RecorderStatus), "finalized")
}

func (b *BurnBridge) invalidateFinalizeLayoutTranscript(bucket string) {
	if err := b.meta.DeleteBurnbridgeFinalizeLayoutJSON(bucket); err != nil && !errors.Is(err, meta.ErrNoSuchKey) {
		slog.Warn("burnbridge: failed to invalidate cached FinalizeLayout transcript",
			"bucket", bucket, "err", err)
		return
	}
	slog.Info("burnbridge: invalidated cached FinalizeLayout transcript", "bucket", bucket)
}

func (b *BurnBridge) loadOrFinalizeLayoutTranscript(ctx context.Context, bucket string, closeDisc bool) ([]byte, error) {
	if prev, err := b.meta.GetBurnbridgeFinalizeLayoutJSON(bucket); err == nil && shouldReuseFinalizeLayoutTranscript(bucket, prev) {
		return prev, nil
	}
	return b.invokeFinalizeLayoutAgainstRecorder(ctx, bucket, closeDisc)
}

func (b *BurnBridge) HeadObject(ctx context.Context, input *s3.HeadObjectInput) (*s3.HeadObjectOutput, error) {
	if input == nil || input.Bucket == nil || input.Key == nil {
		return nil, fmt.Errorf("bucket/key required")
	}
	bucket := *input.Bucket
	key := *input.Key
	if !b.burnbridgeBucketExists(bucket) {
		return nil, s3err.GetAPIError(s3err.ErrNoSuchBucket)
	}

	if err := b.requireRecorderReady(ctx); err != nil {
		return nil, err
	}

	if key == meta.BurnbridgeFinalizeLayoutObjectKey {
		raw, err := b.loadOrFinalizeLayoutTranscript(ctx, bucket, false)
		if err != nil {
			if errors.Is(err, meta.ErrNoSuchKey) {
				return nil, s3err.GetAPIError(s3err.ErrNoSuchKey)
			}
			return nil, err
		}
		clen := int64(len(raw))
		etag := quotedMD5Bytes(raw)
		ct := discInfoContentType
		lm := finalizeLayoutLastModifiedFromJSON(raw)
		return &s3.HeadObjectOutput{
			ContentType:   &ct,
			ContentLength: &clen,
			ETag:          &etag,
			LastModified:  backend.GetTimePtr(lm),
		}, nil
	}

	if key == meta.BurnbridgeDiscInfoObjectKey {
		raw, err := b.meta.GetBurnbridgeDiscInfoJSON(bucket)
		if err != nil {
			if errors.Is(err, meta.ErrNoSuchKey) {
				return nil, s3err.GetAPIError(s3err.ErrNoSuchKey)
			}
			return nil, err
		}
		clen := int64(len(raw))
		etag := quotedMD5Bytes(raw)
		ct := discInfoContentType
		lm := discInfoLastModifiedFromJSON(raw)
		return &s3.HeadObjectOutput{
			ContentType:   &ct,
			ContentLength: &clen,
			ETag:          &etag,
			LastModified:  backend.GetTimePtr(lm),
		}, nil
	}

	summary, err := b.meta.GetCommittedObjectSummary(bucket, key)
	if err != nil {
		if errors.Is(err, meta.ErrNoSuchKey) {
			return nil, s3err.GetAPIError(s3err.ErrNoSuchKey)
		}
		return nil, err
	}

	etagCopy := summary.ETag
	if etagCopy == "" {
		etagCopy = emptyQuotedMD5
	}
	clen := summary.Size
	lm := summary.LastModified

	ct := burnbridgeDefaultContentType
	out := &s3.HeadObjectOutput{
		ContentType:   &ct,
		ETag:          &etagCopy,
		LastModified:  backend.GetTimePtr(lm),
		ContentLength: &clen,
	}
	return out, nil
}

func parseCommittedGetRange(objSize int64, rangeHdr string) (startOffset, length int64, contentRange *string, err error) {
	startOffset = 0
	length = objSize
	if rangeHdr == "" {
		return
	}
	start, lgth, isValid, perr := backend.ParseObjectRange(objSize, rangeHdr)
	if perr != nil || !isValid {
		return 0, 0, nil, s3err.GetAPIError(s3err.ErrInvalidRange)
	}
	startOffset, length = start, lgth
	if objSize > 0 {
		contentRange = backend.GetPtrFromString(fmt.Sprintf("bytes %d-%d/%d", startOffset, startOffset+length-1, objSize))
	}
	return
}

// bbSafeObjectPath returns an absolute path under readMount for bucket/key, rejecting ".." traversal.
func bbSafeObjectPath(mountRoot, bucket, key string) (string, error) {
	mountRoot = filepath.Clean(mountRoot)
	if mountRoot == "" || mountRoot == "." {
		return "", fmt.Errorf("burnbridge: read mount path invalid")
	}
	rm, err := filepath.Abs(mountRoot)
	if err != nil {
		return "", err
	}
	rel := filepath.FromSlash(strings.TrimPrefix(key, "/"))
	if rel == "" || rel == "." {
		return "", fmt.Errorf("burnbridge: empty object key")
	}
	full := filepath.Join(rm, bucket, rel)
	rp, err := filepath.Abs(full)
	if err != nil {
		return "", err
	}
	relOut, err := filepath.Rel(rm, rp)
	if err != nil || relOut == ".." || strings.HasPrefix(relOut, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("burnbridge: object path escapes read mount")
	}
	return rp, nil
}

func (b *BurnBridge) openCommittedObjectFile(bucket, key string) (*os.File, os.FileInfo, error) {
	if b.readMount == "" {
		return nil, nil, errors.New("burnbridge: read mount not configured")
	}
	objPath, err := bbSafeObjectPath(b.readMount, bucket, key)
	if err != nil {
		return nil, nil, err
	}
	f, err := os.Open(objPath)
	if err != nil {
		return nil, nil, err
	}
	fi, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, nil, err
	}
	if fi.IsDir() {
		_ = f.Close()
		return nil, nil, syscall.EISDIR
	}
	return f, fi, nil
}

func mapOpenError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, os.ErrNotExist) || errors.Is(err, syscall.ENOTDIR) {
		return s3err.GetAPIError(s3err.ErrNoSuchKey)
	}
	if errors.Is(err, syscall.EISDIR) {
		return s3err.GetAPIError(s3err.ErrNoSuchKey)
	}
	if errors.Is(err, syscall.ENAMETOOLONG) {
		return s3err.GetAPIError(s3err.ErrKeyTooLong)
	}
	return err
}

// ------------------------------
// Object APIs (Head/Get/List/Put/Delete)
// ------------------------------

func (b *BurnBridge) GetObject(ctx context.Context, input *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	if input == nil || input.Bucket == nil || input.Key == nil {
		return nil, fmt.Errorf("bucket/key required")
	}
	bucket := *input.Bucket
	key := *input.Key
	if !b.burnbridgeBucketExists(bucket) {
		return nil, s3err.GetAPIError(s3err.ErrNoSuchBucket)
	}

	if input.PartNumber != nil && *input.PartNumber > 1 {
		return nil, s3err.GetAPIError(s3err.ErrInvalidPartNumber)
	}

	if err := b.requireRecorderReady(ctx); err != nil {
		return nil, err
	}

	if key == meta.BurnbridgeFinalizeLayoutObjectKey {
		raw, err := b.loadOrFinalizeLayoutTranscript(ctx, bucket, false)
		if err != nil {
			return nil, err
		}
		objSize := int64(len(raw))
		startOffset, length, contentRange, err := parseCommittedGetRange(objSize, backend.GetStringFromPtr(input.Range))
		if err != nil {
			return nil, err
		}
		slice := raw[startOffset : startOffset+length]
		etag := quotedMD5Bytes(raw)
		lm := finalizeLayoutLastModifiedFromJSON(raw)
		ct := discInfoContentType
		clen := length
		return &s3.GetObjectOutput{
			Body:          io.NopCloser(bytes.NewReader(slice)),
			AcceptRanges:  backend.GetPtrFromString("bytes"),
			ETag:          &etag,
			LastModified:  backend.GetTimePtr(lm),
			ContentLength: &clen,
			ContentRange:  contentRange,
			StorageClass:  types.StorageClassStandard,
			ContentType:   &ct,
		}, nil
	}

	if key == meta.BurnbridgeDiscInfoObjectKey {
		raw, err := b.meta.GetBurnbridgeDiscInfoJSON(bucket)
		if err != nil {
			if errors.Is(err, meta.ErrNoSuchKey) {
				return nil, s3err.GetAPIError(s3err.ErrNoSuchKey)
			}
			return nil, err
		}
		objSize := int64(len(raw))
		startOffset, length, contentRange, err := parseCommittedGetRange(objSize, backend.GetStringFromPtr(input.Range))
		if err != nil {
			return nil, err
		}
		slice := raw[startOffset : startOffset+length]
		etag := quotedMD5Bytes(raw)
		lm := discInfoLastModifiedFromJSON(raw)
		ct := discInfoContentType
		clen := length
		return &s3.GetObjectOutput{
			Body:          io.NopCloser(bytes.NewReader(slice)),
			AcceptRanges:  backend.GetPtrFromString("bytes"),
			ETag:          &etag,
			LastModified:  backend.GetTimePtr(lm),
			ContentLength: &clen,
			ContentRange:  contentRange,
			StorageClass:  types.StorageClassStandard,
			ContentType:   &ct,
		}, nil
	}

	idx := objectLockIndex(bucket, key)
	b.objectLocks[idx].Lock()
	unlock := func() { b.objectLocks[idx].Unlock() }
	wrapBody := func(r io.ReadCloser) io.ReadCloser {
		return &unlockOnCloseReadCloser{r: r, unlock: unlock}
	}
	fail := func(err error) (*s3.GetObjectOutput, error) {
		unlock()
		return nil, err
	}

	summary, err := b.meta.GetCommittedObjectSummary(bucket, key)
	if err != nil {
		if errors.Is(err, meta.ErrNoSuchKey) {
			return fail(s3err.GetAPIError(s3err.ErrNoSuchKey))
		}
		return fail(err)
	}

	etagCopy := summary.ETag
	if etagCopy == "" {
		etagCopy = emptyQuotedMD5
	}

	ct := burnbridgeDefaultContentType
	objSize := summary.Size
	rangeHdr := backend.GetStringFromPtr(input.Range)

	openLocal := b.readMount != ""
	var f *os.File
	var fi os.FileInfo
	if openLocal {
		f, fi, err = b.openCommittedObjectFile(bucket, key)
		if err == nil {
			objSize = fi.Size()
		} else if errors.Is(err, os.ErrNotExist) || errors.Is(err, syscall.ENOTDIR) {
			openLocal = false
		} else if errors.Is(err, syscall.EISDIR) {
			return fail(s3err.GetAPIError(s3err.ErrNoSuchKey))
		} else {
			return fail(mapOpenError(err))
		}
	}

	startOffset, length, contentRange, err := parseCommittedGetRange(objSize, rangeHdr)
	if err != nil {
		if f != nil {
			_ = f.Close()
		}
		return fail(err)
	}

	if openLocal && f != nil {
		lm := fi.ModTime().UTC()
		var body io.ReadCloser = f
		if startOffset != 0 || length != objSize {
			rdr := io.NewSectionReader(f, startOffset, length)
			body = &backend.FileSectionReadCloser{R: rdr, F: f}
		}
		clen := length
		return &s3.GetObjectOutput{
			Body:          wrapBody(body),
			AcceptRanges:  backend.GetPtrFromString("bytes"),
			ETag:          &etagCopy,
			LastModified:  backend.GetTimePtr(lm),
			ContentLength: &clen,
			ContentRange:  contentRange,
			StorageClass:  types.StorageClassStandard,
			ContentType:   &ct,
		}, nil
	}

	var body io.ReadCloser
	if length == 0 {
		body = io.NopCloser(bytes.NewReader(nil))
	} else {
		readCtx := ctx
		if readCtx == nil {
			readCtx = context.Background()
		}
		stream, err := b.grpc.ReadObject(readCtx, &burnbridgev1.ReadObjectRequest{
			Bucket:    bucket,
			ObjectKey: key,
			Offset:    startOffset,
			Length:    length,
		})
		if err != nil {
			return fail(mapReadFallbackError(err))
		}
		body = &grpcObjectReadCloser{stream: stream, left: length}
	}

	clen := length
	return &s3.GetObjectOutput{
		Body:          wrapBody(body),
		AcceptRanges:  backend.GetPtrFromString("bytes"),
		ETag:          &etagCopy,
		LastModified:  backend.GetTimePtr(summary.LastModified),
		ContentLength: &clen,
		ContentRange:  contentRange,
		StorageClass:  types.StorageClassStandard,
		ContentType:   &ct,
	}, nil
}

func (b *BurnBridge) prepareCommittedListing(ctx context.Context, bucket string) (fstest.MapFS, map[string]meta.CommittedObjectSummary, error) {
	if !b.burnbridgeBucketExists(bucket) {
		return nil, nil, s3err.GetAPIError(s3err.ErrNoSuchBucket)
	}
	if err := b.requireRecorderReady(ctx); err != nil {
		return nil, nil, err
	}
	return b.committedMapFSAndSummaries(bucket)
}

func (b *BurnBridge) walkObjectMeta(bucket string, byKey map[string]meta.CommittedObjectSummary) backend.GetObjFunc {
	return func(path string, d fs.DirEntry) (s3response.Object, error) {
		if d.IsDir() {
			return s3response.Object{}, backend.ErrSkipObj
		}
		sum, ok := byKey[path]
		if !ok {
			return s3response.Object{}, backend.ErrSkipObj
		}
		lm := sum.LastModified
		etagCopy := sum.ETag
		if etagCopy == "" {
			etagCopy = emptyQuotedMD5
		}
		sz := sum.Size
		sc := types.ObjectStorageClassStandard
		k := path
		return s3response.Object{
			Key:          &k,
			ETag:         &etagCopy,
			LastModified: &lm,
			Size:         &sz,
			StorageClass: sc,
		}, nil
	}
}

func (b *BurnBridge) committedMapFSAndSummaries(bucket string) (fstest.MapFS, map[string]meta.CommittedObjectSummary, error) {
	summaries, err := b.meta.ListCommittedObjects(bucket)
	if err != nil {
		return nil, nil, err
	}
	byKey := make(map[string]meta.CommittedObjectSummary, len(summaries))
	fsys := fstest.MapFS{}
	for _, sum := range summaries {
		k := strings.TrimPrefix(strings.ReplaceAll(sum.ObjectKey, `\`, `/`), "/")
		byKey[k] = sum
		parts := strings.Split(k, "/")
		path := ""
		for i, seg := range parts {
			if i > 0 {
				path += "/"
			}
			path += seg
			if i < len(parts)-1 {
				if _, ok := fsys[path]; !ok {
					fsys[path] = &fstest.MapFile{Mode: fs.ModeDir | 0o755}
				}
			} else {
				fsys[path] = &fstest.MapFile{Mode: 0o644}
			}
		}
	}
	return fsys, byKey, nil
}

func listObjectsV2RequestTokens(input *s3.ListObjectsV2Input) (startAfter, contTok string) {
	if input.StartAfter != nil {
		startAfter = *input.StartAfter
	}
	if input.ContinuationToken != nil {
		contTok = *input.ContinuationToken
	}
	return
}

// ------------------------------
// List APIs
// ------------------------------

// ListObjects lists object keys that have completed PutObject (burnbridge committed JSON in metadata).
func (b *BurnBridge) ListObjects(ctx context.Context, input *s3.ListObjectsInput) (s3response.ListObjectsResult, error) {
	if input == nil || input.Bucket == nil {
		return s3response.ListObjectsResult{}, s3err.GetAPIError(s3err.ErrInvalidBucketName)
	}
	bucket := *input.Bucket
	fsys, byKey, err := b.prepareCommittedListing(ctx, bucket)
	if err != nil {
		return s3response.ListObjectsResult{}, err
	}
	prefix := backend.GetStringFromPtr(input.Prefix)
	delim := backend.GetStringFromPtr(input.Delimiter)
	marker := backend.GetStringFromPtr(input.Marker)
	maxkeys := listDefaultMaxKeys
	if input.MaxKeys != nil {
		maxkeys = *input.MaxKeys
	}
	if maxkeys == 0 {
		isFalse := false
		return s3response.ListObjectsResult{
			IsTruncated:    &isFalse,
			MaxKeys:        &maxkeys,
			Name:           &bucket,
			Prefix:         backend.GetPtrFromString(prefix),
			Marker:         backend.GetPtrFromString(marker),
			Delimiter:      backend.GetPtrFromString(delim),
			CommonPrefixes: []types.CommonPrefix{},
		}, nil
	}
	results, err := backend.Walk(ctx, fsys, prefix, delim, marker, maxkeys, b.walkObjectMeta(bucket, byKey), nil)
	if err != nil {
		return s3response.ListObjectsResult{}, fmt.Errorf("list objects walk: %w", err)
	}
	return s3response.ListObjectsResult{
		CommonPrefixes: results.CommonPrefixes,
		Contents:       results.Objects,
		Delimiter:      backend.GetPtrFromString(delim),
		IsTruncated:    &results.Truncated,
		Marker:         backend.GetPtrFromString(marker),
		MaxKeys:        &maxkeys,
		Name:           &bucket,
		NextMarker:     backend.GetPtrFromString(results.NextMarker),
		Prefix:         backend.GetPtrFromString(prefix),
	}, nil
}

// ListObjectsV2 lists object keys that have completed PutObject (burnbridge committed JSON in metadata).
func (b *BurnBridge) ListObjectsV2(ctx context.Context, input *s3.ListObjectsV2Input) (s3response.ListObjectsV2Result, error) {
	if input == nil || input.Bucket == nil {
		return s3response.ListObjectsV2Result{}, s3err.GetAPIError(s3err.ErrInvalidBucketName)
	}
	bucket := *input.Bucket
	fsys, byKey, err := b.prepareCommittedListing(ctx, bucket)
	if err != nil {
		return s3response.ListObjectsV2Result{}, err
	}
	prefix := backend.GetStringFromPtr(input.Prefix)
	delim := backend.GetStringFromPtr(input.Delimiter)
	marker := ""
	if input.ContinuationToken != nil {
		marker = *input.ContinuationToken
	}
	if input.StartAfter != nil && *input.StartAfter > marker {
		marker = *input.StartAfter
	}
	maxkeys := listDefaultMaxKeys
	if input.MaxKeys != nil {
		maxkeys = *input.MaxKeys
	}
	startAfterVal, contTok := listObjectsV2RequestTokens(input)
	if maxkeys == 0 {
		isFalse := false
		return s3response.ListObjectsV2Result{
			IsTruncated:       &isFalse,
			MaxKeys:           &maxkeys,
			Name:              &bucket,
			Prefix:            backend.GetPtrFromString(prefix),
			Delimiter:         backend.GetPtrFromString(delim),
			StartAfter:        backend.GetPtrFromString(startAfterVal),
			ContinuationToken: backend.GetPtrFromString(contTok),
			CommonPrefixes:    []types.CommonPrefix{},
		}, nil
	}
	results, err := backend.Walk(ctx, fsys, prefix, delim, marker, maxkeys, b.walkObjectMeta(bucket, byKey), nil)
	if err != nil {
		return s3response.ListObjectsV2Result{}, fmt.Errorf("list objects v2 walk: %w", err)
	}
	count := int32(len(results.Objects))
	return s3response.ListObjectsV2Result{
		CommonPrefixes:        results.CommonPrefixes,
		Contents:              results.Objects,
		IsTruncated:           &results.Truncated,
		MaxKeys:               &maxkeys,
		Name:                  &bucket,
		KeyCount:              &count,
		Delimiter:             backend.GetPtrFromString(delim),
		ContinuationToken:     backend.GetPtrFromString(contTok),
		NextContinuationToken: backend.GetPtrFromString(results.NextMarker),
		Prefix:                backend.GetPtrFromString(prefix),
		StartAfter:            backend.GetPtrFromString(startAfterVal),
	}, nil
}

type uploadRecoveryStats struct {
	TotalSegments    int64
	SkippedSegments  int64
	ReplayedSegments int64
	TrimmedSegments  int64
}

// ------------------------------
// PutObject streaming helpers
// ------------------------------

func bbProtoDiscExtentsToMeta(in []*burnbridgev1.DiscExtent) []meta.BurnDiscExtent {
	if len(in) == 0 {
		return nil
	}
	out := make([]meta.BurnDiscExtent, 0, len(in))
	for _, e := range in {
		if e == nil {
			continue
		}
		out = append(out, meta.BurnDiscExtent{
			DiscAddress: e.GetDiscAddress(),
			FileSize:    e.GetFileSize(),
		})
	}
	return out
}

func bbSegmentMD5Hex(p []byte) string {
	sum := md5.Sum(p)
	return hex.EncodeToString(sum[:])
}

func quotedETag(md5Hex string) string {
	h := strings.TrimSpace(strings.ToLower(md5Hex))
	if h == "" {
		return emptyQuotedMD5
	}
	h = strings.TrimPrefix(strings.TrimSuffix(h, `"`), `"`)
	return `"` + h + `"`
}

func (b *BurnBridge) loadBurnSegmentSnapshot(bucket, key string) (map[int]meta.BurnObjectSegment, error) {
	segments, err := b.meta.ListBurnObjectSegments(bucket, key)
	if err != nil {
		return nil, err
	}
	snapshot := make(map[int]meta.BurnObjectSegment, len(segments))
	for _, seg := range segments {
		snapshot[seg.SegmentIndex] = seg.BurnObjectSegment
	}
	return snapshot, nil
}

func (b *BurnBridge) burnMaybeInvalidateSegments(bucket, key string, snapshot map[int]meta.BurnObjectSegment, segmentIdx int, digest string) error {
	if segmentIdx != 0 {
		return nil
	}
	prev, ok := snapshot[0]
	if !ok {
		return nil
	}
	if prev.ChecksumMD5 != digest {
		if err := b.meta.DeleteBurnObjectSegments(bucket, key); err != nil {
			return err
		}
		for k := range snapshot {
			delete(snapshot, k)
		}
	}
	return nil
}

func (b *BurnBridge) burnShouldSkipSegment(snapshot map[int]meta.BurnObjectSegment, segmentIdx int, digest string, offset, segLen int64) bool {
	if segmentIdx > 0 {
		prev, ok := snapshot[segmentIdx-1]
		if !ok {
			return false
		}
		if !prev.Burned() {
			return false
		}
	}

	stored, ok := snapshot[segmentIdx]
	if !ok {
		return false
	}
	if stored.ChecksumMD5 != digest || !stored.Burned() || stored.ByteSize != segLen || stored.ByteOffset != offset {
		return false
	}
	return true
}

func (b *BurnBridge) recvSegmentUploadAck(stream grpc.BidiStreamingClient[burnbridgev1.UploadObjectChunk, burnbridgev1.UploadObjectAck],
	jobID, bucket, key string, segmentIdx int, offset, segLen int64, digest string, allowUploadComplete bool, snapshot map[int]meta.BurnObjectSegment) (bool, error) {
	persistSegment := func(state meta.BurnSegmentState, extents []meta.BurnDiscExtent) error {
		if err := b.meta.UpsertBurnObjectSegment(bucket, key, segmentIdx, offset, segLen, digest, state, extents); err != nil {
			return err
		}
		snapshot[segmentIdx] = meta.BurnObjectSegment{
			ByteOffset:  offset,
			ByteSize:    segLen,
			ChecksumMD5: digest,
			State:       state,
			DiscExtents: extents,
		}
		return nil
	}

	ack, err := stream.Recv()
	if err != nil {
		return false, err
	}
	if ack.GetUploadComplete() && !allowUploadComplete {
		_ = persistSegment(meta.BurnSegmentFailed, nil)
		return false, fmt.Errorf("burnbridge: unexpected upload_complete ack before segment %d finished", segmentIdx)
	}
	switch ack.GetSegmentBurnResult() {
	case burnbridgev1.SegmentBurnResult_SEGMENT_BURN_RESULT_FAILED:
		_ = persistSegment(meta.BurnSegmentFailed, nil)
		msg := strings.TrimSpace(ack.GetSegmentBurnError())
		if msg == "" {
			return false, fmt.Errorf("burnbridge: recorder reported segment %d burn failed", segmentIdx)
		}
		return false, fmt.Errorf("burnbridge: recorder reported segment %d burn failed: %s", segmentIdx, msg)
	case burnbridgev1.SegmentBurnResult_SEGMENT_BURN_RESULT_UNSPECIFIED,
		burnbridgev1.SegmentBurnResult_SEGMENT_BURN_RESULT_OK:
	default:
		_ = persistSegment(meta.BurnSegmentFailed, nil)
		return false, fmt.Errorf("burnbridge: unknown segment_burn_result %v for segment %d", ack.GetSegmentBurnResult(), segmentIdx)
	}
	if j := ack.GetJobId(); j != "" && j != jobID {
		_ = persistSegment(meta.BurnSegmentFailed, nil)
		return false, fmt.Errorf("burnbridge: ack job_id mismatch: got %q want %q", j, jobID)
	}
	if ack.GetSegmentIndex() != int32(segmentIdx) {
		_ = persistSegment(meta.BurnSegmentFailed, nil)
		return false, fmt.Errorf("burnbridge: segment_index mismatch: got %d want %d", ack.GetSegmentIndex(), segmentIdx)
	}
	if ack.GetByteOffset() != offset || ack.GetByteSize() != segLen {
		_ = persistSegment(meta.BurnSegmentFailed, nil)
		return false, fmt.Errorf("burnbridge: ack byte range mismatch: got offset=%d size=%d want offset=%d size=%d",
			ack.GetByteOffset(), ack.GetByteSize(), offset, segLen)
	}
	extents := bbProtoDiscExtentsToMeta(ack.GetDiscExtents())
	if err := persistSegment(meta.BurnSegmentSucceeded, extents); err != nil {
		return false, err
	}
	return ack.GetUploadComplete(), nil
}

func (b *BurnBridge) grpcUploadObjectStream(ctx context.Context, jobID, bucket, key string, body io.Reader, _ int64) (int64, *burnbridgev1.UploadObjectAck, error) {
	startedAt := time.Now()
	stream, err := b.grpc.UploadObject(ctx)
	if err != nil {
		return 0, nil, err
	}
	segmentSnapshot, err := b.loadBurnSegmentSnapshot(bucket, key)
	if err != nil {
		return 0, nil, err
	}
	stats := uploadRecoveryStats{}
	uploadCompletedBySegmentAck := false

	segBuf := make([]byte, b.chunkSize)
	offset := int64(0)
	segmentIdx := 0

	sendEOF := func() error {
		return stream.Send(&burnbridgev1.UploadObjectChunk{JobId: jobID, Offset: offset, Eof: true})
	}

	if body == nil {
		if err := sendEOF(); err != nil {
			return 0, nil, err
		}
		if err := stream.CloseSend(); err != nil {
			return 0, nil, err
		}
		final, err := stream.Recv()
		if err != nil {
			return offset, nil, err
		}
		if !final.GetUploadComplete() {
			return offset, nil, fmt.Errorf("burnbridge: expected upload_complete on final ack for empty body")
		}
		return offset, final, nil
	}

	for {
		n, errRead := io.ReadFull(body, segBuf)
		if n == 0 {
			if errRead == io.EOF || errRead == io.ErrUnexpectedEOF {
				break
			}
			return offset, nil, errRead
		}
		if errRead != nil && errRead != io.ErrUnexpectedEOF {
			return offset, nil, errRead
		}
		chunk := segBuf[:n]

		digest := bbSegmentMD5Hex(chunk)
		if err := b.burnMaybeInvalidateSegments(bucket, key, segmentSnapshot, segmentIdx, digest); err != nil {
			return offset, nil, err
		}
		stats.TotalSegments++

		skip := b.burnShouldSkipSegment(segmentSnapshot, segmentIdx, digest, offset, int64(len(chunk)))

		isTailSegment := errRead == io.ErrUnexpectedEOF
		if skip {
			stats.SkippedSegments++
			slog.Warn("burnbridge: skipping chunk already recorded as burned (client retry); not re-sending payload",
				"bucket", bucket, "key", key, "segment", segmentIdx, "offset", offset, "size", len(chunk), "md5", digest)
			ch := &burnbridgev1.UploadObjectChunk{
				JobId: jobID, Offset: offset, Eof: isTailSegment,
				ReusedBurnedBytes: int64(len(chunk)),
			}
			if err := stream.Send(ch); err != nil {
				return offset, nil, err
			}
		} else {
			stats.ReplayedSegments++
			if err := b.meta.UpsertBurnObjectSegment(bucket, key, segmentIdx, offset, int64(len(chunk)), digest, meta.BurnSegmentPending, nil); err != nil {
				return offset, nil, err
			}
			segmentSnapshot[segmentIdx] = meta.BurnObjectSegment{
				ByteOffset:  offset,
				ByteSize:    int64(len(chunk)),
				ChecksumMD5: digest,
				State:       meta.BurnSegmentPending,
				DiscExtents: nil,
			}
			for i := 0; i < len(chunk); {
				end := i + burnbridgeUploadMaxDataPerFrame
				if end > len(chunk) {
					end = len(chunk)
				}
				part := chunk[i:end]
				isLastFrameOfSegment := end == len(chunk)
				frameEOF := isTailSegment && isLastFrameOfSegment
				if err := stream.Send(&burnbridgev1.UploadObjectChunk{JobId: jobID, Offset: offset + int64(i), Data: part, Eof: frameEOF}); err != nil {
					return offset, nil, err
				}
				i = end
			}
		}

		completed, err := b.recvSegmentUploadAck(stream, jobID, bucket, key, segmentIdx, offset, int64(len(chunk)), digest, isTailSegment, segmentSnapshot)
		if err != nil {
			if !skip {
				_ = b.meta.UpsertBurnObjectSegment(bucket, key, segmentIdx, offset, int64(len(chunk)), digest, meta.BurnSegmentFailed, nil)
				segmentSnapshot[segmentIdx] = meta.BurnObjectSegment{
					ByteOffset:  offset,
					ByteSize:    int64(len(chunk)),
					ChecksumMD5: digest,
					State:       meta.BurnSegmentFailed,
					DiscExtents: nil,
				}
			}
			return offset, nil, err
		}
		uploadCompletedBySegmentAck = uploadCompletedBySegmentAck || completed

		offset += int64(len(chunk))
		segmentIdx++
		if errRead == io.ErrUnexpectedEOF {
			break
		}
	}

	trimmedRows, err := b.meta.DeleteBurnObjectSegmentsFromWithCount(bucket, key, segmentIdx)
	if err != nil {
		return offset, nil, err
	}
	stats.TrimmedSegments = trimmedRows

	var final *burnbridgev1.UploadObjectAck
	if !uploadCompletedBySegmentAck {
		if err := sendEOF(); err != nil {
			return offset, nil, err
		}
		if err := stream.CloseSend(); err != nil {
			return offset, nil, err
		}
		final, err = stream.Recv()
		if err != nil {
			return offset, nil, err
		}
		if !final.GetUploadComplete() {
			return offset, nil, fmt.Errorf("burnbridge: expected upload_complete on final ack")
		}
	} else {
		if err := stream.CloseSend(); err != nil {
			return offset, nil, err
		}
		final = &burnbridgev1.UploadObjectAck{
			JobId:          jobID,
			UploadComplete: true,
			BytesReceived:  offset,
		}
	}
	hitRate := 0.0
	if stats.TotalSegments > 0 {
		hitRate = (float64(stats.SkippedSegments) / float64(stats.TotalSegments)) * 100
	}
	slog.Info("burnbridge: object stream finished; final ack received",
		"bucket", bucket, "key", key, "jobId", jobID, "bytes", offset)
	slog.Info("burnbridge: upload recovery stats",
		"bucket", bucket,
		"key", key,
		"jobId", jobID,
		"segments_total", stats.TotalSegments,
		"segments_skipped", stats.SkippedSegments,
		"segments_replayed", stats.ReplayedSegments,
		"segments_trimmed", stats.TrimmedSegments,
		"skip_hit_rate_pct", hitRate,
		"elapsed_ms", time.Since(startedAt).Milliseconds())
	return offset, final, nil
}

func (b *BurnBridge) registerRecorderS3PullSource(ctx context.Context, jobID, bucket, key string, contentLen int64) error {
	if b.recorderS3Endpoint == "" {
		return nil
	}
	if jobID == "" {
		return fmt.Errorf("burnbridge: RegisterS3ObjectPullSource: empty job id")
	}
	src := &burnbridgev1.S3ObjectPullSource{
		EndpointUrl:       b.recorderS3Endpoint,
		Region:            b.recorderS3Region,
		Bucket:            bucket,
		ObjectKey:         key,
		ForcePathStyle:    b.recorderS3PathStyle,
		ContentLengthHint: contentLen,
		PresignedGetUrl:   b.recorderS3PresignedGetURL,
	}
	if b.recorderS3AccessKey != "" || b.recorderS3SecretKey != "" {
		src.Credentials = &burnbridgev1.S3PullCredentials{
			AccessKeyId:     b.recorderS3AccessKey,
			SecretAccessKey: b.recorderS3SecretKey,
			SessionToken:    b.recorderS3SessionToken,
		}
	}
	_, err := b.grpc.RegisterS3ObjectPullSource(ctx, &burnbridgev1.RegisterS3ObjectPullSourceRequest{
		JobId:  jobID,
		Source: src,
	})
	if err != nil {
		return fmt.Errorf("burnbridge RegisterS3ObjectPullSource: %w", err)
	}
	slog.Info("burnbridge: recorder S3 pull source registered", "jobId", jobID, "endpoint", b.recorderS3Endpoint, "bucket", bucket, "key", key)
	return nil
}

func (b *BurnBridge) PutObject(ctx context.Context, input s3response.PutObjectInput) (s3response.PutObjectOutput, error) {
	if input.Bucket == nil || input.Key == nil {
		return s3response.PutObjectOutput{}, fmt.Errorf("bucket/key required")
	}
	select {
	case b.putQueueSem <- struct{}{}:
	case <-ctx.Done():
		return s3response.PutObjectOutput{}, ctx.Err()
	}
	defer func() { <-b.putQueueSem }()

	if b.putObjectTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, b.putObjectTimeout)
		defer cancel()
	}

	bucket := *input.Bucket
	key := *input.Key
	if !b.burnbridgeBucketExists(bucket) {
		return s3response.PutObjectOutput{}, s3err.GetAPIError(s3err.ErrNoSuchBucket)
	}

	if err := b.requireRecorderReady(ctx); err != nil {
		return s3response.PutObjectOutput{}, err
	}

	if key == meta.BurnbridgeFinalizeLayoutObjectKey {
		return s3response.PutObjectOutput{}, s3err.GetAPIError(s3err.ErrAccessDenied)
	}

	if key == meta.BurnbridgeDiscInfoObjectKey {
		return s3response.PutObjectOutput{}, s3err.GetAPIError(s3err.ErrAccessDenied)
	}

	b.putSerialMu.Lock()
	defer b.putSerialMu.Unlock()

	idx := objectLockIndex(bucket, key)
	b.objectLocks[idx].Lock()
	defer b.objectLocks[idx].Unlock()

	var contentLen int64
	if input.ContentLength != nil {
		contentLen = *input.ContentLength
	}

	createResp, err := b.grpc.CreateJob(ctx, &burnbridgev1.CreateJobRequest{
		Bucket:        bucket,
		ObjectKey:     key,
		ContentLength: contentLen,
	})
	if err != nil {
		return s3response.PutObjectOutput{}, err
	}
	jobID := createResp.GetJobId()
	if jobID == "" {
		return s3response.PutObjectOutput{}, fmt.Errorf("burnbridge: empty job id from CreateJob")
	}

	if err := b.registerRecorderS3PullSource(ctx, jobID, bucket, key, contentLen); err != nil {
		return s3response.PutObjectOutput{}, err
	}

	var committed bool
	defer func() {
		if committed || jobID == "" {
			return
		}
		cctx, cancel := context.WithTimeout(context.Background(), b.cancelJobTimeout)
		defer cancel()
		_, _ = b.grpc.CancelJob(cctx, &burnbridgev1.CancelJobRequest{JobId: jobID})
	}()

	offset, uploadResp, err := b.grpcUploadObjectStream(ctx, jobID, bucket, key, input.Body, contentLen)
	if err != nil {
		return s3response.PutObjectOutput{}, err
	}

	finalizeManifest, err := b.buildFinalizeManifest(bucket, key, offset)
	if err != nil {
		return s3response.PutObjectOutput{}, err
	}
	if finalizeManifest == nil {
		slog.Warn("burnbridge: no usable disc extents in SQLite segments; commit without finalize_manifest fallback",
			"bucket", bucket, "key", key, "jobId", jobID, "bytes", offset)
	}

	commitResp, err := b.grpc.CommitJob(ctx, &burnbridgev1.CommitJobRequest{
		JobId:            jobID,
		UdfVolumeLabel:   b.udfLabel,
		FinalizeManifest: finalizeManifest,
	})
	if err != nil {
		return s3response.PutObjectOutput{}, err
	}
	committed = true
	slog.Info("burnbridge: CommitJob sent after full object stream (client transfer complete)",
		"bucket", bucket, "key", key, "jobId", jobID, "status", commitResp.GetStatus(), "bytes", offset)

	etag := quotedETag(uploadResp.GetChecksumMd5())
	checksumMD5 := uploadResp.GetChecksumMd5()
	lm := time.Now().UTC().Format(time.RFC3339Nano)

	committedRec := &meta.BurnbridgeCommittedRecord{
		JobID:        jobID,
		Status:       commitResp.GetStatus(),
		ETag:         etag,
		LastModified: lm,
		Size:         offset,
	}
	if err := b.meta.StoreBurnbridgeCommitted(nil, bucket, key, committedRec); err != nil {
		return s3response.PutObjectOutput{}, err
	}
	b.invalidateFinalizeLayoutTranscript(bucket)

	out := s3response.PutObjectOutput{
		ETag: etag,
		Size: &offset,
	}
	if checksumMD5 != "" {
		out.ChecksumMD5 = &checksumMD5
	}
	return out, nil
}

// DeleteObject is rejected: BurnBridge maps to write-once read-many optical storage.
func (b *BurnBridge) DeleteObject(_ context.Context, input *s3.DeleteObjectInput) (*s3.DeleteObjectOutput, error) {
	if input == nil || input.Bucket == nil || input.Key == nil {
		return nil, fmt.Errorf("bucket/key required")
	}
	if !b.burnbridgeBucketExists(*input.Bucket) {
		return nil, s3err.GetAPIError(s3err.ErrNoSuchBucket)
	}
	return nil, burnbridgeWORMNoDelete
}

// DeleteObjects reports MethodNotAllowed for every key (WORM).
func (b *BurnBridge) DeleteObjects(_ context.Context, input *s3.DeleteObjectsInput) (s3response.DeleteResult, error) {
	if input == nil || input.Bucket == nil || input.Delete == nil {
		return s3response.DeleteResult{}, fmt.Errorf("bucket/delete payload required")
	}
	if !b.burnbridgeBucketExists(*input.Bucket) {
		return s3response.DeleteResult{}, s3err.GetAPIError(s3err.ErrNoSuchBucket)
	}
	code := burnbridgeWORMNoDelete.Code
	msg := burnbridgeWORMNoDelete.Description
	errs := make([]types.Error, 0, len(input.Delete.Objects))
	for _, obj := range input.Delete.Objects {
		if obj.Key == nil {
			continue
		}
		errs = append(errs, types.Error{
			Key:     obj.Key,
			Code:    &code,
			Message: &msg,
		})
	}
	return s3response.DeleteResult{Error: errs}, nil
}

func (b *BurnBridge) buildFinalizeManifest(bucket, key string, objectSize int64) (*burnbridgev1.FinalizeManifest, error) {
	segments, err := b.meta.ListBurnObjectSegments(bucket, key)
	if err != nil {
		return nil, err
	}
	if len(segments) == 0 {
		return &burnbridgev1.FinalizeManifest{
			Files: []*burnbridgev1.FinalizeFile{
				{
					ObjectKey: key,
					FileSize:  objectSize,
				},
			},
		}, nil
	}

	layouts := make([]*burnbridgev1.SegmentLayout, 0, len(segments))
	hasUsableExtent := false
	for idx, seg := range segments {
		if seg.SegmentIndex != idx {
			return nil, fmt.Errorf("burnbridge: finalize manifest segment sequence mismatch for %s/%s: got=%d want=%d", bucket, key, seg.SegmentIndex, idx)
		}
		if seg.State != meta.BurnSegmentSucceeded {
			return nil, fmt.Errorf("burnbridge: finalize manifest segment state not succeeded for %s/%s segment=%d state=%d", bucket, key, seg.SegmentIndex, seg.State)
		}

		extents := make([]*burnbridgev1.DiscExtent, 0, len(seg.DiscExtents))
		for _, ex := range seg.DiscExtents {
			discAddress := strings.TrimSpace(ex.DiscAddress)
			if discAddress == "" || ex.FileSize <= 0 {
				continue
			}
			hasUsableExtent = true
			extents = append(extents, &burnbridgev1.DiscExtent{
				DiscAddress: discAddress,
				FileSize:    ex.FileSize,
			})
		}

		layouts = append(layouts, &burnbridgev1.SegmentLayout{
			SegmentIndex: int32(seg.SegmentIndex),
			ByteOffset:   seg.ByteOffset,
			ByteSize:     seg.ByteSize,
			ChecksumMd5:  seg.ChecksumMD5,
			DiscExtents:  extents,
		})
	}

	if !hasUsableExtent {
		return nil, nil
	}

	return &burnbridgev1.FinalizeManifest{
		Files: []*burnbridgev1.FinalizeFile{
			{
				ObjectKey: key,
				FileSize:  objectSize,
				Segments:  layouts,
			},
		},
	}, nil
}

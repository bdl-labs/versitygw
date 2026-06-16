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
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing/fstest"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/google/uuid"
	"github.com/versity/versitygw/archiveconfig"
	"github.com/versity/versitygw/auth"
	"github.com/versity/versitygw/backend"
	burnbridgev1 "github.com/versity/versitygw/backend/burnbridge/proto"
	meta "github.com/versity/versitygw/backend/meta"
	"github.com/versity/versitygw/s3err"
	"github.com/versity/versitygw/s3response"
	"golang.org/x/sync/singleflight"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	burnbridgeControlAPIVersion   = "v1"
	burnbridgeBucketTypeTagKey    = "burnbridge:bucket-type"
	burnbridgeBucketTypeControl   = "control"
	burnbridgeBucketTypeData      = "data"
	burnbridgeControlBucketTagKey = "burnbridge:control-bucket"
)

type burnbridgeControlAction string

const (
	burnbridgeControlActionDriveInfo      burnbridgeControlAction = "drive-info"
	burnbridgeControlActionDiscInfo       burnbridgeControlAction = "disc-info"
	burnbridgeControlActionFinalizeLayout burnbridgeControlAction = "finalize-layout"
	burnbridgeControlActionCloseDisc      burnbridgeControlAction = "close-disc"
	burnbridgeControlActionMediaRemoved   burnbridgeControlAction = "media-removed"
	burnbridgeControlActionMediaInserted  burnbridgeControlAction = "media-inserted"
	burnbridgeControlActionTrayOpen       burnbridgeControlAction = "tray-open"
	burnbridgeControlActionTrayClose      burnbridgeControlAction = "tray-close"
)

type burnbridgeControlRequest struct {
	Action      burnbridgeControlAction
	RequestTime int64
	RequestID   string
	Key         string
}

type burnbridgeControlError struct {
	Code    string `json:"code,omitempty"`
	Message string `json:"message,omitempty"`
}

type burnbridgeControlEnvelope struct {
	APIVersion  string                  `json:"apiVersion"`
	Action      string                  `json:"action"`
	RequestID   string                  `json:"requestId"`
	RequestTime int64                   `json:"requestTime"`
	Bucket      string                  `json:"bucket"`
	Ok          bool                    `json:"ok"`
	Data        any                     `json:"data,omitempty"`
	Error       *burnbridgeControlError `json:"error,omitempty"`
}

type burnbridgeControlPayload struct {
	Raw          []byte
	LastModified time.Time
}

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

	AllowCreateBucketBinding     bool
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

func burnbridgeInvalidRequest(message string) error {
	apiErr := s3err.GetAPIError(s3err.ErrInvalidRequest)
	apiErr.Description = message
	return apiErr
}

func burnbridgeControlActionFromString(value string) (burnbridgeControlAction, bool) {
	switch strings.TrimSpace(value) {
	case string(burnbridgeControlActionDriveInfo):
		return burnbridgeControlActionDriveInfo, true
	case string(burnbridgeControlActionDiscInfo):
		return burnbridgeControlActionDiscInfo, true
	case string(burnbridgeControlActionFinalizeLayout):
		return burnbridgeControlActionFinalizeLayout, true
	case string(burnbridgeControlActionCloseDisc):
		return burnbridgeControlActionCloseDisc, true
	case string(burnbridgeControlActionMediaRemoved):
		return burnbridgeControlActionMediaRemoved, true
	case string(burnbridgeControlActionMediaInserted):
		return burnbridgeControlActionMediaInserted, true
	case string(burnbridgeControlActionTrayOpen):
		return burnbridgeControlActionTrayOpen, true
	case string(burnbridgeControlActionTrayClose):
		return burnbridgeControlActionTrayClose, true
	default:
		return "", false
	}
}

func (b *BurnBridge) parseControlRequestForBucket(bucket, key string) (*burnbridgeControlRequest, bool, error) {
	trimmed := strings.Trim(strings.TrimSpace(key), "/")
	parts := strings.Split(trimmed, "/")
	if len(parts) != 2 || parts[0] != burnbridgeControlAPIVersion {
		return nil, false, nil
	}
	if !b.isDriveControlBucket(bucket) {
		return nil, false, nil
	}
	action, ok := burnbridgeControlActionFromString(parts[1])
	if !ok {
		return nil, true, burnbridgeInvalidRequest(fmt.Sprintf("unsupported burnbridge control action %q", parts[1]))
	}
	return &burnbridgeControlRequest{
		Action:      action,
		RequestTime: time.Now().UTC().UnixMilli(),
		RequestID:   uuid.NewString(),
		Key:         trimmed,
	}, true, nil
}

func burnbridgeControlRequiresExistingBucket(action burnbridgeControlAction) bool {
	switch action {
	case burnbridgeControlActionDriveInfo, burnbridgeControlActionMediaRemoved, burnbridgeControlActionMediaInserted,
		burnbridgeControlActionTrayOpen, burnbridgeControlActionTrayClose:
		return false
	default:
		return true
	}
}

func buildBurnbridgeControlPayload(req *burnbridgeControlRequest, bucket string, ok bool, data any, controlErr *burnbridgeControlError, lastModified time.Time) (burnbridgeControlPayload, error) {
	raw, err := json.Marshal(burnbridgeControlEnvelope{
		APIVersion:  burnbridgeControlAPIVersion,
		Action:      string(req.Action),
		RequestID:   req.RequestID,
		RequestTime: req.RequestTime,
		Bucket:      bucket,
		Ok:          ok,
		Data:        data,
		Error:       controlErr,
	})
	if err != nil {
		return burnbridgeControlPayload{}, err
	}

	return burnbridgeControlPayload{
		Raw:          raw,
		LastModified: lastModified,
	}, nil
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
	// finalizeGroup deduplicates concurrent FinalizeLayout requests for the same bucket/closeDisc tuple.
	finalizeGroup singleflight.Group
	// recorderStateGroup deduplicates concurrent TestUnitReady probes.
	recorderStateGroup singleflight.Group
	// importedStateGroup deduplicates concurrent imported-state refreshes for the same bucket.
	importedStateGroup singleflight.Group

	recorderS3Endpoint        string
	recorderS3Region          string
	recorderS3AccessKey       string
	recorderS3SecretKey       string
	recorderS3SessionToken    string
	recorderS3PathStyle       bool
	recorderS3PresignedGetURL string

	// activeBucket is the S3 bucket name for this session, derived from the disc volume label at New().
	activeBucket                string
	volumeLabelRaw              string
	allowBucketBinding          bool
	stateMu                     sync.Mutex
	lastDiscSerialHex           string
	lastReadyVolumeLabel        string
	lastNoDiscBackupPath        string
	lastNoDiscBackupBucket      string
	lastNoDiscObservedAt        time.Time
	noDiscLatched               bool
	lastRecorderReadyBucket     string
	lastRecorderWritableState   string
	lastRecorderReadyObservedAt time.Time
	lastDriveControlBucket      string
	lastDriveSerialNumber       string
	lastDriveVendorID           string
	lastDriveProductID          string
	lastDriveProductRevision    string
	lastDriveIsMMCUnit          bool
	lastDriveObservedAt         time.Time
	importedBucketState         map[string]bool
	pendingImportedConvergence  map[string]bool
	statusWatchCancel           context.CancelFunc
	statusWatchEnabled          atomic.Bool
	metaDBPath                  string
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

	defaultChunkSizeBytes            = 1 << 20
	burnbridgeImplicitSingleUploadID = "__single_put__"
	burnbridgeMultipartInitAttrPref  = "bb-multipart-init:"
	burnbridgeMultipartMetaAttr      = "mp-metadata"
	burnbridgeMultipartInternalPref  = ".__bbmeta__/multipart/"
	burnbridgeRecorderETagMetaKey    = "x-burn-etag"

	listDefaultMaxKeys          int32 = 1000
	defaultPutQueueLimit              = 512
	burnbridgeACLAttribute            = "acl"
	noDiscProbeCooldown               = 3 * time.Second
	recorderReadyRetryAttempts        = 5
	recorderReadyRetryDelay           = 750 * time.Millisecond
	mountedReadFallbackMaxFiles       = 500000
)

type burnbridgeMultipartInitState struct {
	Metadata          map[string]string       `json:"metadata,omitempty"`
	ChecksumAlgorithm types.ChecksumAlgorithm `json:"checksumAlgorithm,omitempty"`
	ChecksumType      types.ChecksumType      `json:"checksumType,omitempty"`
}

type burnbridgeUploadStreamOptions struct {
	AllowReuse                     bool
	AllowInvalidateOnFirstMismatch bool
}

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

var burnbridgeControlBucketReadOnly = s3err.APIError{
	Code:           "MethodNotAllowed",
	Description:    "BurnBridge drive control bucket is read-only and only exposes v1 control objects.",
	HTTPStatusCode: http.StatusMethodNotAllowed,
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

func sanitizeDriveControlBucketFromSerial(serial string) (string, error) {
	trimmed := strings.TrimSpace(serial)
	if trimmed == "" {
		return "", fmt.Errorf("empty drive serial number")
	}
	name, err := sanitizeS3BucketFromVolumeLabel(trimmed)
	if err == nil {
		return name, nil
	}
	prefixed, prefixedErr := sanitizeS3BucketFromVolumeLabel("drive-" + trimmed)
	if prefixedErr != nil {
		return "", err
	}
	return prefixed, nil
}

func defaultBucketACL(owner string) auth.ACL {
	trimmedOwner := strings.TrimSpace(owner)
	return auth.ACL{
		Owner: trimmedOwner,
		Grantees: []auth.Grantee{
			{
				Permission: auth.PermissionFullControl,
				Access:     trimmedOwner,
				Type:       types.TypeCanonicalUser,
			},
		},
	}
}

func (b *BurnBridge) storeBucketACL(bucket string, data []byte) error {
	if strings.TrimSpace(bucket) == "" {
		return s3err.GetAPIError(s3err.ErrInvalidBucketName)
	}
	if len(data) == 0 {
		return fmt.Errorf("bucket acl is empty")
	}
	if _, err := auth.ParseACL(data); err != nil {
		return fmt.Errorf("parse bucket acl: %w", err)
	}
	return b.meta.StoreAttribute(nil, bucket, "", burnbridgeACLAttribute, data)
}

func (b *BurnBridge) loadBucketACL(bucket string) (auth.ACL, []byte, error) {
	trimmedBucket := strings.TrimSpace(bucket)
	if trimmedBucket == "" {
		return auth.ACL{}, nil, s3err.GetAPIError(s3err.ErrInvalidBucketName)
	}

	raw, err := b.meta.RetrieveAttribute(nil, trimmedBucket, "", burnbridgeACLAttribute)
	if err == nil {
		acl, parseErr := auth.ParseACL(raw)
		if parseErr != nil {
			return auth.ACL{}, nil, fmt.Errorf("parse bucket acl: %w", parseErr)
		}
		return acl, raw, nil
	}
	if !errors.Is(err, meta.ErrNoSuchKey) {
		return auth.ACL{}, nil, err
	}

	legacy := defaultBucketACL(trimmedBucket)
	legacyRaw, marshalErr := json.Marshal(legacy)
	if marshalErr != nil {
		return auth.ACL{}, nil, fmt.Errorf("marshal legacy bucket acl: %w", marshalErr)
	}
	return legacy, legacyRaw, nil
}

func (b *BurnBridge) ensureBucketACLForOwner(bucket, owner string) (auth.ACL, []byte, error) {
	acl, raw, err := b.loadBucketACL(bucket)
	if err != nil {
		return auth.ACL{}, nil, err
	}

	trimmedOwner := strings.TrimSpace(owner)
	trimmedBucket := strings.TrimSpace(bucket)
	if trimmedOwner == "" || !strings.EqualFold(strings.TrimSpace(acl.Owner), trimmedBucket) {
		return acl, raw, nil
	}

	bootstrap := defaultBucketACL(trimmedOwner)
	bootstrapRaw, marshalErr := json.Marshal(bootstrap)
	if marshalErr != nil {
		return auth.ACL{}, nil, fmt.Errorf("marshal bucket acl bootstrap: %w", marshalErr)
	}
	if err := b.storeBucketACL(trimmedBucket, bootstrapRaw); err != nil {
		return auth.ACL{}, nil, err
	}
	return bootstrap, bootstrapRaw, nil
}

func (b *BurnBridge) bootstrapBucketACL(bucket, owner string) error {
	trimmedBucket := strings.TrimSpace(bucket)
	trimmedOwner := strings.TrimSpace(owner)
	if trimmedBucket == "" || trimmedOwner == "" {
		return nil
	}

	_, err := b.meta.RetrieveAttribute(nil, trimmedBucket, "", burnbridgeACLAttribute)
	if err == nil {
		return nil
	}
	if !errors.Is(err, meta.ErrNoSuchKey) {
		return err
	}

	raw, marshalErr := json.Marshal(defaultBucketACL(trimmedOwner))
	if marshalErr != nil {
		return fmt.Errorf("marshal bucket acl bootstrap: %w", marshalErr)
	}
	return b.storeBucketACL(trimmedBucket, raw)
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

func loadDiscBucketBinding(metaStore meta.SqlMeta, rawVolume string) (*meta.BurnbridgeDiscBucketBindingDocument, bool) {
	doc, err := metaStore.GetBurnbridgeDiscBucketBinding(rawVolume)
	if err != nil {
		return nil, false
	}
	return doc, true
}

func persistDiscBucketBinding(metaStore meta.SqlMeta, rawVolume, bucket, udfVolumeLabel string) error {
	return metaStore.StoreBurnbridgeDiscBucketBinding(&meta.BurnbridgeDiscBucketBindingDocument{
		ProbeVolumeLabel: rawVolume,
		Bucket:           bucket,
		UdfVolumeLabel:   udfVolumeLabel,
	})
}

func (b *BurnBridge) clearStaleBlankDiscBinding(binding *meta.BurnbridgeDiscBucketBindingDocument) (bool, error) {
	if binding == nil {
		return false, nil
	}
	bucket := strings.TrimSpace(binding.Bucket)
	if bucket == "" {
		return false, nil
	}

	committed, err := b.meta.ListCommittedObjects(bucket)
	if err != nil {
		return false, fmt.Errorf("burnbridge inspect blank-disc binding bucket %s: %w", bucket, err)
	}
	if len(committed) == 0 {
		return false, nil
	}

	if err := b.meta.DeleteBurnbridgeBucket(bucket); err != nil {
		return false, fmt.Errorf("burnbridge delete stale blank-disc bucket metadata %s: %w", bucket, err)
	}
	if err := b.meta.DeleteBurnbridgeDiscBucketBindings(bucket); err != nil {
		return false, fmt.Errorf("burnbridge delete stale blank-disc bucket bindings %s: %w", bucket, err)
	}
	slog.Info("burnbridge: cleared stale blank-disc bucket binding with committed metadata",
		"bucket", bucket,
		"probe_volume_label", strings.TrimSpace(binding.ProbeVolumeLabel),
		"committed_count", len(committed))
	return true, nil
}

func discInfoDocFromProto(
	s3Bucket string,
	resp *burnbridgev1.TestUnitReadyResponse,
	discResp *burnbridgev1.GetDiscInfoResponse,
	finalizeDoc *meta.BurnbridgeFinalizeLayoutDocument,
) *meta.BurnbridgeDiscInfoDocument {
	if resp == nil || !resp.GetReady() {
		return nil
	}

	doc := &meta.BurnbridgeDiscInfoDocument{
		Bucket:                        s3Bucket,
		VolumeLabel:                   strings.TrimSpace(resp.GetVolumeLabel()),
		UpdatedAt:                     time.Now().UTC().Format(time.RFC3339Nano),
		DiscSerialNumberHex:           strings.TrimSpace(resp.GetDiscSerialNumberHex()),
		TotalCapacityBytes:            resp.GetTotalCapacityBytes(),
		FreeCapacityBytes:             resp.GetFreeCapacityBytes(),
		UsedCapacityBytes:             resp.GetUsedCapacityBytes(),
		WritableCapacityBytes:         resp.GetWritableCapacityBytes(),
		FinalizeReserveBytes:          resp.GetFinalizeReserveBytes(),
		MediaType:                     strings.TrimSpace(resp.GetMediaType()),
		BlockSizeBytes:                resp.GetBlockSizeBytes(),
		TotalBlocks:                   resp.GetTotalBlocks(),
		FreeBlocks:                    resp.GetFreeBlocks(),
		RecordableCapacityBlocks:      resp.GetRecordableCapacityBlocks(),
		TrackNextWritableAddress:      resp.GetTrackNextWritableAddress(),
		TrackNextWritableAddressValid: resp.GetTrackNextWritableAddressValid(),
		WritableState:                 strings.TrimSpace(resp.GetWritableState()),
	}

	if disc := discResp.GetDisc(); disc != nil {
		if serial := strings.TrimSpace(disc.GetDiscSerialNumberHex()); serial != "" {
			doc.DiscSerialNumberHex = serial
		}
		if mediaType := strings.TrimSpace(disc.GetProfileName()); mediaType != "" {
			doc.MediaType = mediaType
		}
		if blockSize := disc.GetBlockSizeBytes(); blockSize > 0 {
			doc.BlockSizeBytes = blockSize
		}
		if totalBlocks := disc.GetTotalBlocks(); totalBlocks > 0 {
			doc.TotalBlocks = totalBlocks
		}
		if freeBlocks := disc.GetFreeBlocks(); freeBlocks > 0 {
			doc.FreeBlocks = freeBlocks
		}
		if recordableCapacityBlocks := disc.GetRecordableCapacityBlocks(); recordableCapacityBlocks > 0 {
			doc.RecordableCapacityBlocks = recordableCapacityBlocks
		}
		if trackNwa := disc.GetTrackNextWritableAddress(); trackNwa > 0 {
			doc.TrackNextWritableAddress = trackNwa
		}
		if disc.GetTrackNextWritableAddressValid() {
			doc.TrackNextWritableAddressValid = true
		}
		if writableState := strings.TrimSpace(disc.GetWritableState()); writableState != "" {
			doc.WritableState = writableState
		}
		if discStatusName := strings.TrimSpace(disc.GetDiscStatusName()); discStatusName != "" {
			doc.DiscStatusName = discStatusName
		}
		if mediaCapacity := disc.GetMediaCapacity(); mediaCapacity > 0 {
			doc.TotalCapacityBytes = mediaCapacity
		}
		if mediaFree := disc.GetMediaFreeSpace(); mediaFree > 0 {
			doc.FreeCapacityBytes = mediaFree
		}
		if mediaUsed := disc.GetMediaUsedSpace(); mediaUsed > 0 {
			doc.UsedCapacityBytes = mediaUsed
		}
		if session := disc.GetSessionDiscId(); session != nil {
			doc.SessionIsFinalized = session.GetIsFinalized()
			doc.SessionTempDiscId = strings.TrimSpace(session.GetTempDiscId())
		}
	}

	if finalizeDoc != nil {
		doc.LayoutStatus = strings.TrimSpace(finalizeDoc.RecorderStatus)
		doc.LayoutMessage = strings.TrimSpace(finalizeDoc.RecorderMessage)
		doc.LayoutCompletedAtUtc = strings.TrimSpace(finalizeDoc.CompletedAtUtc)
		doc.LayoutCloseDisc = finalizeDoc.CloseDisc
	}

	return doc
}

func driveInfoDocFromProto(bucket string, drive *burnbridgev1.OpticalDriveIdentity) *meta.BurnbridgeDriveInfoDocument {
	if drive == nil {
		return nil
	}
	serial := strings.TrimSpace(drive.GetSerialNumber())
	if serial == "" {
		return nil
	}
	controlBucket, err := sanitizeDriveControlBucketFromSerial(serial)
	if err != nil {
		return nil
	}
	return &meta.BurnbridgeDriveInfoDocument{
		Bucket:          strings.TrimSpace(bucket),
		ControlBucket:   controlBucket,
		UpdatedAt:       time.Now().UTC().Format(time.RFC3339Nano),
		VendorID:        strings.TrimSpace(drive.GetVendorId()),
		ProductID:       strings.TrimSpace(drive.GetProductId()),
		ProductRevision: strings.TrimSpace(drive.GetProductRevision()),
		SerialNumber:    serial,
		IsMMCUnit:       drive.GetIsMmcUnit(),
	}
}

func readFinalizeLayoutTranscript(metaStore meta.SqlMeta, bucket string, objectKey string) *meta.BurnbridgeFinalizeLayoutDocument {
	raw, err := metaStore.GetBurnbridgeFinalizeLayoutJSON(bucket, objectKey)
	if err != nil || len(raw) == 0 {
		return nil
	}

	var doc meta.BurnbridgeFinalizeLayoutDocument
	if err := json.Unmarshal(raw, &doc); err != nil {
		return nil
	}
	return &doc
}

func (b *BurnBridge) cacheDriveInfo(doc *meta.BurnbridgeDriveInfoDocument) {
	if doc == nil {
		return
	}
	b.stateMu.Lock()
	defer b.stateMu.Unlock()
	b.lastDriveControlBucket = strings.TrimSpace(doc.ControlBucket)
	b.lastDriveSerialNumber = strings.TrimSpace(doc.SerialNumber)
	b.lastDriveVendorID = strings.TrimSpace(doc.VendorID)
	b.lastDriveProductID = strings.TrimSpace(doc.ProductID)
	b.lastDriveProductRevision = strings.TrimSpace(doc.ProductRevision)
	b.lastDriveIsMMCUnit = doc.IsMMCUnit
	if strings.TrimSpace(doc.UpdatedAt) != "" {
		if t, err := time.Parse(time.RFC3339Nano, doc.UpdatedAt); err == nil {
			b.lastDriveObservedAt = t.UTC()
		} else if t, err := time.Parse(time.RFC3339, doc.UpdatedAt); err == nil {
			b.lastDriveObservedAt = t.UTC()
		} else {
			b.lastDriveObservedAt = time.Now().UTC()
		}
	} else {
		b.lastDriveObservedAt = time.Now().UTC()
	}
}

func (b *BurnBridge) cachedDriveInfoDoc() *meta.BurnbridgeDriveInfoDocument {
	b.stateMu.Lock()
	defer b.stateMu.Unlock()
	if strings.TrimSpace(b.lastDriveControlBucket) == "" {
		return nil
	}
	updatedAt := b.lastDriveObservedAt
	if updatedAt.IsZero() {
		updatedAt = time.Now().UTC()
	}
	return &meta.BurnbridgeDriveInfoDocument{
		Bucket:          strings.TrimSpace(b.lastDriveControlBucket),
		ControlBucket:   strings.TrimSpace(b.lastDriveControlBucket),
		UpdatedAt:       updatedAt.Format(time.RFC3339Nano),
		VendorID:        strings.TrimSpace(b.lastDriveVendorID),
		ProductID:       strings.TrimSpace(b.lastDriveProductID),
		ProductRevision: strings.TrimSpace(b.lastDriveProductRevision),
		SerialNumber:    strings.TrimSpace(b.lastDriveSerialNumber),
		IsMMCUnit:       b.lastDriveIsMMCUnit,
	}
}

func (b *BurnBridge) driveControlBucketHint() (string, bool) {
	b.stateMu.Lock()
	defer b.stateMu.Unlock()
	controlBucket := strings.TrimSpace(b.lastDriveControlBucket)
	if controlBucket == "" {
		return "", false
	}
	return controlBucket, true
}

func (b *BurnBridge) isDriveControlBucket(bucket string) bool {
	trimmedBucket := strings.TrimSpace(bucket)
	if trimmedBucket == "" {
		return false
	}
	if controlBucket, ok := b.driveControlBucketHint(); ok && strings.EqualFold(trimmedBucket, controlBucket) {
		return true
	}
	if !b.meta.IsOpen() {
		return false
	}
	if raw, err := b.meta.GetBurnbridgeDriveInfoJSON(trimmedBucket); err == nil && len(raw) > 0 {
		doc := parseDriveInfoControlDocument(raw)
		if doc != nil && strings.EqualFold(strings.TrimSpace(doc.ControlBucket), trimmedBucket) {
			b.cacheDriveInfo(doc)
			return true
		}
	}
	return false
}

func (b *BurnBridge) refreshDriveInfoDocument(ctx context.Context) ([]byte, *meta.BurnbridgeDriveInfoDocument, error) {
	if b.grpc == nil {
		return nil, nil, fmt.Errorf("burnbridge: recorder grpc client unavailable")
	}
	discResp, err := b.grpc.GetDiscInfo(ctx, &burnbridgev1.GetDiscInfoRequest{
		IncludeSessionDiscId: false,
		IncludeDriveIdentity: true,
	})
	if err != nil {
		return nil, nil, err
	}
	doc := driveInfoDocFromProto("", discResp.GetDrive())
	if doc == nil {
		return nil, nil, s3err.GetAPIError(s3err.ErrNoSuchKey)
	}
	doc.Bucket = doc.ControlBucket
	b.cacheDriveInfo(doc)
	if err := b.meta.StoreBurnbridgeDriveInfo(doc); err != nil {
		return nil, nil, err
	}
	raw, err := json.Marshal(doc)
	if err != nil {
		return nil, nil, err
	}
	return raw, doc, nil
}

func (b *BurnBridge) ensureDriveControlBucket(ctx context.Context) (string, error) {
	if controlBucket, ok := b.driveControlBucketHint(); ok {
		return controlBucket, nil
	}
	_, doc, err := b.refreshDriveInfoDocument(ctx)
	if err != nil {
		if cached := b.cachedDriveInfoDoc(); cached != nil {
			return strings.TrimSpace(cached.ControlBucket), nil
		}
		return "", err
	}
	return strings.TrimSpace(doc.ControlBucket), nil
}

func (b *BurnBridge) cachedDriveControlBucket() (string, bool) {
	if controlBucket, ok := b.driveControlBucketHint(); ok {
		return controlBucket, true
	}
	if cached := b.cachedDriveInfoDoc(); cached != nil {
		controlBucket := strings.TrimSpace(cached.ControlBucket)
		return controlBucket, controlBucket != ""
	}
	return "", false
}

func (b *BurnBridge) resolveActiveBucketForControl(ctx context.Context) (string, error) {
	_ = b.ensureActiveBucketLoaded(ctx)
	if b.noDiscLatchedState() {
		return "", s3err.GetAPIError(s3err.ErrNoSuchBucket)
	}
	if mountedBucket, ok := b.mountedFallbackBucketHint(); ok && strings.TrimSpace(mountedBucket) != "" {
		return strings.TrimSpace(mountedBucket), nil
	}
	activeBucket := strings.TrimSpace(b.activeBucket)
	if activeBucket == "" {
		return "", s3err.GetAPIError(s3err.ErrNoSuchBucket)
	}
	if !b.burnbridgeBucketExists(activeBucket) {
		return "", s3err.GetAPIError(s3err.ErrNoSuchBucket)
	}
	return activeBucket, nil
}

func (b *BurnBridge) resolveControlPayloadBucket(ctx context.Context, requestBucket string, req *burnbridgeControlRequest) (payloadBucket string, targetBucket string, err error) {
	payloadBucket = strings.TrimSpace(requestBucket)
	if req == nil {
		return payloadBucket, "", fmt.Errorf("burnbridge: control request is required")
	}
	if !b.isDriveControlBucket(payloadBucket) {
		return payloadBucket, payloadBucket, nil
	}
	switch req.Action {
	case burnbridgeControlActionDiscInfo:
		targetBucket, err = b.resolveActiveBucketForControl(ctx)
		if err != nil {
			return payloadBucket, payloadBucket, nil
		}
		return payloadBucket, targetBucket, nil
	case burnbridgeControlActionFinalizeLayout, burnbridgeControlActionCloseDisc:
		targetBucket, err = b.resolveActiveBucketForControl(ctx)
		if err != nil {
			return payloadBucket, "", err
		}
		return payloadBucket, targetBucket, nil
	default:
		return payloadBucket, payloadBucket, nil
	}
}

func (b *BurnBridge) controlRequestBucketAllowed(bucket string, req *burnbridgeControlRequest) bool {
	trimmedBucket := strings.TrimSpace(bucket)
	if trimmedBucket == "" || req == nil {
		return false
	}
	return b.isDriveControlBucket(trimmedBucket)
}

func (b *BurnBridge) refreshDiscInfoDocument(ctx context.Context, bucket string) ([]byte, *meta.BurnbridgeDiscInfoDocument, error) {
	if err := b.requireRecorderReady(ctx); err != nil {
		return nil, nil, err
	}

	resp, err := b.grpc.TestUnitReady(ctx, &burnbridgev1.TestUnitReadyRequest{})
	if err != nil {
		return nil, nil, err
	}
	if resp == nil || !resp.GetReady() {
		return nil, nil, s3err.GetAPIError(s3err.ErrNoSuchKey)
	}

	discResp, err := b.grpc.GetDiscInfo(ctx, &burnbridgev1.GetDiscInfoRequest{
		IncludeSessionDiscId: true,
		IncludeDriveIdentity: false,
	})
	if err != nil {
		return nil, nil, err
	}

	includeFinalizeTranscript := strings.EqualFold(strings.TrimSpace(bucket), strings.TrimSpace(b.activeBucket))
	var finalizeDoc *meta.BurnbridgeFinalizeLayoutDocument
	if includeFinalizeTranscript {
		finalizeDoc = readFinalizeLayoutTranscript(b.meta, bucket, meta.BurnbridgeFinalizeLayoutObjectKey)
	}
	doc := discInfoDocFromProto(bucket, resp, discResp, finalizeDoc)
	if doc == nil {
		return nil, nil, s3err.GetAPIError(s3err.ErrNoSuchKey)
	}

	if includeFinalizeTranscript {
		if err := b.meta.StoreBurnbridgeDiscInfo(doc); err != nil {
			return nil, nil, err
		}
	}

	raw, err := json.Marshal(doc)
	if err != nil {
		return nil, nil, err
	}

	return raw, doc, nil
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

func readyResponseIndicatesNoDisc(resp *burnbridgev1.TestUnitReadyResponse) bool {
	if resp == nil {
		return false
	}
	if resp.GetReady() {
		return false
	}
	reasonCode, _ := parseReadyReason(resp.GetMessage())
	return strings.EqualFold(strings.TrimSpace(reasonCode), "NoDisc")
}

func readyResponseIndicatesBlankWritable(resp *burnbridgev1.TestUnitReadyResponse) bool {
	if resp == nil || !resp.GetReady() {
		return false
	}
	return strings.EqualFold(strings.TrimSpace(resp.GetWritableState()), "Blank")
}

func ensureWritableCapacity(doc *meta.BurnbridgeDiscInfoDocument, contentLen int64) error {
	if doc == nil || contentLen <= 0 {
		return nil
	}
	if doc.WritableCapacityBytes <= 0 && doc.TotalCapacityBytes <= 0 && doc.FreeCapacityBytes <= 0 {
		return nil
	}
	if doc.WritableCapacityBytes <= 0 {
		return s3err.GetAPIError(s3err.ErrNoSpaceLeftOnDevice)
	}
	if contentLen > doc.WritableCapacityBytes {
		return s3err.GetAPIError(s3err.ErrNoSpaceLeftOnDevice)
	}
	return nil
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
	if binding, ok := loadDiscBucketBinding(metaStore, rawVol); ok {
		if strings.TrimSpace(binding.Bucket) != "" {
			activeBucket = strings.TrimSpace(binding.Bucket)
		}
		if strings.TrimSpace(opts.UDFVolumeLabel) == "" && strings.TrimSpace(binding.UdfVolumeLabel) != "" {
			opts.UDFVolumeLabel = strings.TrimSpace(binding.UdfVolumeLabel)
		}
	}
	if activeBucket != "" {
		if doc := discInfoDocFromProto(activeBucket, turResp, nil, readFinalizeLayoutTranscript(metaStore, activeBucket, meta.BurnbridgeFinalizeLayoutObjectKey)); doc != nil {
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
			slog.Warn("burnbridge: startup grpc ping failed; continuing in degraded mode",
				"error", pingErr)
		}
	}

	readMount := strings.TrimSpace(opts.ReadMountPath)
	if readMount == "" {
		readMount = strings.TrimSpace(sharedReadMountPath())
	}
	if readMount != "" {
		readMount = filepath.Clean(readMount)
	}

	udfLabel := strings.TrimSpace(opts.UDFVolumeLabel)
	if udfLabel == "" {
		udfLabel = strings.TrimSpace(rawVol)
	}

	bridge := &BurnBridge{
		meta:             metaStore,
		grpc:             client,
		grpcConn:         conn,
		chunkSize:        opts.ChunkSize,
		udfLabel:         udfLabel,
		readMount:        readMount,
		cancelJobTimeout: opts.CancelJobTimeout,
		putObjectTimeout: opts.PutObjectTimeout,

		activeBucket:       activeBucket,
		volumeLabelRaw:     rawVol,
		allowBucketBinding: opts.AllowCreateBucketBinding,

		recorderS3Endpoint:         strings.TrimSpace(opts.RecorderS3Endpoint),
		recorderS3Region:           strings.TrimSpace(opts.RecorderS3Region),
		recorderS3AccessKey:        opts.RecorderS3AccessKey,
		recorderS3SecretKey:        opts.RecorderS3SecretKey,
		recorderS3SessionToken:     opts.RecorderS3SessionToken,
		recorderS3PathStyle:        opts.RecorderS3ForcePathStyle,
		recorderS3PresignedGetURL:  strings.TrimSpace(opts.RecorderS3PresignedGetURL),
		putQueueSem:                make(chan struct{}, defaultPutQueueLimit),
		importedBucketState:        make(map[string]bool),
		pendingImportedConvergence: make(map[string]bool),
		metaDBPath:                 opts.DBPath,
	}
	if _, _, err := bridge.refreshDriveInfoDocument(context.Background()); err != nil {
		slog.Warn("burnbridge: startup drive identity probe failed; virtual control bucket will appear after drive-info succeeds",
			"error", err)
	}
	if strings.TrimSpace(activeBucket) != "" {
		if err := bridge.syncImportedBucketState(context.Background(), activeBucket); err != nil {
			slog.Warn("burnbridge: startup sync imported bucket state failed; continuing with current runtime state",
				"bucket", activeBucket,
				"error", err)
		}
	}
	bridge.startRecorderStatusWatcher()
	return bridge, nil
}

func sharedReadMountPath() string {
	cfg, _, err := archiveconfig.Load("")
	if err != nil {
		return ""
	}
	return strings.TrimSpace(cfg.OpticalArchive.ReadMountPath)
}

func normalizeRuntimeBucket(bucket string) string {
	return strings.ToLower(strings.TrimSpace(bucket))
}

func (b *BurnBridge) markImportedBucketSynced(bucket string) {
	normalized := normalizeRuntimeBucket(bucket)
	if normalized == "" {
		return
	}

	b.stateMu.Lock()
	defer b.stateMu.Unlock()
	if b.importedBucketState == nil {
		b.importedBucketState = make(map[string]bool)
	}
	b.importedBucketState[normalized] = true
	if b.pendingImportedConvergence != nil {
		delete(b.pendingImportedConvergence, normalized)
	}
}

func (b *BurnBridge) importedBucketSynced(bucket string) bool {
	normalized := normalizeRuntimeBucket(bucket)
	if normalized == "" {
		return false
	}

	b.stateMu.Lock()
	defer b.stateMu.Unlock()
	return b.importedBucketState[normalized]
}

func (b *BurnBridge) resetImportedBucketSyncState() {
	b.stateMu.Lock()
	defer b.stateMu.Unlock()
	b.importedBucketState = make(map[string]bool)
	b.pendingImportedConvergence = make(map[string]bool)
}

func (b *BurnBridge) markImportedBucketConvergencePending(bucket string) {
	normalized := normalizeRuntimeBucket(bucket)
	if normalized == "" {
		return
	}

	b.stateMu.Lock()
	defer b.stateMu.Unlock()
	if b.pendingImportedConvergence == nil {
		b.pendingImportedConvergence = make(map[string]bool)
	}
	b.pendingImportedConvergence[normalized] = true
}

func (b *BurnBridge) importedBucketConvergencePending(bucket string) bool {
	normalized := normalizeRuntimeBucket(bucket)
	if normalized == "" {
		return false
	}

	b.stateMu.Lock()
	defer b.stateMu.Unlock()
	return b.pendingImportedConvergence[normalized]
}

func (b *BurnBridge) runRecorderStateProbe(ctx context.Context) error {
	_, err, _ := b.recorderStateGroup.Do("test-unit-ready", func() (interface{}, error) {
		return nil, b.requireRecorderReady(ctx)
	})
	return err
}

func (b *BurnBridge) startRecorderStatusWatcher() {
	if b.grpc == nil || b.statusWatchEnabled.Load() {
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	b.statusWatchCancel = cancel
	b.statusWatchEnabled.Store(true)

	go b.runRecorderStatusWatcher(ctx)
}

func (b *BurnBridge) runRecorderStatusWatcher(ctx context.Context) {
	defer b.statusWatchEnabled.Store(false)

	for {
		if ctx.Err() != nil {
			return
		}

		err := b.watchRecorderStatusStream(ctx)
		if err == nil || ctx.Err() != nil {
			return
		}
		if isGRPCUnimplemented(err) {
			slog.Info("burnbridge: recorder WatchUnitStatus unavailable; continuing with pull-based readiness fallback")
			return
		}

		slog.Warn("burnbridge: recorder status stream ended; retrying",
			"error", err)

		select {
		case <-ctx.Done():
			return
		case <-time.After(2 * time.Second):
		}
	}
}

func (b *BurnBridge) watchRecorderStatusStream(ctx context.Context) error {
	stream, err := b.grpc.WatchUnitStatus(ctx, &burnbridgev1.WatchUnitStatusRequest{
		IncludeInitialSnapshot: true,
	})
	if err != nil {
		return err
	}

	slog.Info("burnbridge: recorder status stream connected")
	for {
		event, err := stream.Recv()
		if err != nil {
			return err
		}
		if err := b.applyRecorderStatusEvent(event); err != nil {
			slog.Warn("burnbridge: failed to apply recorder status event",
				"error", err)
		}
	}
}

func (b *BurnBridge) applyRecorderStatusEvent(event *burnbridgev1.UnitStatusEvent) error {
	if event == nil || event.GetSnapshot() == nil {
		return nil
	}

	resp := event.GetSnapshot()
	if !resp.GetReady() {
		if readyResponseIndicatesNoDisc(resp) {
			return b.handleNoDiscState()
		}
		return nil
	}

	if err := b.syncActiveDiscState(resp); err != nil {
		return err
	}
	b.recordRecorderReadyState(resp)
	if doc := discInfoDocFromProto(b.activeBucket, resp, nil, readFinalizeLayoutTranscript(b.meta, b.activeBucket, meta.BurnbridgeFinalizeLayoutObjectKey)); doc != nil {
		if err := b.meta.StoreBurnbridgeDiscInfo(doc); err != nil {
			return fmt.Errorf("burnbridge: persist streamed disc info: %w", err)
		}
	}
	return nil
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
		if readyResponseIndicatesNoDisc(resp) {
			if syncErr := b.handleNoDiscState(); syncErr != nil {
				return syncErr
			}
		}
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
	if err := b.syncActiveDiscState(resp); err != nil {
		return err
	}
	b.recordRecorderReadyState(resp)
	if doc := discInfoDocFromProto(b.activeBucket, resp, nil, readFinalizeLayoutTranscript(b.meta, b.activeBucket, meta.BurnbridgeFinalizeLayoutObjectKey)); doc != nil {
		if err := b.meta.StoreBurnbridgeDiscInfo(doc); err != nil {
			return fmt.Errorf("burnbridge: persist disc info: %w", err)
		}
	}
	return nil
}

func (b *BurnBridge) syncActiveDiscState(resp *burnbridgev1.TestUnitReadyResponse) error {
	if resp == nil {
		return nil
	}
	if !resp.GetReady() {
		if readyResponseIndicatesNoDisc(resp) {
			return b.handleNoDiscState()
		}
		return nil
	}

	rawVolume := strings.TrimSpace(resp.GetVolumeLabel())
	if rawVolume == "" {
		return nil
	}

	previousBucket := strings.TrimSpace(b.activeBucket)
	b.clearNoDiscLatch()
	b.captureReadyDiscIdentity(resp)

	if readyResponseIndicatesBlankWritable(resp) {
		if binding, ok := loadDiscBucketBinding(b.meta, rawVolume); ok && binding != nil && strings.TrimSpace(binding.Bucket) != "" {
			cleared, err := b.clearStaleBlankDiscBinding(binding)
			if err != nil {
				return err
			}
			if cleared {
				previousBucket = ""
				b.activeBucket = ""
				b.volumeLabelRaw = rawVolume
				b.udfLabel = rawVolume
				b.resetImportedBucketSyncState()
			} else {
				b.activeBucket = strings.TrimSpace(binding.Bucket)
				if strings.TrimSpace(binding.UdfVolumeLabel) != "" {
					b.udfLabel = strings.TrimSpace(binding.UdfVolumeLabel)
				} else {
					b.udfLabel = rawVolume
				}
				b.volumeLabelRaw = rawVolume
				if previousBucket == "" || !strings.EqualFold(previousBucket, b.activeBucket) {
					b.markImportedBucketConvergencePending(b.activeBucket)
				}
				if err := b.maybeRestoreNoDiscBackup(resp); err != nil {
					return err
				}
				return b.ensureImportedBucketStateForConvergence(context.Background(), b.activeBucket)
			}
		}

		sanitizedBucket, err := sanitizeS3BucketFromVolumeLabel(rawVolume)
		if err != nil {
			b.activeBucket = ""
			b.volumeLabelRaw = rawVolume
			b.udfLabel = rawVolume
			b.resetImportedBucketSyncState()
			return nil
		}

		if previousBucket != "" && !strings.EqualFold(previousBucket, sanitizedBucket) {
			if err := b.backupAndClearBucketMetadata(previousBucket); err != nil {
				return err
			}
		}

		b.activeBucket = sanitizedBucket
		b.volumeLabelRaw = rawVolume
		b.udfLabel = rawVolume
		b.resetImportedBucketSyncState()
		if err := persistDiscBucketBinding(b.meta, rawVolume, b.activeBucket, b.udfLabel); err != nil {
			return err
		}
		return nil
	}

	if strings.EqualFold(strings.TrimSpace(b.volumeLabelRaw), rawVolume) && strings.TrimSpace(b.activeBucket) != "" {
		if err := b.maybeRestoreNoDiscBackup(resp); err != nil {
			return err
		}
		return b.ensureImportedBucketStateForConvergence(context.Background(), b.activeBucket)
	}

	if binding, ok := loadDiscBucketBinding(b.meta, rawVolume); ok && binding != nil {
		b.activeBucket = strings.TrimSpace(binding.Bucket)
		if strings.TrimSpace(binding.UdfVolumeLabel) != "" {
			b.udfLabel = strings.TrimSpace(binding.UdfVolumeLabel)
		}
		b.volumeLabelRaw = rawVolume
		if previousBucket == "" || !strings.EqualFold(previousBucket, b.activeBucket) {
			b.markImportedBucketConvergencePending(b.activeBucket)
		}
		if err := b.maybeRestoreNoDiscBackup(resp); err != nil {
			return err
		}
		return b.ensureImportedBucketStateForConvergence(context.Background(), b.activeBucket)
	}

	sanitizedBucket, err := sanitizeS3BucketFromVolumeLabel(rawVolume)
	if err != nil {
		b.activeBucket = ""
		b.volumeLabelRaw = rawVolume
		b.udfLabel = rawVolume
		return nil
	}

	b.activeBucket = sanitizedBucket
	b.volumeLabelRaw = rawVolume
	b.udfLabel = rawVolume
	if previousBucket == "" || !strings.EqualFold(previousBucket, b.activeBucket) {
		b.markImportedBucketConvergencePending(b.activeBucket)
	}
	if err := persistDiscBucketBinding(b.meta, rawVolume, b.activeBucket, b.udfLabel); err != nil {
		return err
	}
	if err := b.maybeRestoreNoDiscBackup(resp); err != nil {
		return err
	}
	return b.ensureImportedBucketStateForConvergence(context.Background(), b.activeBucket)
}

func (b *BurnBridge) syncImportedBucketState(ctx context.Context, bucket string) error {
	requestedBucket := strings.TrimSpace(bucket)

	resp, err := b.grpc.GetImportedBucketState(ctx, &burnbridgev1.GetImportedBucketStateRequest{})
	if err != nil {
		if isGRPCUnimplemented(err) {
			return nil
		}
		return fmt.Errorf("burnbridge GetImportedBucketState: %w", err)
	}
	if resp == nil || !resp.GetLoaded() {
		return nil
	}

	resolvedBucket := strings.TrimSpace(resp.GetBucket())
	if resolvedBucket == "" {
		resolvedBucket = requestedBucket
	}
	if resolvedBucket == "" {
		return nil
	}
	if mountedBucket, ok := b.mountedFallbackBucketHint(); ok && !strings.EqualFold(resolvedBucket, mountedBucket) {
		slog.Warn("burnbridge: ignoring imported bucket state because mounted disc bucket differs",
			"imported_bucket", resolvedBucket,
			"mounted_bucket", mountedBucket)
		return nil
	}

	if resolvedBucket != "" {
		b.activeBucket = resolvedBucket
	}

	if strings.TrimSpace(resp.GetUdfVolumeLabel()) != "" {
		b.udfLabel = strings.TrimSpace(resp.GetUdfVolumeLabel())
	}
	if strings.TrimSpace(b.volumeLabelRaw) != "" {
		if err := persistDiscBucketBinding(b.meta, b.volumeLabelRaw, resolvedBucket, b.udfLabel); err != nil {
			return err
		}
	}
	if err := b.syncImportedBucketMetadata(resolvedBucket, resp.GetBucketMetadata()); err != nil {
		return err
	}

	importedKeys := make(map[string]struct{}, len(resp.GetObjects()))
	for _, object := range resp.GetObjects() {
		if object == nil || strings.TrimSpace(object.GetObjectKey()) == "" {
			continue
		}
		objectKey := strings.TrimPrefix(strings.ReplaceAll(object.GetObjectKey(), `\`, `/`), "/")
		if objectKey == "" {
			continue
		}
		importedKeys[objectKey] = struct{}{}
		userMetadata := make(map[string]string, len(object.GetMetadata()))
		for _, kv := range object.GetMetadata() {
			if kv == nil || strings.TrimSpace(kv.GetKey()) == "" {
				continue
			}
			userMetadata[strings.TrimSpace(kv.GetKey())] = kv.GetValue()
		}

		rec := &meta.BurnbridgeCommittedRecord{
			Status:             "imported",
			ETag:               object.GetEtag(),
			LastModified:       object.GetLastModifiedUtc(),
			Size:               object.GetSize(),
			ContentType:        object.GetContentType(),
			ContentEncoding:    object.GetContentEncoding(),
			ContentDisposition: object.GetContentDisposition(),
			ContentLanguage:    object.GetContentLanguage(),
			CacheControl:       object.GetCacheControl(),
			Expires:            object.GetExpires(),
			Metadata:           userMetadata,
		}
		if mpMeta, metaErr := b.loadMultipartObjectMetadata(resolvedBucket, objectKey); metaErr == nil && mpMeta != nil {
			if strings.TrimSpace(mpMeta.ETag) != "" {
				rec.ETag = mpMeta.ETag
			}
			if partCount := len(mpMeta.Parts); partCount > 0 && mpMeta.Parts[partCount-1] > 0 {
				rec.Size = mpMeta.Parts[partCount-1]
			}
		}
		if err := b.meta.StoreBurnbridgeCommitted(nil, resolvedBucket, objectKey, rec); err != nil {
			return fmt.Errorf("burnbridge sync imported object %s/%s: %w", resolvedBucket, objectKey, err)
		}
	}

	if removed, err := b.meta.PruneBurnbridgeCommitted(resolvedBucket, importedKeys); err != nil {
		return fmt.Errorf("burnbridge prune stale imported metadata for bucket %s: %w", resolvedBucket, err)
	} else if len(removed) > 0 {
		slog.Info("burnbridge: pruned stale committed object metadata after imported state sync",
			"bucket", resolvedBucket,
			"removed_count", len(removed))
	}

	if err := b.pruneOtherBurnbridgeBuckets(resolvedBucket); err != nil {
		return err
	}

	b.markImportedBucketSynced(resolvedBucket)
	return nil
}

func (b *BurnBridge) syncImportedBucketMetadata(bucket string, items []*burnbridgev1.ObjectMetadata) error {
	trimmedBucket := strings.TrimSpace(bucket)
	if trimmedBucket == "" {
		return nil
	}
	for _, item := range items {
		if item == nil {
			continue
		}
		key := strings.TrimSpace(item.GetKey())
		if key == "" {
			continue
		}
		if !strings.HasPrefix(strings.ToLower(key), "redundancy_") {
			continue
		}
		if err := b.meta.StoreAttribute(nil, trimmedBucket, "", key, []byte(strings.TrimSpace(item.GetValue()))); err != nil {
			return fmt.Errorf("burnbridge sync imported bucket metadata %s/%s: %w", trimmedBucket, key, err)
		}
	}
	return nil
}

func (b *BurnBridge) pruneOtherBurnbridgeBuckets(activeBucket string) error {
	activeBucket = strings.TrimSpace(activeBucket)
	if activeBucket == "" {
		return nil
	}

	buckets, err := b.meta.ListBurnbridgeBuckets()
	if err != nil {
		return fmt.Errorf("burnbridge list metadata buckets for prune: %w", err)
	}
	for _, bucket := range buckets {
		trimmed := strings.TrimSpace(bucket)
		if trimmed == "" ||
			strings.EqualFold(trimmed, activeBucket) ||
			strings.EqualFold(trimmed, meta.BurnbridgeRuntimeBindingBucket) {
			continue
		}
		if err := b.meta.DeleteBurnbridgeBucket(trimmed); err != nil {
			return fmt.Errorf("burnbridge delete stale bucket metadata %s: %w", trimmed, err)
		}
		if err := b.meta.DeleteBurnbridgeDiscBucketBindings(trimmed); err != nil {
			return fmt.Errorf("burnbridge delete stale bucket bindings %s: %w", trimmed, err)
		}
		slog.Info("burnbridge: deleted stale bucket metadata after imported state sync",
			"active_bucket", activeBucket,
			"deleted_bucket", trimmed)
	}
	return nil
}

func (b *BurnBridge) restoreBucketStateFromMetadata(bucket string) bool {
	trimmedBucket := strings.TrimSpace(bucket)
	if trimmedBucket == "" {
		return false
	}

	bindings, err := b.meta.ListBurnbridgeDiscBucketBindings(trimmedBucket)
	if err == nil {
		for _, binding := range bindings {
			if strings.TrimSpace(binding.Bucket) == "" {
				continue
			}
			b.activeBucket = strings.TrimSpace(binding.Bucket)
			if strings.TrimSpace(binding.ProbeVolumeLabel) != "" {
				b.volumeLabelRaw = strings.TrimSpace(binding.ProbeVolumeLabel)
			}
			if strings.TrimSpace(binding.UdfVolumeLabel) != "" {
				b.udfLabel = strings.TrimSpace(binding.UdfVolumeLabel)
			}
			return true
		}
	}

	raw, err := b.meta.GetBurnbridgeDiscInfoJSON(trimmedBucket)
	if err != nil || len(raw) == 0 {
		return false
	}

	var discInfo meta.BurnbridgeDiscInfoDocument
	if err := json.Unmarshal(raw, &discInfo); err != nil {
		return false
	}
	if !strings.EqualFold(strings.TrimSpace(discInfo.Bucket), trimmedBucket) {
		return false
	}

	b.activeBucket = trimmedBucket
	if strings.TrimSpace(discInfo.VolumeLabel) != "" {
		b.volumeLabelRaw = strings.TrimSpace(discInfo.VolumeLabel)
		if strings.TrimSpace(b.udfLabel) == "" {
			b.udfLabel = strings.TrimSpace(discInfo.VolumeLabel)
		}
	}
	return true
}

func (b *BurnBridge) ensureImportedBucketState(ctx context.Context, bucket string) error {
	return b.ensureImportedBucketStateWithMode(ctx, bucket, false)
}

func (b *BurnBridge) ensureImportedBucketStateForConvergence(ctx context.Context, bucket string) error {
	return b.ensureImportedBucketStateWithMode(ctx, bucket, b.importedBucketConvergencePending(bucket))
}

func (b *BurnBridge) ensureImportedBucketStateWithMode(ctx context.Context, bucket string, forceConvergence bool) error {
	if strings.TrimSpace(bucket) == "" {
		return nil
	}

	var err error
	if !forceConvergence {
		var committed []meta.CommittedObjectSummary
		committed, err = b.meta.ListCommittedObjects(bucket)
		if err == nil && len(committed) > 0 {
			return nil
		}
		if err != nil {
			return err
		}
	}

	if b.importedBucketSynced(bucket) {
		return nil
	}

	_, err, _ = b.importedStateGroup.Do(normalizeRuntimeBucket(bucket), func() (interface{}, error) {
		return nil, b.syncImportedBucketState(ctx, bucket)
	})
	return err
}

func (b *BurnBridge) ensureActiveBucketLoaded(ctx context.Context) error {
	if b.noDiscLatchedState() {
		return nil
	}

	if mountedBucket, ok := b.mountedFallbackBucketHint(); ok {
		currentBucket := strings.TrimSpace(b.activeBucket)
		if currentBucket == "" || !strings.EqualFold(currentBucket, mountedBucket) {
			if currentBucket != "" {
				slog.Info("burnbridge: switching active bucket to mounted disc bucket",
					"previous_bucket", currentBucket,
					"mounted_bucket", mountedBucket)
			}
			b.activeBucket = mountedBucket
			b.markImportedBucketConvergencePending(mountedBucket)
		}
		if b.importedBucketConvergencePending(mountedBucket) {
			return b.ensureImportedBucketStateForConvergence(ctx, mountedBucket)
		}
	}

	if strings.TrimSpace(b.activeBucket) != "" {
		if b.restoreBucketStateFromMetadata(b.activeBucket) {
			b.markImportedBucketConvergencePending(b.activeBucket)
		}
		if strings.TrimSpace(b.activeBucket) != "" {
			return nil
		}
	}

	if b.noDiscRecentlyObserved() {
		return nil
	}

	return b.runRecorderStateProbe(ctx)
}

func (b *BurnBridge) noDiscRecentlyObserved() bool {
	b.stateMu.Lock()
	defer b.stateMu.Unlock()
	if b.noDiscLatched {
		return true
	}
	if !b.lastNoDiscObservedAt.IsZero() && time.Since(b.lastNoDiscObservedAt) < noDiscProbeCooldown {
		return true
	}
	return false
}

func (b *BurnBridge) noDiscLatchedState() bool {
	b.stateMu.Lock()
	defer b.stateMu.Unlock()
	return b.noDiscLatched
}

func (b *BurnBridge) clearNoDiscLatch() {
	b.stateMu.Lock()
	b.noDiscLatched = false
	b.lastNoDiscObservedAt = time.Time{}
	b.stateMu.Unlock()
}

func (b *BurnBridge) captureReadyDiscIdentity(resp *burnbridgev1.TestUnitReadyResponse) {
	b.stateMu.Lock()
	defer b.stateMu.Unlock()
	b.lastDiscSerialHex = strings.TrimSpace(resp.GetDiscSerialNumberHex())
	b.lastReadyVolumeLabel = strings.TrimSpace(resp.GetVolumeLabel())
}

func writableStateAllowsWrite(state string) bool {
	switch strings.ToLower(strings.TrimSpace(state)) {
	case "blank", "appendable":
		return true
	default:
		return false
	}
}

func (b *BurnBridge) recordRecorderReadyState(resp *burnbridgev1.TestUnitReadyResponse) {
	if resp == nil || !resp.GetReady() {
		return
	}

	bucket := strings.TrimSpace(b.activeBucket)
	if bucket == "" {
		rawVolume := strings.TrimSpace(resp.GetVolumeLabel())
		if rawVolume != "" {
			sanitizedBucket, err := sanitizeS3BucketFromVolumeLabel(rawVolume)
			if err == nil {
				bucket = sanitizedBucket
			}
		}
	}

	b.stateMu.Lock()
	defer b.stateMu.Unlock()
	b.lastRecorderReadyBucket = bucket
	b.lastRecorderWritableState = strings.TrimSpace(resp.GetWritableState())
	b.lastRecorderReadyObservedAt = time.Now().UTC()
}

func (b *BurnBridge) clearRecorderReadyStateLocked() {
	b.lastRecorderReadyBucket = ""
	b.lastRecorderWritableState = ""
	b.lastRecorderReadyObservedAt = time.Time{}
}

func (b *BurnBridge) cachedRecorderReadyAllowsWrite(bucket string) (string, time.Duration, bool) {
	trimmedBucket := strings.TrimSpace(bucket)
	if trimmedBucket == "" {
		return "", 0, false
	}
	if !b.statusWatchEnabled.Load() {
		return "", 0, false
	}

	b.stateMu.Lock()
	defer b.stateMu.Unlock()

	if !strings.EqualFold(trimmedBucket, strings.TrimSpace(b.lastRecorderReadyBucket)) {
		return "", 0, false
	}

	writableState := strings.TrimSpace(b.lastRecorderWritableState)
	if !writableStateAllowsWrite(writableState) {
		return "", 0, false
	}

	return writableState, time.Since(b.lastRecorderReadyObservedAt), true
}

func (b *BurnBridge) handleNoDiscState() error {
	b.stateMu.Lock()
	activeBucket := strings.TrimSpace(b.activeBucket)
	lastBackupBucket := strings.TrimSpace(b.lastNoDiscBackupBucket)
	b.stateMu.Unlock()

	if activeBucket != "" && !strings.EqualFold(activeBucket, lastBackupBucket) {
		if err := b.backupAndClearBucketMetadata(activeBucket); err != nil {
			return err
		}
	}

	b.stateMu.Lock()
	b.activeBucket = ""
	b.volumeLabelRaw = ""
	b.udfLabel = ""
	b.lastNoDiscObservedAt = time.Now().UTC()
	b.noDiscLatched = true
	b.importedBucketState = make(map[string]bool)
	b.pendingImportedConvergence = make(map[string]bool)
	b.clearRecorderReadyStateLocked()
	b.stateMu.Unlock()
	return nil
}

func (b *BurnBridge) backupAndClearBucketMetadata(bucket string) error {
	trimmedBucket := strings.TrimSpace(bucket)
	if trimmedBucket == "" {
		return nil
	}

	backup, err := b.meta.ExportBurnbridgeBucket(trimmedBucket)
	if err != nil {
		return fmt.Errorf("burnbridge: export bucket backup for no-disc state: %w", err)
	}

	backupDir := filepath.Join(filepath.Dir(b.metaDBPathHint()), "disc-backups")
	if err := os.MkdirAll(backupDir, 0o755); err != nil {
		return fmt.Errorf("burnbridge: create no-disc backup directory: %w", err)
	}

	fileName := fmt.Sprintf("%s-%s.json", sanitizeBackupFilePart(trimmedBucket), time.Now().UTC().Format("20060102T150405.000000000Z"))
	backupPath := filepath.Join(backupDir, fileName)
	payload, err := json.MarshalIndent(backup, "", "  ")
	if err != nil {
		return fmt.Errorf("burnbridge: encode no-disc bucket backup: %w", err)
	}
	if err := os.WriteFile(backupPath, payload, 0o644); err != nil {
		return fmt.Errorf("burnbridge: write no-disc bucket backup: %w", err)
	}

	if err := b.meta.DeleteBurnbridgeBucket(trimmedBucket); err != nil {
		return fmt.Errorf("burnbridge: clear bucket state for no-disc state: %w", err)
	}
	if err := b.meta.DeleteBurnbridgeDiscBucketBindings(trimmedBucket); err != nil {
		return fmt.Errorf("burnbridge: clear disc bucket binding for no-disc state: %w", err)
	}

	b.stateMu.Lock()
	b.lastNoDiscBackupPath = backupPath
	b.lastNoDiscBackupBucket = trimmedBucket
	b.stateMu.Unlock()

	slog.Info("burnbridge: no-disc state backed up and cleared",
		"bucket", trimmedBucket,
		"backup_path", backupPath)
	return nil
}

func (b *BurnBridge) metaDBPathHint() string {
	if strings.TrimSpace(b.metaDBPath) == "" {
		return "."
	}
	return b.metaDBPath
}

func sanitizeBackupFilePart(value string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return "bucket"
	}
	replacer := strings.NewReplacer("\\", "-", "/", "-", ":", "-", "*", "-", "?", "-", "\"", "-", "<", "-", ">", "-", "|", "-")
	normalized := replacer.Replace(trimmed)
	normalized = strings.Trim(normalized, ".- ")
	if normalized == "" {
		return "bucket"
	}
	return normalized
}

func matchesDiscInfoDocToResponse(doc *meta.BurnbridgeDiscInfoDocument, resp *burnbridgev1.TestUnitReadyResponse) bool {
	if doc == nil || resp == nil || !resp.GetReady() {
		return false
	}

	normalizedSerial := strings.TrimSpace(doc.DiscSerialNumberHex)
	normalizedVolume := strings.TrimSpace(doc.VolumeLabel)
	currentSerial := strings.TrimSpace(resp.GetDiscSerialNumberHex())
	currentVolume := strings.TrimSpace(resp.GetVolumeLabel())

	matchesCurrentIdentity := func(value string) bool {
		trimmed := strings.TrimSpace(value)
		if trimmed == "" {
			return false
		}

		return strings.EqualFold(trimmed, currentSerial) || strings.EqualFold(trimmed, currentVolume)
	}

	if (normalizedSerial != "" || normalizedVolume != "") &&
		!matchesCurrentIdentity(normalizedSerial) &&
		!matchesCurrentIdentity(normalizedVolume) {
		return false
	}

	if doc.TrackNextWritableAddressValid &&
		doc.TrackNextWritableAddress > 0 &&
		resp.GetTrackNextWritableAddressValid() &&
		resp.GetTrackNextWritableAddress() > 0 {
		delta := doc.TrackNextWritableAddress - resp.GetTrackNextWritableAddress()
		if delta < 0 {
			delta = -delta
		}
		if delta > 32 {
			return false
		}
	}

	if doc.FreeBlocks > 0 && resp.GetFreeBlocks() > 0 {
		delta := doc.FreeBlocks - resp.GetFreeBlocks()
		if delta < 0 {
			delta = -delta
		}
		if delta > 32 {
			return false
		}
	}

	if doc.TotalBlocks > 0 && resp.GetTotalBlocks() > 0 && doc.TotalBlocks != resp.GetTotalBlocks() {
		return false
	}

	if doc.RecordableCapacityBlocks > 0 && resp.GetRecordableCapacityBlocks() > 0 {
		delta := doc.RecordableCapacityBlocks - resp.GetRecordableCapacityBlocks()
		if delta < 0 {
			delta = -delta
		}
		if delta > 32 {
			return false
		}
	}

	return true
}

func readDiscInfoFromBackup(backup *meta.BurnbridgeBucketBackup) *meta.BurnbridgeDiscInfoDocument {
	if backup == nil {
		return nil
	}

	for _, row := range backup.MetadataRows {
		if row.ObjectName != meta.BurnbridgeDiscInfoObjectKey || row.Attribute != meta.BurnbridgeDiscInfoAttribute {
			continue
		}

		var doc meta.BurnbridgeDiscInfoDocument
		if err := json.Unmarshal(row.Value, &doc); err != nil {
			return nil
		}
		return &doc
	}

	return nil
}

func (b *BurnBridge) maybeRestoreNoDiscBackup(resp *burnbridgev1.TestUnitReadyResponse) error {
	if resp == nil || !resp.GetReady() {
		return nil
	}

	b.stateMu.Lock()
	backupPath := strings.TrimSpace(b.lastNoDiscBackupPath)
	backupBucket := strings.TrimSpace(b.lastNoDiscBackupBucket)
	lastSerial := strings.TrimSpace(b.lastDiscSerialHex)
	lastVolume := strings.TrimSpace(b.lastReadyVolumeLabel)
	currentBucket := strings.TrimSpace(b.activeBucket)
	b.stateMu.Unlock()

	if backupPath == "" || backupBucket == "" || currentBucket == "" {
		return nil
	}

	currentSerial := strings.TrimSpace(resp.GetDiscSerialNumberHex())
	currentVolume := strings.TrimSpace(resp.GetVolumeLabel())
	sameDisc := false
	if currentSerial != "" && lastSerial != "" && strings.EqualFold(currentSerial, lastSerial) {
		sameDisc = true
	} else if currentVolume != "" && lastVolume != "" && strings.EqualFold(currentVolume, lastVolume) {
		sameDisc = true
	}
	if !sameDisc || !strings.EqualFold(currentBucket, backupBucket) {
		return nil
	}

	raw, err := os.ReadFile(backupPath)
	if err != nil {
		return fmt.Errorf("burnbridge: read no-disc backup: %w", err)
	}
	var backup meta.BurnbridgeBucketBackup
	if err := json.Unmarshal(raw, &backup); err != nil {
		return fmt.Errorf("burnbridge: decode no-disc backup: %w", err)
	}
	if backupDiscInfo := readDiscInfoFromBackup(&backup); backupDiscInfo != nil && !matchesDiscInfoDocToResponse(backupDiscInfo, resp) {
		return nil
	}
	if err := b.meta.RestoreBurnbridgeBucket(&backup); err != nil {
		return fmt.Errorf("burnbridge: restore no-disc backup: %w", err)
	}

	b.stateMu.Lock()
	b.lastNoDiscBackupPath = ""
	b.lastNoDiscBackupBucket = ""
	b.lastNoDiscObservedAt = time.Time{}
	b.noDiscLatched = false
	b.stateMu.Unlock()
	b.markImportedBucketConvergencePending(currentBucket)

	slog.Info("burnbridge: restored no-disc backup after same disc reinserted",
		"bucket", backupBucket,
		"backup_path", backupPath)
	return nil
}

func (b *BurnBridge) burnbridgeBucketExists(name string) bool {
	trimmedName := strings.TrimSpace(name)
	if trimmedName == "" {
		return false
	}
	if b.isDriveControlBucket(trimmedName) {
		return true
	}
	if b.noDiscLatchedState() {
		return false
	}
	if mountedBucket, ok := b.mountedFallbackBucketHint(); ok {
		return strings.EqualFold(trimmedName, mountedBucket)
	}
	if trimmedName == strings.TrimSpace(b.activeBucket) {
		return true
	}
	return b.restoreBucketStateFromMetadata(trimmedName)
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
	b.resetImportedBucketSyncState()
	if originalProbe != "" {
		if err := persistDiscBucketBinding(b.meta, originalProbe, bucket, volumeLabel); err != nil {
			return err
		}
	}
	return b.meta.StoreBurnbridgeDiscInfo(&meta.BurnbridgeDiscInfoDocument{
		Bucket:                        bucket,
		VolumeLabel:                   volumeLabel,
		UpdatedAt:                     time.Now().UTC().Format(time.RFC3339Nano),
		DiscSerialNumberHex:           "",
		TotalCapacityBytes:            0,
		FreeCapacityBytes:             0,
		UsedCapacityBytes:             0,
		WritableCapacityBytes:         0,
		FinalizeReserveBytes:          0,
		MediaType:                     "uninitialized",
		BlockSizeBytes:                0,
		TotalBlocks:                   0,
		FreeBlocks:                    0,
		RecordableCapacityBlocks:      0,
		TrackNextWritableAddress:      0,
		TrackNextWritableAddressValid: false,
		WritableState:                 "Unknown",
	})
}

// ------------------------------
// Bucket APIs
// ------------------------------

func (b *BurnBridge) ListBuckets(ctx context.Context, input s3response.ListBucketsInput) (s3response.ListAllMyBucketsResult, error) {
	_ = b.ensureActiveBucketLoaded(ctx)
	result := s3response.ListAllMyBucketsResult{
		Buckets: s3response.ListAllMyBucketsList{Bucket: []s3response.ListAllMyBucketsEntry{}},
		Owner:   s3response.CanonicalUser{ID: input.Owner},
		Prefix:  input.Prefix,
	}
	controlBucket, hasControlBucket := b.cachedDriveControlBucket()
	addBucket := func(name string) error {
		name = strings.TrimSpace(name)
		if name == "" {
			return nil
		}
		if input.Prefix != "" && !strings.HasPrefix(name, input.Prefix) {
			return nil
		}
		if input.ContinuationToken != "" && name <= input.ContinuationToken {
			return nil
		}
		if input.MaxBuckets <= 0 {
			return nil
		}
		if int32(len(result.Buckets.Bucket)) >= input.MaxBuckets {
			return nil
		}
		if !input.IsAdmin {
			if err := b.bootstrapBucketACL(name, input.Owner); err != nil {
				return err
			}
			acl, _, err := b.ensureBucketACLForOwner(name, input.Owner)
			if err != nil {
				return err
			}
			if !strings.EqualFold(strings.TrimSpace(acl.Owner), strings.TrimSpace(input.Owner)) {
				return nil
			}
		}
		result.Buckets.Bucket = append(result.Buckets.Bucket, s3response.ListAllMyBucketsEntry{Name: name, CreationDate: time.Now()})
		return nil
	}
	if hasControlBucket {
		if err := addBucket(controlBucket); err != nil {
			return s3response.ListAllMyBucketsResult{}, err
		}
	}
	if b.noDiscLatchedState() {
		return result, nil
	}
	name := strings.TrimSpace(b.activeBucket)
	if name == "" {
		return result, nil
	}
	if !strings.EqualFold(name, controlBucket) {
		if err := addBucket(name); err != nil {
			return s3response.ListAllMyBucketsResult{}, err
		}
	}
	return result, nil
}

func (b *BurnBridge) ListBucketsAndOwners(ctx context.Context) ([]s3response.Bucket, error) {
	_ = b.ensureActiveBucketLoaded(ctx)
	out := []s3response.Bucket{}
	if controlBucket, ok := b.cachedDriveControlBucket(); ok {
		acl, _, err := b.loadBucketACL(controlBucket)
		if err != nil {
			return nil, err
		}
		out = append(out, s3response.Bucket{
			Name:  controlBucket,
			Owner: acl.Owner,
		})
	}
	if b.noDiscLatchedState() {
		return out, nil
	}
	name := strings.TrimSpace(b.activeBucket)
	if name == "" {
		return out, nil
	}
	if len(out) > 0 && strings.EqualFold(out[0].Name, name) {
		return out, nil
	}

	acl, _, err := b.loadBucketACL(name)
	if err != nil {
		return nil, err
	}

	out = append(out, s3response.Bucket{
		Name:  name,
		Owner: acl.Owner,
	})
	return out, nil
}

func (b *BurnBridge) ChangeBucketOwner(ctx context.Context, bucket, owner string) error {
	if !b.burnbridgeBucketExists(bucket) {
		return s3err.GetAPIError(s3err.ErrNoSuchBucket)
	}
	return auth.UpdateBucketACLOwner(ctx, b, bucket, owner)
}

func (b *BurnBridge) CreateBucket(_ context.Context, input *s3.CreateBucketInput, defaultACL []byte) error {
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

	if len(defaultACL) == 0 {
		defaultACL, err = json.Marshal(defaultBucketACL(requestedBucket))
		if err != nil {
			return fmt.Errorf("marshal default bucket acl: %w", err)
		}
	}

	if strings.TrimSpace(b.activeBucket) == "" {
		if err := b.bindActiveBucket(requestedBucket, targetVolumeLabel); err != nil {
			return err
		}
		return b.storeBucketACL(requestedBucket, defaultACL)
	}
	if strings.EqualFold(b.activeBucket, requestedBucket) {
		return s3err.GetAPIError(s3err.ErrBucketAlreadyOwnedByYou)
	}
	if !b.canRebindActiveBucket(requestedBucket) {
		return s3err.GetAPIError(s3err.ErrBucketAlreadyExists)
	}

	if err := b.bindActiveBucket(requestedBucket, targetVolumeLabel); err != nil {
		return err
	}
	return b.storeBucketACL(requestedBucket, defaultACL)
}

// HeadBucket confirms the bucket exists and the recorder reports the optical unit ready for this bucket.
func (b *BurnBridge) HeadBucket(ctx context.Context, input *s3.HeadBucketInput) (*s3.HeadBucketOutput, error) {
	if input == nil || input.Bucket == nil {
		return nil, fmt.Errorf("bucket required")
	}
	bucket := *input.Bucket
	if b.isDriveControlBucket(bucket) {
		return &s3.HeadBucketOutput{}, nil
	}
	if err := b.ensureActiveBucketLoaded(ctx); err != nil {
		return nil, err
	}
	if !b.burnbridgeBucketExists(bucket) {
		return nil, s3err.GetAPIError(s3err.ErrNoSuchBucket)
	}
	return &s3.HeadBucketOutput{}, nil
}

func (b *BurnBridge) requireRecorderReadyWithRetry(ctx context.Context, bucket string) error {
	if writableState, age, ok := b.cachedRecorderReadyAllowsWrite(bucket); ok {
		slog.Info("burnbridge: skipping active TestUnitReady probe for PutObject; using cached recorder writable state",
			"bucket", strings.TrimSpace(bucket),
			"writable_state", writableState,
			"cached_age_ms", age.Milliseconds())
		return nil
	}

	var lastErr error
	for attempt := 1; attempt <= recorderReadyRetryAttempts; attempt++ {
		err := b.runRecorderStateProbe(ctx)
		if err == nil {
			return nil
		}
		lastErr = err
		if !shouldRetryRecorderReady(err) || attempt == recorderReadyRetryAttempts {
			return err
		}

		slog.Warn("burnbridge: recorder readiness retry",
			"attempt", attempt,
			"max_attempts", recorderReadyRetryAttempts,
			"error", err)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(recorderReadyRetryDelay):
		}
	}

	return lastErr
}

func shouldRetryRecorderReady(err error) bool {
	if err == nil {
		return false
	}

	var apiErr s3err.APIError
	if errors.As(err, &apiErr) {
		return apiErr.Code == "BurnbridgeUnitNotReady" && apiErr.HTTPStatusCode == http.StatusServiceUnavailable
	}

	if strings.Contains(strings.ToLower(err.Error()), "service unavailable") {
		return true
	}

	return false
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
func (b *BurnBridge) GetBucketAcl(ctx context.Context, input *s3.GetBucketAclInput) ([]byte, error) {
	if input == nil || input.Bucket == nil || !b.burnbridgeBucketExists(*input.Bucket) {
		return nil, s3err.GetAPIError(s3err.ErrNoSuchBucket)
	}
	if acct, ok := ctx.Value("account").(auth.Account); ok {
		if strings.TrimSpace(acct.Access) != "" && acct.Role != auth.RoleAdmin {
			_, raw, err := b.ensureBucketACLForOwner(*input.Bucket, acct.Access)
			return raw, err
		}
	}
	_, raw, err := b.loadBucketACL(*input.Bucket)
	return raw, err
}

func (b *BurnBridge) PutBucketAcl(_ context.Context, bucket string, data []byte) error {
	if !b.burnbridgeBucketExists(bucket) {
		return s3err.GetAPIError(s3err.ErrNoSuchBucket)
	}
	return b.storeBucketACL(bucket, data)
}

// GetBucketTagging returns an empty tag set for compatibility (no backend tag persistence).
func (b *BurnBridge) GetBucketTagging(_ context.Context, bucket string) (map[string]string, error) {
	if !b.burnbridgeBucketExists(bucket) {
		return nil, s3err.GetAPIError(s3err.ErrNoSuchBucket)
	}
	tags := map[string]string{
		burnbridgeBucketTypeTagKey: burnbridgeBucketTypeData,
	}
	if b.isDriveControlBucket(bucket) {
		tags[burnbridgeBucketTypeTagKey] = burnbridgeBucketTypeControl
		tags[burnbridgeControlBucketTagKey] = "true"
	}
	return tags, nil
}

func (b *BurnBridge) PutBucketTagging(_ context.Context, bucket string, tags map[string]string) error {
	if !b.burnbridgeBucketExists(bucket) {
		return s3err.GetAPIError(s3err.ErrNoSuchBucket)
	}
	if b.isDriveControlBucket(bucket) {
		return burnbridgeControlBucketReadOnly
	}
	return nil
}

func (b *BurnBridge) DeleteBucketTagging(_ context.Context, bucket string) error {
	if !b.burnbridgeBucketExists(bucket) {
		return s3err.GetAPIError(s3err.ErrNoSuchBucket)
	}
	if b.isDriveControlBucket(bucket) {
		return burnbridgeControlBucketReadOnly
	}
	return nil
}

// GetBucketPolicy reports no bucket policy for burnbridge buckets.
func (b *BurnBridge) GetBucketPolicy(_ context.Context, bucket string) ([]byte, error) {
	if !b.burnbridgeBucketExists(bucket) {
		return nil, s3err.GetAPIError(s3err.ErrNoSuchBucket)
	}
	return nil, s3err.GetAPIError(s3err.ErrNoSuchBucketPolicy)
}

func (b *BurnBridge) DeleteBucketPolicy(_ context.Context, bucket string) error {
	if !b.burnbridgeBucketExists(bucket) {
		return s3err.GetAPIError(s3err.ErrNoSuchBucket)
	}
	return nil
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

func (b *BurnBridge) CreateMultipartUpload(ctx context.Context, input s3response.CreateMultipartUploadInput) (s3response.InitiateMultipartUploadResult, error) {
	if input.Bucket == nil || input.Key == nil {
		return s3response.InitiateMultipartUploadResult{}, s3err.GetAPIError(s3err.ErrInvalidRequest)
	}
	bucket := *input.Bucket
	key := *input.Key
	if b.isDriveControlBucket(bucket) {
		return s3response.InitiateMultipartUploadResult{}, burnbridgeControlBucketReadOnly
	}
	if !b.burnbridgeBucketExists(bucket) {
		return s3response.InitiateMultipartUploadResult{}, s3err.GetAPIError(s3err.ErrNoSuchBucket)
	}
	if err := b.requireRecorderReadyWithRetry(ctx, bucket); err != nil {
		return s3response.InitiateMultipartUploadResult{}, err
	}

	idx := objectLockIndex(bucket, key)
	b.objectLocks[idx].Lock()
	defer b.objectLocks[idx].Unlock()

	uploadID := uuid.NewString()
	initState := buildMultipartInitState(input)
	createResp, err := b.grpc.CreateJob(ctx, &burnbridgev1.CreateJobRequest{
		Bucket:        bucket,
		ObjectKey:     key,
		ContentLength: 0,
		Metadata:      metadataMapToObjectMetadataItems(initState.Metadata),
	})
	if err != nil {
		return s3response.InitiateMultipartUploadResult{}, mapRecorderWriteRPCError(err)
	}
	jobID := strings.TrimSpace(createResp.GetJobId())
	if jobID == "" {
		return s3response.InitiateMultipartUploadResult{}, fmt.Errorf("burnbridge: empty job id from CreateJob")
	}
	cancelJobNow := func(job string) {
		if strings.TrimSpace(job) == "" {
			return
		}
		cctx, cancel := context.WithTimeout(context.Background(), b.cancelJobTimeout)
		defer cancel()
		_, _ = b.grpc.CancelJob(cctx, &burnbridgev1.CancelJobRequest{JobId: job})
	}
	if err := b.meta.UpsertBurnUploadSession(meta.BurnUploadSessionRecord{
		Bucket:         bucket,
		ObjectName:     key,
		UploadID:       uploadID,
		Kind:           meta.BurnUploadKindMultipart,
		State:          meta.BurnUploadStateWriting,
		MediaID:        b.volumeLabelRaw,
		RecorderJobID:  jobID,
		ContentLength:  0,
		BytesReceived:  0,
		NextPartNumber: 1,
	}); err != nil {
		cancelJobNow(jobID)
		return s3response.InitiateMultipartUploadResult{}, err
	}
	if err := b.storeMultipartInitState(bucket, key, uploadID, initState); err != nil {
		_ = b.meta.DeleteBurnUploadSession(bucket, key, uploadID)
		cancelJobNow(jobID)
		return s3response.InitiateMultipartUploadResult{}, err
	}
	return s3response.InitiateMultipartUploadResult{
		Bucket:   bucket,
		Key:      key,
		UploadId: uploadID,
	}, nil
}

func (b *BurnBridge) UploadPart(ctx context.Context, input *s3.UploadPartInput) (*s3.UploadPartOutput, error) {
	if input == nil || input.Bucket == nil || input.Key == nil || input.UploadId == nil || input.PartNumber == nil {
		return nil, s3err.GetAPIError(s3err.ErrInvalidRequest)
	}
	bucket := *input.Bucket
	key := *input.Key
	uploadID := strings.TrimSpace(*input.UploadId)
	partNumber := int(*input.PartNumber)
	if b.isDriveControlBucket(bucket) {
		return nil, burnbridgeControlBucketReadOnly
	}
	if !b.burnbridgeBucketExists(bucket) {
		return nil, s3err.GetAPIError(s3err.ErrNoSuchBucket)
	}
	if err := b.requireRecorderReadyWithRetry(ctx, bucket); err != nil {
		return nil, err
	}

	b.putSerialMu.Lock()
	defer b.putSerialMu.Unlock()

	idx := objectLockIndex(bucket, key)
	b.objectLocks[idx].Lock()
	defer b.objectLocks[idx].Unlock()

	session, err := b.getMultipartUploadSession(bucket, key, uploadID)
	if err != nil {
		return nil, err
	}
	if session == nil || !b.multipartSessionVisibleOnCurrentMedia(bucket, session) {
		return nil, s3err.GetAPIError(s3err.ErrNoSuchUpload)
	}
	if strings.TrimSpace(session.RecorderJobID) == "" {
		return nil, fmt.Errorf("burnbridge: multipart upload session %s has no recorder job id", uploadID)
	}
	if session.NextPartNumber < 1 {
		session.NextPartNumber = 1
	}
	existingPart, perr := b.meta.GetBurnUploadPart(bucket, key, uploadID, partNumber)
	if perr != nil && !errors.Is(perr, meta.ErrNoSuchKey) {
		return nil, perr
	}
	if errors.Is(perr, meta.ErrNoSuchKey) {
		existingPart = nil
	}

	switch {
	case partNumber == session.NextPartNumber:
	case partNumber == session.NextPartNumber-1:
		if existingPart == nil || existingPart.State != meta.BurnUploadStateCompleted {
			return nil, s3err.GetAPIError(s3err.ErrInvalidPartOrder)
		}
		return completedMultipartReplayOutput(*existingPart, input.Body, b.chunkSize)
	default:
		return nil, s3err.GetAPIError(s3err.ErrInvalidPartOrder)
	}

	var contentLen int64
	if input.ContentLength != nil {
		contentLen = *input.ContentLength
	}

	partStartOffset := session.BytesReceived
	partBytesReceived := int64(0)
	if existingPart != nil {
		switch existingPart.State {
		case meta.BurnUploadStateWriting, meta.BurnUploadStateFailed:
			partStartOffset = existingPart.StartOffset
			if session.BytesReceived < partStartOffset {
				return nil, fmt.Errorf("burnbridge: multipart session bytes_received moved behind current part start: uploadId=%s part=%d start=%d sessionBytes=%d",
					uploadID, partNumber, partStartOffset, session.BytesReceived)
			}
			partBytesReceived = session.BytesReceived - partStartOffset
		case meta.BurnUploadStateCompleted:
			return completedMultipartReplayOutput(*existingPart, input.Body, b.chunkSize)
		}
	}

	additionalBytes := contentLen
	if additionalBytes > 0 && partBytesReceived > 0 {
		additionalBytes -= partBytesReceived
		if additionalBytes < 0 {
			additionalBytes = 0
		}
	}
	if additionalBytes > 0 {
		raw, err := b.meta.GetBurnbridgeDiscInfoJSON(bucket)
		if err == nil && len(raw) > 0 {
			var discInfo meta.BurnbridgeDiscInfoDocument
			if uerr := json.Unmarshal(raw, &discInfo); uerr == nil {
				if err := ensureWritableCapacity(&discInfo, additionalBytes); err != nil {
					return nil, err
				}
			}
		}
	}

	if existingPart != nil && contentLen > 0 && partBytesReceived == contentLen &&
		(existingPart.State == meta.BurnUploadStateWriting || existingPart.State == meta.BurnUploadStateFailed) {
		if input.Body == nil {
			return nil, s3err.GetAPIError(s3err.ErrInvalidRequest)
		}
		size, digest, err := hashReaderMD5(input.Body, b.chunkSize)
		if err != nil {
			return nil, err
		}
		if size != contentLen {
			return nil, s3err.GetAPIError(s3err.ErrInvalidPart)
		}
		if existingPart.ChecksumMD5 != "" && !strings.EqualFold(existingPart.ChecksumMD5, digest) {
			return nil, s3err.GetAPIError(s3err.ErrInvalidPart)
		}
		offset := partStartOffset + size
		etag := quotedETag(digest)
		checksumMD5 := digest
		if err := b.meta.UpsertBurnUploadPart(meta.BurnUploadPartRecord{
			Bucket:        bucket,
			ObjectName:    key,
			UploadID:      uploadID,
			PartNumber:    partNumber,
			StartOffset:   partStartOffset,
			BytesReceived: offset - partStartOffset,
			PartSize:      size,
			ChecksumMD5:   checksumMD5,
			ETag:          etag,
			State:         meta.BurnUploadStateCompleted,
			SegmentCount:  existingPart.SegmentCount,
		}); err != nil {
			return nil, err
		}
		nextPartNumber := session.NextPartNumber
		if partNumber >= nextPartNumber {
			nextPartNumber = partNumber + 1
		}
		if err := b.meta.UpsertBurnUploadSession(meta.BurnUploadSessionRecord{
			Bucket:         bucket,
			ObjectName:     key,
			UploadID:       uploadID,
			Kind:           meta.BurnUploadKindMultipart,
			State:          meta.BurnUploadStateWriting,
			MediaID:        b.volumeLabelRaw,
			RecorderJobID:  session.RecorderJobID,
			ContentLength:  offset,
			BytesReceived:  offset,
			NextPartNumber: nextPartNumber,
		}); err != nil {
			return nil, err
		}
		out := &s3.UploadPartOutput{ETag: &etag}
		if checksumMD5 != "" {
			out.ChecksumMD5 = &checksumMD5
		}
		slog.Info("burnbridge: multipart part completed from durable retry without recorder replay",
			"bucket", bucket,
			"key", key,
			"uploadId", uploadID,
			"partNumber", partNumber,
			"bytes", size,
			"offset", offset)
		return out, nil
	}

	if err := b.meta.UpsertBurnUploadSession(meta.BurnUploadSessionRecord{
		Bucket:         bucket,
		ObjectName:     key,
		UploadID:       uploadID,
		Kind:           meta.BurnUploadKindMultipart,
		State:          meta.BurnUploadStateWriting,
		MediaID:        b.volumeLabelRaw,
		RecorderJobID:  session.RecorderJobID,
		ContentLength:  session.ContentLength,
		BytesReceived:  session.BytesReceived,
		NextPartNumber: session.NextPartNumber,
	}); err != nil {
		return nil, err
	}
	if err := b.meta.UpsertBurnUploadPart(meta.BurnUploadPartRecord{
		Bucket:        bucket,
		ObjectName:    key,
		UploadID:      uploadID,
		PartNumber:    partNumber,
		StartOffset:   partStartOffset,
		BytesReceived: partBytesReceived,
		PartSize:      contentLen,
		State:         meta.BurnUploadStateWriting,
		SegmentCount:  0,
	}); err != nil {
		return nil, err
	}

	shadowKey := multipartSessionObjectKey(uploadID)
	offset, uploadResp, stats, err := b.grpcUploadMultipartPartStream(
		ctx,
		session.RecorderJobID,
		bucket,
		shadowKey,
		input.Body,
		partStartOffset,
		session.BytesReceived)
	if err != nil {
		failedPartBytes := offset - partStartOffset
		if failedPartBytes < 0 {
			failedPartBytes = 0
		}
		_ = b.meta.UpsertBurnUploadSession(meta.BurnUploadSessionRecord{
			Bucket:         bucket,
			ObjectName:     key,
			UploadID:       uploadID,
			Kind:           meta.BurnUploadKindMultipart,
			State:          meta.BurnUploadStateFailed,
			MediaID:        b.volumeLabelRaw,
			RecorderJobID:  session.RecorderJobID,
			ContentLength:  offset,
			BytesReceived:  offset,
			NextPartNumber: session.NextPartNumber,
		})
		_ = b.meta.UpsertBurnUploadPart(meta.BurnUploadPartRecord{
			Bucket:        bucket,
			ObjectName:    key,
			UploadID:      uploadID,
			PartNumber:    partNumber,
			StartOffset:   partStartOffset,
			BytesReceived: failedPartBytes,
			PartSize:      contentLen,
			State:         meta.BurnUploadStateFailed,
			SegmentCount:  int(stats.TotalSegments),
		})
		return nil, err
	}

	partSize := offset - partStartOffset
	etag := quotedETag(uploadResp.GetChecksumMd5())
	checksumMD5 := uploadResp.GetChecksumMd5()
	if err := b.meta.UpsertBurnUploadPart(meta.BurnUploadPartRecord{
		Bucket:        bucket,
		ObjectName:    key,
		UploadID:      uploadID,
		PartNumber:    partNumber,
		StartOffset:   partStartOffset,
		BytesReceived: partSize,
		PartSize:      partSize,
		ChecksumMD5:   checksumMD5,
		ETag:          etag,
		State:         meta.BurnUploadStateCompleted,
		SegmentCount:  int(stats.TotalSegments),
	}); err != nil {
		return nil, err
	}
	nextPartNumber := session.NextPartNumber
	if partNumber >= nextPartNumber {
		nextPartNumber = partNumber + 1
	}
	if err := b.meta.UpsertBurnUploadSession(meta.BurnUploadSessionRecord{
		Bucket:         bucket,
		ObjectName:     key,
		UploadID:       uploadID,
		Kind:           meta.BurnUploadKindMultipart,
		State:          meta.BurnUploadStateWriting,
		MediaID:        b.volumeLabelRaw,
		RecorderJobID:  session.RecorderJobID,
		ContentLength:  offset,
		BytesReceived:  offset,
		NextPartNumber: nextPartNumber,
	}); err != nil {
		return nil, err
	}

	out := &s3.UploadPartOutput{ETag: &etag}
	if checksumMD5 != "" {
		out.ChecksumMD5 = &checksumMD5
	}
	return out, nil
}

func (b *BurnBridge) ListParts(_ context.Context, input *s3.ListPartsInput) (s3response.ListPartsResult, error) {
	if input == nil || input.Bucket == nil || input.Key == nil || input.UploadId == nil {
		return s3response.ListPartsResult{}, s3err.GetAPIError(s3err.ErrInvalidRequest)
	}
	bucket := *input.Bucket
	key := *input.Key
	uploadID := strings.TrimSpace(*input.UploadId)
	if b.isDriveControlBucket(bucket) {
		return s3response.ListPartsResult{}, burnbridgeControlBucketReadOnly
	}
	if !b.burnbridgeBucketExists(bucket) {
		return s3response.ListPartsResult{}, s3err.GetAPIError(s3err.ErrNoSuchBucket)
	}
	session, err := b.getMultipartUploadSession(bucket, key, uploadID)
	if err != nil {
		return s3response.ListPartsResult{}, err
	}
	if session == nil || !b.multipartSessionVisibleOnCurrentMedia(bucket, session) {
		return s3response.ListPartsResult{}, s3err.GetAPIError(s3err.ErrNoSuchUpload)
	}
	initState, err := b.loadMultipartInitState(bucket, key, uploadID)
	if err != nil && !errors.Is(err, meta.ErrNoSuchKey) {
		return s3response.ListPartsResult{}, err
	}
	var partNumberMarker int
	if input.PartNumberMarker != nil && strings.TrimSpace(*input.PartNumberMarker) != "" {
		partNumberMarker, err = strconv.Atoi(strings.TrimSpace(*input.PartNumberMarker))
		if err != nil {
			return s3response.ListPartsResult{}, s3err.GetInvalidMaxLimiterErr("part-number-marker")
		}
	}
	maxParts := int(listDefaultMaxKeys)
	if input.MaxParts != nil && *input.MaxParts > 0 {
		maxParts = int(*input.MaxParts)
	}
	rows, err := b.meta.ListBurnUploadParts(bucket, key, uploadID)
	if err != nil {
		return s3response.ListPartsResult{}, err
	}
	filtered := make([]s3response.Part, 0, len(rows))
	for _, row := range rows {
		if row.State != meta.BurnUploadStateCompleted || row.PartNumber <= partNumberMarker {
			continue
		}
		part := s3response.Part{
			PartNumber:   row.PartNumber,
			LastModified: row.UpdatedAt,
			ETag:         row.ETag,
			Size:         row.PartSize,
		}
		if row.ChecksumMD5 != "" {
			checksumMD5 := row.ChecksumMD5
			part.ChecksumMD5 = &checksumMD5
		}
		filtered = append(filtered, part)
	}
	resultParts := filtered
	nextPartNumberMarker := 0
	isTruncated := false
	if len(resultParts) > maxParts {
		isTruncated = true
		nextPartNumberMarker = resultParts[maxParts-1].PartNumber
		resultParts = resultParts[:maxParts]
	}
	result := s3response.ListPartsResult{
		Bucket:               bucket,
		Key:                  key,
		UploadID:             uploadID,
		StorageClass:         types.StorageClassStandard,
		PartNumberMarker:     partNumberMarker,
		NextPartNumberMarker: nextPartNumberMarker,
		MaxParts:             maxParts,
		IsTruncated:          isTruncated,
		Parts:                resultParts,
	}
	if initState != nil {
		result.ChecksumAlgorithm = initState.ChecksumAlgorithm
		result.ChecksumType = initState.ChecksumType
	}
	return result, nil
}

func (b *BurnBridge) CompleteMultipartUpload(ctx context.Context, input *s3.CompleteMultipartUploadInput) (s3response.CompleteMultipartUploadResult, string, error) {
	if input == nil || input.Bucket == nil || input.Key == nil || input.UploadId == nil || input.MultipartUpload == nil {
		return s3response.CompleteMultipartUploadResult{}, "", s3err.GetAPIError(s3err.ErrInvalidRequest)
	}
	bucket := *input.Bucket
	key := *input.Key
	uploadID := strings.TrimSpace(*input.UploadId)
	if b.isDriveControlBucket(bucket) {
		return s3response.CompleteMultipartUploadResult{}, "", burnbridgeControlBucketReadOnly
	}
	if !b.burnbridgeBucketExists(bucket) {
		return s3response.CompleteMultipartUploadResult{}, "", s3err.GetAPIError(s3err.ErrNoSuchBucket)
	}
	if err := b.requireRecorderReadyWithRetry(ctx, bucket); err != nil {
		return s3response.CompleteMultipartUploadResult{}, "", err
	}

	b.putSerialMu.Lock()
	defer b.putSerialMu.Unlock()

	idx := objectLockIndex(bucket, key)
	b.objectLocks[idx].Lock()
	defer b.objectLocks[idx].Unlock()

	session, err := b.getMultipartUploadSession(bucket, key, uploadID)
	if err != nil {
		return s3response.CompleteMultipartUploadResult{}, "", err
	}
	if session == nil || !b.multipartSessionVisibleOnCurrentMedia(bucket, session) {
		mpMeta, metaErr := b.loadMultipartObjectMetadata(bucket, key)
		if metaErr == nil && strings.EqualFold(strings.TrimSpace(mpMeta.UploadID), uploadID) {
			committedRec, recErr := b.meta.GetBurnbridgeCommittedRecord(bucket, key)
			if recErr == nil {
				return s3response.CompleteMultipartUploadResult{
					Bucket: &bucket,
					Key:    &key,
					ETag:   &committedRec.ETag,
				}, "", nil
			}
		}
		return s3response.CompleteMultipartUploadResult{}, "", s3err.GetAPIError(s3err.ErrNoSuchUpload)
	}
	initState, err := b.loadMultipartInitState(bucket, key, uploadID)
	if err != nil {
		if errors.Is(err, meta.ErrNoSuchKey) {
			return s3response.CompleteMultipartUploadResult{}, "", s3err.GetAPIError(s3err.ErrNoSuchUpload)
		}
		return s3response.CompleteMultipartUploadResult{}, "", err
	}

	existingCommitted, committedErr := b.meta.GetBurnbridgeCommittedRecord(bucket, key)
	if committedErr != nil && !errors.Is(committedErr, meta.ErrNoSuchKey) {
		return s3response.CompleteMultipartUploadResult{}, "", committedErr
	}
	if err := backend.EvaluateObjectPutPreconditions(func() string {
		if committedErr == nil {
			return existingCommitted.ETag
		}
		return ""
	}(), input.IfMatch, input.IfNoneMatch, committedErr == nil); err != nil {
		return s3response.CompleteMultipartUploadResult{}, "", err
	}

	partRows, err := b.meta.ListBurnUploadParts(bucket, key, uploadID)
	if err != nil {
		return s3response.CompleteMultipartUploadResult{}, "", err
	}
	partRowByNumber := make(map[int]meta.BurnUploadPartRecord, len(partRows))
	for _, row := range partRows {
		partRowByNumber[row.PartNumber] = row
	}

	completedParts := input.MultipartUpload.Parts
	last := len(completedParts) - 1
	cumulativePartSizes := make([]int64, 0, len(completedParts))
	var (
		prevPartNumber int32
		totalSize      int64
	)
	for idxPart, part := range completedParts {
		if part.PartNumber == nil || *part.PartNumber < 1 {
			return s3response.CompleteMultipartUploadResult{}, "", s3err.GetAPIError(s3err.ErrInvalidPart)
		}
		if *part.PartNumber <= prevPartNumber {
			return s3response.CompleteMultipartUploadResult{}, "", s3err.GetAPIError(s3err.ErrInvalidPartOrder)
		}
		prevPartNumber = *part.PartNumber
		row, ok := partRowByNumber[int(*part.PartNumber)]
		if !ok || row.State != meta.BurnUploadStateCompleted {
			return s3response.CompleteMultipartUploadResult{}, "", s3err.GetAPIError(s3err.ErrInvalidPart)
		}
		if idxPart < last && row.PartSize < backend.MinPartSize {
			return s3response.CompleteMultipartUploadResult{}, "", s3err.GetAPIError(s3err.ErrEntityTooSmall)
		}
		if part.ETag == nil || !backend.AreEtagsSame(row.ETag, *part.ETag) {
			return s3response.CompleteMultipartUploadResult{}, "", s3err.GetAPIError(s3err.ErrInvalidPart)
		}
		totalSize += row.PartSize
		cumulativePartSizes = append(cumulativePartSizes, totalSize)
	}
	if input.MpuObjectSize != nil && totalSize != *input.MpuObjectSize {
		return s3response.CompleteMultipartUploadResult{}, "", s3err.GetIncorrectMpObjectSizeErr(totalSize, *input.MpuObjectSize)
	}
	etag, err := backend.GetMultipartMD5(completedParts)
	if err != nil {
		return s3response.CompleteMultipartUploadResult{}, "", s3err.GetAPIError(s3err.ErrInvalidPart)
	}
	shadowKey := multipartSessionObjectKey(uploadID)
	finalizeManifest, err := b.buildFinalizeManifestForObject(bucket, shadowKey, key, totalSize)
	if err != nil {
		return s3response.CompleteMultipartUploadResult{}, "", err
	}
	if strings.TrimSpace(session.RecorderJobID) == "" {
		return s3response.CompleteMultipartUploadResult{}, "", fmt.Errorf("burnbridge: multipart upload session %s has no recorder job id", uploadID)
	}
	if finalizeManifest == nil {
		slog.Warn("burnbridge: multipart complete without usable disc extents; commit without finalize_manifest fallback",
			"bucket", bucket, "key", key, "uploadId", uploadID, "jobId", session.RecorderJobID, "bytes", totalSize)
	}

	commitResp, err := b.grpc.CommitJob(ctx, &burnbridgev1.CommitJobRequest{
		JobId:                  session.RecorderJobID,
		UdfVolumeLabel:         b.udfLabel,
		FinalizeManifest:       finalizeManifest,
		CommittedEtag:          etag,
		CommittedContentLength: totalSize,
	})
	if err != nil {
		return s3response.CompleteMultipartUploadResult{}, "", mapRecorderWriteRPCError(err)
	}

	shadowSegments, err := b.meta.ListBurnObjectSegments(bucket, shadowKey)
	if err != nil && !errors.Is(err, meta.ErrNoSuchKey) {
		return s3response.CompleteMultipartUploadResult{}, "", err
	}
	if err := b.meta.DeleteBurnObjectSegments(bucket, key); err != nil && !errors.Is(err, meta.ErrNoSuchKey) {
		return s3response.CompleteMultipartUploadResult{}, "", err
	}
	for _, seg := range shadowSegments {
		if err := b.meta.UpsertBurnObjectSegment(bucket, key, seg.MediaID, seg.SegmentIndex, seg.ByteOffset, seg.ByteSize, seg.ChecksumMD5, seg.State, seg.DiscExtents); err != nil {
			return s3response.CompleteMultipartUploadResult{}, "", err
		}
	}
	if err := b.storeMultipartObjectMetadata(bucket, key, backend.MpUploadMetadata{
		UploadID: uploadID,
		ETag:     etag,
		Parts:    cumulativePartSizes,
	}); err != nil {
		return s3response.CompleteMultipartUploadResult{}, "", err
	}
	lm := time.Now().UTC().Format(time.RFC3339Nano)
	committedRec := &meta.BurnbridgeCommittedRecord{
		JobID:        session.RecorderJobID,
		Status:       commitResp.GetStatus(),
		ETag:         etag,
		LastModified: lm,
		Size:         totalSize,
	}
	applyCommittedRecordMetadata(committedRec, initState.Metadata)
	if err := b.meta.StoreBurnbridgeCommitted(nil, bucket, key, committedRec); err != nil {
		return s3response.CompleteMultipartUploadResult{}, "", err
	}
	if err := b.cleanupCompletedMultipartObjectState(bucket, key, uploadID, partRows); err != nil {
		slog.Warn("burnbridge: multipart cleanup after complete failed", "bucket", bucket, "key", key, "uploadId", uploadID, "error", err)
	}
	b.invalidateFinalizeLayoutTranscript(bucket)

	return s3response.CompleteMultipartUploadResult{
		Bucket: &bucket,
		Key:    &key,
		ETag:   &etag,
	}, "", nil
}

func (b *BurnBridge) AbortMultipartUpload(_ context.Context, input *s3.AbortMultipartUploadInput) error {
	if input == nil || input.Bucket == nil || input.Key == nil || input.UploadId == nil {
		return s3err.GetAPIError(s3err.ErrInvalidRequest)
	}
	bucket := *input.Bucket
	key := *input.Key
	uploadID := strings.TrimSpace(*input.UploadId)
	if !b.burnbridgeBucketExists(bucket) {
		return s3err.GetAPIError(s3err.ErrNoSuchBucket)
	}

	idx := objectLockIndex(bucket, key)
	b.objectLocks[idx].Lock()
	defer b.objectLocks[idx].Unlock()

	session, err := b.getMultipartUploadSession(bucket, key, uploadID)
	if err != nil {
		return err
	}
	if session == nil || !b.multipartSessionVisibleOnCurrentMedia(bucket, session) {
		return s3err.GetAPIError(s3err.ErrNoSuchUpload)
	}
	if input.IfMatchInitiatedTime != nil && input.IfMatchInitiatedTime.Unix() != session.CreatedAt.Unix() {
		return s3err.GetAPIError(s3err.ErrPreconditionFailed)
	}
	partRows, err := b.meta.ListBurnUploadParts(bucket, key, uploadID)
	if err != nil {
		return err
	}
	if strings.TrimSpace(session.RecorderJobID) != "" {
		cctx, cancel := context.WithTimeout(context.Background(), b.cancelJobTimeout)
		defer cancel()
		if _, err := b.grpc.CancelJob(cctx, &burnbridgev1.CancelJobRequest{JobId: session.RecorderJobID}); err != nil {
			slog.Warn("burnbridge: abort multipart cancel job failed", "bucket", bucket, "key", key, "uploadId", uploadID, "jobId", session.RecorderJobID, "error", err)
		}
	}
	if err := b.cleanupMultipartUploadState(bucket, key, uploadID, partRows); err != nil {
		return err
	}
	return nil
}

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
	keyMarker := ""
	if input.KeyMarker != nil {
		keyMarker = *input.KeyMarker
	}
	uploadIDMarker := ""
	if input.UploadIdMarker != nil {
		uploadIDMarker = *input.UploadIdMarker
	}
	maxUploads := int(listDefaultMaxKeys)
	if input.MaxUploads != nil && *input.MaxUploads > 0 {
		maxUploads = int(*input.MaxUploads)
	}

	sessions, err := b.meta.ListBurnUploadSessionsByBucket(bucket)
	if err != nil {
		return s3response.ListMultipartUploadsResult{}, err
	}
	uploads := make([]s3response.Upload, 0, len(sessions))
	for _, session := range sessions {
		if session.Kind != meta.BurnUploadKindMultipart {
			continue
		}
		if session.State == meta.BurnUploadStateCompleted || session.State == meta.BurnUploadStateAborted {
			continue
		}
		if !b.multipartSessionVisibleOnCurrentMedia(bucket, &session) {
			continue
		}
		if prefix != "" && !strings.HasPrefix(session.ObjectName, prefix) {
			continue
		}
		if keyMarker != "" && session.ObjectName <= keyMarker {
			continue
		}
		upload := s3response.Upload{
			Key:          session.ObjectName,
			UploadID:     session.UploadID,
			Initiated:    session.CreatedAt,
			StorageClass: types.StorageClassStandard,
		}
		if initState, ierr := b.loadMultipartInitState(bucket, session.ObjectName, session.UploadID); ierr == nil && initState != nil {
			upload.ChecksumAlgorithm = initState.ChecksumAlgorithm
			upload.ChecksumType = initState.ChecksumType
		}
		uploads = append(uploads, upload)
	}
	sort.SliceStable(uploads, func(i, j int) bool {
		if uploads[i].Key == uploads[j].Key {
			return uploads[i].Initiated.Before(uploads[j].Initiated)
		}
		return uploads[i].Key < uploads[j].Key
	})
	page, err := backend.ListMultipartUploads(uploads, prefix, delimiter, keyMarker, uploadIDMarker, maxUploads)
	if err != nil {
		return s3response.ListMultipartUploadsResult{}, err
	}
	return s3response.ListMultipartUploadsResult{
		Bucket:             bucket,
		KeyMarker:          keyMarker,
		UploadIDMarker:     uploadIDMarker,
		NextKeyMarker:      page.NextKeyMarker,
		NextUploadIDMarker: page.NextUploadIDMarker,
		Delimiter:          delimiter,
		Prefix:             prefix,
		MaxUploads:         maxUploads,
		IsTruncated:        page.IsTruncated,
		Uploads:            page.Uploads,
		CommonPrefixes:     page.CommonPrefixes,
	}, nil
}

func (b *BurnBridge) String() string { return "BurnBridge" }

// Close releases gRPC resources.
func (b *BurnBridge) Close() error {
	if b.statusWatchCancel != nil {
		b.statusWatchCancel()
		b.statusWatchCancel = nil
	}
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

func mediaChangeLastModifiedFromJSON(raw []byte) time.Time {
	var d meta.BurnbridgeMediaChangeDocument
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

func trayLastModifiedFromJSON(raw []byte) time.Time {
	var d meta.BurnbridgeTrayDocument
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

func buildDiscInfoControlJSON(req *burnbridgeControlRequest, bucket string, doc *meta.BurnbridgeDiscInfoDocument) (burnbridgeControlPayload, error) {
	if req == nil {
		return burnbridgeControlPayload{}, fmt.Errorf("burnbridge: disc info control request is required")
	}
	if doc == nil {
		return buildBurnbridgeControlPayload(req, bucket, false, nil, &burnbridgeControlError{
			Code:    s3err.GetAPIError(s3err.ErrNoSuchKey).Code,
			Message: "disc info unavailable",
		}, time.Now().UTC())
	}
	lastModified := time.Now().UTC()
	if strings.TrimSpace(doc.UpdatedAt) != "" {
		if t, err := time.Parse(time.RFC3339Nano, doc.UpdatedAt); err == nil {
			lastModified = t.UTC()
		} else if t, err := time.Parse(time.RFC3339, doc.UpdatedAt); err == nil {
			lastModified = t.UTC()
		}
	}
	return buildBurnbridgeControlPayload(req, bucket, true, doc, nil, lastModified)
}

func buildDriveInfoControlJSON(req *burnbridgeControlRequest, bucket string, doc *meta.BurnbridgeDriveInfoDocument) (burnbridgeControlPayload, error) {
	if req == nil {
		return burnbridgeControlPayload{}, fmt.Errorf("burnbridge: drive info control request is required")
	}
	if doc == nil {
		return buildBurnbridgeControlPayload(req, bucket, false, nil, &burnbridgeControlError{
			Code:    s3err.GetAPIError(s3err.ErrNoSuchKey).Code,
			Message: "drive info unavailable",
		}, time.Now().UTC())
	}
	lastModified := time.Now().UTC()
	if strings.TrimSpace(doc.UpdatedAt) != "" {
		if t, err := time.Parse(time.RFC3339Nano, doc.UpdatedAt); err == nil {
			lastModified = t.UTC()
		} else if t, err := time.Parse(time.RFC3339, doc.UpdatedAt); err == nil {
			lastModified = t.UTC()
		}
	}
	return buildBurnbridgeControlPayload(req, bucket, true, doc, nil, lastModified)
}

func buildFinalizeControlJSON(req *burnbridgeControlRequest, bucket string, doc *meta.BurnbridgeFinalizeLayoutDocument) (burnbridgeControlPayload, error) {
	if req == nil {
		return burnbridgeControlPayload{}, fmt.Errorf("burnbridge: finalize control request is required")
	}
	if doc == nil {
		return buildBurnbridgeControlPayload(req, bucket, false, nil, &burnbridgeControlError{
			Code:    s3err.GetAPIError(s3err.ErrNoSuchKey).Code,
			Message: "finalize transcript unavailable",
		}, time.Now().UTC())
	}
	lastModified := time.Now().UTC()
	if strings.TrimSpace(doc.CompletedAtUtc) != "" {
		if t, err := time.Parse(time.RFC3339Nano, doc.CompletedAtUtc); err == nil {
			lastModified = t.UTC()
		} else if t, err := time.Parse(time.RFC3339, doc.CompletedAtUtc); err == nil {
			lastModified = t.UTC()
		}
	}
	if doc.GrpcOK {
		return buildBurnbridgeControlPayload(req, bucket, true, doc, nil, lastModified)
	}
	errCode := doc.GrpcCode
	if strings.TrimSpace(errCode) == "" {
		errCode = "InternalError"
	}
	errMessage := doc.GrpcDetails
	if strings.TrimSpace(errMessage) == "" {
		errMessage = doc.Error
	}
	if strings.TrimSpace(errMessage) == "" {
		errMessage = doc.RecorderMessage
	}
	if strings.TrimSpace(errMessage) == "" {
		errMessage = "finalize failed"
	}
	return buildBurnbridgeControlPayload(req, bucket, false, doc, &burnbridgeControlError{
		Code:    errCode,
		Message: errMessage,
	}, lastModified)
}

func buildMediaChangeControlJSON(req *burnbridgeControlRequest, bucket string, doc *meta.BurnbridgeMediaChangeDocument) (burnbridgeControlPayload, error) {
	if req == nil {
		return burnbridgeControlPayload{}, fmt.Errorf("burnbridge: media change control request is required")
	}
	if doc == nil {
		return buildBurnbridgeControlPayload(req, bucket, false, nil, &burnbridgeControlError{
			Code:    s3err.GetAPIError(s3err.ErrNoSuchKey).Code,
			Message: "media change transcript unavailable",
		}, time.Now().UTC())
	}
	lastModified := mediaChangeLastModifiedFromJSON(mustMarshalJSON(doc))
	if doc.GrpcOK {
		return buildBurnbridgeControlPayload(req, bucket, true, doc, nil, lastModified)
	}
	errCode := doc.GrpcCode
	if strings.TrimSpace(errCode) == "" {
		errCode = "InternalError"
	}
	errMessage := doc.GrpcDetails
	if strings.TrimSpace(errMessage) == "" {
		errMessage = doc.Error
	}
	if strings.TrimSpace(errMessage) == "" {
		errMessage = doc.RecorderMessage
	}
	if strings.TrimSpace(errMessage) == "" {
		errMessage = "media change failed"
	}
	return buildBurnbridgeControlPayload(req, bucket, false, doc, &burnbridgeControlError{
		Code:    errCode,
		Message: errMessage,
	}, lastModified)
}

func buildTrayControlJSON(req *burnbridgeControlRequest, bucket string, doc *meta.BurnbridgeTrayDocument) (burnbridgeControlPayload, error) {
	if req == nil {
		return burnbridgeControlPayload{}, fmt.Errorf("burnbridge: tray control request is required")
	}
	if doc == nil {
		return buildBurnbridgeControlPayload(req, bucket, false, nil, &burnbridgeControlError{
			Code:    s3err.GetAPIError(s3err.ErrNoSuchKey).Code,
			Message: "tray transcript unavailable",
		}, time.Now().UTC())
	}
	lastModified := trayLastModifiedFromJSON(mustMarshalJSON(doc))
	if doc.GrpcOK {
		return buildBurnbridgeControlPayload(req, bucket, true, doc, nil, lastModified)
	}
	errCode := doc.GrpcCode
	if strings.TrimSpace(errCode) == "" {
		errCode = "InternalError"
	}
	errMessage := doc.GrpcDetails
	if strings.TrimSpace(errMessage) == "" {
		errMessage = doc.Error
	}
	if strings.TrimSpace(errMessage) == "" {
		errMessage = doc.RecorderMessage
	}
	if strings.TrimSpace(errMessage) == "" {
		errMessage = "tray control failed"
	}
	return buildBurnbridgeControlPayload(req, bucket, false, doc, &burnbridgeControlError{
		Code:    errCode,
		Message: errMessage,
	}, lastModified)
}

func mustMarshalJSON(v any) []byte {
	raw, _ := json.Marshal(v)
	return raw
}

func parseFinalizeControlDocument(raw []byte) *meta.BurnbridgeFinalizeLayoutDocument {
	if len(raw) == 0 {
		return nil
	}
	var doc meta.BurnbridgeFinalizeLayoutDocument
	if err := json.Unmarshal(raw, &doc); err != nil {
		return nil
	}
	return &doc
}

func parseMediaChangeControlDocument(raw []byte) *meta.BurnbridgeMediaChangeDocument {
	if len(raw) == 0 {
		return nil
	}
	var doc meta.BurnbridgeMediaChangeDocument
	if err := json.Unmarshal(raw, &doc); err != nil {
		return nil
	}
	return &doc
}

func parseTrayControlDocument(raw []byte) *meta.BurnbridgeTrayDocument {
	if len(raw) == 0 {
		return nil
	}
	var doc meta.BurnbridgeTrayDocument
	if err := json.Unmarshal(raw, &doc); err != nil {
		return nil
	}
	return &doc
}

func parseDiscInfoControlDocument(raw []byte) *meta.BurnbridgeDiscInfoDocument {
	if len(raw) == 0 {
		return nil
	}
	var doc meta.BurnbridgeDiscInfoDocument
	if err := json.Unmarshal(raw, &doc); err != nil {
		return nil
	}
	return &doc
}

func parseDriveInfoControlDocument(raw []byte) *meta.BurnbridgeDriveInfoDocument {
	if len(raw) == 0 {
		return nil
	}
	var doc meta.BurnbridgeDriveInfoDocument
	if err := json.Unmarshal(raw, &doc); err != nil {
		return nil
	}
	return &doc
}

func (b *BurnBridge) loadControlPayload(ctx context.Context, bucket string, req *burnbridgeControlRequest) (burnbridgeControlPayload, error) {
	if req == nil {
		return burnbridgeControlPayload{}, fmt.Errorf("burnbridge: control request is required")
	}

	switch req.Action {
	case burnbridgeControlActionDriveInfo:
		raw, doc, err := b.refreshDriveInfoDocument(ctx)
		if err != nil || len(raw) == 0 {
			raw, err = b.meta.GetBurnbridgeDriveInfoJSON(bucket)
			if err != nil {
				if errors.Is(err, meta.ErrNoSuchKey) {
					return burnbridgeControlPayload{}, s3err.GetAPIError(s3err.ErrNoSuchKey)
				}
				return burnbridgeControlPayload{}, err
			}
			doc = parseDriveInfoControlDocument(raw)
		}
		if doc == nil {
			doc = parseDriveInfoControlDocument(raw)
		}
		return buildDriveInfoControlJSON(req, bucket, doc)

	case burnbridgeControlActionDiscInfo:
		payloadBucket, targetBucket, err := b.resolveControlPayloadBucket(ctx, bucket, req)
		if err != nil {
			return burnbridgeControlPayload{}, err
		}
		raw, doc, err := b.refreshDiscInfoDocument(ctx, targetBucket)
		if err != nil || len(raw) == 0 {
			raw, err = b.meta.GetBurnbridgeDiscInfoJSON(targetBucket)
			if err != nil {
				if errors.Is(err, meta.ErrNoSuchKey) {
					return burnbridgeControlPayload{}, s3err.GetAPIError(s3err.ErrNoSuchKey)
				}
				return burnbridgeControlPayload{}, err
			}
			doc = parseDiscInfoControlDocument(raw)
		}
		if doc == nil {
			doc = parseDiscInfoControlDocument(raw)
		}
		return buildDiscInfoControlJSON(req, payloadBucket, doc)

	case burnbridgeControlActionFinalizeLayout, burnbridgeControlActionCloseDisc:
		payloadBucket, targetBucket, err := b.resolveControlPayloadBucket(ctx, bucket, req)
		if err != nil {
			return burnbridgeControlPayload{}, err
		}
		raw, err := b.loadOrFinalizeLayoutTranscript(ctx, targetBucket, req)
		if err != nil {
			if errors.Is(err, meta.ErrNoSuchKey) {
				return burnbridgeControlPayload{}, s3err.GetAPIError(s3err.ErrNoSuchKey)
			}
			return burnbridgeControlPayload{}, err
		}
		doc := parseFinalizeControlDocument(raw)
		if payloadBucket != targetBucket {
			if doc != nil {
				doc.Bucket = targetBucket
			}
		}
		return buildFinalizeControlJSON(req, payloadBucket, doc)

	case burnbridgeControlActionMediaRemoved, burnbridgeControlActionMediaInserted:
		raw, err := b.loadOrHandleMediaChangeTranscript(ctx, bucket, req)
		if err != nil {
			if errors.Is(err, meta.ErrNoSuchKey) {
				return burnbridgeControlPayload{}, s3err.GetAPIError(s3err.ErrNoSuchKey)
			}
			return burnbridgeControlPayload{}, err
		}
		doc := parseMediaChangeControlDocument(raw)
		return buildMediaChangeControlJSON(req, bucket, doc)
	case burnbridgeControlActionTrayOpen, burnbridgeControlActionTrayClose:
		raw, err := b.loadOrHandleTrayTranscript(ctx, bucket, req)
		if err != nil {
			if errors.Is(err, meta.ErrNoSuchKey) {
				return burnbridgeControlPayload{}, s3err.GetAPIError(s3err.ErrNoSuchKey)
			}
			return burnbridgeControlPayload{}, err
		}
		doc := parseTrayControlDocument(raw)
		return buildTrayControlJSON(req, bucket, doc)
	default:
		return burnbridgeControlPayload{}, burnbridgeInvalidRequest(fmt.Sprintf("unsupported burnbridge control action %q", req.Action))
	}
}

func buildFinalizeLayoutResultJSON(bucket string, req *burnbridgeControlRequest, closeDisc bool, resp *burnbridgev1.FinalizeLayoutResponse, grpcErr error) ([]byte, error) {
	doc := meta.BurnbridgeFinalizeLayoutDocument{
		Bucket:         bucket,
		RequestID:      strings.TrimSpace(req.RequestID),
		RequestTime:    req.RequestTime,
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

func finalizeObjectKeyForCloseDisc(closeDisc bool) string {
	if closeDisc {
		return meta.BurnbridgeCloseDiscObjectKey
	}
	return meta.BurnbridgeFinalizeLayoutObjectKey
}

func mediaChangeObjectKeyForAction(action burnbridgeControlAction) (string, burnbridgev1.MediaChangeAction, error) {
	switch action {
	case burnbridgeControlActionMediaRemoved:
		return meta.BurnbridgeMediaRemovedObjectKey, burnbridgev1.MediaChangeAction_MEDIA_CHANGE_ACTION_REMOVED, nil
	case burnbridgeControlActionMediaInserted:
		return meta.BurnbridgeMediaInsertedObjectKey, burnbridgev1.MediaChangeAction_MEDIA_CHANGE_ACTION_INSERTED, nil
	default:
		return "", burnbridgev1.MediaChangeAction_MEDIA_CHANGE_ACTION_UNSPECIFIED,
			burnbridgeInvalidRequest(fmt.Sprintf("unsupported media change action %q", action))
	}
}

func trayObjectKeyForAction(action burnbridgeControlAction) (string, burnbridgev1.TrayAction, error) {
	switch action {
	case burnbridgeControlActionTrayOpen:
		return meta.BurnbridgeTrayOpenObjectKey, burnbridgev1.TrayAction_TRAY_ACTION_OPEN, nil
	case burnbridgeControlActionTrayClose:
		return meta.BurnbridgeTrayCloseObjectKey, burnbridgev1.TrayAction_TRAY_ACTION_CLOSE, nil
	default:
		return "", burnbridgev1.TrayAction_TRAY_ACTION_UNSPECIFIED,
			burnbridgeInvalidRequest(fmt.Sprintf("unsupported tray action %q", action))
	}
}

// invokeFinalizeLayoutAgainstRecorder persists a JSON transcript (success or RPC error) for a reserved finalize-style object key.
func shouldReuseFinalizeLayoutTranscript(bucket string, req *burnbridgeControlRequest, closeDisc bool, raw []byte) bool {
	if req == nil {
		return false
	}

	var doc meta.BurnbridgeFinalizeLayoutDocument
	if err := json.Unmarshal(raw, &doc); err != nil {
		return false
	}
	if !strings.EqualFold(strings.TrimSpace(doc.Bucket), strings.TrimSpace(bucket)) {
		return false
	}
	if doc.CloseDisc != closeDisc {
		return false
	}
	if doc.RequestTime != req.RequestTime {
		return false
	}
	return strings.EqualFold(strings.TrimSpace(doc.RequestID), strings.TrimSpace(req.RequestID))
}

func (b *BurnBridge) invokeFinalizeLayoutAgainstRecorder(ctx context.Context, bucket string, req *burnbridgeControlRequest) ([]byte, error) {
	b.putSerialMu.Lock()
	defer b.putSerialMu.Unlock()

	closeDisc := req.Action == burnbridgeControlActionCloseDisc
	objectKey := finalizeObjectKeyForCloseDisc(closeDisc)
	if prev, err := b.meta.GetBurnbridgeFinalizeLayoutJSON(bucket, objectKey); err == nil && shouldReuseFinalizeLayoutTranscript(bucket, req, closeDisc, prev) {
		return prev, nil
	}

	recCtx := ctx
	if recCtx == nil {
		recCtx = context.Background()
	}
	resp, grpcErr := b.grpc.FinalizeLayout(recCtx, &burnbridgev1.FinalizeLayoutRequest{
		Bucket:         bucket,
		UdfVolumeLabel: b.udfLabel,
		CloseDisc:      closeDisc,
	})

	payload, mErr := buildFinalizeLayoutResultJSON(bucket, req, closeDisc, resp, grpcErr)
	if mErr != nil {
		return nil, fmt.Errorf("burnbridge finalize layout json: %w", mErr)
	}
	if err := b.meta.StoreBurnbridgeFinalizeLayoutJSON(bucket, objectKey, payload); err != nil {
		return nil, err
	}
	if grpcErr != nil {
		slog.Warn("burnbridge: FinalizeLayout gRPC reported failure (transcript stored for GET)",
			"bucket", bucket, "grpc_err", grpcErr)
	}
	return payload, nil
}

func buildMediaChangeResultJSON(bucket string, req *burnbridgeControlRequest, resp *burnbridgev1.HandleMediaChangeResponse, grpcErr error) ([]byte, error) {
	doc := meta.BurnbridgeMediaChangeDocument{
		Bucket:         bucket,
		RequestID:      strings.TrimSpace(req.RequestID),
		RequestTime:    req.RequestTime,
		Action:         string(req.Action),
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
			doc.ImportedBucket = resp.GetImportedBucket()
			if snapshot := resp.GetSnapshot(); snapshot != nil {
				doc.Ready = snapshot.GetReady()
				doc.VolumeLabel = snapshot.GetVolumeLabel()
				doc.WritableState = snapshot.GetWritableState()
			}
		}
	}
	return json.Marshal(doc)
}

func shouldReuseMediaChangeTranscript(bucket string, req *burnbridgeControlRequest, raw []byte) bool {
	if req == nil {
		return false
	}

	var doc meta.BurnbridgeMediaChangeDocument
	if err := json.Unmarshal(raw, &doc); err != nil {
		return false
	}
	if !strings.EqualFold(strings.TrimSpace(doc.Bucket), strings.TrimSpace(bucket)) {
		return false
	}
	if doc.Action != string(req.Action) {
		return false
	}
	if doc.RequestTime != req.RequestTime {
		return false
	}
	return strings.EqualFold(strings.TrimSpace(doc.RequestID), strings.TrimSpace(req.RequestID))
}

func (b *BurnBridge) invokeMediaChangeAgainstRecorder(ctx context.Context, bucket string, req *burnbridgeControlRequest) ([]byte, error) {
	b.putSerialMu.Lock()
	defer b.putSerialMu.Unlock()

	objectKey, action, err := mediaChangeObjectKeyForAction(req.Action)
	if err != nil {
		return nil, err
	}
	if prev, err := b.meta.GetBurnbridgeMediaChangeJSON(bucket, objectKey); err == nil && shouldReuseMediaChangeTranscript(bucket, req, prev) {
		return prev, nil
	}

	recCtx := ctx
	if recCtx == nil {
		recCtx = context.Background()
	}
	resp, grpcErr := b.grpc.HandleMediaChange(recCtx, &burnbridgev1.HandleMediaChangeRequest{
		Action: action,
		Reason: "gateway-control-" + string(req.Action),
	})

	payload, mErr := buildMediaChangeResultJSON(bucket, req, resp, grpcErr)
	if mErr != nil {
		return nil, fmt.Errorf("burnbridge media change json: %w", mErr)
	}
	if err := b.meta.StoreBurnbridgeMediaChangeJSON(bucket, objectKey, payload); err != nil {
		return nil, err
	}
	if grpcErr != nil {
		slog.Warn("burnbridge: HandleMediaChange gRPC reported failure (transcript stored for GET)",
			"bucket", bucket, "action", req.Action, "grpc_err", grpcErr)
	}
	if resp != nil && resp.GetSnapshot() != nil {
		if err := b.syncActiveDiscState(resp.GetSnapshot()); err != nil {
			slog.Warn("burnbridge: failed to sync active disc state after media change",
				"bucket", bucket, "action", req.Action, "err", err)
		}
		b.recordRecorderReadyState(resp.GetSnapshot())
	}
	return payload, nil
}

func buildTrayResultJSON(bucket string, req *burnbridgeControlRequest, resp *burnbridgev1.HandleTrayResponse, grpcErr error) ([]byte, error) {
	doc := meta.BurnbridgeTrayDocument{
		Bucket:         bucket,
		RequestID:      strings.TrimSpace(req.RequestID),
		RequestTime:    req.RequestTime,
		Action:         string(req.Action),
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

func shouldReuseTrayTranscript(bucket string, req *burnbridgeControlRequest, raw []byte) bool {
	if req == nil {
		return false
	}

	var doc meta.BurnbridgeTrayDocument
	if err := json.Unmarshal(raw, &doc); err != nil {
		return false
	}
	if !strings.EqualFold(strings.TrimSpace(doc.Bucket), strings.TrimSpace(bucket)) {
		return false
	}
	if doc.Action != string(req.Action) {
		return false
	}
	if doc.RequestTime != req.RequestTime {
		return false
	}
	return strings.EqualFold(strings.TrimSpace(doc.RequestID), strings.TrimSpace(req.RequestID))
}

func (b *BurnBridge) invokeTrayAgainstRecorder(ctx context.Context, bucket string, req *burnbridgeControlRequest) ([]byte, error) {
	b.putSerialMu.Lock()
	defer b.putSerialMu.Unlock()

	objectKey, action, err := trayObjectKeyForAction(req.Action)
	if err != nil {
		return nil, err
	}
	if prev, err := b.meta.GetBurnbridgeTrayJSON(bucket, objectKey); err == nil && shouldReuseTrayTranscript(bucket, req, prev) {
		return prev, nil
	}

	recCtx := ctx
	if recCtx == nil {
		recCtx = context.Background()
	}
	resp, grpcErr := b.grpc.HandleTray(recCtx, &burnbridgev1.HandleTrayRequest{
		Action: action,
		Reason: "gateway-control-" + string(req.Action),
	})

	payload, mErr := buildTrayResultJSON(bucket, req, resp, grpcErr)
	if mErr != nil {
		return nil, fmt.Errorf("burnbridge tray json: %w", mErr)
	}
	if err := b.meta.StoreBurnbridgeTrayJSON(bucket, objectKey, payload); err != nil {
		return nil, err
	}
	if grpcErr != nil {
		slog.Warn("burnbridge: HandleTray gRPC reported failure (transcript stored for GET)",
			"bucket", bucket, "action", req.Action, "grpc_err", grpcErr)
	}
	return payload, nil
}

func (b *BurnBridge) invalidateFinalizeLayoutTranscript(bucket string) {
	for _, objectKey := range []string{meta.BurnbridgeFinalizeLayoutObjectKey, meta.BurnbridgeCloseDiscObjectKey} {
		if err := b.meta.DeleteBurnbridgeFinalizeLayoutJSON(bucket, objectKey); err != nil && !errors.Is(err, meta.ErrNoSuchKey) {
			slog.Warn("burnbridge: failed to invalidate cached finalize transcript",
				"bucket", bucket, "object_key", objectKey, "err", err)
			continue
		}
		slog.Info("burnbridge: invalidated cached finalize transcript",
			"bucket", bucket, "object_key", objectKey)
	}
}

func (b *BurnBridge) loadOrFinalizeLayoutTranscript(ctx context.Context, bucket string, req *burnbridgeControlRequest) ([]byte, error) {
	closeDisc := req.Action == burnbridgeControlActionCloseDisc
	objectKey := finalizeObjectKeyForCloseDisc(closeDisc)
	if prev, err := b.meta.GetBurnbridgeFinalizeLayoutJSON(bucket, objectKey); err == nil && shouldReuseFinalizeLayoutTranscript(bucket, req, closeDisc, prev) {
		return prev, nil
	}

	groupKey := bucket + "|" + req.Key

	value, err, _ := b.finalizeGroup.Do(groupKey, func() (interface{}, error) {
		if prev, err := b.meta.GetBurnbridgeFinalizeLayoutJSON(bucket, objectKey); err == nil && shouldReuseFinalizeLayoutTranscript(bucket, req, closeDisc, prev) {
			return prev, nil
		}
		return b.invokeFinalizeLayoutAgainstRecorder(ctx, bucket, req)
	})
	if err != nil {
		return nil, err
	}
	payload, ok := value.([]byte)
	if !ok {
		return nil, fmt.Errorf("burnbridge: unexpected finalize transcript type %T", value)
	}
	return payload, nil
}

func (b *BurnBridge) loadOrHandleMediaChangeTranscript(ctx context.Context, bucket string, req *burnbridgeControlRequest) ([]byte, error) {
	objectKey, _, err := mediaChangeObjectKeyForAction(req.Action)
	if err != nil {
		return nil, err
	}
	if prev, err := b.meta.GetBurnbridgeMediaChangeJSON(bucket, objectKey); err == nil && shouldReuseMediaChangeTranscript(bucket, req, prev) {
		return prev, nil
	}

	groupKey := bucket + "|" + req.Key

	value, err, _ := b.finalizeGroup.Do(groupKey, func() (interface{}, error) {
		if prev, err := b.meta.GetBurnbridgeMediaChangeJSON(bucket, objectKey); err == nil && shouldReuseMediaChangeTranscript(bucket, req, prev) {
			return prev, nil
		}
		return b.invokeMediaChangeAgainstRecorder(ctx, bucket, req)
	})
	if err != nil {
		return nil, err
	}
	payload, ok := value.([]byte)
	if !ok {
		return nil, fmt.Errorf("burnbridge: unexpected media change transcript type %T", value)
	}
	return payload, nil
}

func (b *BurnBridge) loadOrHandleTrayTranscript(ctx context.Context, bucket string, req *burnbridgeControlRequest) ([]byte, error) {
	objectKey, _, err := trayObjectKeyForAction(req.Action)
	if err != nil {
		return nil, err
	}
	if prev, err := b.meta.GetBurnbridgeTrayJSON(bucket, objectKey); err == nil && shouldReuseTrayTranscript(bucket, req, prev) {
		return prev, nil
	}

	groupKey := bucket + "|" + req.Key

	value, err, _ := b.finalizeGroup.Do(groupKey, func() (interface{}, error) {
		if prev, err := b.meta.GetBurnbridgeTrayJSON(bucket, objectKey); err == nil && shouldReuseTrayTranscript(bucket, req, prev) {
			return prev, nil
		}
		return b.invokeTrayAgainstRecorder(ctx, bucket, req)
	})
	if err != nil {
		return nil, err
	}
	payload, ok := value.([]byte)
	if !ok {
		return nil, fmt.Errorf("burnbridge: unexpected tray transcript type %T", value)
	}
	return payload, nil
}

func (b *BurnBridge) HeadObject(ctx context.Context, input *s3.HeadObjectInput) (*s3.HeadObjectOutput, error) {
	if input == nil || input.Bucket == nil || input.Key == nil {
		return nil, fmt.Errorf("bucket/key required")
	}
	bucket := *input.Bucket
	key := *input.Key
	if controlReq, isControl, err := b.parseControlRequestForBucket(bucket, key); isControl {
		if err != nil {
			return nil, err
		}
		if !b.controlRequestBucketAllowed(bucket, controlReq) {
			return nil, s3err.GetAPIError(s3err.ErrNoSuchBucket)
		}
		payload, err := b.loadControlPayload(ctx, bucket, controlReq)
		if err != nil {
			return nil, err
		}
		clen := int64(len(payload.Raw))
		etag := quotedMD5Bytes(payload.Raw)
		ct := discInfoContentType
		return &s3.HeadObjectOutput{
			ContentType:   &ct,
			ContentLength: &clen,
			ETag:          &etag,
			LastModified:  backend.GetTimePtr(payload.LastModified),
		}, nil
	}

	if !b.burnbridgeBucketExists(bucket) {
		return nil, s3err.GetAPIError(s3err.ErrNoSuchBucket)
	}

	summary, err := b.meta.GetCommittedObjectSummary(bucket, key)
	if err != nil {
		if errors.Is(err, meta.ErrNoSuchKey) {
			if syncErr := b.ensureImportedBucketState(ctx, bucket); syncErr != nil {
				return nil, syncErr
			}
			summary, err = b.meta.GetCommittedObjectSummary(bucket, key)
		}
		if err != nil {
			if errors.Is(err, meta.ErrNoSuchKey) {
				return b.headMountedFallbackObject(ctx, bucket, key)
			}
			return nil, err
		}
	}

	etagCopy := summary.ETag
	if etagCopy == "" {
		etagCopy = emptyQuotedMD5
	}
	clen := summary.Size
	lm := summary.LastModified
	if b.mountedFallbackAvailable(bucket) {
		obj, err := b.openMountedFallbackObject(bucket, key)
		if err != nil {
			return nil, mapOpenError(err)
		}
		_ = obj.file.Close()
		clen = obj.info.Size()
		lm = obj.info.ModTime().UTC()
	}

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

func bbSafeObjectPathUnder(root, key string) (string, error) {
	root = filepath.Clean(root)
	if root == "" || root == "." {
		return "", fmt.Errorf("burnbridge: read mount path invalid")
	}
	absRoot, err := filepath.Abs(root)
	if err != nil {
		return "", err
	}
	rel := filepath.FromSlash(strings.TrimPrefix(key, "/"))
	if rel == "" || rel == "." {
		return "", fmt.Errorf("burnbridge: empty object key")
	}
	full := filepath.Join(absRoot, rel)
	absFull, err := filepath.Abs(full)
	if err != nil {
		return "", err
	}
	relOut, err := filepath.Rel(absRoot, absFull)
	if err != nil || relOut == ".." || strings.HasPrefix(relOut, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("burnbridge: object path escapes read mount")
	}
	return absFull, nil
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

type mountedFallbackObject struct {
	file *os.File
	info os.FileInfo
	path string
	root string
	key  string
}

func (b *BurnBridge) mountedFallbackRoot(bucket string) (string, bool) {
	if b.readMount == "" || b.noDiscLatchedState() {
		return "", false
	}
	activeBucket := strings.TrimSpace(b.activeBucket)
	if activeBucket == "" || !strings.EqualFold(strings.TrimSpace(bucket), activeBucket) {
		return "", false
	}

	mountRoot := filepath.Clean(b.readMount)
	if st, err := os.Stat(mountRoot); err != nil || !st.IsDir() {
		return "", false
	}

	if mountedRootHasArchiveMetadata(mountRoot) && b.bucketUsesRedundancy(bucket) {
		return "", false
	}
	if !mountRootHasVisibleEntries(mountRoot) {
		return "", false
	}
	return mountRoot, true
}

func (b *BurnBridge) bucketUsesRedundancy(bucket string) bool {
	trimmedBucket := strings.TrimSpace(bucket)
	if trimmedBucket == "" {
		return true
	}

	enabledRaw, err := b.meta.RetrieveAttribute(nil, trimmedBucket, "", "redundancy_enabled")
	if err != nil {
		return true
	}
	enabledText := strings.TrimSpace(string(enabledRaw))
	enabled, err := strconv.ParseBool(enabledText)
	if err != nil {
		return true
	}
	if !enabled {
		return false
	}

	parityRaw, err := b.meta.RetrieveAttribute(nil, trimmedBucket, "", "redundancy_parity_block_count")
	if err != nil {
		return true
	}
	parityCount, err := strconv.Atoi(strings.TrimSpace(string(parityRaw)))
	if err != nil {
		return true
	}
	return parityCount > 0
}

func (b *BurnBridge) mountedFallbackBucketHint() (string, bool) {
	if b.readMount == "" || b.noDiscLatchedState() {
		return "", false
	}
	mountRoot := filepath.Clean(b.readMount)
	if st, err := os.Stat(mountRoot); err != nil || !st.IsDir() {
		return "", false
	}

	entries, err := os.ReadDir(mountRoot)
	if err != nil {
		return "", false
	}

	var bucketHint string
	for _, entry := range entries {
		name := strings.TrimSpace(entry.Name())
		if !entry.IsDir() {
			if bucket := bucketFromMountedMetadataFileName(name); bucket != "" {
				if bucketHint != "" && !strings.EqualFold(bucketHint, bucket) {
					return "", false
				}
				bucketHint = bucket
				continue
			}
		}
		if shouldSkipMountedFallbackEntry(name, entry.IsDir()) {
			continue
		}
	}
	if bucketHint == "" {
		return "", false
	}
	return bucketHint, true
}

func bucketFromMountedMetadataFileName(name string) string {
	trimmed := strings.TrimSpace(name)
	lower := strings.ToLower(trimmed)
	if !strings.HasPrefix(lower, "oa") || !strings.HasSuffix(lower, ".sqlite3") {
		return ""
	}
	bucket := trimmed[2 : len(trimmed)-len(".sqlite3")]
	if isS3BucketNameLike(bucket) {
		return bucket
	}
	return ""
}

func isS3BucketNameLike(name string) bool {
	trimmed := strings.TrimSpace(name)
	if len(trimmed) < 3 || len(trimmed) > 63 {
		return false
	}
	if !isS3BucketNameRune(rune(trimmed[0])) || !isS3BucketNameRune(rune(trimmed[len(trimmed)-1])) {
		return false
	}
	for i, r := range trimmed {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-' || r == '.' {
			if i > 0 {
				prev := trimmed[i-1]
				if (prev == '.' && (r == '.' || r == '-')) || (prev == '-' && r == '.') {
					return false
				}
			}
			continue
		}
		return false
	}
	return true
}

func isS3BucketNameRune(r rune) bool {
	return (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9')
}

func mountedRootHasArchiveMetadata(root string) bool {
	entries, err := os.ReadDir(root)
	if err != nil {
		return false
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if bucketFromMountedMetadataFileName(entry.Name()) != "" {
			return true
		}
	}
	return false
}

func mountRootHasVisibleEntries(root string) bool {
	entries, err := os.ReadDir(root)
	if err != nil {
		return false
	}
	for _, entry := range entries {
		if shouldSkipMountedFallbackEntry(entry.Name(), entry.IsDir()) {
			continue
		}
		return true
	}
	return false
}

func (b *BurnBridge) openMountedFallbackObject(bucket, key string) (*mountedFallbackObject, error) {
	root, ok := b.mountedFallbackRoot(bucket)
	if !ok {
		return nil, s3err.GetAPIError(s3err.ErrNoSuchKey)
	}
	objPath, err := bbSafeObjectPathUnder(root, key)
	if err != nil {
		return nil, err
	}
	f, err := os.Open(objPath)
	if err != nil {
		return nil, err
	}
	fi, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	if fi.IsDir() {
		_ = f.Close()
		return nil, syscall.EISDIR
	}
	return &mountedFallbackObject{file: f, info: fi, path: objPath, root: root, key: key}, nil
}

func (b *BurnBridge) mountedFallbackAvailable(bucket string) bool {
	_, ok := b.mountedFallbackRoot(bucket)
	return ok
}

func mountedFallbackETag(fi os.FileInfo) string {
	payload := fmt.Sprintf("%s:%d:%d", fi.Name(), fi.Size(), fi.ModTime().UTC().UnixNano())
	sum := md5.Sum([]byte(payload))
	return `"` + hex.EncodeToString(sum[:]) + `"`
}

func (b *BurnBridge) headMountedFallbackObject(ctx context.Context, bucket, key string) (*s3.HeadObjectOutput, error) {
	_ = b.ensureActiveBucketLoaded(ctx)
	obj, err := b.openMountedFallbackObject(bucket, key)
	if err != nil {
		return nil, mapOpenError(err)
	}
	_ = obj.file.Close()

	ct := burnbridgeDefaultContentType
	etag := mountedFallbackETag(obj.info)
	clen := obj.info.Size()
	lm := obj.info.ModTime().UTC()
	return &s3.HeadObjectOutput{
		ContentType:   &ct,
		ETag:          &etag,
		LastModified:  backend.GetTimePtr(lm),
		ContentLength: &clen,
	}, nil
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
	if controlReq, isControl, err := b.parseControlRequestForBucket(bucket, key); isControl {
		if err != nil {
			return nil, err
		}
		if !b.controlRequestBucketAllowed(bucket, controlReq) {
			return nil, s3err.GetAPIError(s3err.ErrNoSuchBucket)
		}
		payload, err := b.loadControlPayload(ctx, bucket, controlReq)
		if err != nil {
			return nil, err
		}
		objSize := int64(len(payload.Raw))
		startOffset, length, contentRange, err := parseCommittedGetRange(objSize, backend.GetStringFromPtr(input.Range))
		if err != nil {
			return nil, err
		}
		slice := payload.Raw[startOffset : startOffset+length]
		etag := quotedMD5Bytes(payload.Raw)
		ct := discInfoContentType
		clen := length
		return &s3.GetObjectOutput{
			Body:          io.NopCloser(bytes.NewReader(slice)),
			AcceptRanges:  backend.GetPtrFromString("bytes"),
			ETag:          &etag,
			LastModified:  backend.GetTimePtr(payload.LastModified),
			ContentLength: &clen,
			ContentRange:  contentRange,
			StorageClass:  types.StorageClassStandard,
			ContentType:   &ct,
		}, nil
	}

	if !b.burnbridgeBucketExists(bucket) {
		return nil, s3err.GetAPIError(s3err.ErrNoSuchBucket)
	}

	if input.PartNumber != nil && *input.PartNumber > 1 {
		return nil, s3err.GetAPIError(s3err.ErrInvalidPartNumber)
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
			if syncErr := b.ensureImportedBucketState(ctx, bucket); syncErr != nil {
				return fail(syncErr)
			}
			summary, err = b.meta.GetCommittedObjectSummary(bucket, key)
		}
		if err != nil {
			if errors.Is(err, meta.ErrNoSuchKey) {
				return b.getMountedFallbackObject(ctx, bucket, key, backend.GetStringFromPtr(input.Range), wrapBody, fail)
			}
			return fail(err)
		}
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
			if b.mountedFallbackAvailable(bucket) {
				return b.getMountedFallbackObject(ctx, bucket, key, rangeHdr, wrapBody, fail)
			}
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
		readCtx, cancel := context.WithCancel(context.Background())
		stream, err := b.grpc.ReadObject(readCtx, &burnbridgev1.ReadObjectRequest{
			Bucket:    bucket,
			ObjectKey: key,
			Offset:    startOffset,
			Length:    length,
		})
		if err != nil {
			cancel()
			return fail(mapReadFallbackError(err))
		}
		body = &grpcObjectReadCloser{stream: stream, cancel: cancel, left: length}
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

func (b *BurnBridge) getMountedFallbackObject(
	ctx context.Context,
	bucket string,
	key string,
	rangeHdr string,
	wrapBody func(io.ReadCloser) io.ReadCloser,
	fail func(error) (*s3.GetObjectOutput, error),
) (*s3.GetObjectOutput, error) {
	_ = b.ensureActiveBucketLoaded(ctx)
	obj, err := b.openMountedFallbackObject(bucket, key)
	if err != nil {
		return fail(mapOpenError(err))
	}

	objSize := obj.info.Size()
	startOffset, length, contentRange, err := parseCommittedGetRange(objSize, rangeHdr)
	if err != nil {
		_ = obj.file.Close()
		return fail(err)
	}

	var body io.ReadCloser = obj.file
	if startOffset != 0 || length != objSize {
		rdr := io.NewSectionReader(obj.file, startOffset, length)
		body = &backend.FileSectionReadCloser{R: rdr, F: obj.file}
	}

	ct := burnbridgeDefaultContentType
	etag := mountedFallbackETag(obj.info)
	lm := obj.info.ModTime().UTC()
	clen := length
	return &s3.GetObjectOutput{
		Body:          wrapBody(body),
		AcceptRanges:  backend.GetPtrFromString("bytes"),
		ETag:          &etag,
		LastModified:  backend.GetTimePtr(lm),
		ContentLength: &clen,
		ContentRange:  contentRange,
		StorageClass:  types.StorageClassStandard,
		ContentType:   &ct,
	}, nil
}

func (b *BurnBridge) prepareCommittedListing(ctx context.Context, bucket string) (fstest.MapFS, map[string]meta.CommittedObjectSummary, error) {
	if b.isDriveControlBucket(bucket) {
		return fstest.MapFS{}, map[string]meta.CommittedObjectSummary{}, nil
	}
	if !b.burnbridgeBucketExists(bucket) {
		return nil, nil, s3err.GetAPIError(s3err.ErrNoSuchBucket)
	}
	fsys, byKey, err := b.committedMapFSAndSummaries(bucket)
	if err != nil {
		return nil, nil, err
	}
	if len(byKey) > 0 {
		return b.mergeMountedFallbackListing(ctx, bucket, fsys, byKey)
	}

	if err := b.ensureImportedBucketState(ctx, bucket); err != nil {
		return nil, nil, err
	}
	fsys, byKey, err = b.committedMapFSAndSummaries(bucket)
	if err != nil {
		return nil, nil, err
	}
	if len(byKey) > 0 {
		return b.mergeMountedFallbackListing(ctx, bucket, fsys, byKey)
	}
	return b.mountedFallbackMapFSAndSummaries(ctx, bucket)
}

func (b *BurnBridge) mergeMountedFallbackListing(
	ctx context.Context,
	bucket string,
	baseFS fstest.MapFS,
	baseByKey map[string]meta.CommittedObjectSummary,
) (fstest.MapFS, map[string]meta.CommittedObjectSummary, error) {
	mountFS, mountByKey, err := b.mountedFallbackMapFSAndSummaries(ctx, bucket)
	if err != nil {
		return nil, nil, err
	}
	if len(mountByKey) == 0 {
		return baseFS, baseByKey, nil
	}

	mergedFS := fstest.MapFS{}
	mergedByKey := make(map[string]meta.CommittedObjectSummary, len(mountByKey))
	for key, sum := range baseByKey {
		if _, existsOnDisc := mountByKey[key]; !existsOnDisc {
			continue
		}
		addMapFSPath(mergedFS, key)
		mergedByKey[key] = sum
	}

	for path, file := range mountFS {
		if _, exists := mergedFS[path]; !exists {
			mergedFS[path] = file
		}
	}
	for key, sum := range mountByKey {
		if _, exists := mergedByKey[key]; exists {
			continue
		}
		mergedByKey[key] = sum
	}
	return mergedFS, mergedByKey, nil
}

func addMapFSPath(fsys fstest.MapFS, key string) {
	k := strings.TrimPrefix(strings.ReplaceAll(key, `\`, `/`), "/")
	if k == "" || k == "." {
		return
	}
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
		} else if _, ok := fsys[path]; !ok {
			fsys[path] = &fstest.MapFile{Mode: 0o644}
		}
	}
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
		addMapFSPath(fsys, k)
	}
	return fsys, byKey, nil
}

func (b *BurnBridge) mountedFallbackMapFSAndSummaries(ctx context.Context, bucket string) (fstest.MapFS, map[string]meta.CommittedObjectSummary, error) {
	_ = b.ensureActiveBucketLoaded(ctx)
	root, ok := b.mountedFallbackRoot(bucket)
	if !ok {
		return fstest.MapFS{}, map[string]meta.CommittedObjectSummary{}, nil
	}

	fsys := fstest.MapFS{}
	byKey := map[string]meta.CommittedObjectSummary{}
	count := 0
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if path == root {
			return nil
		}
		name := d.Name()
		if d.IsDir() && strings.EqualFold(strings.TrimSpace(name), strings.TrimSpace(bucket)) {
			return filepath.SkipDir
		}
		if shouldSkipMountedFallbackEntry(name, d.IsDir()) {
			if d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}

		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		key := filepath.ToSlash(rel)
		key = strings.TrimPrefix(key, "/")
		if key == "" || key == "." {
			return nil
		}

		if d.IsDir() {
			fsys[key] = &fstest.MapFile{Mode: fs.ModeDir | 0o755}
			return nil
		}

		if count >= mountedReadFallbackMaxFiles {
			return fmt.Errorf("burnbridge: mounted read fallback exceeded max file count %d", mountedReadFallbackMaxFiles)
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		count++
		fsys[key] = &fstest.MapFile{Mode: 0o644}
		byKey[key] = meta.CommittedObjectSummary{
			ObjectKey:    key,
			Size:         info.Size(),
			LastModified: info.ModTime().UTC(),
			ETag:         mountedFallbackETag(info),
			ContentType:  burnbridgeDefaultContentType,
		}
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	return fsys, byKey, nil
}

func shouldSkipMountedFallbackEntry(name string, isDir bool) bool {
	trimmed := strings.TrimSpace(name)
	lower := strings.ToLower(trimmed)
	if trimmed == "" {
		return true
	}
	if isDir {
		return strings.EqualFold(trimmed, "__MACOSX")
	}
	if strings.EqualFold(trimmed, ".DS_Store") {
		return true
	}
	if strings.HasPrefix(lower, "oa") && strings.HasSuffix(lower, ".sqlite3") {
		return true
	}
	switch lower {
	case "oa.sqlite3", "metadata.sqlite3", "archive.sqlite3", "thumbs.db", "desktop.ini":
		return true
	default:
		return false
	}
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

func (s uploadRecoveryStats) AllSegmentsSkipped() bool {
	return s.TotalSegments > 0 && s.SkippedSegments == s.TotalSegments && s.ReplayedSegments == 0
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

func multipartSegmentIndexForOffset(snapshot map[int]meta.BurnObjectSegment, startOffset int64) int {
	if len(snapshot) == 0 || startOffset <= 0 {
		return 0
	}
	exactIdx := -1
	prevIdx := -1
	prevOffset := int64(-1)
	for idx, seg := range snapshot {
		switch {
		case seg.ByteOffset == startOffset:
			if exactIdx == -1 || idx < exactIdx {
				exactIdx = idx
			}
		case seg.ByteOffset < startOffset:
			if seg.ByteOffset > prevOffset || (seg.ByteOffset == prevOffset && idx > prevIdx) {
				prevIdx = idx
				prevOffset = seg.ByteOffset
			}
		}
	}
	if exactIdx >= 0 {
		return exactIdx
	}
	if prevIdx >= 0 {
		return prevIdx + 1
	}
	return 0
}

func hashReaderMD5(r io.Reader, bufferSize int) (int64, string, error) {
	if bufferSize <= 0 {
		bufferSize = defaultChunkSizeBytes
	}
	buf := make([]byte, bufferSize)
	hash := md5.New()
	var total int64
	for {
		n, err := r.Read(buf)
		if n > 0 {
			total += int64(n)
			_, _ = hash.Write(buf[:n])
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return total, "", err
		}
	}
	return total, hex.EncodeToString(hash.Sum(nil)), nil
}

func completedMultipartReplayOutput(part meta.BurnUploadPartRecord, body io.Reader, bufferSize int) (*s3.UploadPartOutput, error) {
	size, digest, err := hashReaderMD5(body, bufferSize)
	if err != nil {
		return nil, err
	}
	if size != part.PartSize {
		return nil, s3err.GetAPIError(s3err.ErrInvalidPart)
	}
	if part.ChecksumMD5 != "" && !strings.EqualFold(part.ChecksumMD5, digest) {
		return nil, s3err.GetAPIError(s3err.ErrInvalidPart)
	}
	etag := part.ETag
	out := &s3.UploadPartOutput{ETag: &etag}
	if part.ChecksumMD5 != "" {
		checksumMD5 := part.ChecksumMD5
		out.ChecksumMD5 = &checksumMD5
	}
	return out, nil
}

func quotedETag(md5Hex string) string {
	h := strings.TrimSpace(strings.ToLower(md5Hex))
	if h == "" {
		return emptyQuotedMD5
	}
	h = strings.TrimPrefix(strings.TrimSuffix(h, `"`), `"`)
	return `"` + h + `"`
}

func buildCreateJobMetadata(input s3response.PutObjectInput) []*burnbridgev1.ObjectMetadata {
	items := make([]*burnbridgev1.ObjectMetadata, 0, 16)
	appendKV := func(key string, value *string) {
		if value == nil || strings.TrimSpace(*value) == "" {
			return
		}
		items = append(items, &burnbridgev1.ObjectMetadata{Key: key, Value: strings.TrimSpace(*value)})
	}

	appendKV("content-type", input.ContentType)
	appendKV("content-encoding", input.ContentEncoding)
	appendKV("content-disposition", input.ContentDisposition)
	appendKV("content-language", input.ContentLanguage)
	appendKV("cache-control", input.CacheControl)
	appendKV("expires", input.Expires)

	for key, value := range input.Metadata {
		trimmedKey := strings.TrimSpace(key)
		if trimmedKey == "" {
			continue
		}
		items = append(items, &burnbridgev1.ObjectMetadata{
			Key:   "x-amz-meta-" + trimmedKey,
			Value: value,
		})
	}

	return items
}

func objectMetadataItemsToMap(items []*burnbridgev1.ObjectMetadata) map[string]string {
	if len(items) == 0 {
		return nil
	}
	out := make(map[string]string, len(items))
	for _, item := range items {
		if item == nil {
			continue
		}
		key := strings.TrimSpace(item.GetKey())
		if key == "" {
			continue
		}
		out[key] = item.GetValue()
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func metadataMapToObjectMetadataItems(values map[string]string) []*burnbridgev1.ObjectMetadata {
	if len(values) == 0 {
		return nil
	}
	keys := make([]string, 0, len(values))
	for key := range values {
		if strings.TrimSpace(key) == "" {
			continue
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)
	items := make([]*burnbridgev1.ObjectMetadata, 0, len(keys))
	for _, key := range keys {
		items = append(items, &burnbridgev1.ObjectMetadata{
			Key:   key,
			Value: values[key],
		})
	}
	return items
}

func buildMultipartInitState(input s3response.CreateMultipartUploadInput) burnbridgeMultipartInitState {
	items := buildCreateJobMetadata(s3response.PutObjectInput{
		ContentType:        input.ContentType,
		ContentEncoding:    input.ContentEncoding,
		ContentDisposition: input.ContentDisposition,
		ContentLanguage:    input.ContentLanguage,
		CacheControl:       input.CacheControl,
		Expires:            input.Expires,
		Metadata:           input.Metadata,
	})
	return burnbridgeMultipartInitState{
		Metadata:          objectMetadataItemsToMap(items),
		ChecksumAlgorithm: input.ChecksumAlgorithm,
		ChecksumType:      input.ChecksumType,
	}
}

func multipartInitAttribute(uploadID string) string {
	return burnbridgeMultipartInitAttrPref + strings.TrimSpace(uploadID)
}

func multipartSessionObjectKey(uploadID string) string {
	return fmt.Sprintf("%ssession/%s", burnbridgeMultipartInternalPref, strings.TrimSpace(uploadID))
}

func (b *BurnBridge) storeMultipartInitState(bucket, key, uploadID string, state burnbridgeMultipartInitState) error {
	raw, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("burnbridge: encode multipart init state: %w", err)
	}
	return b.meta.StoreAttribute(nil, bucket, key, multipartInitAttribute(uploadID), raw)
}

func (b *BurnBridge) loadMultipartInitState(bucket, key, uploadID string) (*burnbridgeMultipartInitState, error) {
	raw, err := b.meta.RetrieveAttribute(nil, bucket, key, multipartInitAttribute(uploadID))
	if err != nil {
		if errors.Is(err, meta.ErrNoSuchKey) {
			return nil, meta.ErrNoSuchKey
		}
		return nil, err
	}
	var state burnbridgeMultipartInitState
	if err := json.Unmarshal(raw, &state); err != nil {
		return nil, fmt.Errorf("burnbridge: decode multipart init state: %w", err)
	}
	return &state, nil
}

func (b *BurnBridge) deleteMultipartInitState(bucket, key, uploadID string) error {
	return b.meta.DeleteAttribute(bucket, key, multipartInitAttribute(uploadID))
}

func (b *BurnBridge) storeMultipartObjectMetadata(bucket, key string, mpMeta backend.MpUploadMetadata) error {
	raw, err := json.Marshal(mpMeta)
	if err != nil {
		return fmt.Errorf("burnbridge: encode multipart object metadata: %w", err)
	}
	return b.meta.StoreAttribute(nil, bucket, key, burnbridgeMultipartMetaAttr, raw)
}

func (b *BurnBridge) loadMultipartObjectMetadata(bucket, key string) (*backend.MpUploadMetadata, error) {
	raw, err := b.meta.RetrieveAttribute(nil, bucket, key, burnbridgeMultipartMetaAttr)
	if err != nil {
		if errors.Is(err, meta.ErrNoSuchKey) {
			return nil, meta.ErrNoSuchKey
		}
		return nil, err
	}
	var doc backend.MpUploadMetadata
	if err := json.Unmarshal(raw, &doc); err != nil {
		return nil, fmt.Errorf("burnbridge: decode multipart object metadata: %w", err)
	}
	return &doc, nil
}

func applyCommittedRecordMetadata(rec *meta.BurnbridgeCommittedRecord, metadata map[string]string) {
	if rec == nil || len(metadata) == 0 {
		return
	}
	rec.ContentType = metadata["content-type"]
	rec.ContentEncoding = metadata["content-encoding"]
	rec.ContentDisposition = metadata["content-disposition"]
	rec.ContentLanguage = metadata["content-language"]
	rec.CacheControl = metadata["cache-control"]
	rec.Expires = metadata["expires"]
	userMeta := make(map[string]string)
	for key, value := range metadata {
		if strings.HasPrefix(strings.ToLower(strings.TrimSpace(key)), "x-amz-meta-") {
			userMeta[key[len("x-amz-meta-"):]] = value
		}
	}
	if len(userMeta) > 0 {
		rec.Metadata = userMeta
	}
}

func (b *BurnBridge) loadBurnSegmentSnapshot(bucket, key string) (map[int]meta.BurnObjectSegment, error) {
	segments, err := b.meta.ListBurnObjectSegments(bucket, key)
	if err != nil {
		return nil, err
	}
	snapshot := make(map[int]meta.BurnObjectSegment, len(segments))
	acceptedMediaIDs := b.acceptedSegmentMediaIDs(bucket)
	for _, seg := range segments {
		if !acceptedMediaIDs.accepts(seg.BurnObjectSegment.MediaID) {
			continue
		}
		snapshot[seg.SegmentIndex] = seg.BurnObjectSegment
	}
	return snapshot, nil
}

func (b *BurnBridge) getSingleUploadSession(bucket, key string) (*meta.BurnUploadSessionRecord, error) {
	session, err := b.meta.GetBurnUploadSession(bucket, key, burnbridgeImplicitSingleUploadID)
	if err != nil {
		if errors.Is(err, meta.ErrNoSuchKey) {
			return nil, nil
		}
		return nil, err
	}
	return session, nil
}

func (b *BurnBridge) upsertSingleUploadSession(bucket, key string, contentLen int64, state meta.BurnUploadState) error {
	return b.meta.UpsertBurnUploadSession(meta.BurnUploadSessionRecord{
		Bucket:         bucket,
		ObjectName:     key,
		UploadID:       burnbridgeImplicitSingleUploadID,
		Kind:           meta.BurnUploadKindSingle,
		State:          state,
		MediaID:        b.volumeLabelRaw,
		ContentLength:  contentLen,
		NextPartNumber: 1,
	})
}

func (b *BurnBridge) singleUploadSessionAllowsResume(bucket, key string) (bool, error) {
	session, err := b.getSingleUploadSession(bucket, key)
	if err != nil {
		return false, err
	}
	if session == nil {
		return false, nil
	}
	if !b.acceptedSegmentMediaIDs(bucket).accepts(session.MediaID) {
		return false, nil
	}
	return session.State == meta.BurnUploadStateWriting || session.State == meta.BurnUploadStateFailed, nil
}

func (b *BurnBridge) getMultipartUploadSession(bucket, key, uploadID string) (*meta.BurnUploadSessionRecord, error) {
	session, err := b.meta.GetBurnUploadSession(bucket, key, uploadID)
	if err != nil {
		if errors.Is(err, meta.ErrNoSuchKey) {
			return nil, nil
		}
		return nil, err
	}
	if session.Kind != meta.BurnUploadKindMultipart {
		return nil, nil
	}
	return session, nil
}

func (b *BurnBridge) multipartSessionVisibleOnCurrentMedia(bucket string, session *meta.BurnUploadSessionRecord) bool {
	if session == nil {
		return false
	}
	return b.acceptedSegmentMediaIDs(bucket).accepts(session.MediaID)
}

func (b *BurnBridge) recorderImportedStateContainsObject(ctx context.Context, bucket, key string) (bool, error) {
	resp, err := b.grpc.GetImportedBucketState(ctx, &burnbridgev1.GetImportedBucketStateRequest{})
	if err != nil {
		if isGRPCUnimplemented(err) {
			return true, nil
		}
		return false, fmt.Errorf("burnbridge GetImportedBucketState: %w", err)
	}
	if resp == nil || !resp.GetLoaded() {
		// Recorder has not positively loaded/imported its current layout view yet.
		// Preserve local resume/committed state rather than incorrectly discarding it.
		return true, nil
	}

	resolvedBucket := strings.TrimSpace(resp.GetBucket())
	if resolvedBucket != "" && !strings.EqualFold(resolvedBucket, strings.TrimSpace(bucket)) {
		return false, nil
	}

	for _, object := range resp.GetObjects() {
		if object == nil {
			continue
		}
		if strings.EqualFold(strings.TrimSpace(object.GetObjectKey()), strings.TrimSpace(key)) {
			return true, nil
		}
	}

	return false, nil
}

func (b *BurnBridge) clearStaleLocalObjectState(bucket, key string) error {
	sessions, err := b.meta.ListBurnUploadSessions(bucket, key)
	if err != nil && !errors.Is(err, meta.ErrNoSuchKey) {
		return err
	}
	for _, session := range sessions {
		if session.Kind != meta.BurnUploadKindMultipart {
			continue
		}
		if err := b.meta.DeleteBurnObjectSegments(bucket, multipartSessionObjectKey(session.UploadID)); err != nil && !errors.Is(err, meta.ErrNoSuchKey) {
			return err
		}
	}
	if err := b.meta.DeleteBurnUploadParts(bucket, key, ""); err != nil && !errors.Is(err, meta.ErrNoSuchKey) {
		return err
	}
	if err := b.meta.DeleteBurnUploadSession(bucket, key, ""); err != nil && !errors.Is(err, meta.ErrNoSuchKey) {
		return err
	}
	if err := b.meta.DeleteBurnObjectSegments(bucket, key); err != nil && !errors.Is(err, meta.ErrNoSuchKey) {
		return err
	}
	if err := b.meta.DeleteAttribute(bucket, key, meta.BurnbridgeCommittedAttribute); err != nil && !errors.Is(err, meta.ErrNoSuchKey) {
		return err
	}
	if err := b.meta.DeleteAttribute(bucket, key, burnbridgeMultipartMetaAttr); err != nil && !errors.Is(err, meta.ErrNoSuchKey) {
		return err
	}
	return nil
}

func (b *BurnBridge) invalidateStaleResumeStateIfRecorderMissing(ctx context.Context, bucket, key string) error {
	_, committedErr := b.meta.GetBurnbridgeCommittedRecord(bucket, key)
	hasCommitted := committedErr == nil
	if committedErr != nil && !errors.Is(committedErr, meta.ErrNoSuchKey) {
		return committedErr
	}

	segments, segmentsErr := b.meta.ListBurnObjectSegments(bucket, key)
	if segmentsErr != nil {
		return segmentsErr
	}
	if !hasCommitted {
		activeResume, err := b.singleUploadSessionAllowsResume(bucket, key)
		if err != nil {
			return err
		}
		if activeResume {
			return nil
		}
		if len(segments) == 0 {
			return nil
		}
	}
	present, err := b.recorderImportedStateContainsObject(ctx, bucket, key)
	if err != nil {
		return err
	}
	if present {
		return nil
	}

	slog.Warn("burnbridge: clearing stale local resume/committed state because recorder imported layout does not contain object",
		"bucket", bucket,
		"key", key,
		"had_committed", hasCommitted,
		"segment_count", len(segments))
	return b.clearStaleLocalObjectState(bucket, key)
}

type acceptedMediaSet map[string]struct{}

func (s acceptedMediaSet) accepts(mediaID string) bool {
	if len(s) == 0 {
		return true
	}
	trimmed := strings.TrimSpace(mediaID)
	if trimmed == "" {
		return true
	}
	_, ok := s[strings.ToUpper(trimmed)]
	return ok
}

func (b *BurnBridge) acceptedSegmentMediaIDs(bucket string) acceptedMediaSet {
	accepted := make(acceptedMediaSet)
	add := func(value string) {
		trimmed := strings.TrimSpace(value)
		if trimmed == "" {
			return
		}
		accepted[strings.ToUpper(trimmed)] = struct{}{}
	}

	add(b.volumeLabelRaw)

	if strings.TrimSpace(bucket) == "" {
		return accepted
	}

	if bindings, err := b.meta.ListBurnbridgeDiscBucketBindings(bucket); err == nil {
		for _, binding := range bindings {
			add(binding.ProbeVolumeLabel)
		}
	}

	if raw, err := b.meta.GetBurnbridgeDiscInfoJSON(bucket); err == nil && len(raw) > 0 {
		var discInfo meta.BurnbridgeDiscInfoDocument
		if err := json.Unmarshal(raw, &discInfo); err == nil &&
			strings.EqualFold(strings.TrimSpace(discInfo.Bucket), strings.TrimSpace(bucket)) {
			add(discInfo.VolumeLabel)
		}
	}

	return accepted
}

func (b *BurnBridge) cleanupMultipartUploadState(bucket, key, uploadID string, parts []meta.BurnUploadPartRecord) error {
	var errs []error
	_ = parts
	if err := b.meta.DeleteBurnObjectSegments(bucket, multipartSessionObjectKey(uploadID)); err != nil && !errors.Is(err, meta.ErrNoSuchKey) {
		errs = append(errs, err)
	}
	if err := b.meta.DeleteBurnUploadParts(bucket, key, uploadID); err != nil && !errors.Is(err, meta.ErrNoSuchKey) {
		errs = append(errs, err)
	}
	if err := b.meta.DeleteBurnUploadSession(bucket, key, uploadID); err != nil && !errors.Is(err, meta.ErrNoSuchKey) {
		errs = append(errs, err)
	}
	if err := b.deleteMultipartInitState(bucket, key, uploadID); err != nil && !errors.Is(err, meta.ErrNoSuchKey) {
		errs = append(errs, err)
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

func (b *BurnBridge) cleanupCompletedSingleUploadState(bucket, key string) error {
	var errs []error
	if err := b.meta.DeleteBurnUploadParts(bucket, key, burnbridgeImplicitSingleUploadID); err != nil && !errors.Is(err, meta.ErrNoSuchKey) {
		errs = append(errs, err)
	}
	if err := b.meta.DeleteBurnUploadSession(bucket, key, burnbridgeImplicitSingleUploadID); err != nil && !errors.Is(err, meta.ErrNoSuchKey) {
		errs = append(errs, err)
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

func (b *BurnBridge) cleanupCompletedMultipartObjectState(bucket, key, uploadID string, parts []meta.BurnUploadPartRecord) error {
	var errs []error
	uploadIDs := map[string]struct{}{}
	addUploadID := func(id string) {
		id = strings.TrimSpace(id)
		if id != "" {
			uploadIDs[id] = struct{}{}
		}
	}
	addUploadID(uploadID)
	for _, part := range parts {
		addUploadID(part.UploadID)
	}

	sessions, err := b.meta.ListBurnUploadSessions(bucket, key)
	if err != nil && !errors.Is(err, meta.ErrNoSuchKey) {
		errs = append(errs, err)
	}
	for _, session := range sessions {
		if session.Kind == meta.BurnUploadKindMultipart {
			addUploadID(session.UploadID)
		}
	}

	attrs, err := b.meta.ListAttributes(bucket, key)
	if err != nil && !errors.Is(err, meta.ErrNoSuchKey) {
		errs = append(errs, err)
	}
	for _, attr := range attrs {
		if strings.HasPrefix(attr, burnbridgeMultipartInitAttrPref) {
			addUploadID(strings.TrimPrefix(attr, burnbridgeMultipartInitAttrPref))
		}
	}

	for id := range uploadIDs {
		if err := b.meta.DeleteBurnObjectSegments(bucket, multipartSessionObjectKey(id)); err != nil && !errors.Is(err, meta.ErrNoSuchKey) {
			errs = append(errs, err)
		}
		if err := b.deleteMultipartInitState(bucket, key, id); err != nil && !errors.Is(err, meta.ErrNoSuchKey) {
			errs = append(errs, err)
		}
	}
	if err := b.meta.DeleteBurnUploadParts(bucket, key, ""); err != nil && !errors.Is(err, meta.ErrNoSuchKey) {
		errs = append(errs, err)
	}
	if err := b.meta.DeleteBurnUploadSession(bucket, key, ""); err != nil && !errors.Is(err, meta.ErrNoSuchKey) {
		errs = append(errs, err)
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

func buildFinalizeManifestFromSegments(key string, objectSize int64, segments []meta.BurnObjectSegmentDetail) (*burnbridgev1.FinalizeManifest, error) {
	if len(segments) == 0 {
		return &burnbridgev1.FinalizeManifest{
			Files: []*burnbridgev1.FinalizeFile{{
				ObjectKey: key,
				FileSize:  objectSize,
			}},
		}, nil
	}
	layouts := make([]*burnbridgev1.SegmentLayout, 0, len(segments))
	hasUsableExtent := false
	for idx, seg := range segments {
		if seg.SegmentIndex != idx {
			return nil, fmt.Errorf("burnbridge: finalize manifest segment sequence mismatch for %s: got=%d want=%d", key, seg.SegmentIndex, idx)
		}
		if seg.State != meta.BurnSegmentSucceeded {
			return nil, fmt.Errorf("burnbridge: finalize manifest segment state not succeeded for %s segment=%d state=%d", key, seg.SegmentIndex, seg.State)
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
		Files: []*burnbridgev1.FinalizeFile{{
			ObjectKey: key,
			FileSize:  objectSize,
			Segments:  layouts,
		}},
	}, nil
}

func (b *BurnBridge) burnMaybeInvalidateSegments(bucket, key string, snapshot map[int]meta.BurnObjectSegment, segmentIdx int, digest string, allowInvalidate bool) error {
	if segmentIdx != 0 {
		return nil
	}
	prev, ok := snapshot[0]
	if !ok {
		return nil
	}
	if prev.ChecksumMD5 != digest {
		if !allowInvalidate {
			return fmt.Errorf("burnbridge: upload payload differs from previously recorded segment 0 for %s/%s; overwrite is not allowed", bucket, key)
		}
		if err := b.meta.DeleteBurnObjectSegments(bucket, key); err != nil {
			return err
		}
		for k := range snapshot {
			delete(snapshot, k)
		}
	}
	return nil
}

func (b *BurnBridge) burnShouldSkipSegment(allowReuse bool, snapshot map[int]meta.BurnObjectSegment, segmentIdx int, digest string, offset, segLen int64) bool {
	if !allowReuse {
		return false
	}
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
	jobID, bucket, key string, segmentIdx int, ackSegmentIdx int, offset, segLen int64, digest string, allowUploadComplete bool, snapshot map[int]meta.BurnObjectSegment) (bool, error) {
	persistSegment := func(state meta.BurnSegmentState, extents []meta.BurnDiscExtent) error {
		if err := b.meta.UpsertBurnObjectSegment(bucket, key, b.volumeLabelRaw, segmentIdx, offset, segLen, digest, state, extents); err != nil {
			return err
		}
		snapshot[segmentIdx] = meta.BurnObjectSegment{
			MediaID:     b.volumeLabelRaw,
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
	if ack.GetSegmentIndex() != int32(ackSegmentIdx) {
		_ = persistSegment(meta.BurnSegmentFailed, nil)
		return false, fmt.Errorf("burnbridge: segment_index mismatch: got %d want %d", ack.GetSegmentIndex(), ackSegmentIdx)
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

func (b *BurnBridge) grpcUploadMultipartPartStream(
	ctx context.Context,
	jobID, bucket, segmentKey string,
	body io.Reader,
	partStartOffset, resumeOffset int64,
) (int64, *burnbridgev1.UploadObjectAck, uploadRecoveryStats, error) {
	startedAt := time.Now()
	stream, err := b.grpc.UploadObject(ctx)
	if err != nil {
		return resumeOffset, nil, uploadRecoveryStats{}, err
	}
	segmentSnapshot, err := b.loadBurnSegmentSnapshot(bucket, segmentKey)
	if err != nil {
		return resumeOffset, nil, uploadRecoveryStats{}, err
	}
	if resumeOffset < partStartOffset {
		return resumeOffset, nil, uploadRecoveryStats{}, fmt.Errorf(
			"burnbridge: multipart resume offset moved before part start. segmentKey=%s partStart=%d resume=%d",
			segmentKey, partStartOffset, resumeOffset)
	}

	stats := uploadRecoveryStats{}
	partMD5 := md5.New()
	segBuf := make([]byte, b.chunkSize)
	currentOffset := resumeOffset
	logicalOffset := partStartOffset
	segmentIdx := multipartSegmentIndexForOffset(segmentSnapshot, partStartOffset)
	streamSegmentIdx := 0
	uploadCompletedBySegmentAck := false

	sendEOF := func() error {
		return stream.Send(&burnbridgev1.UploadObjectChunk{JobId: jobID, Offset: currentOffset, Eof: true})
	}

	if body == nil {
		if err := sendEOF(); err != nil {
			return currentOffset, nil, stats, err
		}
		if err := stream.CloseSend(); err != nil {
			return currentOffset, nil, stats, err
		}
		final, err := stream.Recv()
		if err != nil {
			return currentOffset, nil, stats, err
		}
		if !final.GetUploadComplete() {
			return currentOffset, nil, stats, fmt.Errorf("burnbridge: expected upload_complete on multipart final ack for empty part")
		}
		if final.GetChecksumMd5() == "" {
			final.ChecksumMd5 = hex.EncodeToString(partMD5.Sum(nil))
		}
		return currentOffset, final, stats, nil
	}

	for {
		n, errRead := io.ReadFull(body, segBuf)
		if n == 0 {
			if errRead == io.EOF || errRead == io.ErrUnexpectedEOF {
				break
			}
			return currentOffset, nil, stats, errRead
		}
		if errRead != nil && errRead != io.ErrUnexpectedEOF {
			return currentOffset, nil, stats, errRead
		}

		chunk := segBuf[:n]
		_, _ = partMD5.Write(chunk)
		digest := bbSegmentMD5Hex(chunk)
		stats.TotalSegments++
		isTailSegment := errRead == io.ErrUnexpectedEOF

		if logicalOffset < resumeOffset {
			if logicalOffset+int64(len(chunk)) > resumeOffset {
				return currentOffset, nil, stats, fmt.Errorf(
					"burnbridge: multipart resumed within a segment boundary; unsupported state. segmentKey=%s partStart=%d resume=%d logicalOffset=%d chunkBytes=%d",
					segmentKey, partStartOffset, resumeOffset, logicalOffset, len(chunk))
			}
			if !b.burnShouldSkipSegment(true, segmentSnapshot, segmentIdx, digest, logicalOffset, int64(len(chunk))) {
				return currentOffset, nil, stats, fmt.Errorf(
					"burnbridge: multipart retry payload mismatch against already-recorded prefix. segmentKey=%s segment=%d offset=%d size=%d",
					segmentKey, segmentIdx, logicalOffset, len(chunk))
			}
			stats.SkippedSegments++
			logicalOffset += int64(len(chunk))
			segmentIdx++
			if errRead == io.ErrUnexpectedEOF {
				break
			}
			continue
		}

		if logicalOffset != currentOffset {
			return currentOffset, nil, stats, fmt.Errorf(
				"burnbridge: multipart logical offset diverged from recorder offset. segmentKey=%s logicalOffset=%d currentOffset=%d",
				segmentKey, logicalOffset, currentOffset)
		}

		stats.ReplayedSegments++
		if err := b.meta.UpsertBurnObjectSegment(bucket, segmentKey, b.volumeLabelRaw, segmentIdx, logicalOffset, int64(len(chunk)), digest, meta.BurnSegmentPending, nil); err != nil {
			return currentOffset, nil, stats, err
		}
		segmentSnapshot[segmentIdx] = meta.BurnObjectSegment{
			MediaID:     b.volumeLabelRaw,
			ByteOffset:  logicalOffset,
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
			frameEOF := isTailSegment && end == len(chunk)
			if err := stream.Send(&burnbridgev1.UploadObjectChunk{
				JobId:  jobID,
				Offset: logicalOffset + int64(i),
				Data:   part,
				Eof:    frameEOF,
			}); err != nil {
				return currentOffset, nil, stats, err
			}
			i = end
		}

		completed, err := b.recvSegmentUploadAck(stream, jobID, bucket, segmentKey, segmentIdx, streamSegmentIdx, logicalOffset, int64(len(chunk)), digest, isTailSegment, segmentSnapshot)
		if err != nil {
			if !errors.Is(err, io.EOF) {
				_ = b.meta.UpsertBurnObjectSegment(bucket, segmentKey, b.volumeLabelRaw, segmentIdx, logicalOffset, int64(len(chunk)), digest, meta.BurnSegmentFailed, nil)
			}
			return currentOffset, nil, stats, err
		}
		uploadCompletedBySegmentAck = uploadCompletedBySegmentAck || completed

		currentOffset += int64(len(chunk))
		logicalOffset = currentOffset
		segmentIdx++
		streamSegmentIdx++
		if errRead == io.ErrUnexpectedEOF {
			break
		}
	}

	trimmedRows, err := b.meta.DeleteBurnObjectSegmentsFromWithCount(bucket, segmentKey, segmentIdx)
	if err != nil {
		return currentOffset, nil, stats, err
	}
	stats.TrimmedSegments = trimmedRows

	var final *burnbridgev1.UploadObjectAck
	if !uploadCompletedBySegmentAck {
		if err := sendEOF(); err != nil {
			return currentOffset, nil, stats, err
		}
		if err := stream.CloseSend(); err != nil {
			return currentOffset, nil, stats, err
		}
		final, err = stream.Recv()
		if err != nil {
			return currentOffset, nil, stats, err
		}
		if !final.GetUploadComplete() {
			return currentOffset, nil, stats, fmt.Errorf("burnbridge: expected upload_complete on multipart part final ack")
		}
	} else {
		if err := stream.CloseSend(); err != nil {
			return currentOffset, nil, stats, err
		}
		final = &burnbridgev1.UploadObjectAck{
			JobId:          jobID,
			UploadComplete: true,
			BytesReceived:  currentOffset,
		}
	}
	if final.GetChecksumMd5() == "" {
		final.ChecksumMd5 = hex.EncodeToString(partMD5.Sum(nil))
	}
	slog.Info("burnbridge: multipart part stream finished",
		"bucket", bucket,
		"segmentKey", segmentKey,
		"jobId", jobID,
		"bytes", currentOffset-partStartOffset,
		"absoluteBytesReceived", currentOffset,
		"segments_total", stats.TotalSegments,
		"segments_skipped", stats.SkippedSegments,
		"segments_replayed", stats.ReplayedSegments,
		"segments_trimmed", stats.TrimmedSegments,
		"elapsed_ms", time.Since(startedAt).Milliseconds())
	return currentOffset, final, stats, nil
}

func (b *BurnBridge) grpcUploadObjectStream(ctx context.Context, jobID, bucket, key string, body io.Reader, _ int64, opts burnbridgeUploadStreamOptions) (int64, *burnbridgev1.UploadObjectAck, uploadRecoveryStats, error) {
	startedAt := time.Now()
	stream, err := b.grpc.UploadObject(ctx)
	if err != nil {
		return 0, nil, uploadRecoveryStats{}, err
	}
	segmentSnapshot, err := b.loadBurnSegmentSnapshot(bucket, key)
	if err != nil {
		return 0, nil, uploadRecoveryStats{}, err
	}
	allowReuse := opts.AllowReuse
	stats := uploadRecoveryStats{}
	uploadCompletedBySegmentAck := false
	objectMD5 := md5.New()

	segBuf := make([]byte, b.chunkSize)
	offset := int64(0)
	segmentIdx := 0
	streamSegmentIdx := 0

	sendEOF := func() error {
		return stream.Send(&burnbridgev1.UploadObjectChunk{JobId: jobID, Offset: offset, Eof: true})
	}

	if body == nil {
		if err := sendEOF(); err != nil {
			return 0, nil, stats, err
		}
		if err := stream.CloseSend(); err != nil {
			return 0, nil, stats, err
		}
		final, err := stream.Recv()
		if err != nil {
			return offset, nil, stats, err
		}
		if !final.GetUploadComplete() {
			return offset, nil, stats, fmt.Errorf("burnbridge: expected upload_complete on final ack for empty body")
		}
		if final.GetChecksumMd5() == "" {
			final.ChecksumMd5 = hex.EncodeToString(objectMD5.Sum(nil))
		}
		return offset, final, stats, nil
	}

	for {
		n, errRead := io.ReadFull(body, segBuf)
		if n == 0 {
			if errRead == io.EOF || errRead == io.ErrUnexpectedEOF {
				break
			}
			return offset, nil, stats, errRead
		}
		if errRead != nil && errRead != io.ErrUnexpectedEOF {
			return offset, nil, stats, errRead
		}
		chunk := segBuf[:n]
		_, _ = objectMD5.Write(chunk)

		digest := bbSegmentMD5Hex(chunk)
		if err := b.burnMaybeInvalidateSegments(bucket, key, segmentSnapshot, segmentIdx, digest, opts.AllowInvalidateOnFirstMismatch); err != nil {
			return offset, nil, stats, err
		}
		stats.TotalSegments++

		skip := b.burnShouldSkipSegment(allowReuse, segmentSnapshot, segmentIdx, digest, offset, int64(len(chunk)))

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
				return offset, nil, stats, err
			}
		} else {
			stats.ReplayedSegments++
			if err := b.meta.UpsertBurnObjectSegment(bucket, key, b.volumeLabelRaw, segmentIdx, offset, int64(len(chunk)), digest, meta.BurnSegmentPending, nil); err != nil {
				return offset, nil, stats, err
			}
			segmentSnapshot[segmentIdx] = meta.BurnObjectSegment{
				MediaID:     b.volumeLabelRaw,
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
					return offset, nil, stats, err
				}
				i = end
			}
		}

		completed, err := b.recvSegmentUploadAck(stream, jobID, bucket, key, segmentIdx, streamSegmentIdx, offset, int64(len(chunk)), digest, isTailSegment, segmentSnapshot)
		if err != nil {
			if !skip {
				_ = b.meta.UpsertBurnObjectSegment(bucket, key, b.volumeLabelRaw, segmentIdx, offset, int64(len(chunk)), digest, meta.BurnSegmentFailed, nil)
				segmentSnapshot[segmentIdx] = meta.BurnObjectSegment{
					MediaID:     b.volumeLabelRaw,
					ByteOffset:  offset,
					ByteSize:    int64(len(chunk)),
					ChecksumMD5: digest,
					State:       meta.BurnSegmentFailed,
					DiscExtents: nil,
				}
			}
			return offset, nil, stats, err
		}
		uploadCompletedBySegmentAck = uploadCompletedBySegmentAck || completed

		offset += int64(len(chunk))
		segmentIdx++
		streamSegmentIdx++
		if errRead == io.ErrUnexpectedEOF {
			break
		}
	}

	trimmedRows, err := b.meta.DeleteBurnObjectSegmentsFromWithCount(bucket, key, segmentIdx)
	if err != nil {
		return offset, nil, stats, err
	}
	stats.TrimmedSegments = trimmedRows

	var final *burnbridgev1.UploadObjectAck
	if !uploadCompletedBySegmentAck {
		if err := sendEOF(); err != nil {
			return offset, nil, stats, err
		}
		if err := stream.CloseSend(); err != nil {
			return offset, nil, stats, err
		}
		final, err = stream.Recv()
		if err != nil {
			return offset, nil, stats, err
		}
		if !final.GetUploadComplete() {
			return offset, nil, stats, fmt.Errorf("burnbridge: expected upload_complete on final ack")
		}
	} else {
		if err := stream.CloseSend(); err != nil {
			return offset, nil, stats, err
		}
		final = &burnbridgev1.UploadObjectAck{
			JobId:          jobID,
			UploadComplete: true,
			BytesReceived:  offset,
		}
	}
	if final.GetChecksumMd5() == "" {
		final.ChecksumMd5 = hex.EncodeToString(objectMD5.Sum(nil))
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
	return offset, final, stats, nil
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
	if b.isDriveControlBucket(bucket) {
		return s3response.PutObjectOutput{}, burnbridgeControlBucketReadOnly
	}
	if !b.burnbridgeBucketExists(bucket) {
		return s3response.PutObjectOutput{}, s3err.GetAPIError(s3err.ErrNoSuchBucket)
	}

	if err := b.requireRecorderReadyWithRetry(ctx, bucket); err != nil {
		return s3response.PutObjectOutput{}, err
	}

	b.putSerialMu.Lock()
	defer b.putSerialMu.Unlock()

	idx := objectLockIndex(bucket, key)
	b.objectLocks[idx].Lock()
	defer b.objectLocks[idx].Unlock()

	if err := b.invalidateStaleResumeStateIfRecorderMissing(ctx, bucket, key); err != nil {
		return s3response.PutObjectOutput{}, err
	}

	var contentLen int64
	if input.ContentLength != nil {
		contentLen = *input.ContentLength
	}
	if contentLen > 0 {
		raw, err := b.meta.GetBurnbridgeDiscInfoJSON(bucket)
		if err == nil && len(raw) > 0 {
			var discInfo meta.BurnbridgeDiscInfoDocument
			if uerr := json.Unmarshal(raw, &discInfo); uerr == nil {
				if err := ensureWritableCapacity(&discInfo, contentLen); err != nil {
					return s3response.PutObjectOutput{}, err
				}
			}
		}
	}

	createResp, err := b.grpc.CreateJob(ctx, &burnbridgev1.CreateJobRequest{
		Bucket:        bucket,
		ObjectKey:     key,
		ContentLength: contentLen,
		Metadata:      buildCreateJobMetadata(input),
	})
	if err != nil {
		return s3response.PutObjectOutput{}, mapRecorderWriteRPCError(err)
	}
	jobID := createResp.GetJobId()
	if jobID == "" {
		return s3response.PutObjectOutput{}, fmt.Errorf("burnbridge: empty job id from CreateJob")
	}

	if err := b.registerRecorderS3PullSource(ctx, jobID, bucket, key, contentLen); err != nil {
		return s3response.PutObjectOutput{}, err
	}

	var committed bool
	sessionStarted := false
	cancelJobNow := func(job string) {
		if strings.TrimSpace(job) == "" {
			return
		}
		cctx, cancel := context.WithTimeout(context.Background(), b.cancelJobTimeout)
		defer cancel()
		_, _ = b.grpc.CancelJob(cctx, &burnbridgev1.CancelJobRequest{JobId: job})
	}
	defer func() {
		if !committed && sessionStarted {
			_ = b.upsertSingleUploadSession(bucket, key, contentLen, meta.BurnUploadStateFailed)
			_ = b.meta.UpsertBurnUploadPart(meta.BurnUploadPartRecord{
				Bucket:       bucket,
				ObjectName:   key,
				UploadID:     burnbridgeImplicitSingleUploadID,
				PartNumber:   1,
				PartSize:     contentLen,
				State:        meta.BurnUploadStateFailed,
				SegmentCount: 0,
			})
		}
		if committed || jobID == "" {
			return
		}
		cancelJobNow(jobID)
	}()
	if err := b.upsertSingleUploadSession(bucket, key, contentLen, meta.BurnUploadStateWriting); err != nil {
		return s3response.PutObjectOutput{}, err
	}
	if err := b.meta.UpsertBurnUploadPart(meta.BurnUploadPartRecord{
		Bucket:       bucket,
		ObjectName:   key,
		UploadID:     burnbridgeImplicitSingleUploadID,
		PartNumber:   1,
		PartSize:     contentLen,
		State:        meta.BurnUploadStateWriting,
		SegmentCount: 0,
	}); err != nil {
		return s3response.PutObjectOutput{}, err
	}
	sessionStarted = true

	_, committedRecordErr := b.meta.GetBurnbridgeCommittedRecord(bucket, key)
	objectCommitted := committedRecordErr == nil
	if committedRecordErr != nil && !errors.Is(committedRecordErr, meta.ErrNoSuchKey) {
		return s3response.PutObjectOutput{}, committedRecordErr
	}
	sessionAllowsResume, err := b.singleUploadSessionAllowsResume(bucket, key)
	if err != nil {
		return s3response.PutObjectOutput{}, err
	}

	offset, uploadResp, stats, err := b.grpcUploadObjectStream(ctx, jobID, bucket, key, input.Body, contentLen, burnbridgeUploadStreamOptions{
		AllowReuse:                     objectCommitted || sessionAllowsResume,
		AllowInvalidateOnFirstMismatch: true,
	})
	if err != nil {
		return s3response.PutObjectOutput{}, err
	}

	if stats.AllSegmentsSkipped() {
		committedRec, recErr := b.meta.GetBurnbridgeCommittedRecord(bucket, key)
		if recErr != nil && !errors.Is(recErr, meta.ErrNoSuchKey) {
			return s3response.PutObjectOutput{}, recErr
		}
		if recErr == nil {
			committedChecksumMD5 := strings.Trim(strings.TrimSpace(committedRec.ETag), "\"")
			if committedRec.Size == offset && (uploadResp.GetChecksumMd5() == "" || strings.EqualFold(committedChecksumMD5, uploadResp.GetChecksumMd5())) {
				cancelJobNow(jobID)
				jobID = ""
				committed = true
				_ = b.upsertSingleUploadSession(bucket, key, contentLen, meta.BurnUploadStateCompleted)
				_ = b.meta.UpsertBurnUploadPart(meta.BurnUploadPartRecord{
					Bucket:       bucket,
					ObjectName:   key,
					UploadID:     burnbridgeImplicitSingleUploadID,
					PartNumber:   1,
					PartSize:     committedRec.Size,
					ChecksumMD5:  committedChecksumMD5,
					ETag:         committedRec.ETag,
					State:        meta.BurnUploadStateCompleted,
					SegmentCount: int(stats.TotalSegments),
				})
				if err := b.cleanupCompletedSingleUploadState(bucket, key); err != nil {
					return s3response.PutObjectOutput{}, err
				}
				slog.Info("burnbridge: object already committed on media; skipping CommitJob for idempotent PutObject retry",
					"bucket", bucket, "key", key, "jobId", jobID, "bytes", offset)

				out := s3response.PutObjectOutput{
					ETag: committedRec.ETag,
					Size: &committedRec.Size,
				}
				checksumMD5 := committedChecksumMD5
				if checksumMD5 != "" {
					out.ChecksumMD5 = &checksumMD5
				}
				return out, nil
			}

			slog.Info("burnbridge: all segments were reusable but committed object metadata differs; continuing with CommitJob",
				"bucket", bucket,
				"key", key,
				"committed_size", committedRec.Size,
				"retry_size", offset,
				"committed_md5", committedChecksumMD5,
				"retry_md5", uploadResp.GetChecksumMd5())
		}
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
		JobId:                  jobID,
		UdfVolumeLabel:         b.udfLabel,
		FinalizeManifest:       finalizeManifest,
		CommittedContentLength: offset,
	})
	if err != nil {
		return s3response.PutObjectOutput{}, mapRecorderWriteRPCError(err)
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
	applyCommittedRecordMetadata(committedRec, objectMetadataItemsToMap(buildCreateJobMetadata(input)))
	if err := b.meta.StoreBurnbridgeCommitted(nil, bucket, key, committedRec); err != nil {
		return s3response.PutObjectOutput{}, err
	}
	if err := b.upsertSingleUploadSession(bucket, key, contentLen, meta.BurnUploadStateCompleted); err != nil {
		return s3response.PutObjectOutput{}, err
	}
	if err := b.meta.UpsertBurnUploadPart(meta.BurnUploadPartRecord{
		Bucket:       bucket,
		ObjectName:   key,
		UploadID:     burnbridgeImplicitSingleUploadID,
		PartNumber:   1,
		PartSize:     offset,
		ChecksumMD5:  checksumMD5,
		ETag:         etag,
		State:        meta.BurnUploadStateCompleted,
		SegmentCount: int(stats.TotalSegments),
	}); err != nil {
		return s3response.PutObjectOutput{}, err
	}
	if err := b.cleanupCompletedSingleUploadState(bucket, key); err != nil {
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
	if b.isDriveControlBucket(*input.Bucket) {
		return nil, burnbridgeControlBucketReadOnly
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
	if b.isDriveControlBucket(*input.Bucket) {
		return s3response.DeleteResult{}, burnbridgeControlBucketReadOnly
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
	return b.buildFinalizeManifestForObject(bucket, key, key, objectSize)
}

func (b *BurnBridge) buildFinalizeManifestForObject(bucket, segmentKey, objectKey string, objectSize int64) (*burnbridgev1.FinalizeManifest, error) {
	segments, err := b.meta.ListBurnObjectSegments(bucket, segmentKey)
	if err != nil {
		return nil, err
	}
	return buildFinalizeManifestFromSegments(objectKey, objectSize, segments)
}

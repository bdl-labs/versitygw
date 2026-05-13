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
	"context"
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
	"testing/fstest"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
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
}

var _ backend.Backend = &BurnBridge{}

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

func probeRecorderDiscAtStartup(ctx context.Context, client burnbridgev1.BurnBridgeClient) (bucket string, rawVolume string, readyResp *burnbridgev1.TestUnitReadyResponse, err error) {
	resp, err := client.TestUnitReady(ctx, &burnbridgev1.TestUnitReadyRequest{})
	if err != nil {
		st, ok := status.FromError(err)
		if ok && st.Code() == codes.Unimplemented {
			return "", "", nil, fmt.Errorf("burnbridge: recorder must implement TestUnitReady and return volume_label when ready (got Unimplemented)")
		}
		return "", "", nil, fmt.Errorf("burnbridge TestUnitReady at startup: %w", err)
	}
	if !resp.GetReady() {
		msg := strings.TrimSpace(resp.GetMessage())
		if msg == "" {
			msg = "recorder reports not ready"
		}
		return "", "", nil, fmt.Errorf("burnbridge: recorder not ready at startup: %s", msg)
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
	if doc := discInfoDocFromProto(activeBucket, turResp); doc != nil {
		if perr := metaStore.StoreBurnbridgeDiscInfo(doc); perr != nil {
			_ = conn.Close()
			return nil, fmt.Errorf("burnbridge: persist disc info: %w", perr)
		}
	}
	slog.Info("burnbridge: startup TestUnitReady OK; volume label mapped to S3 bucket",
		"volumeLabel", rawVol, "bucket", activeBucket)

	if !opts.GRPCSkipPing {
		pingCtx, pingCancel := context.WithTimeout(dialCtx, opts.PingTimeout)
		pingErr := grpcConnectivityPing(pingCtx, client)
		pingCancel()
		if pingErr != nil {
			_ = conn.Close()
			return nil, fmt.Errorf("burnbridge grpc ping: %w", pingErr)
		}
	}

	readMount := strings.TrimSpace(opts.ReadMountPath)
	if readMount != "" {
		readMount = filepath.Clean(readMount)
	}

	return &BurnBridge{
		meta:             metaStore,
		grpc:             client,
		grpcConn:         conn,
		chunkSize:        opts.ChunkSize,
		udfLabel:         opts.UDFVolumeLabel,
		readMount:        readMount,
		cancelJobTimeout: opts.CancelJobTimeout,
		putObjectTimeout: opts.PutObjectTimeout,

		activeBucket:   activeBucket,
		volumeLabelRaw: rawVol,

		recorderS3Endpoint:        strings.TrimSpace(opts.RecorderS3Endpoint),
		recorderS3Region:          strings.TrimSpace(opts.RecorderS3Region),
		recorderS3AccessKey:       opts.RecorderS3AccessKey,
		recorderS3SecretKey:       opts.RecorderS3SecretKey,
		recorderS3SessionToken:    opts.RecorderS3SessionToken,
		recorderS3PathStyle:       opts.RecorderS3ForcePathStyle,
		recorderS3PresignedGetURL: strings.TrimSpace(opts.RecorderS3PresignedGetURL),
	}, nil
}

func (b *BurnBridge) requireRecorderReady(ctx context.Context) error {
	resp, err := b.grpc.TestUnitReady(ctx, &burnbridgev1.TestUnitReadyRequest{})
	if err != nil {
		st, ok := status.FromError(err)
		if ok && st.Code() == codes.Unimplemented {
			slog.Warn("burnbridge: TestUnitReady unimplemented on recorder; treating unit as ready (upgrade recorder to enforce readiness)")
			return nil
		}
		return fmt.Errorf("burnbridge TestUnitReady: %w", err)
	}
	if !resp.GetReady() {
		msg := strings.TrimSpace(resp.GetMessage())
		if msg == "" {
			msg = "Optical recorder unit is not ready (disc not loaded, busy, or not finalized)."
		}
		return s3err.APIError{
			Code:           "BurnbridgeUnitNotReady",
			Description:    msg,
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
		if b.readMount != "" {
			if p, err := bbSafeObjectPath(b.readMount, bucket, path); err == nil {
				if fi, err := os.Stat(p); err == nil && !fi.IsDir() {
					sz = fi.Size()
					lm = fi.ModTime().UTC()
				}
			}
		}
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

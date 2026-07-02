package burnbridge

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/versity/versitygw/auth"
	"github.com/versity/versitygw/backend"
	burnbridgev1 "github.com/versity/versitygw/backend/burnbridge/proto"
	"github.com/versity/versitygw/backend/meta"
	"github.com/versity/versitygw/s3response"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type testBurnBridgeClient struct {
	createJobFn           func(context.Context, *burnbridgev1.CreateJobRequest, ...grpc.CallOption) (*burnbridgev1.CreateJobResponse, error)
	uploadObjectFn        func(context.Context, ...grpc.CallOption) (grpc.BidiStreamingClient[burnbridgev1.UploadObjectChunk, burnbridgev1.UploadObjectAck], error)
	commitJobFn           func(context.Context, *burnbridgev1.CommitJobRequest, ...grpc.CallOption) (*burnbridgev1.CommitJobResponse, error)
	commitJobBatchFn      func(context.Context, *burnbridgev1.CommitJobBatchRequest, ...grpc.CallOption) (*burnbridgev1.CommitJobBatchResponse, error)
	cancelJobFn           func(context.Context, *burnbridgev1.CancelJobRequest, ...grpc.CallOption) (*burnbridgev1.CancelJobResponse, error)
	registerPullSourceFn  func(context.Context, *burnbridgev1.RegisterS3ObjectPullSourceRequest, ...grpc.CallOption) (*burnbridgev1.RegisterS3ObjectPullSourceResponse, error)
	finalizeFn            func(context.Context, *burnbridgev1.FinalizeLayoutRequest, ...grpc.CallOption) (*burnbridgev1.FinalizeLayoutResponse, error)
	getDiscInfoFn         func(context.Context, *burnbridgev1.GetDiscInfoRequest, ...grpc.CallOption) (*burnbridgev1.GetDiscInfoResponse, error)
	handleMediaChangeFn   func(context.Context, *burnbridgev1.HandleMediaChangeRequest, ...grpc.CallOption) (*burnbridgev1.HandleMediaChangeResponse, error)
	handleTrayFn          func(context.Context, *burnbridgev1.HandleTrayRequest, ...grpc.CallOption) (*burnbridgev1.HandleTrayResponse, error)
	importedBucketStateFn func(context.Context, *burnbridgev1.GetImportedBucketStateRequest, ...grpc.CallOption) (*burnbridgev1.GetImportedBucketStateResponse, error)
	testUnitReadyFn       func(context.Context, *burnbridgev1.TestUnitReadyRequest, ...grpc.CallOption) (*burnbridgev1.TestUnitReadyResponse, error)
	readObjectFn          func(context.Context, *burnbridgev1.ReadObjectRequest, ...grpc.CallOption) (grpc.ServerStreamingClient[burnbridgev1.ReadObjectChunk], error)
	watchUnitStatusFn     func(context.Context, *burnbridgev1.WatchUnitStatusRequest, ...grpc.CallOption) (grpc.ServerStreamingClient[burnbridgev1.UnitStatusEvent], error)
}

type testReadObjectStream struct {
	grpc.ClientStream
	chunks [][]byte
	idx    int
}

func newTestReadObjectStream(chunks ...[]byte) *testReadObjectStream {
	return &testReadObjectStream{chunks: chunks}
}

func (s *testReadObjectStream) Recv() (*burnbridgev1.ReadObjectChunk, error) {
	if s.idx >= len(s.chunks) {
		return nil, io.EOF
	}
	data := s.chunks[s.idx]
	s.idx++
	return &burnbridgev1.ReadObjectChunk{Data: data}, nil
}

func (s *testReadObjectStream) CloseSend() error {
	return nil
}

func (c testBurnBridgeClient) CreateJob(ctx context.Context, req *burnbridgev1.CreateJobRequest, opts ...grpc.CallOption) (*burnbridgev1.CreateJobResponse, error) {
	if c.createJobFn != nil {
		return c.createJobFn(ctx, req, opts...)
	}
	panic("unexpected CreateJob call")
}

func (testBurnBridgeClient) GetVersion(context.Context, *burnbridgev1.GetVersionRequest, ...grpc.CallOption) (*burnbridgev1.GetVersionResponse, error) {
	return &burnbridgev1.GetVersionResponse{}, nil
}

func (c testBurnBridgeClient) UploadObject(ctx context.Context, opts ...grpc.CallOption) (grpc.BidiStreamingClient[burnbridgev1.UploadObjectChunk, burnbridgev1.UploadObjectAck], error) {
	if c.uploadObjectFn != nil {
		return c.uploadObjectFn(ctx, opts...)
	}
	return nil, nil
}

func (c testBurnBridgeClient) CommitJob(ctx context.Context, req *burnbridgev1.CommitJobRequest, opts ...grpc.CallOption) (*burnbridgev1.CommitJobResponse, error) {
	if c.commitJobFn != nil {
		return c.commitJobFn(ctx, req, opts...)
	}
	panic("unexpected CommitJob call")
}

func (c testBurnBridgeClient) CommitJobBatch(ctx context.Context, req *burnbridgev1.CommitJobBatchRequest, opts ...grpc.CallOption) (*burnbridgev1.CommitJobBatchResponse, error) {
	if c.commitJobBatchFn != nil {
		return c.commitJobBatchFn(ctx, req, opts...)
	}
	panic("unexpected CommitJobBatch call")
}

func (testBurnBridgeClient) GetJobStatus(context.Context, *burnbridgev1.GetJobStatusRequest, ...grpc.CallOption) (*burnbridgev1.GetJobStatusResponse, error) {
	return &burnbridgev1.GetJobStatusResponse{}, nil
}

func (c testBurnBridgeClient) CancelJob(ctx context.Context, req *burnbridgev1.CancelJobRequest, opts ...grpc.CallOption) (*burnbridgev1.CancelJobResponse, error) {
	if c.cancelJobFn != nil {
		return c.cancelJobFn(ctx, req, opts...)
	}
	return &burnbridgev1.CancelJobResponse{}, nil
}

func (c testBurnBridgeClient) ReadObject(ctx context.Context, req *burnbridgev1.ReadObjectRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[burnbridgev1.ReadObjectChunk], error) {
	if c.readObjectFn != nil {
		return c.readObjectFn(ctx, req, opts...)
	}
	return nil, nil
}

func (c testBurnBridgeClient) RegisterS3ObjectPullSource(ctx context.Context, req *burnbridgev1.RegisterS3ObjectPullSourceRequest, opts ...grpc.CallOption) (*burnbridgev1.RegisterS3ObjectPullSourceResponse, error) {
	if c.registerPullSourceFn != nil {
		return c.registerPullSourceFn(ctx, req, opts...)
	}
	return &burnbridgev1.RegisterS3ObjectPullSourceResponse{}, nil
}

func (c testBurnBridgeClient) TestUnitReady(ctx context.Context, req *burnbridgev1.TestUnitReadyRequest, opts ...grpc.CallOption) (*burnbridgev1.TestUnitReadyResponse, error) {
	if c.testUnitReadyFn != nil {
		return c.testUnitReadyFn(ctx, req, opts...)
	}
	return &burnbridgev1.TestUnitReadyResponse{Ready: true}, nil
}

func (c testBurnBridgeClient) WatchUnitStatus(ctx context.Context, req *burnbridgev1.WatchUnitStatusRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[burnbridgev1.UnitStatusEvent], error) {
	if c.watchUnitStatusFn != nil {
		return c.watchUnitStatusFn(ctx, req, opts...)
	}
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (c testBurnBridgeClient) GetDiscInfo(ctx context.Context, req *burnbridgev1.GetDiscInfoRequest, opts ...grpc.CallOption) (*burnbridgev1.GetDiscInfoResponse, error) {
	if c.getDiscInfoFn != nil {
		return c.getDiscInfoFn(ctx, req, opts...)
	}
	panic("unexpected GetDiscInfo call")
}

func (c testBurnBridgeClient) HandleMediaChange(ctx context.Context, req *burnbridgev1.HandleMediaChangeRequest, opts ...grpc.CallOption) (*burnbridgev1.HandleMediaChangeResponse, error) {
	if c.handleMediaChangeFn != nil {
		return c.handleMediaChangeFn(ctx, req, opts...)
	}
	return &burnbridgev1.HandleMediaChangeResponse{}, nil
}

func (c testBurnBridgeClient) HandleTray(ctx context.Context, req *burnbridgev1.HandleTrayRequest, opts ...grpc.CallOption) (*burnbridgev1.HandleTrayResponse, error) {
	if c.handleTrayFn != nil {
		return c.handleTrayFn(ctx, req, opts...)
	}
	return &burnbridgev1.HandleTrayResponse{}, nil
}

func (c testBurnBridgeClient) GetImportedBucketState(ctx context.Context, req *burnbridgev1.GetImportedBucketStateRequest, opts ...grpc.CallOption) (*burnbridgev1.GetImportedBucketStateResponse, error) {
	if c.importedBucketStateFn != nil {
		return c.importedBucketStateFn(ctx, req, opts...)
	}
	return &burnbridgev1.GetImportedBucketStateResponse{}, nil
}

func (c testBurnBridgeClient) FinalizeLayout(ctx context.Context, req *burnbridgev1.FinalizeLayoutRequest, opts ...grpc.CallOption) (*burnbridgev1.FinalizeLayoutResponse, error) {
	if c.finalizeFn != nil {
		return c.finalizeFn(ctx, req, opts...)
	}
	panic("unexpected FinalizeLayout call")
}

func (testBurnBridgeClient) UpdateLicense(context.Context, *burnbridgev1.UpdateLicenseRequest, ...grpc.CallOption) (*burnbridgev1.UpdateLicenseResponse, error) {
	panic("unexpected UpdateLicense call")
}

func (testBurnBridgeClient) UploadUpgradePackage(context.Context, ...grpc.CallOption) (grpc.ClientStreamingClient[burnbridgev1.UploadUpgradePackageChunk, burnbridgev1.UploadUpgradePackageResponse], error) {
	panic("unexpected UploadUpgradePackage call")
}

func (testBurnBridgeClient) ApplyUpgrade(context.Context, *burnbridgev1.ApplyUpgradeRequest, ...grpc.CallOption) (*burnbridgev1.ApplyUpgradeResponse, error) {
	panic("unexpected ApplyUpgrade call")
}

func TestCreateBucketAllowsBlankDiscBinding(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "meta.db")
	store, err := meta.NewSqlMeta(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	b := &BurnBridge{
		meta:               store,
		grpc:               testBurnBridgeClient{},
		allowBucketBinding: true,
		activeBucket:       "DISC0001",
		volumeLabelRaw:     "DISC0001",
		udfLabel:           "DISC0001",
	}

	err = b.CreateBucket(context.Background(), &s3.CreateBucketInput{
		Bucket: ptr("archive-20260523"),
	}, nil)
	if err != nil {
		t.Fatalf("CreateBucket returned error: %v", err)
	}
	if b.activeBucket != "ARCHIVE-20260523" {
		t.Fatalf("active bucket mismatch: %q", b.activeBucket)
	}
	if b.udfLabel != "ARCHIVE-20260523" {
		t.Fatalf("udf label mismatch: %q", b.udfLabel)
	}
}

func TestCreateBucketNormalizesCustomDataBucketToUppercase(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "meta.db")
	store, err := meta.NewSqlMeta(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	b := &BurnBridge{
		meta:               store,
		grpc:               testBurnBridgeClient{},
		allowBucketBinding: true,
		activeBucket:       "DISC0001",
		volumeLabelRaw:     "DISC0001",
		udfLabel:           "DISC0001",
	}

	err = b.CreateBucket(context.Background(), &s3.CreateBucketInput{
		Bucket: ptr("archive-20260626"),
	}, nil)
	if err != nil {
		t.Fatalf("CreateBucket returned error: %v", err)
	}
	if b.activeBucket != "ARCHIVE-20260626" {
		t.Fatalf("active bucket mismatch: %q", b.activeBucket)
	}
	if b.udfLabel != "ARCHIVE-20260626" {
		t.Fatalf("udf label mismatch: %q", b.udfLabel)
	}
}

func TestCreateBucketPersistsProvidedACLAndFiltersListBuckets(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "meta.db")
	store, err := meta.NewSqlMeta(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	aclRaw, err := json.Marshal(auth.ACL{
		Owner: "drive",
		Grantees: []auth.Grantee{{
			Permission: auth.PermissionFullControl,
			Access:     "drive",
			Type:       types.TypeCanonicalUser,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}

	b := &BurnBridge{
		meta:               store,
		grpc:               testBurnBridgeClient{},
		allowBucketBinding: true,
		activeBucket:       "DISC0001",
		volumeLabelRaw:     "DISC0001",
		udfLabel:           "DISC0001",
	}

	err = b.CreateBucket(context.Background(), &s3.CreateBucketInput{
		Bucket: ptr("archive-20260523"),
	}, aclRaw)
	if err != nil {
		t.Fatalf("CreateBucket returned error: %v", err)
	}

	gotACLRaw, err := b.GetBucketAcl(context.Background(), &s3.GetBucketAclInput{Bucket: ptr("ARCHIVE-20260523")})
	if err != nil {
		t.Fatalf("GetBucketAcl returned error: %v", err)
	}
	gotACL, err := auth.ParseACL(gotACLRaw)
	if err != nil {
		t.Fatalf("ParseACL returned error: %v", err)
	}
	if gotACL.Owner != "drive" {
		t.Fatalf("expected owner drive, got %q", gotACL.Owner)
	}

	userList, err := b.ListBuckets(context.Background(), s3response.ListBucketsInput{
		Owner:      "drive",
		MaxBuckets: 1000,
	})
	if err != nil {
		t.Fatalf("ListBuckets user returned error: %v", err)
	}
	if len(userList.Buckets.Bucket) != 1 || userList.Buckets.Bucket[0].Name != "ARCHIVE-20260523" {
		t.Fatalf("expected user bucket list to contain ARCHIVE-20260523, got %+v", userList.Buckets.Bucket)
	}

	otherList, err := b.ListBuckets(context.Background(), s3response.ListBucketsInput{
		Owner:      "other",
		MaxBuckets: 1000,
	})
	if err != nil {
		t.Fatalf("ListBuckets other returned error: %v", err)
	}
	if len(otherList.Buckets.Bucket) != 0 {
		t.Fatalf("expected other user to see no buckets, got %+v", otherList.Buckets.Bucket)
	}

	adminBuckets, err := b.ListBucketsAndOwners(context.Background())
	if err != nil {
		t.Fatalf("ListBucketsAndOwners returned error: %v", err)
	}
	if len(adminBuckets) != 1 || adminBuckets[0].Owner != "drive" {
		t.Fatalf("expected admin list owner drive, got %+v", adminBuckets)
	}
}

func TestProbeRecorderDiscAtStartupUsesUppercaseDiscSerialBucket(t *testing.T) {
	bucket, rawVolume, _, err := probeRecorderDiscAtStartup(context.Background(), testBurnBridgeClient{
		testUnitReadyFn: func(context.Context, *burnbridgev1.TestUnitReadyRequest, ...grpc.CallOption) (*burnbridgev1.TestUnitReadyResponse, error) {
			return &burnbridgev1.TestUnitReadyResponse{
				Ready:               true,
				VolumeLabel:         "volume-label-should-not-win",
				DiscSerialNumberHex: "e60502460000000026426b95",
			}, nil
		},
	})
	if err != nil {
		t.Fatalf("probeRecorderDiscAtStartup returned error: %v", err)
	}
	if bucket != "E60502460000000026426B95" {
		t.Fatalf("expected uppercase disc serial bucket, got %q", bucket)
	}
	if rawVolume != "volume-label-should-not-win" {
		t.Fatalf("expected raw volume label to be preserved, got %q", rawVolume)
	}
}

func TestChangeBucketOwnerUpdatesACL(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "meta.db")
	store, err := meta.NewSqlMeta(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	b := &BurnBridge{
		meta:         store,
		grpc:         testBurnBridgeClient{},
		activeBucket: "ARCHIVE-20260523",
	}

	initialACL, err := json.Marshal(defaultBucketACL("drive"))
	if err != nil {
		t.Fatal(err)
	}
	if err := b.storeBucketACL("ARCHIVE-20260523", initialACL); err != nil {
		t.Fatal(err)
	}

	if err := b.ChangeBucketOwner(context.Background(), "archive-20260523", "admin"); err != nil {
		t.Fatalf("ChangeBucketOwner returned error: %v", err)
	}

	gotACLRaw, err := b.GetBucketAcl(context.Background(), &s3.GetBucketAclInput{Bucket: ptr("ARCHIVE-20260523")})
	if err != nil {
		t.Fatalf("GetBucketAcl returned error: %v", err)
	}
	gotACL, err := auth.ParseACL(gotACLRaw)
	if err != nil {
		t.Fatalf("ParseACL returned error: %v", err)
	}
	if gotACL.Owner != "admin" {
		t.Fatalf("expected owner admin, got %q", gotACL.Owner)
	}
}

func TestListBucketsBootstrapsACLForAutoMappedDisc(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "meta.db")
	store, err := meta.NewSqlMeta(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	b := &BurnBridge{
		meta:         store,
		grpc:         testBurnBridgeClient{},
		activeBucket: "E60302480000000025891A00",
		udfLabel:     "E60302480000000025891A00",
	}

	res, err := b.ListBuckets(context.Background(), s3response.ListBucketsInput{
		Owner:      "drive",
		MaxBuckets: 1000,
	})
	if err != nil {
		t.Fatalf("ListBuckets returned error: %v", err)
	}
	if len(res.Buckets.Bucket) != 1 || res.Buckets.Bucket[0].Name != "E60302480000000025891A00" {
		t.Fatalf("expected uppercase auto-mapped bucket in result, got %+v", res.Buckets.Bucket)
	}

	raw, err := b.GetBucketAcl(context.Background(), &s3.GetBucketAclInput{Bucket: ptr("E60302480000000025891A00")})
	if err != nil {
		t.Fatalf("GetBucketAcl returned error: %v", err)
	}
	acl, err := auth.ParseACL(raw)
	if err != nil {
		t.Fatalf("ParseACL returned error: %v", err)
	}
	if acl.Owner != "drive" {
		t.Fatalf("expected bootstrapped owner drive, got %q", acl.Owner)
	}
}

func TestGetBucketAclBootstrapsACLForAuthenticatedUser(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "meta.db")
	store, err := meta.NewSqlMeta(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	b := &BurnBridge{
		meta:         store,
		grpc:         testBurnBridgeClient{},
		activeBucket: "E60302480000000025891A00",
		udfLabel:     "E60302480000000025891A00",
	}

	ctx := context.WithValue(context.Background(), "account", auth.Account{
		Access: "drive",
		Role:   auth.RoleUser,
	})

	raw, err := b.GetBucketAcl(ctx, &s3.GetBucketAclInput{Bucket: ptr("E60302480000000025891A00")})
	if err != nil {
		t.Fatalf("GetBucketAcl returned error: %v", err)
	}

	acl, err := auth.ParseACL(raw)
	if err != nil {
		t.Fatalf("ParseACL returned error: %v", err)
	}
	if acl.Owner != "drive" {
		t.Fatalf("expected bootstrapped owner drive, got %q", acl.Owner)
	}

	persistedRaw, err := store.RetrieveAttribute(nil, "E60302480000000025891A00", "", burnbridgeACLAttribute)
	if err != nil {
		t.Fatalf("RetrieveAttribute returned error: %v", err)
	}
	persistedACL, err := auth.ParseACL(persistedRaw)
	if err != nil {
		t.Fatalf("ParseACL persisted returned error: %v", err)
	}
	if persistedACL.Owner != "drive" {
		t.Fatalf("expected persisted owner drive, got %q", persistedACL.Owner)
	}
}

func TestCreateBucketRejectsRebindAfterCommittedObjects(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "meta.db")
	store, err := meta.NewSqlMeta(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	if err := store.StoreBurnbridgeCommitted(nil, "disc-a", "file.bin", &meta.BurnbridgeCommittedRecord{
		Size:         10,
		ETag:         "\"abc\"",
		LastModified: time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatal(err)
	}

	b := &BurnBridge{
		meta:               store,
		grpc:               testBurnBridgeClient{},
		allowBucketBinding: true,
		activeBucket:       "disc-a",
		volumeLabelRaw:     "DISC-A",
		udfLabel:           "DISC-A",
	}

	err = b.CreateBucket(context.Background(), &s3.CreateBucketInput{
		Bucket: ptr("disc-b"),
	}, nil)
	if err == nil {
		t.Fatal("expected CreateBucket rebind to fail after committed objects exist")
	}
}

func TestCreateBucketPersistsRuntimeDiscBindingInSQLite(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "meta.db")
	store, err := meta.NewSqlMeta(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	b := &BurnBridge{
		meta:               store,
		grpc:               testBurnBridgeClient{},
		allowBucketBinding: true,
		activeBucket:       "DISC0001",
		volumeLabelRaw:     "E60402990000000025DD119F",
		udfLabel:           "DISC0001",
	}

	err = b.CreateBucket(context.Background(), &s3.CreateBucketInput{
		Bucket: ptr("202605241338000"),
	}, nil)
	if err != nil {
		t.Fatalf("CreateBucket returned error: %v", err)
	}

	doc, err := store.GetBurnbridgeDiscBucketBinding("E60402990000000025DD119F")
	if err != nil {
		t.Fatalf("GetBurnbridgeDiscBucketBinding returned error: %v", err)
	}
	if doc == nil {
		t.Fatal("expected persisted disc bucket binding")
	}
	if doc.Bucket != "202605241338000" {
		t.Fatalf("bucket mismatch: %q", doc.Bucket)
	}
	if doc.UdfVolumeLabel != "202605241338000" {
		t.Fatalf("udf volume label mismatch: %q", doc.UdfVolumeLabel)
	}
}

func TestLoadDiscBucketBindingReadsSQLiteRuntimeState(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "meta.db")
	store, err := meta.NewSqlMeta(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	if err := store.StoreBurnbridgeDiscBucketBinding(&meta.BurnbridgeDiscBucketBindingDocument{
		ProbeVolumeLabel: "RAWDISC001",
		Bucket:           "bucket-a",
		UdfVolumeLabel:   "BUCKET-A",
	}); err != nil {
		t.Fatal(err)
	}

	doc, ok := loadDiscBucketBinding(store, "RAWDISC001")
	if !ok {
		t.Fatal("expected loadDiscBucketBinding to find persisted runtime binding")
	}
	if doc == nil {
		t.Fatal("expected non-nil binding document")
	}
	if doc.Bucket != "bucket-a" || doc.UdfVolumeLabel != "BUCKET-A" {
		t.Fatalf("unexpected binding document: %#v", doc)
	}
}

func TestLoadBurnSegmentSnapshotFiltersDifferentMedia(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "meta.db")
	store, err := meta.NewSqlMeta(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	if err := store.UpsertBurnObjectSegment("bucket1", "file.bin", "DISC-A", 0, 0, 8, "aaa", meta.BurnSegmentSucceeded, nil); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertBurnObjectSegment("bucket1", "file.bin", "DISC-B", 1, 8, 8, "bbb", meta.BurnSegmentSucceeded, nil); err != nil {
		t.Fatal(err)
	}

	b := &BurnBridge{
		meta:           store,
		grpc:           testBurnBridgeClient{},
		activeBucket:   "bucket1",
		volumeLabelRaw: "DISC-A",
	}

	snapshot, err := b.loadBurnSegmentSnapshot("bucket1", "file.bin")
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot) != 1 {
		t.Fatalf("expected 1 segment for current media, got %d", len(snapshot))
	}
	if _, ok := snapshot[0]; !ok {
		t.Fatalf("expected segment 0 to remain in snapshot: %#v", snapshot)
	}
}

func TestLoadBurnSegmentSnapshotAcceptsHistoricalProbeForSameBucket(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "meta.db")
	store, err := meta.NewSqlMeta(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	if err := store.UpsertBurnObjectSegment("bucket1", "file.bin", "JOB-OLD", 0, 0, 8, "aaa", meta.BurnSegmentSucceeded, nil); err != nil {
		t.Fatal(err)
	}
	if err := store.StoreBurnbridgeDiscBucketBinding(&meta.BurnbridgeDiscBucketBindingDocument{
		ProbeVolumeLabel: "JOB-OLD",
		Bucket:           "bucket1",
		UdfVolumeLabel:   "DISC-NEW",
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.StoreBurnbridgeDiscInfo(&meta.BurnbridgeDiscInfoDocument{
		Bucket:      "bucket1",
		VolumeLabel: "JOB-OLD",
		UpdatedAt:   time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatal(err)
	}

	b := &BurnBridge{
		meta:           store,
		grpc:           testBurnBridgeClient{},
		activeBucket:   "bucket1",
		volumeLabelRaw: "DISC-NEW",
	}

	snapshot, err := b.loadBurnSegmentSnapshot("bucket1", "file.bin")
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot) != 1 {
		t.Fatalf("expected historical same-bucket segment to remain visible, got %d entries", len(snapshot))
	}
	if _, ok := snapshot[0]; !ok {
		t.Fatalf("expected segment 0 in snapshot: %#v", snapshot)
	}
}

func TestHeadAndListUseMetadataOnly(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "meta.db")
	store, err := meta.NewSqlMeta(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	now := time.Date(2026, 5, 23, 11, 22, 33, 0, time.UTC)
	rec := &meta.BurnbridgeCommittedRecord{
		Size:         1234,
		ETag:         "\"abc\"",
		LastModified: now.Format(time.RFC3339Nano),
	}
	if err := store.StoreBurnbridgeCommitted(nil, "BUCKET1", "file.txt", rec); err != nil {
		t.Fatal(err)
	}

	b := &BurnBridge{
		meta:         store,
		grpc:         testBurnBridgeClient{},
		readMount:    t.TempDir(),
		activeBucket: "BUCKET1",
	}

	head, err := b.HeadObject(context.Background(), &s3.HeadObjectInput{
		Bucket: ptr("BUCKET1"),
		Key:    ptr("file.txt"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if head == nil || head.ContentLength == nil || *head.ContentLength != 1234 {
		t.Fatalf("unexpected content length: %#v", head)
	}

	list, err := b.ListObjects(context.Background(), &s3.ListObjectsInput{
		Bucket: ptr("BUCKET1"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(list.Contents) != 1 || list.Contents[0].Size == nil || *list.Contents[0].Size != 1234 {
		t.Fatalf("unexpected list output: %#v", list.Contents)
	}
}

func TestMountedReadFallbackListsAndGetsPlainDiscFiles(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "meta.db")
	store, err := meta.NewSqlMeta(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	readMount := t.TempDir()
	if err := os.MkdirAll(filepath.Join(readMount, "dir"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(readMount, "dir", "file.txt"), []byte("hello mounted disc"), 0o644); err != nil {
		t.Fatal(err)
	}

	b := &BurnBridge{
		meta:         store,
		grpc:         testBurnBridgeClient{},
		readMount:    readMount,
		activeBucket: "disc-a",
	}

	list, err := b.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
		Bucket: ptr("disc-a"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(list.Contents) != 1 || list.Contents[0].Key == nil || *list.Contents[0].Key != "dir/file.txt" {
		t.Fatalf("expected only mounted user file in listing, got %#v", list.Contents)
	}

	head, err := b.HeadObject(context.Background(), &s3.HeadObjectInput{
		Bucket: ptr("disc-a"),
		Key:    ptr("dir/file.txt"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if head.ContentLength == nil || *head.ContentLength != int64(len("hello mounted disc")) {
		t.Fatalf("unexpected fallback head: %#v", head)
	}

	out, err := b.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: ptr("disc-a"),
		Key:    ptr("dir/file.txt"),
		Range:  ptr("bytes=6-12"),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = out.Body.Close() }()
	body, err := io.ReadAll(out.Body)
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != "mounted" {
		t.Fatalf("unexpected fallback get body %q", string(body))
	}
}

func TestMountedReadFallbackMergesWithCommittedListing(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "meta.db")
	store, err := meta.NewSqlMeta(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	now := time.Date(2026, 6, 14, 8, 0, 0, 0, time.UTC)
	if err := store.StoreBurnbridgeCommitted(nil, "DISC-A", "db-only.txt", &meta.BurnbridgeCommittedRecord{
		Size:         7,
		ETag:         "\"db\"",
		LastModified: now.Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.StoreBurnbridgeCommitted(nil, "DISC-A", "smoke/mounted.bin", &meta.BurnbridgeCommittedRecord{
		Size:         99,
		ETag:         "\"db-mounted\"",
		LastModified: now.Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatal(err)
	}

	readMount := t.TempDir()
	if err := os.MkdirAll(filepath.Join(readMount, "smoke"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(readMount, "smoke", "mounted.bin"), []byte("mounted"), 0o644); err != nil {
		t.Fatal(err)
	}

	b := &BurnBridge{
		meta: store,
		grpc: testBurnBridgeClient{
			readObjectFn: func(_ context.Context, req *burnbridgev1.ReadObjectRequest, _ ...grpc.CallOption) (grpc.ServerStreamingClient[burnbridgev1.ReadObjectChunk], error) {
				if req.GetObjectKey() != "db-only.txt" {
					t.Fatalf("unexpected recorder read for %s", req.GetObjectKey())
				}
				return newTestReadObjectStream([]byte("db-only")), nil
			},
		},
		readMount:    readMount,
		activeBucket: "DISC-A",
	}

	list, err := b.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
		Bucket: ptr("DISC-A"),
		Prefix: ptr("smoke/"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(list.Contents) != 1 || list.Contents[0].Key == nil || *list.Contents[0].Key != "smoke/mounted.bin" {
		t.Fatalf("expected mounted fallback object to merge into listing, got %#v", list.Contents)
	}
	if list.Contents[0].Size == nil || *list.Contents[0].Size != 99 {
		t.Fatalf("expected DB metadata to win for mounted object, got %#v", list.Contents[0].Size)
	}

	rootList, err := b.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
		Bucket: ptr("DISC-A"),
	})
	if err != nil {
		t.Fatal(err)
	}
	foundDBOnly := false
	for _, obj := range rootList.Contents {
		if obj.Key != nil && *obj.Key == "db-only.txt" {
			foundDBOnly = true
		}
	}
	if !foundDBOnly {
		t.Fatalf("expected DB-only object to remain visible when committed metadata exists: %#v", rootList.Contents)
	}

	dbOnlyHead, err := b.HeadObject(context.Background(), &s3.HeadObjectInput{
		Bucket: ptr("DISC-A"),
		Key:    ptr("db-only.txt"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if dbOnlyHead.ContentLength == nil || *dbOnlyHead.ContentLength != 7 {
		t.Fatalf("expected HeadObject to use committed metadata for DB-only object, got %#v", dbOnlyHead.ContentLength)
	}
	dbOnlyGet, err := b.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: ptr("DISC-A"),
		Key:    ptr("db-only.txt"),
	})
	if err != nil {
		t.Fatal(err)
	}
	dbOnlyBody, err := io.ReadAll(dbOnlyGet.Body)
	_ = dbOnlyGet.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if string(dbOnlyBody) != "db-only" {
		t.Fatalf("expected DB-only object to read through recorder, got %q", string(dbOnlyBody))
	}

	head, err := b.HeadObject(context.Background(), &s3.HeadObjectInput{
		Bucket: ptr("DISC-A"),
		Key:    ptr("smoke/mounted.bin"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if head.ContentLength == nil || *head.ContentLength != int64(len("mounted")) {
		t.Fatalf("expected HeadObject size to follow mounted file, got %#v", head.ContentLength)
	}
}

func TestMountedReadFallbackUsesDiscRootAndSkipsBucketDirectory(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "meta.db")
	store, err := meta.NewSqlMeta(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	readMount := t.TempDir()
	if err := os.MkdirAll(filepath.Join(readMount, "disc-a"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(readMount, "disc-a", "file.txt"), []byte("bucket layout"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(readMount, "root.txt"), []byte("plain root"), 0o644); err != nil {
		t.Fatal(err)
	}

	b := &BurnBridge{
		meta:         store,
		grpc:         testBurnBridgeClient{},
		readMount:    readMount,
		activeBucket: "disc-a",
	}

	list, err := b.ListObjects(context.Background(), &s3.ListObjectsInput{
		Bucket: ptr("disc-a"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(list.Contents) != 1 || list.Contents[0].Key == nil || *list.Contents[0].Key != "root.txt" {
		t.Fatalf("expected disc root listing only, got %#v", list.Contents)
	}
}

func TestMountedReadFallbackDisabledWhenNoDiscLatched(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "meta.db")
	store, err := meta.NewSqlMeta(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	readMount := t.TempDir()
	if err := os.WriteFile(filepath.Join(readMount, "file.txt"), []byte("stale"), 0o644); err != nil {
		t.Fatal(err)
	}

	b := &BurnBridge{
		meta:           store,
		grpc:           testBurnBridgeClient{},
		readMount:      readMount,
		activeBucket:   "disc-a",
		noDiscLatched:  true,
		volumeLabelRaw: "DISC-A",
	}

	list, err := b.ListBuckets(context.Background(), s3response.ListBucketsInput{
		Owner:      "owner",
		MaxBuckets: 1000,
		IsAdmin:    true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(list.Buckets.Bucket) != 0 {
		t.Fatalf("expected no buckets while no-disc is latched, got %#v", list.Buckets.Bucket)
	}

	if _, err := b.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: ptr("disc-a"),
		Key:    ptr("file.txt"),
	}); err == nil {
		t.Fatal("expected no-disc latch to block stale mounted fallback object")
	}
}

func TestMetadataHotPathsDoNotProbeRecorder(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "meta.db")
	store, err := meta.NewSqlMeta(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	now := time.Date(2026, 5, 29, 9, 10, 11, 0, time.UTC)
	rec := &meta.BurnbridgeCommittedRecord{
		Size:         4096,
		ETag:         "\"etag-hot\"",
		LastModified: now.Format(time.RFC3339Nano),
	}
	if err := store.StoreBurnbridgeCommitted(nil, "BUCKET1", "file.bin", rec); err != nil {
		t.Fatal(err)
	}

	var readyCalls int32
	var importedCalls int32
	var readCalls int32
	b := &BurnBridge{
		meta:         store,
		activeBucket: "BUCKET1",
		grpc: testBurnBridgeClient{
			testUnitReadyFn: func(context.Context, *burnbridgev1.TestUnitReadyRequest, ...grpc.CallOption) (*burnbridgev1.TestUnitReadyResponse, error) {
				atomic.AddInt32(&readyCalls, 1)
				return &burnbridgev1.TestUnitReadyResponse{Ready: true, VolumeLabel: "DISC-1"}, nil
			},
			importedBucketStateFn: func(context.Context, *burnbridgev1.GetImportedBucketStateRequest, ...grpc.CallOption) (*burnbridgev1.GetImportedBucketStateResponse, error) {
				atomic.AddInt32(&importedCalls, 1)
				return &burnbridgev1.GetImportedBucketStateResponse{Loaded: true, Bucket: "BUCKET1"}, nil
			},
			readObjectFn: func(context.Context, *burnbridgev1.ReadObjectRequest, ...grpc.CallOption) (grpc.ServerStreamingClient[burnbridgev1.ReadObjectChunk], error) {
				atomic.AddInt32(&readCalls, 1)
				return nil, nil
			},
		},
		importedBucketState: map[string]bool{},
	}

	if _, err := b.HeadBucket(context.Background(), &s3.HeadBucketInput{Bucket: ptr("BUCKET1")}); err != nil {
		t.Fatal(err)
	}
	if _, err := b.HeadObject(context.Background(), &s3.HeadObjectInput{
		Bucket: ptr("BUCKET1"),
		Key:    ptr("file.bin"),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := b.ListObjects(context.Background(), &s3.ListObjectsInput{
		Bucket: ptr("BUCKET1"),
	}); err != nil {
		t.Fatal(err)
	}

	if atomic.LoadInt32(&readyCalls) != 0 {
		t.Fatalf("expected no TestUnitReady calls on metadata hot paths, got %d", readyCalls)
	}
	if atomic.LoadInt32(&importedCalls) != 0 {
		t.Fatalf("expected no GetImportedBucketState calls on metadata hot paths, got %d", importedCalls)
	}
	if atomic.LoadInt32(&readCalls) != 0 {
		t.Fatalf("expected no ReadObject fallback on metadata hot paths, got %d", readCalls)
	}
}

func TestEnsureActiveBucketLoadedSingleflight(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "meta.db")
	store, err := meta.NewSqlMeta(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	var readyCalls int32
	start := make(chan struct{})
	b := &BurnBridge{
		meta: store,
		grpc: testBurnBridgeClient{
			testUnitReadyFn: func(context.Context, *burnbridgev1.TestUnitReadyRequest, ...grpc.CallOption) (*burnbridgev1.TestUnitReadyResponse, error) {
				atomic.AddInt32(&readyCalls, 1)
				<-start
				return &burnbridgev1.TestUnitReadyResponse{
					Ready:       true,
					VolumeLabel: "DISC-A",
				}, nil
			},
			importedBucketStateFn: func(context.Context, *burnbridgev1.GetImportedBucketStateRequest, ...grpc.CallOption) (*burnbridgev1.GetImportedBucketStateResponse, error) {
				return &burnbridgev1.GetImportedBucketStateResponse{
					Loaded: true,
					Bucket: "disc-a",
				}, nil
			},
		},
		importedBucketState: map[string]bool{},
	}

	const workers = 4
	errs := make([]error, workers)
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func(idx int) {
			defer wg.Done()
			errs[idx] = b.ensureActiveBucketLoaded(context.Background())
		}(i)
	}

	time.Sleep(100 * time.Millisecond)
	close(start)
	wg.Wait()

	if got := atomic.LoadInt32(&readyCalls); got != 1 {
		t.Fatalf("expected one TestUnitReady call, got %d", got)
	}
	for i, err := range errs {
		if err != nil {
			t.Fatalf("worker %d returned error: %v", i, err)
		}
	}
	if got := strings.TrimSpace(b.activeBucket); got != "disc-a" {
		t.Fatalf("expected active bucket disc-a, got %q", got)
	}
}

func TestEnsureActiveBucketLoadedSkipsProbeWhileNoDiscLatched(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "meta.db")
	store, err := meta.NewSqlMeta(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	var readyCalls int32
	b := &BurnBridge{
		meta: store,
		grpc: testBurnBridgeClient{
			testUnitReadyFn: func(context.Context, *burnbridgev1.TestUnitReadyRequest, ...grpc.CallOption) (*burnbridgev1.TestUnitReadyResponse, error) {
				atomic.AddInt32(&readyCalls, 1)
				return &burnbridgev1.TestUnitReadyResponse{
					Ready:       true,
					VolumeLabel: "DISC-A",
				}, nil
			},
		},
		lastNoDiscObservedAt: time.Now().UTC().Add(-time.Hour),
		noDiscLatched:        true,
		importedBucketState:  map[string]bool{},
	}

	if err := b.ensureActiveBucketLoaded(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := atomic.LoadInt32(&readyCalls); got != 0 {
		t.Fatalf("expected no TestUnitReady call while no-disc is latched, got %d", got)
	}
	if strings.TrimSpace(b.activeBucket) != "" {
		t.Fatalf("expected active bucket to remain empty while no-disc is latched, got %q", b.activeBucket)
	}
}

func TestSyncActiveDiscStateClearsNoDiscLatchOnReady(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "meta.db")
	store, err := meta.NewSqlMeta(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	b := &BurnBridge{
		meta: store,
		grpc: testBurnBridgeClient{
			importedBucketStateFn: func(context.Context, *burnbridgev1.GetImportedBucketStateRequest, ...grpc.CallOption) (*burnbridgev1.GetImportedBucketStateResponse, error) {
				return &burnbridgev1.GetImportedBucketStateResponse{
					Loaded: true,
					Bucket: "disc-a",
				}, nil
			},
		},
		lastNoDiscObservedAt: time.Now().UTC().Add(-time.Hour),
		noDiscLatched:        true,
		importedBucketState:  map[string]bool{},
	}

	if err := b.syncActiveDiscState(&burnbridgev1.TestUnitReadyResponse{
		Ready:       true,
		VolumeLabel: "DISC-A",
	}); err != nil {
		t.Fatal(err)
	}
	if b.noDiscLatchedState() {
		t.Fatal("expected ready state to clear no-disc latch")
	}
	if got := strings.TrimSpace(b.activeBucket); got != "disc-a" {
		t.Fatalf("expected ready state to restore active bucket disc-a, got %q", got)
	}
}

func TestProbeRecorderDiscAtStartupReturnsSanitizedBucketForBlankDisc(t *testing.T) {
	client := testBurnBridgeClient{
		testUnitReadyFn: func(context.Context, *burnbridgev1.TestUnitReadyRequest, ...grpc.CallOption) (*burnbridgev1.TestUnitReadyResponse, error) {
			return &burnbridgev1.TestUnitReadyResponse{
				Ready:         true,
				VolumeLabel:   "DISC-BLANK",
				WritableState: "Blank",
			}, nil
		},
	}

	bucket, rawVolume, resp, err := probeRecorderDiscAtStartup(context.Background(), client)
	if err != nil {
		t.Fatal(err)
	}
	if bucket != "DISC-BLANK" {
		t.Fatalf("expected uppercase startup bucket DISC-BLANK for blank disc, got %q", bucket)
	}
	if rawVolume != "DISC-BLANK" {
		t.Fatalf("expected raw volume DISC-BLANK, got %q", rawVolume)
	}
	if resp == nil || !resp.GetReady() {
		t.Fatal("expected ready blank-disc response")
	}
}

func TestEnsureImportedBucketStateSingleflightForEmptyBucket(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "meta.db")
	store, err := meta.NewSqlMeta(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	var importedCalls int32
	start := make(chan struct{})
	b := &BurnBridge{
		meta:         store,
		activeBucket: "bucket1",
		grpc: testBurnBridgeClient{
			importedBucketStateFn: func(context.Context, *burnbridgev1.GetImportedBucketStateRequest, ...grpc.CallOption) (*burnbridgev1.GetImportedBucketStateResponse, error) {
				atomic.AddInt32(&importedCalls, 1)
				<-start
				return &burnbridgev1.GetImportedBucketStateResponse{
					Loaded: true,
					Bucket: "bucket1",
					Objects: []*burnbridgev1.ImportedObjectState{
						{
							ObjectKey:       "dir/file.txt",
							Size:            42,
							Etag:            "\"abc\"",
							LastModifiedUtc: time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC).Format(time.RFC3339Nano),
						},
					},
				}, nil
			},
		},
		importedBucketState: map[string]bool{},
	}

	const workers = 4
	errs := make([]error, workers)
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func(idx int) {
			defer wg.Done()
			errs[idx] = b.ensureImportedBucketState(context.Background(), "bucket1")
		}(i)
	}

	time.Sleep(100 * time.Millisecond)
	close(start)
	wg.Wait()

	if got := atomic.LoadInt32(&importedCalls); got != 1 {
		t.Fatalf("expected one GetImportedBucketState call, got %d", got)
	}
	for i, err := range errs {
		if err != nil {
			t.Fatalf("worker %d returned error: %v", i, err)
		}
	}
	sum, err := store.GetCommittedObjectSummary("bucket1", "dir/file.txt")
	if err != nil {
		t.Fatal(err)
	}
	if sum.Size != 42 {
		t.Fatalf("expected imported size 42, got %d", sum.Size)
	}
}

func TestSyncImportedBucketStatePrunesStaleCommittedObjects(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "meta.db")
	store, err := meta.NewSqlMeta(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	now := time.Date(2026, 6, 14, 9, 0, 0, 0, time.UTC)
	if err := store.StoreBurnbridgeCommitted(nil, "disc-a", "stale.txt", &meta.BurnbridgeCommittedRecord{
		Size:         7,
		ETag:         "\"stale\"",
		LastModified: now.Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.StoreBurnbridgeCommitted(nil, "disc-a", "keep.txt", &meta.BurnbridgeCommittedRecord{
		Size:         1,
		ETag:         "\"old\"",
		LastModified: now.Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatal(err)
	}

	b := &BurnBridge{
		meta:         store,
		activeBucket: "disc-a",
		grpc: testBurnBridgeClient{
			importedBucketStateFn: func(context.Context, *burnbridgev1.GetImportedBucketStateRequest, ...grpc.CallOption) (*burnbridgev1.GetImportedBucketStateResponse, error) {
				return &burnbridgev1.GetImportedBucketStateResponse{
					Loaded: true,
					Bucket: "disc-a",
					Objects: []*burnbridgev1.ImportedObjectState{
						{
							ObjectKey:       "keep.txt",
							Size:            42,
							Etag:            "\"fresh\"",
							LastModifiedUtc: now.Add(time.Minute).Format(time.RFC3339Nano),
						},
					},
				}, nil
			},
		},
		importedBucketState: map[string]bool{},
	}

	if err := b.syncImportedBucketState(context.Background(), "disc-a"); err != nil {
		t.Fatal(err)
	}

	if _, err := store.GetCommittedObjectSummary("disc-a", "stale.txt"); !errors.Is(err, meta.ErrNoSuchKey) {
		t.Fatalf("expected stale committed object to be pruned, got err=%v", err)
	}
	sum, err := store.GetCommittedObjectSummary("disc-a", "keep.txt")
	if err != nil {
		t.Fatal(err)
	}
	if sum.Size != 42 || sum.ETag != "\"fresh\"" {
		t.Fatalf("expected imported object to be refreshed, got %#v", sum)
	}
}

func TestSyncImportedBucketStateDeletesOtherBucketMetadata(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "meta.db")
	store, err := meta.NewSqlMeta(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	now := time.Date(2026, 6, 14, 9, 30, 0, 0, time.UTC)
	if err := store.StoreBurnbridgeCommitted(nil, "disc-old", "ghost.txt", &meta.BurnbridgeCommittedRecord{
		Size:         7,
		LastModified: now.Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertBurnUploadSession(meta.BurnUploadSessionRecord{
		Bucket:        "disc-old",
		ObjectName:    "ghost.txt",
		UploadID:      "upload-old",
		Kind:          meta.BurnUploadKindMultipart,
		State:         meta.BurnUploadStateWriting,
		RecorderJobID: "job-old",
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertBurnUploadPart(meta.BurnUploadPartRecord{
		Bucket:        "disc-old",
		ObjectName:    "ghost.txt",
		UploadID:      "upload-old",
		PartNumber:    1,
		BytesReceived: 1,
		State:         meta.BurnUploadStateWriting,
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.StoreBurnbridgeDiscBucketBinding(&meta.BurnbridgeDiscBucketBindingDocument{
		ProbeVolumeLabel: "OLD-DISC",
		Bucket:           "disc-old",
		UdfVolumeLabel:   "OLD-DISC",
	}); err != nil {
		t.Fatal(err)
	}

	b := &BurnBridge{
		meta:         store,
		activeBucket: "disc-new",
		grpc: testBurnBridgeClient{
			importedBucketStateFn: func(context.Context, *burnbridgev1.GetImportedBucketStateRequest, ...grpc.CallOption) (*burnbridgev1.GetImportedBucketStateResponse, error) {
				return &burnbridgev1.GetImportedBucketStateResponse{
					Loaded: true,
					Bucket: "disc-new",
					Objects: []*burnbridgev1.ImportedObjectState{
						{
							ObjectKey:       "current.txt",
							Size:            42,
							LastModifiedUtc: now.Format(time.RFC3339Nano),
						},
					},
				}, nil
			},
		},
		importedBucketState: map[string]bool{},
	}

	if err := b.syncImportedBucketState(context.Background(), "disc-new"); err != nil {
		t.Fatal(err)
	}

	if _, err := store.GetCommittedObjectSummary("disc-old", "ghost.txt"); !errors.Is(err, meta.ErrNoSuchKey) {
		t.Fatalf("expected old committed metadata to be deleted, got %v", err)
	}
	if _, err := store.GetBurnUploadSession("disc-old", "ghost.txt", "upload-old"); !errors.Is(err, meta.ErrNoSuchKey) {
		t.Fatalf("expected old upload session to be deleted, got %v", err)
	}
	if _, err := store.GetBurnUploadPart("disc-old", "ghost.txt", "upload-old", 1); !errors.Is(err, meta.ErrNoSuchKey) {
		t.Fatalf("expected old upload part to be deleted, got %v", err)
	}
	if bindings, err := store.ListBurnbridgeDiscBucketBindings("disc-old"); err != nil {
		t.Fatal(err)
	} else if len(bindings) != 0 {
		t.Fatalf("expected old runtime bindings to be deleted, got %#v", bindings)
	}
	if _, err := store.GetCommittedObjectSummary("disc-new", "current.txt"); err != nil {
		t.Fatalf("expected current imported object to remain, got %v", err)
	}
}

func TestSyncImportedBucketStateKeepsDriveControlBucketMetadata(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "meta.db")
	store, err := meta.NewSqlMeta(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	now := time.Date(2026, 6, 25, 14, 30, 0, 0, time.UTC)
	if err := store.StoreBurnbridgeCommitted(nil, "drive-control-001", "v1/state/drive-info", &meta.BurnbridgeCommittedRecord{
		Size:         1,
		LastModified: now.Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.StoreBurnbridgeCommitted(nil, "disc-old", "ghost.txt", &meta.BurnbridgeCommittedRecord{
		Size:         7,
		LastModified: now.Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatal(err)
	}

	b := &BurnBridge{
		meta:                   store,
		activeBucket:           "disc-new",
		lastDriveControlBucket: "drive-control-001",
		grpc: testBurnBridgeClient{
			importedBucketStateFn: func(context.Context, *burnbridgev1.GetImportedBucketStateRequest, ...grpc.CallOption) (*burnbridgev1.GetImportedBucketStateResponse, error) {
				return &burnbridgev1.GetImportedBucketStateResponse{
					Loaded: true,
					Bucket: "disc-new",
				}, nil
			},
		},
		importedBucketState: map[string]bool{},
	}

	if err := b.syncImportedBucketState(context.Background(), "disc-new"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.GetCommittedObjectSummary("disc-old", "ghost.txt"); !errors.Is(err, meta.ErrNoSuchKey) {
		t.Fatalf("expected old committed metadata to be deleted, got %v", err)
	}
	if _, err := store.GetCommittedObjectSummary("drive-control-001", "v1/state/drive-info"); err != nil {
		t.Fatalf("expected drive control bucket metadata to remain, got %v", err)
	}
}

func TestSyncImportedBucketStateIgnoresBucketMismatchedWithMountedDisc(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "meta.db")
	store, err := meta.NewSqlMeta(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	readMount := t.TempDir()
	if err := os.MkdirAll(filepath.Join(readMount, "disc-current"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(readMount, "OAdisc-current.sqlite3"), []byte("metadata"), 0o644); err != nil {
		t.Fatal(err)
	}

	b := &BurnBridge{
		meta:         store,
		readMount:    readMount,
		activeBucket: "disc-current",
		grpc: testBurnBridgeClient{
			importedBucketStateFn: func(context.Context, *burnbridgev1.GetImportedBucketStateRequest, ...grpc.CallOption) (*burnbridgev1.GetImportedBucketStateResponse, error) {
				return &burnbridgev1.GetImportedBucketStateResponse{
					Loaded: true,
					Bucket: "disc-old",
					Objects: []*burnbridgev1.ImportedObjectState{
						{ObjectKey: "ghost.txt", Size: 1},
					},
				}, nil
			},
		},
		importedBucketState: map[string]bool{},
	}

	if err := b.syncImportedBucketState(context.Background(), "disc-current"); err != nil {
		t.Fatal(err)
	}
	if b.activeBucket != "disc-current" {
		t.Fatalf("expected active bucket to remain mounted bucket, got %q", b.activeBucket)
	}
	if _, err := store.GetCommittedObjectSummary("disc-old", "ghost.txt"); !errors.Is(err, meta.ErrNoSuchKey) {
		t.Fatalf("expected mismatched imported object to be ignored, got err=%v", err)
	}
}

func TestEnsureActiveBucketLoadedSwitchesToMountedDiscBucket(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "meta.db")
	store, err := meta.NewSqlMeta(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	readMount := t.TempDir()
	if err := os.MkdirAll(filepath.Join(readMount, "disc-current"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(readMount, "OAdisc-current.sqlite3"), []byte("metadata"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(readMount, "disc-current", "file.txt"), []byte("current"), 0o644); err != nil {
		t.Fatal(err)
	}

	b := &BurnBridge{
		meta:         store,
		readMount:    readMount,
		activeBucket: "disc-old",
		grpc: testBurnBridgeClient{
			importedBucketStateFn: func(context.Context, *burnbridgev1.GetImportedBucketStateRequest, ...grpc.CallOption) (*burnbridgev1.GetImportedBucketStateResponse, error) {
				return &burnbridgev1.GetImportedBucketStateResponse{
					Loaded: true,
					Bucket: "disc-old",
				}, nil
			},
		},
		importedBucketState:        map[string]bool{},
		pendingImportedConvergence: map[string]bool{},
	}

	list, err := b.ListBuckets(context.Background(), s3response.ListBucketsInput{
		Owner:      "owner",
		MaxBuckets: 1000,
		IsAdmin:    true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if b.activeBucket != "disc-current" {
		t.Fatalf("expected active bucket to switch to mounted bucket, got %q", b.activeBucket)
	}
	if len(list.Buckets.Bucket) != 1 || list.Buckets.Bucket[0].Name != "disc-current" {
		t.Fatalf("expected mounted bucket listing, got %#v", list.Buckets.Bucket)
	}
}

func TestMountedDiscBucketHintBlocksHistoricalBucketAccess(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "meta.db")
	store, err := meta.NewSqlMeta(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	if err := store.StoreBurnbridgeCommitted(nil, "disc-old", "ghost.txt", &meta.BurnbridgeCommittedRecord{
		Size:         1,
		LastModified: time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatal(err)
	}

	readMount := t.TempDir()
	if err := os.MkdirAll(filepath.Join(readMount, "disc-current"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(readMount, "OAdisc-current.sqlite3"), []byte("metadata"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(readMount, "disc-current", "file.txt"), []byte("current"), 0o644); err != nil {
		t.Fatal(err)
	}

	b := &BurnBridge{
		meta:         store,
		readMount:    readMount,
		activeBucket: "disc-current",
		grpc:         testBurnBridgeClient{},
	}

	if _, err := b.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
		Bucket: ptr("disc-old"),
	}); err == nil {
		t.Fatal("expected historical bucket access to fail while a different mounted disc bucket is visible")
	}
}

func TestGetObjectUsesRecorderWhenCommittedBucketPathMissing(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "meta.db")
	store, err := meta.NewSqlMeta(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	readMount := t.TempDir()
	if err := os.WriteFile(filepath.Join(readMount, "root-file.txt"), []byte("root-layout"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := store.StoreBurnbridgeCommitted(nil, "DISC-CURRENT", "root-file.txt", &meta.BurnbridgeCommittedRecord{
		Size:         int64(len("record-layout")),
		ETag:         "\"etag\"",
		LastModified: time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatal(err)
	}

	b := &BurnBridge{
		meta:         store,
		readMount:    readMount,
		activeBucket: "disc-current",
		grpc: testBurnBridgeClient{
			readObjectFn: func(_ context.Context, req *burnbridgev1.ReadObjectRequest, _ ...grpc.CallOption) (grpc.ServerStreamingClient[burnbridgev1.ReadObjectChunk], error) {
				if req.GetObjectKey() != "root-file.txt" {
					t.Fatalf("unexpected recorder read for %s", req.GetObjectKey())
				}
				return newTestReadObjectStream([]byte("record-layout")), nil
			},
		},
	}

	out, err := b.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: ptr("disc-current"),
		Key:    ptr("root-file.txt"),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = out.Body.Close() }()
	raw, err := io.ReadAll(out.Body)
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != "record-layout" {
		t.Fatalf("expected recorder content, got %q", string(raw))
	}
}

func TestGetObjectUsesRecorderWhenMountedRootHasArchiveMetadata(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "meta.db")
	store, err := meta.NewSqlMeta(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	readMount := t.TempDir()
	if err := os.WriteFile(filepath.Join(readMount, "OAdisc-current.sqlite3"), []byte("metadata"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(readMount, "root-file.txt"), []byte("mounted-root-layout"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := store.StoreBurnbridgeCommitted(nil, "DISC-CURRENT", "root-file.txt", &meta.BurnbridgeCommittedRecord{
		Size:         int64(len("recorder-layout")),
		ETag:         "\"etag\"",
		LastModified: time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.StoreAttribute(nil, "DISC-CURRENT", "", "redundancy_enabled", []byte("true")); err != nil {
		t.Fatal(err)
	}
	if err := store.StoreAttribute(nil, "DISC-CURRENT", "", "redundancy_parity_block_count", []byte("2")); err != nil {
		t.Fatal(err)
	}

	var readCalls int32
	b := &BurnBridge{
		meta:         store,
		readMount:    readMount,
		activeBucket: "DISC-CURRENT",
		grpc: testBurnBridgeClient{
			readObjectFn: func(_ context.Context, req *burnbridgev1.ReadObjectRequest, _ ...grpc.CallOption) (grpc.ServerStreamingClient[burnbridgev1.ReadObjectChunk], error) {
				atomic.AddInt32(&readCalls, 1)
				if req.GetBucket() != "DISC-CURRENT" || req.GetObjectKey() != "root-file.txt" {
					t.Fatalf("unexpected recorder read request: %#v", req)
				}
				return newTestReadObjectStream([]byte("recorder-layout")), nil
			},
		},
	}

	out, err := b.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: ptr("DISC-CURRENT"),
		Key:    ptr("root-file.txt"),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = out.Body.Close() }()
	raw, err := io.ReadAll(out.Body)
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != "recorder-layout" {
		t.Fatalf("expected recorder content when archive metadata exists, got %q", string(raw))
	}
	if got := atomic.LoadInt32(&readCalls); got != 1 {
		t.Fatalf("expected one recorder read, got %d", got)
	}
}

func TestGetObjectUsesMountedRootWhenArchiveMetadataHasRedundancyDisabled(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "meta.db")
	store, err := meta.NewSqlMeta(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	readMount := t.TempDir()
	if err := os.WriteFile(filepath.Join(readMount, "OAdisc-current.sqlite3"), []byte("metadata"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(readMount, "root-file.txt"), []byte("mounted-root-layout"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := store.StoreBurnbridgeCommitted(nil, "DISC-CURRENT", "root-file.txt", &meta.BurnbridgeCommittedRecord{
		Size:         int64(len("recorder-layout")),
		ETag:         "\"etag\"",
		LastModified: time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.StoreAttribute(nil, "DISC-CURRENT", "", "redundancy_enabled", []byte("false")); err != nil {
		t.Fatal(err)
	}
	if err := store.StoreAttribute(nil, "DISC-CURRENT", "", "redundancy_parity_block_count", []byte("0")); err != nil {
		t.Fatal(err)
	}

	var readCalls int32
	b := &BurnBridge{
		meta:         store,
		readMount:    readMount,
		activeBucket: "DISC-CURRENT",
		grpc: testBurnBridgeClient{
			readObjectFn: func(_ context.Context, _ *burnbridgev1.ReadObjectRequest, _ ...grpc.CallOption) (grpc.ServerStreamingClient[burnbridgev1.ReadObjectChunk], error) {
				atomic.AddInt32(&readCalls, 1)
				return newTestReadObjectStream([]byte("recorder-layout")), nil
			},
		},
	}

	out, err := b.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: ptr("DISC-CURRENT"),
		Key:    ptr("root-file.txt"),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = out.Body.Close() }()
	raw, err := io.ReadAll(out.Body)
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != "mounted-root-layout" {
		t.Fatalf("expected mounted content when redundancy is disabled, got %q", string(raw))
	}
	if got := atomic.LoadInt32(&readCalls); got != 0 {
		t.Fatalf("expected no recorder read when redundancy disabled, got %d", got)
	}
}

func TestApplyRecorderStatusEventClearsBucketOnNoDisc(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "meta.db")
	store, err := meta.NewSqlMeta(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	if err := store.StoreBurnbridgeCommitted(nil, "disc-a", "file.txt", &meta.BurnbridgeCommittedRecord{
		Size:         7,
		ETag:         "\"etag\"",
		LastModified: time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.StoreBurnbridgeDiscBucketBinding(&meta.BurnbridgeDiscBucketBindingDocument{
		ProbeVolumeLabel: "DISC-A",
		Bucket:           "disc-a",
		UdfVolumeLabel:   "DISC-A",
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertBurnUploadSession(meta.BurnUploadSessionRecord{
		Bucket:        "disc-a",
		ObjectName:    "file.txt",
		UploadID:      burnbridgeImplicitSingleUploadID,
		Kind:          meta.BurnUploadKindSingle,
		State:         meta.BurnUploadStateCompleted,
		MediaID:       "DISC-A",
		ContentLength: 7,
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertBurnUploadPart(meta.BurnUploadPartRecord{
		Bucket:        "disc-a",
		ObjectName:    "file.txt",
		UploadID:      burnbridgeImplicitSingleUploadID,
		PartNumber:    1,
		BytesReceived: 7,
		PartSize:      7,
		State:         meta.BurnUploadStateCompleted,
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertBurnObjectSegment("disc-a", "file.txt", "DISC-A", 0, 0, 7, "d41d8cd98f00b204e9800998ecf8427e", meta.BurnSegmentSucceeded, nil); err != nil {
		t.Fatal(err)
	}

	b := &BurnBridge{
		meta:           store,
		activeBucket:   "disc-a",
		volumeLabelRaw: "DISC-A",
		udfLabel:       "DISC-A",
		metaDBPath:     dbPath,
		importedBucketState: map[string]bool{
			"disc-a": true,
		},
	}

	err = b.applyRecorderStatusEvent(&burnbridgev1.UnitStatusEvent{
		Snapshot: &burnbridgev1.TestUnitReadyResponse{
			Ready:   false,
			Message: "NoDisc: no disc inserted in optical drive",
		},
		Sequence: 1,
		Source:   "watch:test",
	})
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(b.activeBucket) != "" {
		t.Fatalf("expected active bucket to be cleared, got %q", b.activeBucket)
	}
}

func TestSyncActiveDiscStateImportsCommittedObjectsFromRecorder(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "meta.db")
	store, err := meta.NewSqlMeta(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	b := &BurnBridge{
		meta: store,
		grpc: testBurnBridgeClient{
			importedBucketStateFn: func(context.Context, *burnbridgev1.GetImportedBucketStateRequest, ...grpc.CallOption) (*burnbridgev1.GetImportedBucketStateResponse, error) {
				return &burnbridgev1.GetImportedBucketStateResponse{
					Bucket:         "disc-a",
					UdfVolumeLabel: "DISC-A",
					Loaded:         true,
					BucketMetadata: []*burnbridgev1.ObjectMetadata{
						{Key: "redundancy_enabled", Value: "true"},
						{Key: "redundancy_parity_block_count", Value: "2"},
						{Key: "ignored_non_redundancy", Value: "skip"},
					},
					Objects: []*burnbridgev1.ImportedObjectState{
						{
							ObjectKey:       "dir/file.txt",
							Size:            42,
							Etag:            "\"abc\"",
							LastModifiedUtc: time.Date(2026, 5, 24, 7, 8, 9, 0, time.UTC).Format(time.RFC3339Nano),
							ContentType:     "text/plain",
							Metadata: []*burnbridgev1.ObjectMetadata{
								{Key: "owner", Value: "qa"},
							},
						},
					},
				}, nil
			},
		},
	}

	if err := b.syncActiveDiscState(&burnbridgev1.TestUnitReadyResponse{
		Ready:       true,
		VolumeLabel: "DISC-A",
	}); err != nil {
		t.Fatal(err)
	}

	sum, err := store.GetCommittedObjectSummary("disc-a", "dir/file.txt")
	if err != nil {
		t.Fatal(err)
	}
	if sum.Size != 42 {
		t.Fatalf("expected imported size 42, got %d", sum.Size)
	}
	if sum.ETag != "\"abc\"" {
		t.Fatalf("expected imported etag, got %q", sum.ETag)
	}
	if got := sum.Metadata["owner"]; got != "qa" {
		t.Fatalf("expected imported metadata owner=qa, got %q", got)
	}
	rawEnabled, err := store.RetrieveAttribute(nil, "disc-a", "", "redundancy_enabled")
	if err != nil {
		t.Fatal(err)
	}
	if got := string(rawEnabled); got != "true" {
		t.Fatalf("expected imported redundancy_enabled=true, got %q", got)
	}
	rawParity, err := store.RetrieveAttribute(nil, "disc-a", "", "redundancy_parity_block_count")
	if err != nil {
		t.Fatal(err)
	}
	if got := string(rawParity); got != "2" {
		t.Fatalf("expected imported redundancy_parity_block_count=2, got %q", got)
	}
	if _, err := store.RetrieveAttribute(nil, "disc-a", "", "ignored_non_redundancy"); !errors.Is(err, meta.ErrNoSuchKey) {
		t.Fatalf("expected non-redundancy metadata to be ignored, got %v", err)
	}
}

func TestPruneOtherBurnbridgeBucketsPreservesPersistedControlBucket(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "meta.db")
	store, err := meta.NewSqlMeta(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	if err := store.StoreBurnbridgeCommitted(nil, "DISC-A", "file.txt", &meta.BurnbridgeCommittedRecord{
		Size:         1,
		ETag:         "\"a\"",
		LastModified: time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.StoreBurnbridgeDriveInfo(&meta.BurnbridgeDriveInfoDocument{
		Bucket:        "DRIVE-001",
		ControlBucket: "DRIVE-001",
		UpdatedAt:     time.Now().UTC().Format(time.RFC3339Nano),
		SerialNumber:  "DRIVE-001",
		IsMMCUnit:     true,
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.StoreBurnbridgeCommitted(nil, "STALE", "old.txt", &meta.BurnbridgeCommittedRecord{
		Size:         1,
		ETag:         "\"old\"",
		LastModified: time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatal(err)
	}

	b := &BurnBridge{meta: store}
	if err := b.pruneOtherBurnbridgeBuckets("DISC-A"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.GetBurnbridgeDriveInfoJSON("DRIVE-001"); err != nil {
		t.Fatalf("expected persisted control bucket to survive prune: %v", err)
	}
	if _, err := store.GetCommittedObjectSummary("STALE", "old.txt"); !errors.Is(err, meta.ErrNoSuchKey) {
		t.Fatalf("expected stale bucket metadata to be pruned, got %v", err)
	}
}

func TestEnsureActiveBucketLoadedDoesNotRestoreCommittedMetadataForKnownBlankDisc(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "meta.db")
	store, err := meta.NewSqlMeta(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	if err := store.StoreBurnbridgeCommitted(nil, "DISC-BLANK", "ghost.txt", &meta.BurnbridgeCommittedRecord{
		Size:         5,
		ETag:         "\"ghost\"",
		LastModified: time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.StoreBurnbridgeDiscInfo(&meta.BurnbridgeDiscInfoDocument{
		Bucket:        "DISC-BLANK",
		VolumeLabel:   "DISC-BLANK",
		UpdatedAt:     time.Now().UTC().Format(time.RFC3339Nano),
		WritableState: "Blank",
	}); err != nil {
		t.Fatal(err)
	}

	probeCalls := int32(0)
	b := &BurnBridge{
		meta:           store,
		activeBucket:   "DISC-BLANK",
		volumeLabelRaw: "DISC-BLANK",
		udfLabel:       "DISC-BLANK",
		grpc: testBurnBridgeClient{
			testUnitReadyFn: func(context.Context, *burnbridgev1.TestUnitReadyRequest, ...grpc.CallOption) (*burnbridgev1.TestUnitReadyResponse, error) {
				atomic.AddInt32(&probeCalls, 1)
				return &burnbridgev1.TestUnitReadyResponse{
					Ready:         true,
					VolumeLabel:   "DISC-BLANK",
					WritableState: "Blank",
				}, nil
			},
		},
	}

	if err := b.ensureActiveBucketLoaded(context.Background()); err != nil {
		t.Fatal(err)
	}
	if atomic.LoadInt32(&probeCalls) != 0 {
		t.Fatalf("expected no recorder probe for hot metadata path, got %d", probeCalls)
	}
	if b.importedBucketConvergencePending("DISC-BLANK") {
		t.Fatal("expected blank-disc committed metadata not to trigger imported convergence")
	}
}

func TestSyncActiveDiscStateSkipsImportedStateProbeWhenMetadataAlreadyPresent(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "meta.db")
	store, err := meta.NewSqlMeta(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	if err := store.StoreBurnbridgeCommitted(nil, "disc-a", "dir/file.txt", &meta.BurnbridgeCommittedRecord{
		Status:       "imported",
		ETag:         "\"abc\"",
		LastModified: time.Now().UTC().Format(time.RFC3339Nano),
		Size:         42,
	}); err != nil {
		t.Fatal(err)
	}

	var importedCalls int32
	b := &BurnBridge{
		meta:           store,
		activeBucket:   "disc-a",
		volumeLabelRaw: "DISC-A",
		udfLabel:       "DISC-A",
		grpc: testBurnBridgeClient{
			importedBucketStateFn: func(context.Context, *burnbridgev1.GetImportedBucketStateRequest, ...grpc.CallOption) (*burnbridgev1.GetImportedBucketStateResponse, error) {
				atomic.AddInt32(&importedCalls, 1)
				return &burnbridgev1.GetImportedBucketStateResponse{
					Bucket:         "disc-a",
					UdfVolumeLabel: "DISC-A",
					Loaded:         true,
				}, nil
			},
		},
		importedBucketState: map[string]bool{},
	}

	if err := b.syncActiveDiscState(&burnbridgev1.TestUnitReadyResponse{
		Ready:       true,
		VolumeLabel: "DISC-A",
	}); err != nil {
		t.Fatal(err)
	}

	if got := atomic.LoadInt32(&importedCalls); got != 0 {
		t.Fatalf("expected no GetImportedBucketState call when metadata already present, got %d", got)
	}
}

func TestSyncActiveDiscStateConvergesImportedStateWhenBucketNotYetSynced(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "meta.db")
	store, err := meta.NewSqlMeta(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	oldTime := time.Date(2026, 5, 24, 7, 8, 9, 0, time.UTC)
	if err := store.StoreBurnbridgeCommitted(nil, "disc-a", "dir/file.txt", &meta.BurnbridgeCommittedRecord{
		Status:       "imported",
		ETag:         "\"old-etag\"",
		LastModified: oldTime.Format(time.RFC3339Nano),
		Size:         42,
	}); err != nil {
		t.Fatal(err)
	}

	var importedCalls int32
	newTime := time.Date(2026, 5, 25, 7, 8, 9, 0, time.UTC)
	b := &BurnBridge{
		meta:           store,
		activeBucket:   "disc-a",
		volumeLabelRaw: "DISC-A",
		udfLabel:       "DISC-A",
		grpc: testBurnBridgeClient{
			importedBucketStateFn: func(context.Context, *burnbridgev1.GetImportedBucketStateRequest, ...grpc.CallOption) (*burnbridgev1.GetImportedBucketStateResponse, error) {
				atomic.AddInt32(&importedCalls, 1)
				return &burnbridgev1.GetImportedBucketStateResponse{
					Bucket:         "disc-a",
					UdfVolumeLabel: "DISC-A",
					Loaded:         true,
					Objects: []*burnbridgev1.ImportedObjectState{
						{
							ObjectKey:       "dir/file.txt",
							Size:            84,
							Etag:            "\"new-etag\"",
							LastModifiedUtc: newTime.Format(time.RFC3339Nano),
						},
						{
							ObjectKey:       "dir/extra.bin",
							Size:            21,
							Etag:            "\"extra-etag\"",
							LastModifiedUtc: newTime.Format(time.RFC3339Nano),
						},
					},
				}, nil
			},
		},
		importedBucketState: map[string]bool{},
		pendingImportedConvergence: map[string]bool{
			"disc-a": true,
		},
	}

	if err := b.syncActiveDiscState(&burnbridgev1.TestUnitReadyResponse{
		Ready:       true,
		VolumeLabel: "DISC-A",
	}); err != nil {
		t.Fatal(err)
	}

	if got := atomic.LoadInt32(&importedCalls); got != 1 {
		t.Fatalf("expected one GetImportedBucketState call for convergence, got %d", got)
	}

	sum, err := store.GetCommittedObjectSummary("disc-a", "dir/file.txt")
	if err != nil {
		t.Fatal(err)
	}
	if sum.Size != 84 {
		t.Fatalf("expected converged size 84, got %d", sum.Size)
	}
	if sum.ETag != "\"new-etag\"" {
		t.Fatalf("expected converged etag, got %q", sum.ETag)
	}

	extra, err := store.GetCommittedObjectSummary("disc-a", "dir/extra.bin")
	if err != nil {
		t.Fatal(err)
	}
	if extra.Size != 21 {
		t.Fatalf("expected converged extra object size 21, got %d", extra.Size)
	}
}

func TestSyncActiveDiscStateSkipsConvergenceWhenBucketAlreadySynced(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "meta.db")
	store, err := meta.NewSqlMeta(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	if err := store.StoreBurnbridgeCommitted(nil, "disc-a", "dir/file.txt", &meta.BurnbridgeCommittedRecord{
		Status:       "imported",
		ETag:         "\"abc\"",
		LastModified: time.Now().UTC().Format(time.RFC3339Nano),
		Size:         42,
	}); err != nil {
		t.Fatal(err)
	}

	var importedCalls int32
	b := &BurnBridge{
		meta:           store,
		activeBucket:   "disc-a",
		volumeLabelRaw: "DISC-A",
		udfLabel:       "DISC-A",
		grpc: testBurnBridgeClient{
			importedBucketStateFn: func(context.Context, *burnbridgev1.GetImportedBucketStateRequest, ...grpc.CallOption) (*burnbridgev1.GetImportedBucketStateResponse, error) {
				atomic.AddInt32(&importedCalls, 1)
				return &burnbridgev1.GetImportedBucketStateResponse{
					Bucket:         "disc-a",
					UdfVolumeLabel: "DISC-A",
					Loaded:         true,
				}, nil
			},
		},
		importedBucketState: map[string]bool{
			"disc-a": true,
		},
		pendingImportedConvergence: map[string]bool{},
	}

	if err := b.syncActiveDiscState(&burnbridgev1.TestUnitReadyResponse{
		Ready:       true,
		VolumeLabel: "DISC-A",
	}); err != nil {
		t.Fatal(err)
	}

	if got := atomic.LoadInt32(&importedCalls); got != 0 {
		t.Fatalf("expected no GetImportedBucketState call when bucket already synced, got %d", got)
	}
}

func TestEnsureActiveBucketLoadedMarksImportedConvergencePendingAfterMetadataRestore(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "meta.db")
	store, err := meta.NewSqlMeta(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	if err := store.StoreBurnbridgeDiscInfo(&meta.BurnbridgeDiscInfoDocument{
		Bucket:      "disc-a",
		VolumeLabel: "DISC-A",
		UpdatedAt:   time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatal(err)
	}

	b := &BurnBridge{
		meta:                       store,
		activeBucket:               "disc-a",
		importedBucketState:        map[string]bool{},
		pendingImportedConvergence: map[string]bool{},
	}

	if err := b.ensureActiveBucketLoaded(context.Background()); err != nil {
		t.Fatal(err)
	}

	if !b.importedBucketConvergencePending("disc-a") {
		t.Fatal("expected imported convergence to be pending after metadata restore")
	}
}

func TestSyncActiveDiscStatePrefersRecorderImportedBucket(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "meta.db")
	store, err := meta.NewSqlMeta(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	b := &BurnBridge{
		meta: store,
		grpc: testBurnBridgeClient{
			importedBucketStateFn: func(context.Context, *burnbridgev1.GetImportedBucketStateRequest, ...grpc.CallOption) (*burnbridgev1.GetImportedBucketStateResponse, error) {
				return &burnbridgev1.GetImportedBucketStateResponse{
					Bucket:         "disc-imported",
					UdfVolumeLabel: "DISC-IMPORTED",
					Loaded:         true,
				}, nil
			},
		},
		activeBucket:   "job-a73fb713bf9b",
		volumeLabelRaw: "JOB-A73FB713BF9B",
		udfLabel:       "JOB-A73FB713BF9B",
	}

	if err := b.syncActiveDiscState(&burnbridgev1.TestUnitReadyResponse{
		Ready:       true,
		VolumeLabel: "JOB-A73FB713BF9B",
	}); err != nil {
		t.Fatal(err)
	}

	if b.activeBucket != "disc-imported" {
		t.Fatalf("expected imported bucket to win, got %q", b.activeBucket)
	}
	if b.udfLabel != "DISC-IMPORTED" {
		t.Fatalf("expected imported udf label to win, got %q", b.udfLabel)
	}

	binding, err := store.GetBurnbridgeDiscBucketBinding("JOB-A73FB713BF9B")
	if err != nil {
		t.Fatal(err)
	}
	if binding.Bucket != "disc-imported" {
		t.Fatalf("expected persisted binding bucket disc-imported, got %q", binding.Bucket)
	}
}

func TestSyncActiveDiscStateMapsUnboundBlankDiscBucket(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "meta.db")
	store, err := meta.NewSqlMeta(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	if err := store.StoreBurnbridgeCommitted(nil, "disc-old", "file.txt", &meta.BurnbridgeCommittedRecord{
		Status:       "imported",
		ETag:         "\"etag\"",
		LastModified: time.Now().UTC().Format(time.RFC3339Nano),
		Size:         123,
	}); err != nil {
		t.Fatal(err)
	}

	var importedCalls int32
	b := &BurnBridge{
		meta: store,
		grpc: testBurnBridgeClient{
			importedBucketStateFn: func(context.Context, *burnbridgev1.GetImportedBucketStateRequest, ...grpc.CallOption) (*burnbridgev1.GetImportedBucketStateResponse, error) {
				atomic.AddInt32(&importedCalls, 1)
				return &burnbridgev1.GetImportedBucketStateResponse{
					Loaded: true,
					Bucket: "disc-old",
				}, nil
			},
		},
		activeBucket:   "disc-old",
		volumeLabelRaw: "DISC-OLD",
		udfLabel:       "DISC-OLD",
		metaDBPath:     dbPath,
		importedBucketState: map[string]bool{
			"disc-old": true,
		},
		pendingImportedConvergence: map[string]bool{
			"disc-old": true,
		},
	}

	if err := b.syncActiveDiscState(&burnbridgev1.TestUnitReadyResponse{
		Ready:         true,
		VolumeLabel:   "DISC-BLANK",
		WritableState: "Blank",
	}); err != nil {
		t.Fatal(err)
	}

	if got := strings.TrimSpace(b.activeBucket); got != "DISC-BLANK" {
		t.Fatalf("expected blank disc to expose uppercase active bucket DISC-BLANK, got %q", got)
	}
	if got := strings.TrimSpace(b.volumeLabelRaw); got != "DISC-BLANK" {
		t.Fatalf("expected raw volume DISC-BLANK, got %q", got)
	}
	if got := atomic.LoadInt32(&importedCalls); got != 0 {
		t.Fatalf("expected no imported-state probe for unbound blank disc, got %d", got)
	}
	if _, err := store.GetCommittedObjectSummary("disc-old", "file.txt"); !errors.Is(err, meta.ErrNoSuchKey) {
		t.Fatalf("expected stale committed metadata to be cleared, got err=%v", err)
	}
	if strings.TrimSpace(b.lastNoDiscBackupBucket) != "disc-old" {
		t.Fatalf("expected backup marker for disc-old, got %q", b.lastNoDiscBackupBucket)
	}
	if strings.TrimSpace(b.lastNoDiscBackupPath) == "" {
		t.Fatal("expected backup path to be recorded for cleared blank-disc state")
	}
	binding, err := store.GetBurnbridgeDiscBucketBinding("DISC-BLANK")
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.TrimSpace(binding.Bucket); got != "DISC-BLANK" {
		t.Fatalf("expected blank-disc binding bucket DISC-BLANK, got %q", got)
	}
}

func TestSyncActiveDiscStateRetainsBoundBlankDiscBucket(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "meta.db")
	store, err := meta.NewSqlMeta(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	if err := store.StoreBurnbridgeDiscBucketBinding(&meta.BurnbridgeDiscBucketBindingDocument{
		ProbeVolumeLabel: "DISC-BLANK",
		Bucket:           "DISC-BLANK-BOUND",
		UdfVolumeLabel:   "DISC-BLANK",
	}); err != nil {
		t.Fatal(err)
	}

	var importedCalls int32
	b := &BurnBridge{
		meta: store,
		grpc: testBurnBridgeClient{
			importedBucketStateFn: func(context.Context, *burnbridgev1.GetImportedBucketStateRequest, ...grpc.CallOption) (*burnbridgev1.GetImportedBucketStateResponse, error) {
				atomic.AddInt32(&importedCalls, 1)
				return &burnbridgev1.GetImportedBucketStateResponse{
					Loaded: false,
				}, nil
			},
		},
		activeBucket:               "disc-old",
		volumeLabelRaw:             "DISC-OLD",
		udfLabel:                   "DISC-OLD",
		importedBucketState:        map[string]bool{},
		pendingImportedConvergence: map[string]bool{},
	}

	if err := b.syncActiveDiscState(&burnbridgev1.TestUnitReadyResponse{
		Ready:         true,
		VolumeLabel:   "DISC-BLANK",
		WritableState: "Blank",
	}); err != nil {
		t.Fatal(err)
	}

	if got := strings.TrimSpace(b.activeBucket); got != "DISC-BLANK-BOUND" {
		t.Fatalf("expected bound blank disc bucket to be retained, got %q", got)
	}
	if got := strings.TrimSpace(b.volumeLabelRaw); got != "DISC-BLANK" {
		t.Fatalf("expected raw volume DISC-BLANK, got %q", got)
	}
	if got := atomic.LoadInt32(&importedCalls); got != 1 {
		t.Fatalf("expected one imported-state probe for bound blank disc, got %d", got)
	}
}

func TestSyncActiveDiscStateClearsStaleCommittedBlankDiscBinding(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "meta.db")
	store, err := meta.NewSqlMeta(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	if err := store.StoreBurnbridgeDiscBucketBinding(&meta.BurnbridgeDiscBucketBindingDocument{
		ProbeVolumeLabel: "DISC-BLANK",
		Bucket:           "disc-old",
		UdfVolumeLabel:   "DISC-OLD",
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.StoreBurnbridgeDiscBucketBinding(&meta.BurnbridgeDiscBucketBindingDocument{
		ProbeVolumeLabel: "DISC-OLD",
		Bucket:           "disc-old",
		UdfVolumeLabel:   "DISC-OLD",
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.StoreBurnbridgeCommitted(nil, "disc-old", "file.txt", &meta.BurnbridgeCommittedRecord{
		Size:         7,
		ETag:         "\"etag\"",
		LastModified: time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatal(err)
	}

	var importedCalls int32
	b := &BurnBridge{
		meta: store,
		grpc: testBurnBridgeClient{
			importedBucketStateFn: func(context.Context, *burnbridgev1.GetImportedBucketStateRequest, ...grpc.CallOption) (*burnbridgev1.GetImportedBucketStateResponse, error) {
				atomic.AddInt32(&importedCalls, 1)
				return &burnbridgev1.GetImportedBucketStateResponse{Loaded: false}, nil
			},
		},
		activeBucket:               "disc-old",
		volumeLabelRaw:             "DISC-OLD",
		udfLabel:                   "DISC-OLD",
		importedBucketState:        map[string]bool{"disc-old": true},
		pendingImportedConvergence: map[string]bool{"disc-old": true},
	}

	if err := b.syncActiveDiscState(&burnbridgev1.TestUnitReadyResponse{
		Ready:         true,
		VolumeLabel:   "DISC-BLANK",
		WritableState: "Blank",
	}); err != nil {
		t.Fatal(err)
	}

	if got := strings.TrimSpace(b.activeBucket); got != "DISC-BLANK" {
		t.Fatalf("expected blank disc to use uppercase bucket DISC-BLANK after stale binding cleanup, got %q", got)
	}
	if got := atomic.LoadInt32(&importedCalls); got != 0 {
		t.Fatalf("expected no imported-state probe after stale blank binding cleanup, got %d", got)
	}
	if _, err := store.GetCommittedObjectSummary("disc-old", "file.txt"); !errors.Is(err, meta.ErrNoSuchKey) {
		t.Fatalf("expected stale committed metadata to be deleted, got err=%v", err)
	}
	if bindings, err := store.ListBurnbridgeDiscBucketBindings("disc-old"); err != nil {
		t.Fatal(err)
	} else if len(bindings) != 0 {
		t.Fatalf("expected stale disc-old bindings to be deleted, got %d", len(bindings))
	}
	binding, err := store.GetBurnbridgeDiscBucketBinding("DISC-BLANK")
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.TrimSpace(binding.Bucket); got != "DISC-BLANK" {
		t.Fatalf("expected replacement blank-disc binding bucket DISC-BLANK, got %q", got)
	}
}

func TestHandleNoDiscStateBacksUpAndClearsActiveBucket(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "meta.db")
	store, err := meta.NewSqlMeta(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	if err := store.StoreBurnbridgeCommitted(nil, "disc-a", "file.txt", &meta.BurnbridgeCommittedRecord{
		Size:         7,
		ETag:         "\"etag\"",
		LastModified: time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatal(err)
	}

	b := &BurnBridge{
		meta:           store,
		grpc:           testBurnBridgeClient{},
		activeBucket:   "disc-a",
		volumeLabelRaw: "DISC-A",
		udfLabel:       "DISC-A",
		metaDBPath:     dbPath,
	}

	if err := b.handleNoDiscState(); err != nil {
		t.Fatal(err)
	}
	if b.activeBucket != "" {
		t.Fatalf("expected active bucket cleared, got %q", b.activeBucket)
	}
	if _, err := store.GetCommittedObjectSummary("disc-a", "file.txt"); err == nil {
		t.Fatal("expected committed object metadata to be cleared after no-disc state")
	}
	if bindings, err := store.ListBurnbridgeDiscBucketBindings("disc-a"); err != nil {
		t.Fatal(err)
	} else if len(bindings) != 0 {
		t.Fatalf("expected disc-a bindings to be cleared after no-disc state, got %d", len(bindings))
	}
	if sessions, err := store.ListBurnUploadSessions("disc-a", "file.txt"); err != nil {
		t.Fatal(err)
	} else if len(sessions) != 0 {
		t.Fatalf("expected upload sessions to be cleared after no-disc state, got %d", len(sessions))
	}
	if parts, err := store.ListBurnUploadParts("disc-a", "file.txt", burnbridgeImplicitSingleUploadID); err != nil {
		t.Fatal(err)
	} else if len(parts) != 0 {
		t.Fatalf("expected upload parts to be cleared after no-disc state, got %d", len(parts))
	}
	if segments, err := store.ListBurnObjectSegments("disc-a", "file.txt"); err != nil {
		t.Fatal(err)
	} else if len(segments) != 0 {
		t.Fatalf("expected object segments to be cleared after no-disc state, got %d", len(segments))
	}
	if strings.TrimSpace(b.lastNoDiscBackupPath) == "" {
		t.Fatal("expected no-disc backup path to be recorded")
	}
	if _, err := os.Stat(b.lastNoDiscBackupPath); err != nil {
		t.Fatalf("expected no-disc backup file to exist: %v", err)
	}
}

func TestMaybeRestoreNoDiscBackupRestoresSameDiscBucket(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "meta.db")
	store, err := meta.NewSqlMeta(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	if err := store.StoreBurnbridgeCommitted(nil, "disc-a", "file.txt", &meta.BurnbridgeCommittedRecord{
		Size:         7,
		ETag:         "\"etag\"",
		LastModified: time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatal(err)
	}
	const segmentCount = 160
	for i := 0; i < segmentCount; i++ {
		if err := store.UpsertBurnObjectSegment(
			"disc-a",
			"file.txt",
			"DISC-A",
			i,
			int64(i*1024),
			1024,
			fmt.Sprintf("%032d", i),
			meta.BurnSegmentSucceeded,
			[]meta.BurnDiscExtent{{DiscAddress: fmt.Sprintf("%d", 1000+i), FileSize: 1024}},
		); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.StoreBurnbridgeDiscInfo(&meta.BurnbridgeDiscInfoDocument{
		Bucket:                        "disc-a",
		VolumeLabel:                   "DISC-A",
		UpdatedAt:                     time.Now().UTC().Format(time.RFC3339Nano),
		DiscSerialNumberHex:           "SERIAL-A",
		TotalBlocks:                   1000,
		FreeBlocks:                    400,
		RecordableCapacityBlocks:      400,
		TrackNextWritableAddress:      600,
		TrackNextWritableAddressValid: true,
		WritableState:                 "Appendable",
	}); err != nil {
		t.Fatal(err)
	}

	b := &BurnBridge{
		meta:           store,
		grpc:           testBurnBridgeClient{},
		activeBucket:   "disc-a",
		volumeLabelRaw: "DISC-A",
		udfLabel:       "DISC-A",
		metaDBPath:     dbPath,
	}

	if err := b.handleNoDiscState(); err != nil {
		t.Fatal(err)
	}

	b.activeBucket = "DISC-A"
	b.volumeLabelRaw = "DISC-A"
	b.lastDiscSerialHex = "SERIAL-A"
	b.lastReadyVolumeLabel = "DISC-A"
	if err := b.maybeRestoreNoDiscBackup(&burnbridgev1.TestUnitReadyResponse{
		Ready:               true,
		VolumeLabel:         "DISC-A",
		DiscSerialNumberHex: "SERIAL-A",
	}); err != nil {
		t.Fatal(err)
	}

	sum, err := store.GetCommittedObjectSummary("disc-a", "file.txt")
	if err != nil {
		t.Fatal(err)
	}
	if sum.Size != 7 {
		t.Fatalf("expected restored size 7, got %d", sum.Size)
	}
	segments, err := store.ListBurnObjectSegments("disc-a", "file.txt")
	if err != nil {
		t.Fatal(err)
	}
	if len(segments) != segmentCount {
		t.Fatalf("expected %d restored segments, got %d", segmentCount, len(segments))
	}
}

func TestMaybeRestoreNoDiscBackupSkipsDifferentDiscGeometry(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "meta.db")
	store, err := meta.NewSqlMeta(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	if err := store.StoreBurnbridgeCommitted(nil, "disc-a", "file.txt", &meta.BurnbridgeCommittedRecord{
		Size:         7,
		ETag:         "\"etag\"",
		LastModified: time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.StoreBurnbridgeDiscInfo(&meta.BurnbridgeDiscInfoDocument{
		Bucket:                        "disc-a",
		VolumeLabel:                   "DISC-A",
		UpdatedAt:                     time.Now().UTC().Format(time.RFC3339Nano),
		DiscSerialNumberHex:           "SERIAL-A",
		TotalBlocks:                   1000,
		FreeBlocks:                    400,
		RecordableCapacityBlocks:      400,
		TrackNextWritableAddress:      600,
		TrackNextWritableAddressValid: true,
		WritableState:                 "Appendable",
	}); err != nil {
		t.Fatal(err)
	}

	b := &BurnBridge{
		meta:           store,
		grpc:           testBurnBridgeClient{},
		activeBucket:   "disc-a",
		volumeLabelRaw: "DISC-A",
		udfLabel:       "DISC-A",
		metaDBPath:     dbPath,
	}

	if err := b.handleNoDiscState(); err != nil {
		t.Fatal(err)
	}

	b.activeBucket = "DISC-A"
	b.volumeLabelRaw = "DISC-A"
	b.lastDiscSerialHex = "SERIAL-A"
	b.lastReadyVolumeLabel = "DISC-A"
	if err := b.maybeRestoreNoDiscBackup(&burnbridgev1.TestUnitReadyResponse{
		Ready:                         true,
		VolumeLabel:                   "DISC-A",
		DiscSerialNumberHex:           "SERIAL-A",
		TotalBlocks:                   1000,
		FreeBlocks:                    200,
		RecordableCapacityBlocks:      200,
		TrackNextWritableAddress:      800,
		TrackNextWritableAddressValid: true,
		WritableState:                 "Appendable",
	}); err != nil {
		t.Fatal(err)
	}

	if _, err := store.GetCommittedObjectSummary("disc-a", "file.txt"); err == nil {
		t.Fatal("expected mismatched disc geometry to skip backup restore")
	}
}

func TestInvokeFinalizeLayoutReusesOnlyMatchingControlRequest(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "meta.db")
	store, err := meta.NewSqlMeta(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	payload, err := json.Marshal(meta.BurnbridgeFinalizeLayoutDocument{
		Bucket:         "bucket1",
		RequestID:      "550e8400-e29b-41d4-a716-446655440000",
		RequestTime:    1780622225123,
		RecorderStatus: "finalized",
		CompletedAtUtc: time.Now().UTC().Format(time.RFC3339Nano),
		GrpcOK:         true,
		GrpcCode:       "OK",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.StoreBurnbridgeFinalizeLayoutJSON("bucket1", meta.BurnbridgeFinalizeLayoutObjectKey, payload); err != nil {
		t.Fatal(err)
	}

	calls := 0
	b := &BurnBridge{
		meta: store,
		grpc: testBurnBridgeClient{
			finalizeFn: func(context.Context, *burnbridgev1.FinalizeLayoutRequest, ...grpc.CallOption) (*burnbridgev1.FinalizeLayoutResponse, error) {
				calls++
				return &burnbridgev1.FinalizeLayoutResponse{
					Bucket: "bucket1",
					Status: "finalized",
				}, nil
			},
		},
		activeBucket: "bucket1",
		udfLabel:     "BUCKET1",
	}

	req := testControlRequest(t, "finalize-layout", 1780622225123, "550e8400-e29b-41d4-a716-446655440000")
	got, err := b.invokeFinalizeLayoutAgainstRecorder(context.Background(), "bucket1", req)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(payload) {
		t.Fatalf("expected cached finalize transcript to be reused")
	}
	if calls != 0 {
		t.Fatalf("expected no grpc finalize call, got %d", calls)
	}

	req = testControlRequest(t, "finalize-layout", 1780622225999, "550e8400-e29b-41d4-a716-446655440001")
	got, err = b.invokeFinalizeLayoutAgainstRecorder(context.Background(), "bucket1", req)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) == string(payload) {
		t.Fatalf("expected a new control request to bypass the cached transcript")
	}
	if calls != 1 {
		t.Fatalf("expected one grpc finalize call for a new request, got %d", calls)
	}
}

func testControlKey(action string) string {
	return "v1/" + action
}

func testEnsureDriveControlBucket(t *testing.T, b *BurnBridge) string {
	t.Helper()
	controlBucket, err := b.ensureDriveControlBucket(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return controlBucket
}

func testDriveInfo(serial string) *burnbridgev1.GetDiscInfoResponse {
	return &burnbridgev1.GetDiscInfoResponse{
		Drive: &burnbridgev1.OpticalDriveIdentity{
			SerialNumber: serial,
		},
	}
}

func testControlRequest(t *testing.T, action string, requestTime int64, requestID string) *burnbridgeControlRequest {
	t.Helper()
	return &burnbridgeControlRequest{
		Action:      burnbridgeControlAction(action),
		RequestTime: requestTime,
		RequestID:   requestID,
		Key:         fmt.Sprintf("v1/%s", action),
	}
}

func TestDiscInfoGetObjectRefreshesRuntimeAndCarriesFinalizeState(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "meta.db")
	store, err := meta.NewSqlMeta(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	finalizePayload, err := json.Marshal(meta.BurnbridgeFinalizeLayoutDocument{
		Bucket:         "bucket1",
		RecorderStatus: "finalized",
		CompletedAtUtc: "2026-06-04T14:50:52.3067628Z",
		CloseDisc:      false,
		GrpcOK:         true,
		GrpcCode:       "OK",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.StoreBurnbridgeFinalizeLayoutJSON("bucket1", meta.BurnbridgeFinalizeLayoutObjectKey, finalizePayload); err != nil {
		t.Fatal(err)
	}

	b := &BurnBridge{
		meta:           store,
		activeBucket:   "bucket1",
		volumeLabelRaw: "DISC001",
		udfLabel:       "DISC001",
		grpc: testBurnBridgeClient{
			testUnitReadyFn: func(context.Context, *burnbridgev1.TestUnitReadyRequest, ...grpc.CallOption) (*burnbridgev1.TestUnitReadyResponse, error) {
				return &burnbridgev1.TestUnitReadyResponse{
					Ready:                         true,
					VolumeLabel:                   "DISC001",
					DiscSerialNumberHex:           "SER-001",
					MediaType:                     "BD-R",
					TotalCapacityBytes:            1000,
					FreeCapacityBytes:             400,
					UsedCapacityBytes:             600,
					WritableCapacityBytes:         300,
					FinalizeReserveBytes:          100,
					BlockSizeBytes:                2048,
					TotalBlocks:                   320,
					FreeBlocks:                    0,
					RecordableCapacityBlocks:      0,
					TrackNextWritableAddress:      0,
					TrackNextWritableAddressValid: false,
					WritableState:                 "Appendable",
				}, nil
			},
			getDiscInfoFn: func(_ context.Context, req *burnbridgev1.GetDiscInfoRequest, _ ...grpc.CallOption) (*burnbridgev1.GetDiscInfoResponse, error) {
				disc := &burnbridgev1.OpticalDiscInfo{
					ProfileName:                   "BD-R",
					DiscStatusName:                "incomplete/appendable",
					DiscSerialNumberHex:           "SER-001",
					BlockSizeBytes:                2048,
					TotalBlocks:                   48878592,
					FreeBlocks:                    1773184,
					RecordableCapacityBlocks:      1773184,
					TrackNextWritableAddress:      47105408,
					TrackNextWritableAddressValid: true,
					WritableState:                 "Appendable",
					MediaCapacity:                 100103356416,
					MediaFreeSpace:                3631470592,
					MediaUsedSpace:                96471885824,
				}
				if req.GetIncludeSessionDiscId() {
					disc.SessionDiscId = &burnbridgev1.SessionDiscId{
						IsFinalized: true,
						TempDiscId:  "DISC001",
					}
				}
				return &burnbridgev1.GetDiscInfoResponse{
					Drive: &burnbridgev1.OpticalDriveIdentity{
						SerialNumber: "DRIVE-SERIAL-DISCINFO-001",
					},
					Disc: disc,
				}, nil
			},
		},
	}
	controlBucket, err := b.ensureDriveControlBucket(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	out, err := b.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: ptr(controlBucket),
		Key:    ptr(testControlKey("disc-info")),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = out.Body.Close() }()

	raw, err := io.ReadAll(out.Body)
	if err != nil {
		t.Fatal(err)
	}

	var envelope burnbridgeControlEnvelope
	if err := json.Unmarshal(raw, &envelope); err != nil {
		t.Fatal(err)
	}
	if !envelope.Ok {
		t.Fatalf("expected ok=true, got false with error %+v", envelope.Error)
	}
	if envelope.Action != "disc-info" {
		t.Fatalf("expected action disc-info, got %q", envelope.Action)
	}
	if strings.TrimSpace(envelope.RequestID) == "" {
		t.Fatal("expected request id to be populated")
	}
	data, err := json.Marshal(envelope.Data)
	if err != nil {
		t.Fatal(err)
	}
	var doc meta.BurnbridgeDiscInfoDocument
	if err := json.Unmarshal(data, &doc); err != nil {
		t.Fatal(err)
	}

	if doc.WritableState != "Appendable" {
		t.Fatalf("expected writableState Appendable, got %q", doc.WritableState)
	}
	if doc.DiscStatusName != "incomplete/appendable" {
		t.Fatalf("expected discStatusName incomplete/appendable, got %q", doc.DiscStatusName)
	}
	if doc.SessionIsFinalized {
		t.Fatal("expected lightweight disc-info to avoid session finalized probing")
	}
	if doc.LayoutStatus != "" {
		t.Fatalf("expected lightweight disc-info to omit layoutStatus, got %q", doc.LayoutStatus)
	}
	if doc.LayoutCompletedAtUtc != "" {
		t.Fatal("expected lightweight disc-info to omit layoutCompletedAtUtc")
	}
	if doc.TotalBlocks != 48878592 {
		t.Fatalf("expected refreshed totalBlocks 48878592, got %d", doc.TotalBlocks)
	}
	if !doc.TrackNextWritableAddressValid {
		t.Fatal("expected trackNextWritableAddressValid true")
	}
}

func TestDriveInfoVirtualControlBucketIsVisibleAndReadable(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "meta.db")
	store, err := meta.NewSqlMeta(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	b := &BurnBridge{
		meta: store,
		grpc: testBurnBridgeClient{
			getDiscInfoFn: func(_ context.Context, req *burnbridgev1.GetDiscInfoRequest, _ ...grpc.CallOption) (*burnbridgev1.GetDiscInfoResponse, error) {
				if !req.GetIncludeDriveIdentity() {
					t.Fatal("expected IncludeDriveIdentity=true")
				}
				return &burnbridgev1.GetDiscInfoResponse{
					Drive: &burnbridgev1.OpticalDriveIdentity{
						VendorId:        "HL-DT-ST",
						ProductId:       "BD-RE BU40N",
						ProductRevision: "1.04",
						SerialNumber:    "DRIVE-SERIAL-001",
						IsMmcUnit:       true,
					},
				}, nil
			},
		},
	}

	controlBucket, err := b.ensureDriveControlBucket(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if controlBucket != "DRIVE-SERIAL-001" {
		t.Fatalf("expected control bucket DRIVE-SERIAL-001, got %q", controlBucket)
	}

	if _, err := b.HeadBucket(context.Background(), &s3.HeadBucketInput{Bucket: ptr(controlBucket)}); err != nil {
		t.Fatalf("HeadBucket control bucket returned error: %v", err)
	}

	list, err := b.ListBuckets(context.Background(), s3response.ListBucketsInput{
		Owner:      "owner1",
		MaxBuckets: 100,
		IsAdmin:    true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(list.Buckets.Bucket) != 1 || list.Buckets.Bucket[0].Name != controlBucket {
		t.Fatalf("expected only control bucket in list, got %+v", list.Buckets.Bucket)
	}

	out, err := b.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: ptr(controlBucket),
		Key:    ptr(testControlKey("drive-info")),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = out.Body.Close() }()

	raw, err := io.ReadAll(out.Body)
	if err != nil {
		t.Fatal(err)
	}
	var envelope burnbridgeControlEnvelope
	if err := json.Unmarshal(raw, &envelope); err != nil {
		t.Fatal(err)
	}
	if !envelope.Ok {
		t.Fatalf("expected ok=true, got false with error %+v", envelope.Error)
	}
	if envelope.Action != "drive-info" {
		t.Fatalf("expected action drive-info, got %q", envelope.Action)
	}
	data, err := json.Marshal(envelope.Data)
	if err != nil {
		t.Fatal(err)
	}
	var doc meta.BurnbridgeDriveInfoDocument
	if err := json.Unmarshal(data, &doc); err != nil {
		t.Fatal(err)
	}
	if doc.ControlBucket != controlBucket {
		t.Fatalf("expected controlBucket %q, got %q", controlBucket, doc.ControlBucket)
	}
	if doc.SerialNumber != "DRIVE-SERIAL-001" {
		t.Fatalf("unexpected serial number %q", doc.SerialNumber)
	}
	if !doc.IsMMCUnit {
		t.Fatal("expected isMmcUnit true")
	}
}

func TestDiscInfoGetObjectSuppressesStaleFinalizeStateOnMismatchedDisc(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "meta.db")
	store, err := meta.NewSqlMeta(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	finalizePayload, err := json.Marshal(meta.BurnbridgeFinalizeLayoutDocument{
		Bucket:         "bucket1",
		RecorderStatus: "finalized",
		CompletedAtUtc: "2026-06-04T14:50:52.3067628Z",
		CloseDisc:      false,
		GrpcOK:         true,
		GrpcCode:       "OK",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.StoreBurnbridgeFinalizeLayoutJSON("bucket1", meta.BurnbridgeFinalizeLayoutObjectKey, finalizePayload); err != nil {
		t.Fatal(err)
	}

	b := &BurnBridge{
		meta:         store,
		activeBucket: "bucket1",
		udfLabel:     "BUCKET1",
		grpc: testBurnBridgeClient{
			testUnitReadyFn: func(context.Context, *burnbridgev1.TestUnitReadyRequest, ...grpc.CallOption) (*burnbridgev1.TestUnitReadyResponse, error) {
				return &burnbridgev1.TestUnitReadyResponse{
					Ready:                         true,
					VolumeLabel:                   "DISC002",
					DiscSerialNumberHex:           "SER-002",
					MediaType:                     "BD-R",
					TotalCapacityBytes:            1000,
					FreeCapacityBytes:             240,
					UsedCapacityBytes:             760,
					WritableCapacityBytes:         140,
					FinalizeReserveBytes:          100,
					BlockSizeBytes:                2048,
					TotalBlocks:                   320,
					FreeBlocks:                    70,
					RecordableCapacityBlocks:      70,
					TrackNextWritableAddress:      250,
					TrackNextWritableAddressValid: true,
					WritableState:                 "Appendable",
				}, nil
			},
			getDiscInfoFn: func(context.Context, *burnbridgev1.GetDiscInfoRequest, ...grpc.CallOption) (*burnbridgev1.GetDiscInfoResponse, error) {
				return &burnbridgev1.GetDiscInfoResponse{
					Drive: &burnbridgev1.OpticalDriveIdentity{
						SerialNumber: "DRIVE-SERIAL-DISCINFO-002",
					},
					Disc: &burnbridgev1.OpticalDiscInfo{
						ProfileName:                   "BD-R",
						DiscStatusName:                "incomplete/appendable",
						DiscSerialNumberHex:           "SER-002",
						BlockSizeBytes:                2048,
						TotalBlocks:                   320,
						FreeBlocks:                    70,
						RecordableCapacityBlocks:      70,
						TrackNextWritableAddress:      250,
						TrackNextWritableAddressValid: true,
						WritableState:                 "Appendable",
						MediaCapacity:                 1000,
						MediaFreeSpace:                240,
						MediaUsedSpace:                760,
					},
				}, nil
			},
			importedBucketStateFn: func(context.Context, *burnbridgev1.GetImportedBucketStateRequest, ...grpc.CallOption) (*burnbridgev1.GetImportedBucketStateResponse, error) {
				return &burnbridgev1.GetImportedBucketStateResponse{
					Loaded: false,
				}, nil
			},
		},
	}
	controlBucket := testEnsureDriveControlBucket(t, b)

	out, err := b.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: ptr(controlBucket),
		Key:    ptr(testControlKey("disc-info")),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = out.Body.Close() }()

	raw, err := io.ReadAll(out.Body)
	if err != nil {
		t.Fatal(err)
	}

	var envelope burnbridgeControlEnvelope
	if err := json.Unmarshal(raw, &envelope); err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(envelope.Data)
	if err != nil {
		t.Fatal(err)
	}
	var doc meta.BurnbridgeDiscInfoDocument
	if err := json.Unmarshal(data, &doc); err != nil {
		t.Fatal(err)
	}
	if doc.LayoutStatus != "" {
		t.Fatalf("expected stale layoutStatus to be suppressed, got %q", doc.LayoutStatus)
	}
	if doc.LayoutCompletedAtUtc != "" {
		t.Fatalf("expected stale layoutCompletedAtUtc to be suppressed, got %q", doc.LayoutCompletedAtUtc)
	}
	if doc.WritableState != "Appendable" {
		t.Fatalf("expected writableState Appendable, got %q", doc.WritableState)
	}
	if doc.VolumeLabel != "bucket1" {
		t.Fatalf("expected lightweight disc-info to preserve active volume label bucket1, got %q", doc.VolumeLabel)
	}
	if _, err := store.GetBurnbridgeDiscInfoJSON("bucket1"); err == nil {
		t.Fatal("expected mismatched-disc disc-info request to avoid overwriting bucket1 disc info cache")
	}
}

func TestLoadOrFinalizeLayoutTranscriptSingleflight(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "meta.db")
	store, err := meta.NewSqlMeta(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	var calls int32
	start := make(chan struct{})
	b := &BurnBridge{
		meta: store,
		grpc: testBurnBridgeClient{
			finalizeFn: func(context.Context, *burnbridgev1.FinalizeLayoutRequest, ...grpc.CallOption) (*burnbridgev1.FinalizeLayoutResponse, error) {
				atomic.AddInt32(&calls, 1)
				<-start
				return &burnbridgev1.FinalizeLayoutResponse{
					Bucket: "bucket1",
					Status: "finalized",
				}, nil
			},
		},
		activeBucket: "bucket1",
		udfLabel:     "BUCKET1",
	}

	const workers = 4
	results := make([][]byte, workers)
	errs := make([]error, workers)
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func(idx int) {
			defer wg.Done()
			req := testControlRequest(t, "finalize-layout", 1780622225123, "550e8400-e29b-41d4-a716-446655440000")
			results[idx], errs[idx] = b.loadOrFinalizeLayoutTranscript(context.Background(), "bucket1", req)
		}(i)
	}

	time.Sleep(100 * time.Millisecond)
	close(start)
	wg.Wait()

	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("expected one grpc finalize call, got %d", got)
	}
	for i, err := range errs {
		if err != nil {
			t.Fatalf("worker %d returned error: %v", i, err)
		}
		if len(results[i]) == 0 {
			t.Fatalf("worker %d returned empty transcript", i)
		}
	}
}

func TestCloseDiscGetObjectInvokesFinalizeWithCloseDisc(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "meta.db")
	store, err := meta.NewSqlMeta(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	var gotCloseDisc bool
	b := &BurnBridge{
		meta: store,
		grpc: testBurnBridgeClient{
			getDiscInfoFn: func(context.Context, *burnbridgev1.GetDiscInfoRequest, ...grpc.CallOption) (*burnbridgev1.GetDiscInfoResponse, error) {
				return testDriveInfo("DRIVE-SERIAL-CLOSEDISC-001"), nil
			},
			finalizeFn: func(_ctx context.Context, req *burnbridgev1.FinalizeLayoutRequest, _opts ...grpc.CallOption) (*burnbridgev1.FinalizeLayoutResponse, error) {
				gotCloseDisc = req.GetCloseDisc()
				return &burnbridgev1.FinalizeLayoutResponse{
					Bucket:  "bucket1",
					Status:  "finalized",
					Message: "disc closed",
				}, nil
			},
		},
		activeBucket: "bucket1",
		udfLabel:     "BUCKET1",
	}
	controlBucket := testEnsureDriveControlBucket(t, b)

	out, err := b.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: ptr(controlBucket),
		Key:    ptr(testControlKey("close-disc")),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = out.Body.Close() }()

	if !gotCloseDisc {
		t.Fatal("expected CloseDisc object to invoke FinalizeLayout with closeDisc=true")
	}

	raw, err := io.ReadAll(out.Body)
	if err != nil {
		t.Fatal(err)
	}

	var envelope burnbridgeControlEnvelope
	if err := json.Unmarshal(raw, &envelope); err != nil {
		t.Fatal(err)
	}
	if !envelope.Ok {
		t.Fatalf("expected ok=true, got false with error %+v", envelope.Error)
	}
	if envelope.Action != "close-disc" {
		t.Fatalf("expected action close-disc, got %q", envelope.Action)
	}
	data, err := json.Marshal(envelope.Data)
	if err != nil {
		t.Fatal(err)
	}
	var doc meta.BurnbridgeFinalizeLayoutDocument
	if err := json.Unmarshal(data, &doc); err != nil {
		t.Fatal(err)
	}
	if !doc.CloseDisc {
		t.Fatal("expected persisted transcript closeDisc=true")
	}
	if doc.RecorderStatus != "finalized" {
		t.Fatalf("expected recorderStatus finalized, got %q", doc.RecorderStatus)
	}
}

func TestCloseDiscGetObjectFailsWhenStagedSmallObjectCannotFlush(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "meta.db")
	store, err := meta.NewSqlMeta(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	const bucket = "bucket1"
	if err := store.StoreBurnbridgeCommitted(nil, bucket, "staged.txt", &meta.BurnbridgeCommittedRecord{
		JobID:        "staged-small-object",
		Status:       "staged",
		Size:         5,
		ETag:         "\"etag\"",
		LastModified: time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatal(err)
	}
	stage := &smallObjectStageRecord{
		Bucket:      bucket,
		Key:         "staged.txt",
		PackPath:    filepath.Join(t.TempDir(), "missing-stage.pack"),
		LogicalSize: 5,
		CreatedAt:   time.Now().UTC().Format(time.RFC3339Nano),
	}
	raw, err := json.Marshal(stage)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.StoreAttribute(nil, bucket, "staged.txt", burnbridgeSmallObjectStageAttr, raw); err != nil {
		t.Fatal(err)
	}

	var finalizeCalled bool
	b := &BurnBridge{
		meta:         store,
		activeBucket: bucket,
		udfLabel:     "BUCKET1",
		smallBatch:   newSmallObjectBatcher(nil, 1024, 8, time.Minute),
		grpc: testBurnBridgeClient{
			getDiscInfoFn: func(context.Context, *burnbridgev1.GetDiscInfoRequest, ...grpc.CallOption) (*burnbridgev1.GetDiscInfoResponse, error) {
				return testDriveInfo("DRIVE-SERIAL-CLOSEDISC-STAGED"), nil
			},
			testUnitReadyFn: func(context.Context, *burnbridgev1.TestUnitReadyRequest, ...grpc.CallOption) (*burnbridgev1.TestUnitReadyResponse, error) {
				return &burnbridgev1.TestUnitReadyResponse{Ready: true, VolumeLabel: bucket, WritableState: "Appendable"}, nil
			},
			finalizeFn: func(context.Context, *burnbridgev1.FinalizeLayoutRequest, ...grpc.CallOption) (*burnbridgev1.FinalizeLayoutResponse, error) {
				finalizeCalled = true
				return nil, nil
			},
		},
	}
	controlBucket := testEnsureDriveControlBucket(t, b)

	_, err = b.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: ptr(controlBucket),
		Key:    ptr(testControlKey("close-disc")),
	})
	if err == nil {
		t.Fatal("expected close-disc to fail while staged small objects cannot flush")
	}
	if !strings.Contains(err.Error(), "staged small objects") {
		t.Fatalf("expected staged small object error, got %v", err)
	}
	if finalizeCalled {
		t.Fatal("FinalizeLayout should not be called when staged small objects cannot flush")
	}
}

func TestMediaInsertedControlGetObjectBypassesMissingBucket(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "meta.db")
	store, err := meta.NewSqlMeta(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	var gotAction burnbridgev1.MediaChangeAction
	b := &BurnBridge{
		meta: store,
		grpc: testBurnBridgeClient{
			getDiscInfoFn: func(context.Context, *burnbridgev1.GetDiscInfoRequest, ...grpc.CallOption) (*burnbridgev1.GetDiscInfoResponse, error) {
				return testDriveInfo("DRIVE-SERIAL-MEDIA-001"), nil
			},
			handleMediaChangeFn: func(_ctx context.Context, req *burnbridgev1.HandleMediaChangeRequest, _opts ...grpc.CallOption) (*burnbridgev1.HandleMediaChangeResponse, error) {
				gotAction = req.GetAction()
				return &burnbridgev1.HandleMediaChangeResponse{
					Status:         "inserted",
					Message:        "manual insert handled",
					ImportedBucket: "inserted-bucket",
					Snapshot: &burnbridgev1.TestUnitReadyResponse{
						Ready:         true,
						VolumeLabel:   "INSERTED-BUCKET",
						WritableState: "Appendable",
					},
				}, nil
			},
		},
	}
	controlBucket := testEnsureDriveControlBucket(t, b)

	out, err := b.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: ptr(controlBucket),
		Key:    ptr(testControlKey("media-inserted")),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = out.Body.Close() }()

	if gotAction != burnbridgev1.MediaChangeAction_MEDIA_CHANGE_ACTION_INSERTED {
		t.Fatalf("expected media inserted action, got %v", gotAction)
	}

	raw, err := io.ReadAll(out.Body)
	if err != nil {
		t.Fatal(err)
	}
	var envelope burnbridgeControlEnvelope
	if err := json.Unmarshal(raw, &envelope); err != nil {
		t.Fatal(err)
	}
	if !envelope.Ok {
		t.Fatalf("expected ok=true, got false with error %+v", envelope.Error)
	}
	if envelope.Action != "media-inserted" {
		t.Fatalf("expected action media-inserted, got %q", envelope.Action)
	}
	data, err := json.Marshal(envelope.Data)
	if err != nil {
		t.Fatal(err)
	}
	var doc meta.BurnbridgeMediaChangeDocument
	if err := json.Unmarshal(data, &doc); err != nil {
		t.Fatal(err)
	}
	if doc.ImportedBucket != "inserted-bucket" {
		t.Fatalf("expected importedBucket inserted-bucket, got %q", doc.ImportedBucket)
	}
	if !doc.Ready {
		t.Fatal("expected ready snapshot in media-inserted transcript")
	}
}

func TestCloseDiscControlStillRequiresExistingBucket(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "meta.db")
	store, err := meta.NewSqlMeta(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	b := &BurnBridge{
		meta: store,
		grpc: testBurnBridgeClient{
			getDiscInfoFn: func(context.Context, *burnbridgev1.GetDiscInfoRequest, ...grpc.CallOption) (*burnbridgev1.GetDiscInfoResponse, error) {
				return testDriveInfo("DRIVE-SERIAL-CLOSEDISC-002"), nil
			},
			finalizeFn: func(context.Context, *burnbridgev1.FinalizeLayoutRequest, ...grpc.CallOption) (*burnbridgev1.FinalizeLayoutResponse, error) {
				t.Fatal("FinalizeLayout should not be called for a missing bucket")
				return nil, nil
			},
		},
	}
	controlBucket := testEnsureDriveControlBucket(t, b)

	_, err = b.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: ptr(controlBucket),
		Key:    ptr(testControlKey("close-disc")),
	})
	if err == nil {
		t.Fatal("expected missing bucket error")
	}
	if !strings.Contains(err.Error(), "NoSuchBucket") {
		t.Fatalf("expected NoSuchBucket error, got %v", err)
	}
}

func TestCloseDiscViaDriveControlBucketTargetsActiveBucket(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "meta.db")
	store, err := meta.NewSqlMeta(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	var gotBucket string
	b := &BurnBridge{
		meta:         store,
		activeBucket: "bucket1",
		udfLabel:     "BUCKET1",
		grpc: testBurnBridgeClient{
			getDiscInfoFn: func(context.Context, *burnbridgev1.GetDiscInfoRequest, ...grpc.CallOption) (*burnbridgev1.GetDiscInfoResponse, error) {
				return &burnbridgev1.GetDiscInfoResponse{
					Drive: &burnbridgev1.OpticalDriveIdentity{
						SerialNumber: "DRIVE-SERIAL-003",
					},
				}, nil
			},
			finalizeFn: func(_ctx context.Context, req *burnbridgev1.FinalizeLayoutRequest, _opts ...grpc.CallOption) (*burnbridgev1.FinalizeLayoutResponse, error) {
				gotBucket = req.GetBucket()
				return &burnbridgev1.FinalizeLayoutResponse{
					Bucket:  req.GetBucket(),
					Status:  "closed",
					Message: "disc closed",
				}, nil
			},
		},
	}

	if err := store.StoreBurnbridgeDiscInfo(&meta.BurnbridgeDiscInfoDocument{
		Bucket:      "bucket1",
		VolumeLabel: "BUCKET1",
		UpdatedAt:   time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatal(err)
	}
	controlBucket, err := b.ensureDriveControlBucket(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	out, err := b.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: ptr(controlBucket),
		Key:    ptr(testControlKey("close-disc")),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = out.Body.Close() }()

	if gotBucket != "bucket1" {
		t.Fatalf("expected FinalizeLayout target bucket1, got %q", gotBucket)
	}
	raw, err := io.ReadAll(out.Body)
	if err != nil {
		t.Fatal(err)
	}
	var envelope burnbridgeControlEnvelope
	if err := json.Unmarshal(raw, &envelope); err != nil {
		t.Fatal(err)
	}
	if envelope.Bucket != controlBucket {
		t.Fatalf("expected response bucket to remain control bucket %q, got %q", controlBucket, envelope.Bucket)
	}
}

func TestTrayOpenControlGetObjectBypassesMissingBucket(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "meta.db")
	store, err := meta.NewSqlMeta(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	var gotAction burnbridgev1.TrayAction
	b := &BurnBridge{
		meta: store,
		grpc: testBurnBridgeClient{
			getDiscInfoFn: func(context.Context, *burnbridgev1.GetDiscInfoRequest, ...grpc.CallOption) (*burnbridgev1.GetDiscInfoResponse, error) {
				return testDriveInfo("DRIVE-SERIAL-TRAY-001"), nil
			},
			handleTrayFn: func(_ctx context.Context, req *burnbridgev1.HandleTrayRequest, _opts ...grpc.CallOption) (*burnbridgev1.HandleTrayResponse, error) {
				gotAction = req.GetAction()
				return &burnbridgev1.HandleTrayResponse{
					Status:  "opened",
					Message: "manual tray open handled",
				}, nil
			},
		},
	}
	controlBucket := testEnsureDriveControlBucket(t, b)

	out, err := b.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: ptr(controlBucket),
		Key:    ptr(testControlKey("tray-open")),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = out.Body.Close() }()

	if gotAction != burnbridgev1.TrayAction_TRAY_ACTION_OPEN {
		t.Fatalf("expected tray open action, got %v", gotAction)
	}

	raw, err := io.ReadAll(out.Body)
	if err != nil {
		t.Fatal(err)
	}
	var envelope burnbridgeControlEnvelope
	if err := json.Unmarshal(raw, &envelope); err != nil {
		t.Fatal(err)
	}
	if !envelope.Ok {
		t.Fatalf("expected ok=true, got false with error %+v", envelope.Error)
	}
	if envelope.Action != "tray-open" {
		t.Fatalf("expected action tray-open, got %q", envelope.Action)
	}
	data, err := json.Marshal(envelope.Data)
	if err != nil {
		t.Fatal(err)
	}
	var doc meta.BurnbridgeTrayDocument
	if err := json.Unmarshal(data, &doc); err != nil {
		t.Fatal(err)
	}
	if doc.RecorderStatus != "opened" {
		t.Fatalf("expected recorderStatus opened, got %q", doc.RecorderStatus)
	}
}

func TestTrayOpenControlGetObjectUsesDriveControlBucket(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "meta.db")
	store, err := meta.NewSqlMeta(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	var gotAction burnbridgev1.TrayAction
	b := &BurnBridge{
		meta: store,
		grpc: testBurnBridgeClient{
			getDiscInfoFn: func(context.Context, *burnbridgev1.GetDiscInfoRequest, ...grpc.CallOption) (*burnbridgev1.GetDiscInfoResponse, error) {
				return &burnbridgev1.GetDiscInfoResponse{
					Drive: &burnbridgev1.OpticalDriveIdentity{
						SerialNumber: "DRIVE-SERIAL-002",
					},
				}, nil
			},
			handleTrayFn: func(_ctx context.Context, req *burnbridgev1.HandleTrayRequest, _opts ...grpc.CallOption) (*burnbridgev1.HandleTrayResponse, error) {
				gotAction = req.GetAction()
				return &burnbridgev1.HandleTrayResponse{
					Status:  "opened",
					Message: "manual tray open handled",
				}, nil
			},
		},
	}

	controlBucket, err := b.ensureDriveControlBucket(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	out, err := b.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: ptr(controlBucket),
		Key:    ptr(testControlKey("tray-open")),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = out.Body.Close() }()

	if gotAction != burnbridgev1.TrayAction_TRAY_ACTION_OPEN {
		t.Fatalf("expected tray open action, got %v", gotAction)
	}
}

func TestShortControlPathOnlyWorksOnDriveControlBucket(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "meta.db")
	store, err := meta.NewSqlMeta(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	var trayCalls int32
	b := &BurnBridge{
		meta:         store,
		activeBucket: "bucket1",
		grpc: testBurnBridgeClient{
			getDiscInfoFn: func(context.Context, *burnbridgev1.GetDiscInfoRequest, ...grpc.CallOption) (*burnbridgev1.GetDiscInfoResponse, error) {
				return &burnbridgev1.GetDiscInfoResponse{
					Drive: &burnbridgev1.OpticalDriveIdentity{
						SerialNumber: "DRIVE-SERIAL-004",
					},
				}, nil
			},
			handleTrayFn: func(context.Context, *burnbridgev1.HandleTrayRequest, ...grpc.CallOption) (*burnbridgev1.HandleTrayResponse, error) {
				atomic.AddInt32(&trayCalls, 1)
				return &burnbridgev1.HandleTrayResponse{Status: "opened"}, nil
			},
		},
	}
	controlBucket, err := b.ensureDriveControlBucket(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	out, err := b.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: ptr(controlBucket),
		Key:    ptr("v1/tray-open"),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = out.Body.Close() }()
	if atomic.LoadInt32(&trayCalls) != 1 {
		t.Fatalf("expected one tray call via short control path, got %d", atomic.LoadInt32(&trayCalls))
	}

	_, err = b.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: ptr("bucket1"),
		Key:    ptr("v1/tray-open"),
	})
	if err == nil {
		t.Fatal("expected real bucket short path to behave like a normal missing object")
	}
	if atomic.LoadInt32(&trayCalls) != 1 {
		t.Fatalf("expected real bucket short path not to trigger tray, got %d calls", atomic.LoadInt32(&trayCalls))
	}
}

func TestTrayCloseControlGetObjectInvokesHandleTray(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "meta.db")
	store, err := meta.NewSqlMeta(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	var gotAction burnbridgev1.TrayAction
	b := &BurnBridge{
		meta: store,
		grpc: testBurnBridgeClient{
			getDiscInfoFn: func(context.Context, *burnbridgev1.GetDiscInfoRequest, ...grpc.CallOption) (*burnbridgev1.GetDiscInfoResponse, error) {
				return testDriveInfo("DRIVE-SERIAL-TRAY-002"), nil
			},
			handleTrayFn: func(_ctx context.Context, req *burnbridgev1.HandleTrayRequest, _opts ...grpc.CallOption) (*burnbridgev1.HandleTrayResponse, error) {
				gotAction = req.GetAction()
				return &burnbridgev1.HandleTrayResponse{
					Status:  "closed",
					Message: "manual tray close handled",
				}, nil
			},
		},
	}
	controlBucket := testEnsureDriveControlBucket(t, b)

	out, err := b.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: ptr(controlBucket),
		Key:    ptr(testControlKey("tray-close")),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = out.Body.Close() }()

	if gotAction != burnbridgev1.TrayAction_TRAY_ACTION_CLOSE {
		t.Fatalf("expected tray close action, got %v", gotAction)
	}
	raw, err := io.ReadAll(out.Body)
	if err != nil {
		t.Fatal(err)
	}
	var envelope burnbridgeControlEnvelope
	if err := json.Unmarshal(raw, &envelope); err != nil {
		t.Fatal(err)
	}
	if !envelope.Ok {
		t.Fatalf("expected ok=true, got false with error %+v", envelope.Error)
	}
	if envelope.Action != "tray-close" {
		t.Fatalf("expected action tray-close, got %q", envelope.Action)
	}
}

func TestLoadOrFinalizeLayoutTranscriptSeparatesCloseDiscCache(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "meta.db")
	store, err := meta.NewSqlMeta(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	var closeCalls int32
	var openCalls int32
	b := &BurnBridge{
		meta: store,
		grpc: testBurnBridgeClient{
			finalizeFn: func(_ctx context.Context, req *burnbridgev1.FinalizeLayoutRequest, _opts ...grpc.CallOption) (*burnbridgev1.FinalizeLayoutResponse, error) {
				if req.GetCloseDisc() {
					atomic.AddInt32(&closeCalls, 1)
					return &burnbridgev1.FinalizeLayoutResponse{Bucket: "bucket1", Status: "closed"}, nil
				}
				atomic.AddInt32(&openCalls, 1)
				return &burnbridgev1.FinalizeLayoutResponse{Bucket: "bucket1", Status: "finalized"}, nil
			},
		},
		activeBucket: "bucket1",
		udfLabel:     "BUCKET1",
	}

	openReq1 := testControlRequest(t, "finalize-layout", 1780622225123, "550e8400-e29b-41d4-a716-446655440000")
	closeReq1 := testControlRequest(t, "close-disc", 1780622225124, "550e8400-e29b-41d4-a716-446655440001")
	openPayload, err := b.loadOrFinalizeLayoutTranscript(context.Background(), "bucket1", openReq1)
	if err != nil {
		t.Fatal(err)
	}
	closePayload, err := b.loadOrFinalizeLayoutTranscript(context.Background(), "bucket1", closeReq1)
	if err != nil {
		t.Fatal(err)
	}
	if atomic.LoadInt32(&openCalls) != 1 {
		t.Fatalf("expected one non-close finalize call, got %d", atomic.LoadInt32(&openCalls))
	}
	if atomic.LoadInt32(&closeCalls) != 1 {
		t.Fatalf("expected one close finalize call, got %d", atomic.LoadInt32(&closeCalls))
	}

	if _, err := b.loadOrFinalizeLayoutTranscript(context.Background(), "bucket1", openReq1); err != nil {
		t.Fatal(err)
	}
	if _, err := b.loadOrFinalizeLayoutTranscript(context.Background(), "bucket1", closeReq1); err != nil {
		t.Fatal(err)
	}
	if atomic.LoadInt32(&openCalls) != 1 {
		t.Fatalf("expected cached non-close transcript reuse, got %d calls", atomic.LoadInt32(&openCalls))
	}
	if atomic.LoadInt32(&closeCalls) != 1 {
		t.Fatalf("expected cached close transcript reuse, got %d calls", atomic.LoadInt32(&closeCalls))
	}
	if bytes.Equal(openPayload, closePayload) {
		t.Fatal("expected close and non-close transcripts to differ")
	}

	openReq2 := testControlRequest(t, "finalize-layout", 1780622225999, "550e8400-e29b-41d4-a716-446655440010")
	closeReq2 := testControlRequest(t, "close-disc", 1780622226000, "550e8400-e29b-41d4-a716-446655440011")
	if _, err := b.loadOrFinalizeLayoutTranscript(context.Background(), "bucket1", openReq2); err != nil {
		t.Fatal(err)
	}
	if _, err := b.loadOrFinalizeLayoutTranscript(context.Background(), "bucket1", closeReq2); err != nil {
		t.Fatal(err)
	}
	if atomic.LoadInt32(&openCalls) != 2 {
		t.Fatalf("expected a new finalize request to invoke grpc again, got %d calls", atomic.LoadInt32(&openCalls))
	}
	if atomic.LoadInt32(&closeCalls) != 2 {
		t.Fatalf("expected a new close request to invoke grpc again, got %d calls", atomic.LoadInt32(&closeCalls))
	}
}

func TestHeadObjectControlKeyReturnsEnvelopeMetadata(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "meta.db")
	store, err := meta.NewSqlMeta(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	if err := store.StoreBurnbridgeDiscInfo(&meta.BurnbridgeDiscInfoDocument{
		Bucket:      "bucket1",
		UpdatedAt:   "2026-06-05T10:11:12.1234567Z",
		VolumeLabel: "DISC001",
	}); err != nil {
		t.Fatal(err)
	}

	b := &BurnBridge{
		meta:         store,
		activeBucket: "bucket1",
		grpc: testBurnBridgeClient{
			getDiscInfoFn: func(_ context.Context, req *burnbridgev1.GetDiscInfoRequest, _ ...grpc.CallOption) (*burnbridgev1.GetDiscInfoResponse, error) {
				if req.GetIncludeDriveIdentity() {
					return testDriveInfo("DRIVE-SERIAL-HEAD-001"), nil
				}
				return nil, status.Error(codes.Unavailable, "offline for cache fallback")
			},
			testUnitReadyFn: func(context.Context, *burnbridgev1.TestUnitReadyRequest, ...grpc.CallOption) (*burnbridgev1.TestUnitReadyResponse, error) {
				return nil, status.Error(codes.Unavailable, "offline for cache fallback")
			},
		},
	}
	controlBucket := testEnsureDriveControlBucket(t, b)
	out, err := b.HeadObject(context.Background(), &s3.HeadObjectInput{
		Bucket: ptr(controlBucket),
		Key:    ptr(testControlKey("disc-info")),
	})
	if err != nil {
		t.Fatal(err)
	}
	if out.ContentLength == nil || *out.ContentLength <= 0 {
		t.Fatal("expected positive content length")
	}
	if out.LastModified == nil {
		t.Fatal("expected last modified")
	}
}

func TestPutObjectRejectsDriveControlBucket(t *testing.T) {
	b := &BurnBridge{
		activeBucket:           "bucket1",
		lastDriveControlBucket: "DRIVE-SERIAL-001",
		putQueueSem:            make(chan struct{}, 1),
	}
	_, err := b.PutObject(context.Background(), s3response.PutObjectInput{
		Bucket: ptr("DRIVE-SERIAL-001"),
		Key:    ptr(testControlKey("disc-info")),
		Body:   io.NopCloser(strings.NewReader("x")),
	})
	if err == nil {
		t.Fatal("expected method not allowed")
	}
	st, ok := err.(interface{ Error() string })
	if !ok || !strings.Contains(st.Error(), "MethodNotAllowed") {
		t.Fatalf("expected method not allowed error, got %v", err)
	}
}

func TestCreateMultipartUploadOversizedObjectDoesNotAutoCloseAppendableDisc(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "meta.db")
	store, err := meta.NewSqlMeta(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	const bucket = "DISC-A"
	if err := store.StoreBurnbridgeCommitted(nil, bucket, "existing.txt", &meta.BurnbridgeCommittedRecord{
		Size:         1,
		ETag:         "\"etag\"",
		LastModified: time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatal(err)
	}

	var finalizeCalls int32
	b := &BurnBridge{
		meta: store,
		grpc: testBurnBridgeClient{
			testUnitReadyFn: func(context.Context, *burnbridgev1.TestUnitReadyRequest, ...grpc.CallOption) (*burnbridgev1.TestUnitReadyResponse, error) {
				return &burnbridgev1.TestUnitReadyResponse{
					Ready:                 true,
					VolumeLabel:           "DISC-A",
					WritableState:         "Appendable",
					TotalCapacityBytes:    1000,
					FreeCapacityBytes:     800,
					UsedCapacityBytes:     200,
					WritableCapacityBytes: 700,
					FinalizeReserveBytes:  100,
				}, nil
			},
			finalizeFn: func(context.Context, *burnbridgev1.FinalizeLayoutRequest, ...grpc.CallOption) (*burnbridgev1.FinalizeLayoutResponse, error) {
				atomic.AddInt32(&finalizeCalls, 1)
				return &burnbridgev1.FinalizeLayoutResponse{Bucket: bucket, Status: "finalized"}, nil
			},
		},
		activeBucket:   bucket,
		volumeLabelRaw: "DISC-A",
		udfLabel:       "DISC-A",
	}

	_, err = b.CreateMultipartUpload(context.Background(), s3response.CreateMultipartUploadInput{
		Bucket: ptr(bucket),
		Key:    ptr("too-large.bin"),
		Metadata: map[string]string{
			"burnbridge-object-size": "900",
		},
	})
	if err == nil {
		t.Fatal("expected capacity error")
	}
	if atomic.LoadInt32(&finalizeCalls) != 0 {
		t.Fatalf("expected no automatic finalize/close while disc still has reserve, got %d calls", atomic.LoadInt32(&finalizeCalls))
	}
}

func TestEnsureWritableCapacityKeepsFinalizeReserve(t *testing.T) {
	doc := &meta.BurnbridgeDiscInfoDocument{
		WritableCapacityBytes: 800,
		FinalizeReserveBytes:  100,
	}
	if err := ensureWritableCapacity(doc, 700); err != nil {
		t.Fatalf("expected object fitting before reserve to be accepted: %v", err)
	}
	if err := ensureWritableCapacity(doc, 701); err == nil {
		t.Fatal("expected object crossing finalize reserve to be rejected")
	}
}

func TestDiscInfoRefreshPreservesFinalizeReserve(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "meta.db")
	store, err := meta.NewSqlMeta(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	const bucket = "DISC-A"
	if err := store.StoreBurnbridgeDiscInfo(&meta.BurnbridgeDiscInfoDocument{
		Bucket:                bucket,
		WritableCapacityBytes: 700,
		FreeCapacityBytes:     800,
		FinalizeReserveBytes:  100,
	}); err != nil {
		t.Fatal(err)
	}

	b := &BurnBridge{
		meta:         store,
		activeBucket: bucket,
		grpc: testBurnBridgeClient{
			getDiscInfoFn: func(context.Context, *burnbridgev1.GetDiscInfoRequest, ...grpc.CallOption) (*burnbridgev1.GetDiscInfoResponse, error) {
				return &burnbridgev1.GetDiscInfoResponse{
					Disc: &burnbridgev1.OpticalDiscInfo{
						DiscSerialNumberHex: bucket,
						WritableState:       "Appendable",
						MediaCapacity:       1000,
						MediaFreeSpace:      800,
						MediaUsedSpace:      200,
						BlockSizeBytes:      2048,
					},
				}, nil
			},
		},
	}

	_, doc, err := b.refreshDiscInfoDocument(context.Background(), bucket)
	if err != nil {
		t.Fatal(err)
	}
	if doc.FinalizeReserveBytes != 100 {
		t.Fatalf("expected reserve to be preserved, got %d", doc.FinalizeReserveBytes)
	}
	if doc.WritableCapacityBytes != 700 {
		t.Fatalf("expected writable capacity to stay free-reserve, got %d", doc.WritableCapacityBytes)
	}
}

func TestCreateMultipartUploadCapacityErrorAutoClosesOnlyAtFinalizeReserve(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "meta.db")
	store, err := meta.NewSqlMeta(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	const bucket = "DISC-A"
	if err := store.StoreBurnbridgeCommitted(nil, bucket, "existing.txt", &meta.BurnbridgeCommittedRecord{
		Size:         1,
		ETag:         "\"etag\"",
		LastModified: time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatal(err)
	}

	var closeDiscValues []bool
	b := &BurnBridge{
		meta: store,
		grpc: testBurnBridgeClient{
			testUnitReadyFn: func(context.Context, *burnbridgev1.TestUnitReadyRequest, ...grpc.CallOption) (*burnbridgev1.TestUnitReadyResponse, error) {
				return &burnbridgev1.TestUnitReadyResponse{
					Ready:                 true,
					VolumeLabel:           "DISC-A",
					WritableState:         "Appendable",
					TotalCapacityBytes:    1000,
					FreeCapacityBytes:     80,
					UsedCapacityBytes:     920,
					WritableCapacityBytes: 80,
					FinalizeReserveBytes:  100,
				}, nil
			},
			finalizeFn: func(_ context.Context, req *burnbridgev1.FinalizeLayoutRequest, _ ...grpc.CallOption) (*burnbridgev1.FinalizeLayoutResponse, error) {
				closeDiscValues = append(closeDiscValues, req.GetCloseDisc())
				return &burnbridgev1.FinalizeLayoutResponse{Bucket: bucket, Status: "finalized"}, nil
			},
		},
		activeBucket:   bucket,
		volumeLabelRaw: "DISC-A",
		udfLabel:       "DISC-A",
	}

	_, err = b.CreateMultipartUpload(context.Background(), s3response.CreateMultipartUploadInput{
		Bucket: ptr(bucket),
		Key:    ptr("last-object.bin"),
		Metadata: map[string]string{
			"burnbridge-object-size": "120",
		},
	})
	if err == nil {
		t.Fatal("expected capacity error")
	}
	if len(closeDiscValues) != 2 || closeDiscValues[0] || !closeDiscValues[1] {
		t.Fatalf("expected automatic FinalizeLayout then CloseDisc at reserve boundary, got %#v", closeDiscValues)
	}
}

func TestCapacityFinalizeStopsWhenStagedSmallObjectFlushFails(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "meta.db")
	store, err := meta.NewSqlMeta(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	const bucket = "DISC-A"
	if err := store.StoreBurnbridgeCommitted(nil, bucket, "staged.txt", &meta.BurnbridgeCommittedRecord{
		JobID:        "staged-small-object",
		Status:       "staged",
		Size:         5,
		ETag:         "\"etag\"",
		LastModified: time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatal(err)
	}
	stage := &smallObjectStageRecord{
		Bucket:      bucket,
		Key:         "staged.txt",
		PackPath:    filepath.Join(t.TempDir(), "missing-stage.pack"),
		LogicalSize: 5,
		CreatedAt:   time.Now().UTC().Format(time.RFC3339Nano),
	}
	raw, err := json.Marshal(stage)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.StoreAttribute(nil, bucket, "staged.txt", burnbridgeSmallObjectStageAttr, raw); err != nil {
		t.Fatal(err)
	}

	var closeDiscValues []bool
	b := &BurnBridge{
		meta: store,
		grpc: testBurnBridgeClient{
			testUnitReadyFn: func(context.Context, *burnbridgev1.TestUnitReadyRequest, ...grpc.CallOption) (*burnbridgev1.TestUnitReadyResponse, error) {
				return &burnbridgev1.TestUnitReadyResponse{
					Ready:                 true,
					VolumeLabel:           bucket,
					WritableState:         "Appendable",
					TotalCapacityBytes:    1000,
					FreeCapacityBytes:     0,
					UsedCapacityBytes:     1000,
					WritableCapacityBytes: 0,
					FinalizeReserveBytes:  100,
				}, nil
			},
			finalizeFn: func(_ context.Context, req *burnbridgev1.FinalizeLayoutRequest, _ ...grpc.CallOption) (*burnbridgev1.FinalizeLayoutResponse, error) {
				closeDiscValues = append(closeDiscValues, req.GetCloseDisc())
				return &burnbridgev1.FinalizeLayoutResponse{Bucket: bucket, Status: "finalized"}, nil
			},
		},
		activeBucket:   bucket,
		volumeLabelRaw: bucket,
		udfLabel:       bucket,
		smallBatch:     newSmallObjectBatcher(nil, 1024, 8, time.Minute),
	}

	b.finalizeAndCloseDiscAfterCapacityExceeded(context.Background(), bucket)

	if len(closeDiscValues) != 0 {
		t.Fatalf("expected automatic close-disc to stop when staged flush fails, got %#v", closeDiscValues)
	}
}

type testUploadObjectStream struct {
	mu         sync.Mutex
	sendChunks []*burnbridgev1.UploadObjectChunk
	ackQueue   []*burnbridgev1.UploadObjectAck
	recvIndex  int
	dynamicAck bool
}

func (s *testUploadObjectStream) Header() (metadata.MD, error) { return nil, nil }
func (s *testUploadObjectStream) Trailer() metadata.MD         { return nil }
func (s *testUploadObjectStream) CloseSend() error             { return nil }
func (s *testUploadObjectStream) Context() context.Context     { return context.Background() }
func (s *testUploadObjectStream) SendMsg(any) error            { return nil }
func (s *testUploadObjectStream) RecvMsg(any) error            { return nil }

func (s *testUploadObjectStream) Send(chunk *burnbridgev1.UploadObjectChunk) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sendChunks = append(s.sendChunks, chunk)
	return nil
}

func (s *testUploadObjectStream) Recv() (*burnbridgev1.UploadObjectAck, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.dynamicAck {
		if s.recvIndex >= len(s.sendChunks) {
			return nil, io.EOF
		}
		chunk := s.sendChunks[s.recvIndex]
		ack := &burnbridgev1.UploadObjectAck{
			JobId:             chunk.GetJobId(),
			SegmentIndex:      int32(s.recvIndex),
			ByteOffset:        chunk.GetOffset(),
			ByteSize:          int64(len(chunk.GetData())),
			BytesReceived:     chunk.GetOffset() + int64(len(chunk.GetData())),
			UploadComplete:    chunk.GetEof(),
			SegmentBurnResult: burnbridgev1.SegmentBurnResult_SEGMENT_BURN_RESULT_OK,
		}
		if chunk.GetEof() && len(s.ackQueue) > 0 {
			template := s.ackQueue[len(s.ackQueue)-1]
			ack.DiscExtents = template.GetDiscExtents()
			if ack.ByteSize == 0 && template.GetByteSize() != 0 {
				ack.ByteSize = template.GetByteSize()
			}
		}
		s.recvIndex++
		return ack, nil
	}
	if s.recvIndex >= len(s.ackQueue) {
		return nil, io.EOF
	}
	ack := s.ackQueue[s.recvIndex]
	s.recvIndex++
	return ack, nil
}

func TestPutObjectSkipsCommitWhenPayloadAlreadyCommitted(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "meta.db")
	store, err := meta.NewSqlMeta(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	const (
		bucket = "DISC-A"
		key    = "big-31-blocks.bin"
	)
	payload := bytes.Repeat([]byte("A"), 2048)
	digest := bbSegmentMD5Hex(payload)
	now := time.Date(2026, 5, 25, 6, 30, 0, 0, time.UTC)

	if err := store.StoreBurnbridgeCommitted(nil, bucket, key, &meta.BurnbridgeCommittedRecord{
		JobID:        "job-prev",
		Status:       "completed",
		ETag:         "\"" + digest + "\"",
		LastModified: now.Format(time.RFC3339Nano),
		Size:         int64(len(payload)),
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertBurnObjectSegment(bucket, key, "DISC-A", 0, 0, int64(len(payload)), digest, meta.BurnSegmentSucceeded, []meta.BurnDiscExtent{
		{DiscAddress: "4288608", FileSize: int64(len(payload))},
	}); err != nil {
		t.Fatal(err)
	}

	stream := &testUploadObjectStream{
		ackQueue: []*burnbridgev1.UploadObjectAck{
			{
				JobId:             "job-new",
				SegmentIndex:      0,
				ByteOffset:        0,
				ByteSize:          int64(len(payload)),
				UploadComplete:    true,
				SegmentBurnResult: burnbridgev1.SegmentBurnResult_SEGMENT_BURN_RESULT_OK,
			},
		},
	}

	commitCalls := 0
	cancelCalls := 0
	b := &BurnBridge{
		meta: store,
		grpc: testBurnBridgeClient{
			createJobFn: func(context.Context, *burnbridgev1.CreateJobRequest, ...grpc.CallOption) (*burnbridgev1.CreateJobResponse, error) {
				return &burnbridgev1.CreateJobResponse{JobId: "job-new"}, nil
			},
			uploadObjectFn: func(context.Context, ...grpc.CallOption) (grpc.BidiStreamingClient[burnbridgev1.UploadObjectChunk, burnbridgev1.UploadObjectAck], error) {
				return stream, nil
			},
			commitJobFn: func(context.Context, *burnbridgev1.CommitJobRequest, ...grpc.CallOption) (*burnbridgev1.CommitJobResponse, error) {
				commitCalls++
				return &burnbridgev1.CommitJobResponse{JobId: "job-new", Status: "completed"}, nil
			},
			cancelJobFn: func(context.Context, *burnbridgev1.CancelJobRequest, ...grpc.CallOption) (*burnbridgev1.CancelJobResponse, error) {
				cancelCalls++
				return &burnbridgev1.CancelJobResponse{}, nil
			},
		},
		chunkSize:        len(payload) + 1024,
		cancelJobTimeout: time.Second,
		putQueueSem:      make(chan struct{}, 4),
		activeBucket:     bucket,
		volumeLabelRaw:   "DISC-A",
		udfLabel:         "DISC-A",
	}

	out, err := b.PutObject(context.Background(), s3response.PutObjectInput{
		Bucket:        ptr(bucket),
		Key:           ptr(key),
		Body:          bytes.NewReader(payload),
		ContentLength: ptr(int64(len(payload))),
	})
	if err != nil {
		t.Fatal(err)
	}
	if commitCalls != 0 {
		t.Fatalf("expected CommitJob to be skipped, got %d calls", commitCalls)
	}
	if cancelCalls != 1 {
		t.Fatalf("expected CancelJob cleanup once, got %d", cancelCalls)
	}
	if out.ETag != "\""+digest+"\"" {
		t.Fatalf("unexpected etag: %q", out.ETag)
	}
	if out.Size == nil || *out.Size != int64(len(payload)) {
		t.Fatalf("unexpected size: %#v", out.Size)
	}
	if len(stream.sendChunks) != 1 {
		t.Fatalf("expected one reused chunk send, got %d", len(stream.sendChunks))
	}
	if stream.sendChunks[0].GetReusedBurnedBytes() != int64(len(payload)) {
		t.Fatalf("expected reused bytes %d, got %d", len(payload), stream.sendChunks[0].GetReusedBurnedBytes())
	}
}

func TestPutObjectCommitsWhenUncommittedResumeSegmentsAlreadyBurned(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "meta.db")
	store, err := meta.NewSqlMeta(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	const (
		bucket = "DISC-A"
		key    = "resume-me.bin"
	)
	payload := bytes.Repeat([]byte("B"), 2048)
	digest := bbSegmentMD5Hex(payload)

	if err := store.UpsertBurnUploadSession(meta.BurnUploadSessionRecord{
		Bucket:         bucket,
		ObjectName:     key,
		UploadID:       burnbridgeImplicitSingleUploadID,
		Kind:           meta.BurnUploadKindSingle,
		State:          meta.BurnUploadStateFailed,
		MediaID:        "DISC-A",
		ContentLength:  int64(len(payload)),
		NextPartNumber: 1,
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertBurnObjectSegment(bucket, key, "DISC-A", 0, 0, int64(len(payload)), digest, meta.BurnSegmentSucceeded, []meta.BurnDiscExtent{
		{DiscAddress: "4288608", FileSize: int64(len(payload))},
	}); err != nil {
		t.Fatal(err)
	}

	stream := &testUploadObjectStream{
		ackQueue: []*burnbridgev1.UploadObjectAck{
			{
				JobId:             "job-new",
				SegmentIndex:      0,
				ByteOffset:        0,
				ByteSize:          int64(len(payload)),
				UploadComplete:    true,
				BytesReceived:     int64(len(payload)),
				SegmentBurnResult: burnbridgev1.SegmentBurnResult_SEGMENT_BURN_RESULT_OK,
			},
		},
	}

	commitCalls := 0
	cancelCalls := 0
	b := &BurnBridge{
		meta: store,
		grpc: testBurnBridgeClient{
			createJobFn: func(context.Context, *burnbridgev1.CreateJobRequest, ...grpc.CallOption) (*burnbridgev1.CreateJobResponse, error) {
				return &burnbridgev1.CreateJobResponse{JobId: "job-new"}, nil
			},
			uploadObjectFn: func(context.Context, ...grpc.CallOption) (grpc.BidiStreamingClient[burnbridgev1.UploadObjectChunk, burnbridgev1.UploadObjectAck], error) {
				return stream, nil
			},
			commitJobFn: func(context.Context, *burnbridgev1.CommitJobRequest, ...grpc.CallOption) (*burnbridgev1.CommitJobResponse, error) {
				commitCalls++
				return &burnbridgev1.CommitJobResponse{JobId: "job-new", Status: "layout_persisted"}, nil
			},
			cancelJobFn: func(context.Context, *burnbridgev1.CancelJobRequest, ...grpc.CallOption) (*burnbridgev1.CancelJobResponse, error) {
				cancelCalls++
				return &burnbridgev1.CancelJobResponse{}, nil
			},
		},
		chunkSize:        len(payload) + 1024,
		cancelJobTimeout: time.Second,
		putQueueSem:      make(chan struct{}, 4),
		activeBucket:     bucket,
		volumeLabelRaw:   "DISC-A",
		udfLabel:         "DISC-A",
	}

	out, err := b.PutObject(context.Background(), s3response.PutObjectInput{
		Bucket:        ptr(bucket),
		Key:           ptr(key),
		Body:          bytes.NewReader(payload),
		ContentLength: ptr(int64(len(payload))),
	})
	if err != nil {
		t.Fatal(err)
	}
	if commitCalls != 1 {
		t.Fatalf("expected CommitJob once for resumed uncommitted object, got %d calls", commitCalls)
	}
	if cancelCalls != 0 {
		t.Fatalf("expected no CancelJob cleanup on successful commit, got %d", cancelCalls)
	}
	if out.ETag != "\""+digest+"\"" {
		t.Fatalf("unexpected etag: %q", out.ETag)
	}
	if len(stream.sendChunks) != 1 {
		t.Fatalf("expected one reused chunk send, got %d", len(stream.sendChunks))
	}
	if stream.sendChunks[0].GetReusedBurnedBytes() != int64(len(payload)) {
		t.Fatalf("expected reused bytes %d, got %d", len(payload), stream.sendChunks[0].GetReusedBurnedBytes())
	}

	committedRec, err := store.GetBurnbridgeCommittedRecord(bucket, key)
	if err != nil {
		t.Fatal(err)
	}
	if committedRec.Size != int64(len(payload)) {
		t.Fatalf("unexpected committed size: %d", committedRec.Size)
	}

	if _, err := store.GetBurnUploadSession(bucket, key, burnbridgeImplicitSingleUploadID); !errors.Is(err, meta.ErrNoSuchKey) {
		t.Fatalf("expected completed implicit session cleanup, got %v", err)
	}
	if parts, err := store.ListBurnUploadParts(bucket, key, burnbridgeImplicitSingleUploadID); err != nil {
		t.Fatal(err)
	} else if len(parts) != 0 {
		t.Fatalf("expected completed implicit part cleanup, got %d rows", len(parts))
	}
}

func TestPutObjectComputesChecksumWhenTailAckOmitsFinalChecksum(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "meta.db")
	store, err := meta.NewSqlMeta(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	const (
		bucket = "DISC-A"
		key    = "small-a.txt"
	)
	payload := []byte("overwrite-payload-v2")
	objectMD5 := bbSegmentMD5Hex(payload)

	stream := &testUploadObjectStream{
		ackQueue: []*burnbridgev1.UploadObjectAck{
			{
				JobId:             "job-new",
				SegmentIndex:      0,
				ByteOffset:        0,
				ByteSize:          int64(len(payload)),
				UploadComplete:    true,
				BytesReceived:     int64(len(payload)),
				SegmentBurnResult: burnbridgev1.SegmentBurnResult_SEGMENT_BURN_RESULT_OK,
			},
		},
	}

	b := &BurnBridge{
		meta: store,
		grpc: testBurnBridgeClient{
			createJobFn: func(context.Context, *burnbridgev1.CreateJobRequest, ...grpc.CallOption) (*burnbridgev1.CreateJobResponse, error) {
				return &burnbridgev1.CreateJobResponse{JobId: "job-new"}, nil
			},
			uploadObjectFn: func(context.Context, ...grpc.CallOption) (grpc.BidiStreamingClient[burnbridgev1.UploadObjectChunk, burnbridgev1.UploadObjectAck], error) {
				return stream, nil
			},
			commitJobFn: func(context.Context, *burnbridgev1.CommitJobRequest, ...grpc.CallOption) (*burnbridgev1.CommitJobResponse, error) {
				return &burnbridgev1.CommitJobResponse{JobId: "job-new", Status: "layout_persisted"}, nil
			},
		},
		chunkSize:        len(payload) + 1024,
		cancelJobTimeout: time.Second,
		putQueueSem:      make(chan struct{}, 4),
		activeBucket:     bucket,
		volumeLabelRaw:   "DISC-A",
		udfLabel:         "DISC-A",
	}

	out, err := b.PutObject(context.Background(), s3response.PutObjectInput{
		Bucket:        ptr(bucket),
		Key:           ptr(key),
		Body:          bytes.NewReader(payload),
		ContentLength: ptr(int64(len(payload))),
	})
	if err != nil {
		t.Fatal(err)
	}
	if out.ETag != "\""+objectMD5+"\"" {
		t.Fatalf("unexpected etag: %q", out.ETag)
	}
	if out.ChecksumMD5 == nil || *out.ChecksumMD5 != objectMD5 {
		t.Fatalf("unexpected checksum_md5: %#v", out.ChecksumMD5)
	}
}

func TestSmallObjectStageFlushesOnePackOnFinalize(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "meta.db")
	store, err := meta.NewSqlMeta(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	const bucket = "DISC-A"
	payloadA := []byte("aa")
	payloadB := []byte("bb")
	stream := &testUploadObjectStream{
		dynamicAck: true,
		ackQueue: []*burnbridgev1.UploadObjectAck{
			{
				DiscExtents: []*burnbridgev1.DiscExtent{
					{DiscAddress: "1000", FileSize: 4096},
				},
			},
		},
	}
	var createCalls atomic.Int32
	var uploadCalls atomic.Int32
	var commitCalls atomic.Int32
	var commitBatchCalls atomic.Int32
	var commitReq atomic.Pointer[burnbridgev1.CommitJobRequest]
	b := &BurnBridge{
		meta: store,
		grpc: testBurnBridgeClient{
			createJobFn: func(_ context.Context, req *burnbridgev1.CreateJobRequest, _ ...grpc.CallOption) (*burnbridgev1.CreateJobResponse, error) {
				createCalls.Add(1)
				if !strings.HasPrefix(req.GetObjectKey(), ".__burnbridge_pack__/") {
					t.Fatalf("expected hidden pack CreateJob, got object key %q", req.GetObjectKey())
				}
				return &burnbridgev1.CreateJobResponse{JobId: "job-pack"}, nil
			},
			uploadObjectFn: func(context.Context, ...grpc.CallOption) (grpc.BidiStreamingClient[burnbridgev1.UploadObjectChunk, burnbridgev1.UploadObjectAck], error) {
				uploadCalls.Add(1)
				return stream, nil
			},
			commitJobFn: func(_ context.Context, req *burnbridgev1.CommitJobRequest, _ ...grpc.CallOption) (*burnbridgev1.CommitJobResponse, error) {
				commitCalls.Add(1)
				commitReq.Store(req)
				return &burnbridgev1.CommitJobResponse{JobId: req.GetJobId(), Status: "layout_persisted"}, nil
			},
			commitJobBatchFn: func(_ context.Context, req *burnbridgev1.CommitJobBatchRequest, _ ...grpc.CallOption) (*burnbridgev1.CommitJobBatchResponse, error) {
				commitBatchCalls.Add(1)
				resp := &burnbridgev1.CommitJobBatchResponse{}
				for _, job := range req.GetJobs() {
					resp.Jobs = append(resp.Jobs, &burnbridgev1.CommitJobResponse{JobId: job.GetJobId(), Status: "layout_persisted"})
				}
				return resp, nil
			},
			cancelJobFn: func(context.Context, *burnbridgev1.CancelJobRequest, ...grpc.CallOption) (*burnbridgev1.CancelJobResponse, error) {
				return &burnbridgev1.CancelJobResponse{}, nil
			},
			finalizeFn: func(_ context.Context, req *burnbridgev1.FinalizeLayoutRequest, _ ...grpc.CallOption) (*burnbridgev1.FinalizeLayoutResponse, error) {
				return &burnbridgev1.FinalizeLayoutResponse{Bucket: req.GetBucket(), Status: "finalized"}, nil
			},
		},
		chunkSize:        1024,
		cancelJobTimeout: time.Second,
		putQueueSem:      make(chan struct{}, 4),
		activeBucket:     bucket,
		volumeLabelRaw:   "DISC-A",
		udfLabel:         "DISC-A",
	}
	b.smallBatch = newSmallObjectBatcher(b, 1024, 8, 50*time.Millisecond)

	put := func(key string, payload []byte) {
		_, err := b.PutObject(context.Background(), s3response.PutObjectInput{
			Bucket:        ptr(bucket),
			Key:           ptr(key),
			Body:          bytes.NewReader(payload),
			ContentLength: ptr(int64(len(payload))),
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	put("a.txt", payloadA)
	put("b.txt", payloadB)

	if got := uploadCalls.Load(); got != 0 {
		t.Fatalf("expected no UploadObject stream before finalize, got %d", got)
	}
	if got := createCalls.Load(); got != 0 {
		t.Fatalf("expected no hidden pack CreateJob before finalize, got %d", got)
	}
	if got := commitCalls.Load(); got != 0 {
		t.Fatalf("expected no hidden pack CommitJob before finalize, got %d", got)
	}

	finalizeReq := &burnbridgeControlRequest{
		Action:      burnbridgeControlActionFinalizeLayout,
		RequestTime: time.Now().UTC().UnixMilli(),
		RequestID:   "finalize-staged-small-objects",
		Key:         "v1/finalize-layout",
	}
	if _, err := b.invokeFinalizeLayoutAgainstRecorder(context.Background(), bucket, finalizeReq); err != nil {
		t.Fatal(err)
	}

	if got := uploadCalls.Load(); got != 1 {
		t.Fatalf("expected one staged UploadObject stream on finalize, got %d", got)
	}
	if got := createCalls.Load(); got != 1 {
		t.Fatalf("expected one hidden pack CreateJob call on finalize, got %d", got)
	}
	if got := commitCalls.Load(); got != 1 {
		t.Fatalf("expected one hidden pack CommitJob call on finalize, got %d", got)
	}
	if got := commitBatchCalls.Load(); got != 0 {
		t.Fatalf("expected no CommitJobBatch calls in safe activation mode, got %d", got)
	}
	if len(stream.sendChunks) != 5 {
		t.Fatalf("expected one request chunk and four payload chunks, got %d", len(stream.sendChunks))
	}
	var dataBytes int
	for i, chunk := range stream.sendChunks {
		if chunk.GetJobId() != "job-pack" {
			t.Fatalf("expected hidden pack job id on stream chunk %d, got %q", i, chunk.GetJobId())
		}
		dataBytes += len(chunk.GetData())
	}
	if dataBytes != 4096 {
		t.Fatalf("expected 4096-byte aligned pack payload, got %d", dataBytes)
	}
	req := commitReq.Load()
	if req == nil {
		t.Fatal("expected CommitJob request")
	}
	if req.GetJobId() != "job-pack" {
		t.Fatalf("expected pack job commit, got %q", req.GetJobId())
	}
	if req.GetFinalizeManifest() == nil || len(req.GetFinalizeManifest().GetFiles()) != 2 {
		t.Fatalf("expected two manifest files, got %#v", req.GetFinalizeManifest())
	}
	filesByKey := make(map[string]*burnbridgev1.FinalizeFile)
	for _, file := range req.GetFinalizeManifest().GetFiles() {
		filesByKey[file.GetObjectKey()] = file
	}
	if filesByKey["a.txt"] == nil || filesByKey["b.txt"] == nil {
		t.Fatalf("unexpected manifest object keys: %#v", filesByKey)
	}
	expectedSizes := map[string]int64{
		"a.txt": int64(len(payloadA)),
		"b.txt": int64(len(payloadB)),
	}
	seenAddresses := make(map[string]bool)
	for key, file := range filesByKey {
		if file.GetFileSize() != expectedSizes[key] {
			t.Fatalf("unexpected %s file size: %d", key, file.GetFileSize())
		}
		if len(file.GetSegments()) != 1 || len(file.GetSegments()[0].GetDiscExtents()) != 1 {
			t.Fatalf("expected one segment and one extent for %s, got %#v", key, file)
		}
		extent := file.GetSegments()[0].GetDiscExtents()[0]
		if extent.GetFileSize() != expectedSizes[key] {
			t.Fatalf("unexpected %s extent size: %#v", key, extent)
		}
		seenAddresses[extent.GetDiscAddress()] = true
	}
	if !seenAddresses["1000"] || !seenAddresses["1001"] {
		t.Fatalf("expected manifest extents to cover pack blocks 1000 and 1001, got %#v", seenAddresses)
	}
}

func TestSmallObjectStageSkipsRecorderImportedProbeForNewObject(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "meta.db")
	store, err := meta.NewSqlMeta(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	const bucket = "DISC-A"
	var importedStateCalls atomic.Int32
	var readyCalls atomic.Int32
	var discInfoCalls atomic.Int32
	b := &BurnBridge{
		meta: store,
		grpc: testBurnBridgeClient{
			importedBucketStateFn: func(context.Context, *burnbridgev1.GetImportedBucketStateRequest, ...grpc.CallOption) (*burnbridgev1.GetImportedBucketStateResponse, error) {
				importedStateCalls.Add(1)
				return nil, fmt.Errorf("unexpected imported state probe")
			},
			testUnitReadyFn: func(context.Context, *burnbridgev1.TestUnitReadyRequest, ...grpc.CallOption) (*burnbridgev1.TestUnitReadyResponse, error) {
				readyCalls.Add(1)
				return nil, fmt.Errorf("unexpected TestUnitReady probe")
			},
			getDiscInfoFn: func(context.Context, *burnbridgev1.GetDiscInfoRequest, ...grpc.CallOption) (*burnbridgev1.GetDiscInfoResponse, error) {
				discInfoCalls.Add(1)
				return nil, fmt.Errorf("unexpected GetDiscInfo probe")
			},
		},
		activeBucket: bucket,
		metaDBPath:   dbPath,
		putQueueSem:  make(chan struct{}, 1),
	}
	b.smallBatch = newSmallObjectBatcher(b, 1024, 8, 50*time.Millisecond)

	payload := []byte("small staged payload")
	out, err := b.PutObject(context.Background(), s3response.PutObjectInput{
		Bucket:        ptr(bucket),
		Key:           ptr("new-small.txt"),
		Body:          bytes.NewReader(payload),
		ContentLength: ptr(int64(len(payload))),
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := importedStateCalls.Load(); got != 0 {
		t.Fatalf("expected no imported state probe for new staged object, got %d", got)
	}
	if got := readyCalls.Load(); got != 0 {
		t.Fatalf("expected no TestUnitReady probe for new staged object, got %d", got)
	}
	if got := discInfoCalls.Load(); got != 0 {
		t.Fatalf("expected no GetDiscInfo probe for new staged object, got %d", got)
	}
	expectedSum := md5.Sum(payload)
	expectedETag := fmt.Sprintf("\"%x\"", expectedSum[:])
	if out.ETag != expectedETag {
		t.Fatalf("unexpected etag: %q", out.ETag)
	}
	stage, err := b.loadSmallObjectStage(bucket, "new-small.txt")
	if err != nil {
		t.Fatal(err)
	}
	if stage == nil || stage.LogicalSize != int64(len(payload)) {
		t.Fatalf("unexpected stage record: %#v", stage)
	}
}

func TestSmallObjectStageCapacityIncludesExistingStagedBytes(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "meta.db")
	store, err := meta.NewSqlMeta(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	const bucket = "DISC-A"
	if err := store.StoreBurnbridgeDiscInfo(&meta.BurnbridgeDiscInfoDocument{
		Bucket:                bucket,
		WritableCapacityBytes: 4096,
		FreeCapacityBytes:     4096,
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.StoreBurnbridgeCommitted(nil, bucket, "already-staged.bin", &meta.BurnbridgeCommittedRecord{
		JobID:        "staged-small-object",
		Status:       "staged",
		Size:         1,
		ETag:         "\"etag\"",
		LastModified: time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatal(err)
	}
	stageRaw, err := json.Marshal(&smallObjectStageRecord{
		Bucket:       bucket,
		Key:          "already-staged.bin",
		PackPath:     filepath.Join(t.TempDir(), "stage.pack"),
		LogicalSize:  1,
		PhysicalSize: 4096,
		CreatedAt:    time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.StoreAttribute(nil, bucket, "already-staged.bin", burnbridgeSmallObjectStageAttr, stageRaw); err != nil {
		t.Fatal(err)
	}

	b := &BurnBridge{
		meta:           store,
		activeBucket:   bucket,
		volumeLabelRaw: bucket,
		metaDBPath:     dbPath,
		putQueueSem:    make(chan struct{}, 1),
	}
	b.smallBatch = newSmallObjectBatcher(b, 1024, 8, time.Minute)

	_, err = b.PutObject(context.Background(), s3response.PutObjectInput{
		Bucket:        ptr(bucket),
		Key:           ptr("new-small.bin"),
		Body:          bytes.NewReader([]byte("x")),
		ContentLength: ptr(int64(1)),
	})
	if err == nil {
		t.Fatal("expected capacity error because existing staged bytes already consume writable capacity")
	}
	if _, stageErr := b.loadSmallObjectStage(bucket, "new-small.bin"); !errors.Is(stageErr, meta.ErrNoSuchKey) {
		t.Fatalf("new object should not be staged after capacity failure, got %v", stageErr)
	}
}

func TestSmallObjectStageCapacityIncludesStripePadding(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "meta.db")
	store, err := meta.NewSqlMeta(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	const bucket = "DISC-A"
	if err := store.StoreBurnbridgeDiscInfo(&meta.BurnbridgeDiscInfoDocument{
		Bucket:                bucket,
		WritableCapacityBytes: 2048,
		FreeCapacityBytes:     2048,
	}); err != nil {
		t.Fatal(err)
	}
	for key, value := range map[string]string{
		"redundancy_enabled":            "true",
		"redundancy_data_block_count":   "2",
		"redundancy_parity_block_count": "1",
		"redundancy_block_size_bytes":   "2048",
	} {
		if err := store.StoreAttribute(nil, bucket, "", key, []byte(value)); err != nil {
			t.Fatal(err)
		}
	}

	b := &BurnBridge{
		meta:           store,
		activeBucket:   bucket,
		volumeLabelRaw: bucket,
		metaDBPath:     dbPath,
		putQueueSem:    make(chan struct{}, 1),
	}
	b.smallBatch = newSmallObjectBatcher(b, 1024, 8, time.Minute)

	_, err = b.PutObject(context.Background(), s3response.PutObjectInput{
		Bucket:        ptr(bucket),
		Key:           ptr("new-small.bin"),
		Body:          bytes.NewReader([]byte("x")),
		ContentLength: ptr(int64(1)),
	})
	if err == nil {
		t.Fatal("expected capacity error because stripe padding exceeds writable capacity")
	}
	if _, stageErr := b.loadSmallObjectStage(bucket, "new-small.bin"); !errors.Is(stageErr, meta.ErrNoSuchKey) {
		t.Fatalf("new object should not be staged after padding capacity failure, got %v", stageErr)
	}
	if _, committedErr := store.GetBurnbridgeCommittedRecord(bucket, "new-small.bin"); !errors.Is(committedErr, meta.ErrNoSuchKey) {
		t.Fatalf("new object should not be visible after padding capacity failure, got %v", committedErr)
	}
}

func TestSmallObjectPackStripePaddingUsesBucketRedundancyConfig(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "meta.db")
	store, err := meta.NewSqlMeta(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	const bucket = "DISC-A"
	for key, value := range map[string]string{
		"redundancy_enabled":            "true",
		"redundancy_data_block_count":   "12",
		"redundancy_parity_block_count": "3",
		"redundancy_block_size_bytes":   "4096",
	} {
		if err := store.StoreAttribute(nil, bucket, "", key, []byte(value)); err != nil {
			t.Fatal(err)
		}
	}

	b := &BurnBridge{meta: store}
	// 12 data blocks * 4096 bytes = 49152 bytes per RS data stripe.
	if got, want := b.smallObjectPackStripePadding(bucket, 50000), int64(48304); got != want {
		t.Fatalf("expected padding from configured stripe size, got %d want %d", got, want)
	}
	if got := b.smallObjectPackStripePadding(bucket, 49152); got != 0 {
		t.Fatalf("expected no padding for exact configured stripe, got %d", got)
	}
}

func TestSmallObjectPackStripePaddingSkipsWhenRedundancyDisabled(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "meta.db")
	store, err := meta.NewSqlMeta(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	const bucket = "DISC-A"
	for key, value := range map[string]string{
		"redundancy_enabled":            "false",
		"redundancy_data_block_count":   "12",
		"redundancy_parity_block_count": "3",
		"redundancy_block_size_bytes":   "4096",
	} {
		if err := store.StoreAttribute(nil, bucket, "", key, []byte(value)); err != nil {
			t.Fatal(err)
		}
	}

	b := &BurnBridge{meta: store}
	if got := b.smallObjectPackStripePadding(bucket, 50000); got != 0 {
		t.Fatalf("expected no padding when RS is disabled, got %d", got)
	}
}

func TestSmallObjectPackStripePaddingSkipsWhenParityDisabled(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "meta.db")
	store, err := meta.NewSqlMeta(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	const bucket = "DISC-A"
	for key, value := range map[string]string{
		"redundancy_enabled":            "true",
		"redundancy_data_block_count":   "12",
		"redundancy_parity_block_count": "0",
		"redundancy_block_size_bytes":   "4096",
	} {
		if err := store.StoreAttribute(nil, bucket, "", key, []byte(value)); err != nil {
			t.Fatal(err)
		}
	}

	b := &BurnBridge{meta: store}
	if got := b.smallObjectPackStripePadding(bucket, 50000); got != 0 {
		t.Fatalf("expected no padding when RS parity is disabled, got %d", got)
	}
}

func TestSmallObjectPackStripePaddingSkipsWithoutCompleteRedundancyConfig(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "meta.db")
	store, err := meta.NewSqlMeta(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	const bucket = "DISC-A"
	if err := store.StoreAttribute(nil, bucket, "", "redundancy_enabled", []byte("true")); err != nil {
		t.Fatal(err)
	}
	if err := store.StoreAttribute(nil, bucket, "", "redundancy_parity_block_count", []byte("2")); err != nil {
		t.Fatal(err)
	}

	b := &BurnBridge{meta: store}
	if got := b.smallObjectPackStripePadding(bucket, 50000); got != 0 {
		t.Fatalf("expected no padding without explicit data block and block size config, got %d", got)
	}
}

func TestSmallObjectPackManifestFallsBackToPersistedSegmentExtents(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "meta.db")
	store, err := meta.NewSqlMeta(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	const (
		bucket  = "DISC-A"
		packKey = ".__burnbridge_pack__/pack-001.pack"
	)
	if err := store.UpsertBurnObjectSegment(bucket, packKey, bucket, 0, 0, 4096, "pack-md5", meta.BurnSegmentSucceeded, []meta.BurnDiscExtent{
		{DiscAddress: "5000", FileSize: 4096},
	}); err != nil {
		t.Fatal(err)
	}

	reqA := &smallObjectBatchRequest{bucket: bucket, key: "a.txt"}
	reqB := &smallObjectBatchRequest{bucket: bucket, key: "b.txt"}
	entries := []smallObjectPackEntry{
		{req: reqA, logicalSize: 2, physicalSize: 2048, startOffset: 0, checksumMD5: bbSegmentMD5Hex([]byte("aa"))},
		{req: reqB, logicalSize: 2, physicalSize: 2048, startOffset: 2048, checksumMD5: bbSegmentMD5Hex([]byte("bb"))},
	}
	b := &BurnBridge{meta: store}
	manifest, err := b.buildSmallObjectPackFinalizeManifest(bucket, packKey, entries, &burnbridgev1.UploadObjectAck{UploadComplete: true})
	if err != nil {
		t.Fatal(err)
	}
	if manifest == nil || len(manifest.GetFiles()) != 2 {
		t.Fatalf("expected two manifest files, got %#v", manifest)
	}
	filesByKey := make(map[string]*burnbridgev1.FinalizeFile)
	for _, file := range manifest.GetFiles() {
		filesByKey[file.GetObjectKey()] = file
	}
	if filesByKey["a.txt"] == nil || filesByKey["b.txt"] == nil {
		t.Fatalf("unexpected manifest files: %#v", filesByKey)
	}
	if got := filesByKey["a.txt"].GetSegments()[0].GetDiscExtents()[0].GetDiscAddress(); got != "5000" {
		t.Fatalf("unexpected a.txt disc address: %q", got)
	}
	if got := filesByKey["b.txt"].GetSegments()[0].GetDiscExtents()[0].GetDiscAddress(); got != "5001" {
		t.Fatalf("unexpected b.txt disc address: %q", got)
	}
}

func TestSmallObjectPackManifestCanDeferExtentSlicingToRecorder(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "meta.db")
	store, err := meta.NewSqlMeta(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	const (
		bucket  = "DISC-A"
		packKey = ".__burnbridge_pack__/pack-002.pack"
	)
	entries := []smallObjectPackEntry{
		{req: &smallObjectBatchRequest{bucket: bucket, key: "a.txt"}, logicalSize: 2, physicalSize: 2048, startOffset: 0, checksumMD5: bbSegmentMD5Hex([]byte("aa"))},
		{req: &smallObjectBatchRequest{bucket: bucket, key: "b.txt"}, logicalSize: 3, physicalSize: 2048, startOffset: 2048, checksumMD5: bbSegmentMD5Hex([]byte("bbb"))},
	}
	b := &BurnBridge{meta: store}
	manifest, err := b.buildSmallObjectPackFinalizeManifest(bucket, packKey, entries, &burnbridgev1.UploadObjectAck{UploadComplete: true})
	if err != nil {
		t.Fatal(err)
	}
	if manifest == nil || len(manifest.GetFiles()) != 2 {
		t.Fatalf("expected two manifest files, got %#v", manifest)
	}
	for _, file := range manifest.GetFiles() {
		if len(file.GetSegments()) != 0 {
			t.Fatalf("expected recorder-deferred extent slicing with no segment extents, got %#v", file)
		}
		if file.GetFileSize() <= 0 {
			t.Fatalf("expected logical file size for %s, got %d", file.GetObjectKey(), file.GetFileSize())
		}
	}
}

func TestUploadPartRejectsOutOfOrderSerialWrite(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "meta.db")
	store, err := meta.NewSqlMeta(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	const (
		bucket   = "DISC-A"
		key      = "multipart.bin"
		uploadID = "upload-001"
	)
	if err := store.UpsertBurnUploadSession(meta.BurnUploadSessionRecord{
		Bucket:         bucket,
		ObjectName:     key,
		UploadID:       uploadID,
		Kind:           meta.BurnUploadKindMultipart,
		State:          meta.BurnUploadStateWriting,
		MediaID:        "DISC-A",
		RecorderJobID:  "job-upload-001",
		BytesReceived:  0,
		NextPartNumber: 1,
	}); err != nil {
		t.Fatal(err)
	}

	createCalls := 0
	b := &BurnBridge{
		meta: store,
		grpc: testBurnBridgeClient{
			createJobFn: func(context.Context, *burnbridgev1.CreateJobRequest, ...grpc.CallOption) (*burnbridgev1.CreateJobResponse, error) {
				createCalls++
				return &burnbridgev1.CreateJobResponse{JobId: "unexpected"}, nil
			},
		},
		chunkSize:      4096,
		putQueueSem:    make(chan struct{}, 4),
		activeBucket:   bucket,
		volumeLabelRaw: "DISC-A",
		udfLabel:       "DISC-A",
	}

	_, err = b.UploadPart(context.Background(), &s3.UploadPartInput{
		Bucket:        ptr(bucket),
		Key:           ptr(key),
		UploadId:      ptr(uploadID),
		PartNumber:    ptr(int32(2)),
		Body:          bytes.NewReader([]byte("out-of-order")),
		ContentLength: ptr(int64(len("out-of-order"))),
	})
	if err == nil || !strings.Contains(err.Error(), "InvalidPartOrder") {
		t.Fatalf("expected InvalidPartOrder, got %v", err)
	}
	if createCalls != 0 {
		t.Fatalf("expected no CreateJob call, got %d", createCalls)
	}
}

func TestUploadPartUsesCachedReadyStateWithoutStatusWatcher(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "meta.db")
	store, err := meta.NewSqlMeta(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	const (
		bucket   = "DISC-A"
		key      = "multipart.bin"
		uploadID = "upload-ready-cache"
		jobID    = "job-ready-cache"
	)
	payload := bytes.Repeat([]byte("P"), 4096)
	payloadMD5 := bbSegmentMD5Hex(payload)
	if err := store.StoreBurnbridgeDiscInfo(&meta.BurnbridgeDiscInfoDocument{
		Bucket:                bucket,
		WritableState:         "Appendable",
		TotalCapacityBytes:    1024 * 1024,
		FreeCapacityBytes:     1024 * 1024,
		WritableCapacityBytes: 1024 * 1024,
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertBurnUploadSession(meta.BurnUploadSessionRecord{
		Bucket:         bucket,
		ObjectName:     key,
		UploadID:       uploadID,
		Kind:           meta.BurnUploadKindMultipart,
		State:          meta.BurnUploadStateWriting,
		MediaID:        bucket,
		RecorderJobID:  jobID,
		BytesReceived:  0,
		NextPartNumber: 1,
	}); err != nil {
		t.Fatal(err)
	}

	var readyCalls int32
	stream := &testUploadObjectStream{
		ackQueue: []*burnbridgev1.UploadObjectAck{
			{
				JobId:             jobID,
				SegmentIndex:      0,
				ByteOffset:        0,
				ByteSize:          int64(len(payload)),
				ChecksumMd5:       bbSegmentMD5Hex(payload),
				SegmentBurnResult: burnbridgev1.SegmentBurnResult_SEGMENT_BURN_RESULT_OK,
			},
			{
				JobId:          jobID,
				UploadComplete: true,
				BytesReceived:  int64(len(payload)),
				ChecksumMd5:    payloadMD5,
			},
		},
	}
	b := &BurnBridge{
		meta:           store,
		activeBucket:   bucket,
		volumeLabelRaw: bucket,
		udfLabel:       bucket,
		chunkSize:      len(payload),
		putQueueSem:    make(chan struct{}, 4),
		grpc: testBurnBridgeClient{
			testUnitReadyFn: func(context.Context, *burnbridgev1.TestUnitReadyRequest, ...grpc.CallOption) (*burnbridgev1.TestUnitReadyResponse, error) {
				atomic.AddInt32(&readyCalls, 1)
				return &burnbridgev1.TestUnitReadyResponse{Ready: true, VolumeLabel: bucket, WritableState: "Appendable"}, nil
			},
			uploadObjectFn: func(context.Context, ...grpc.CallOption) (grpc.BidiStreamingClient[burnbridgev1.UploadObjectChunk, burnbridgev1.UploadObjectAck], error) {
				return stream, nil
			},
		},
	}
	b.recordRecorderReadyState(&burnbridgev1.TestUnitReadyResponse{
		Ready:         true,
		VolumeLabel:   bucket,
		WritableState: "Appendable",
	})

	out, err := b.UploadPart(context.Background(), &s3.UploadPartInput{
		Bucket:        ptr(bucket),
		Key:           ptr(key),
		UploadId:      ptr(uploadID),
		PartNumber:    ptr(int32(1)),
		Body:          bytes.NewReader(payload),
		ContentLength: ptr(int64(len(payload))),
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := atomic.LoadInt32(&readyCalls); got != 0 {
		t.Fatalf("expected UploadPart to reuse cached ready state without TestUnitReady, got %d calls", got)
	}
	if out == nil || out.ETag == nil || *out.ETag != quotedETag(payloadMD5) {
		t.Fatalf("unexpected upload part output: %#v", out)
	}
}

func TestUploadPartReusesBurnedBytesForFailedRetry(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "meta.db")
	store, err := meta.NewSqlMeta(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	const (
		bucket   = "DISC-A"
		key      = "multipart.bin"
		uploadID = "upload-002"
	)
	payload := bytes.Repeat([]byte("P"), 2048)
	digest := bbSegmentMD5Hex(payload)

	if err := store.UpsertBurnUploadSession(meta.BurnUploadSessionRecord{
		Bucket:         bucket,
		ObjectName:     key,
		UploadID:       uploadID,
		Kind:           meta.BurnUploadKindMultipart,
		State:          meta.BurnUploadStateFailed,
		MediaID:        "DISC-A",
		RecorderJobID:  "job-part",
		ContentLength:  int64(len(payload)),
		BytesReceived:  int64(len(payload)),
		NextPartNumber: 1,
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertBurnUploadPart(meta.BurnUploadPartRecord{
		Bucket:        bucket,
		ObjectName:    key,
		UploadID:      uploadID,
		PartNumber:    1,
		StartOffset:   0,
		BytesReceived: int64(len(payload)),
		PartSize:      int64(len(payload)),
		ChecksumMD5:   digest,
		State:         meta.BurnUploadStateFailed,
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertBurnObjectSegment(bucket, multipartSessionObjectKey(uploadID), "DISC-A", 0, 0, int64(len(payload)), digest, meta.BurnSegmentSucceeded, []meta.BurnDiscExtent{
		{DiscAddress: "7000000", FileSize: int64(len(payload))},
	}); err != nil {
		t.Fatal(err)
	}

	stream := &testUploadObjectStream{
		ackQueue: []*burnbridgev1.UploadObjectAck{
			{
				JobId:             "job-part",
				SegmentIndex:      0,
				ByteOffset:        0,
				ByteSize:          int64(len(payload)),
				UploadComplete:    true,
				BytesReceived:     int64(len(payload)),
				SegmentBurnResult: burnbridgev1.SegmentBurnResult_SEGMENT_BURN_RESULT_OK,
			},
		},
	}
	b := &BurnBridge{
		meta: store,
		grpc: testBurnBridgeClient{
			uploadObjectFn: func(context.Context, ...grpc.CallOption) (grpc.BidiStreamingClient[burnbridgev1.UploadObjectChunk, burnbridgev1.UploadObjectAck], error) {
				return stream, nil
			},
		},
		chunkSize:        len(payload) + 1024,
		cancelJobTimeout: time.Second,
		putQueueSem:      make(chan struct{}, 4),
		activeBucket:     bucket,
		volumeLabelRaw:   "DISC-A",
		udfLabel:         "DISC-A",
	}

	out, err := b.UploadPart(context.Background(), &s3.UploadPartInput{
		Bucket:        ptr(bucket),
		Key:           ptr(key),
		UploadId:      ptr(uploadID),
		PartNumber:    ptr(int32(1)),
		Body:          bytes.NewReader(payload),
		ContentLength: ptr(int64(len(payload))),
	})
	if err != nil {
		t.Fatal(err)
	}
	if out == nil || out.ETag == nil || *out.ETag != quotedETag(digest) {
		t.Fatalf("unexpected upload part etag: %#v", out)
	}
	if len(stream.sendChunks) != 0 {
		t.Fatalf("expected no recorder replay when failed part bytes are already durable, got %#v", stream.sendChunks)
	}

	part, err := store.GetBurnUploadPart(bucket, key, uploadID, 1)
	if err != nil {
		t.Fatal(err)
	}
	if part.State != meta.BurnUploadStateCompleted {
		t.Fatalf("expected completed part state, got %s", part.State)
	}
	if part.BytesReceived != int64(len(payload)) || part.StartOffset != 0 {
		t.Fatalf("unexpected resumed part offsets: %#v", part)
	}
	session, err := store.GetBurnUploadSession(bucket, key, uploadID)
	if err != nil {
		t.Fatal(err)
	}
	if session.NextPartNumber != 2 || session.BytesReceived != int64(len(payload)) {
		t.Fatalf("unexpected session after resumed retry: %#v", session)
	}
}

func TestUploadPartAcceptsStreamLocalAckIndicesForLaterParts(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "meta.db")
	store, err := meta.NewSqlMeta(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	const (
		bucket   = "DISC-A"
		key      = "multipart.bin"
		uploadID = "upload-003"
	)
	part1 := bytes.Repeat([]byte("A"), 4096)
	part2 := bytes.Repeat([]byte("B"), 4096)
	part2MD5 := bbSegmentMD5Hex(part2)

	if err := store.UpsertBurnUploadSession(meta.BurnUploadSessionRecord{
		Bucket:         bucket,
		ObjectName:     key,
		UploadID:       uploadID,
		Kind:           meta.BurnUploadKindMultipart,
		State:          meta.BurnUploadStateWriting,
		MediaID:        "DISC-A",
		RecorderJobID:  "job-part-2",
		ContentLength:  int64(len(part1)),
		BytesReceived:  int64(len(part1)),
		NextPartNumber: 2,
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertBurnUploadPart(meta.BurnUploadPartRecord{
		Bucket:        bucket,
		ObjectName:    key,
		UploadID:      uploadID,
		PartNumber:    1,
		StartOffset:   0,
		BytesReceived: int64(len(part1)),
		PartSize:      int64(len(part1)),
		ChecksumMD5:   bbSegmentMD5Hex(part1),
		ETag:          quotedETag(bbSegmentMD5Hex(part1)),
		State:         meta.BurnUploadStateCompleted,
		SegmentCount:  2,
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertBurnObjectSegment(bucket, multipartSessionObjectKey(uploadID), "DISC-A", 0, 0, 2048, bbSegmentMD5Hex(part1[:2048]), meta.BurnSegmentSucceeded, []meta.BurnDiscExtent{
		{DiscAddress: "7000000", FileSize: 2048},
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertBurnObjectSegment(bucket, multipartSessionObjectKey(uploadID), "DISC-A", 1, 2048, 2048, bbSegmentMD5Hex(part1[2048:]), meta.BurnSegmentSucceeded, []meta.BurnDiscExtent{
		{DiscAddress: "7002048", FileSize: 2048},
	}); err != nil {
		t.Fatal(err)
	}

	stream := &testUploadObjectStream{
		ackQueue: []*burnbridgev1.UploadObjectAck{
			{
				JobId:             "job-part-2",
				SegmentIndex:      0,
				ByteOffset:        4096,
				ByteSize:          4096,
				UploadComplete:    false,
				BytesReceived:     8192,
				SegmentBurnResult: burnbridgev1.SegmentBurnResult_SEGMENT_BURN_RESULT_OK,
			},
		},
	}
	b := &BurnBridge{
		meta: store,
		grpc: testBurnBridgeClient{
			uploadObjectFn: func(context.Context, ...grpc.CallOption) (grpc.BidiStreamingClient[burnbridgev1.UploadObjectChunk, burnbridgev1.UploadObjectAck], error) {
				return stream, nil
			},
		},
		chunkSize:        len(part2),
		cancelJobTimeout: time.Second,
		putQueueSem:      make(chan struct{}, 4),
		activeBucket:     bucket,
		volumeLabelRaw:   "DISC-A",
		udfLabel:         "DISC-A",
	}

	out, err := b.UploadPart(context.Background(), &s3.UploadPartInput{
		Bucket:        ptr(bucket),
		Key:           ptr(key),
		UploadId:      ptr(uploadID),
		PartNumber:    ptr(int32(2)),
		Body:          bytes.NewReader(part2),
		ContentLength: ptr(int64(len(part2))),
	})
	if err != nil {
		t.Fatal(err)
	}
	if out == nil || out.ETag == nil || *out.ETag != quotedETag(part2MD5) {
		t.Fatalf("unexpected upload part etag: %#v", out)
	}
	if len(stream.sendChunks) != 1 {
		t.Fatalf("expected one multipart stream send, got %d", len(stream.sendChunks))
	}
	if stream.sendChunks[0].GetOffset() != int64(len(part1)) {
		t.Fatalf("unexpected multipart send offsets: %#v", stream.sendChunks)
	}

	part, err := store.GetBurnUploadPart(bucket, key, uploadID, 2)
	if err != nil {
		t.Fatal(err)
	}
	if part.State != meta.BurnUploadStateCompleted || part.StartOffset != int64(len(part1)) || part.SegmentCount != 1 {
		t.Fatalf("unexpected later multipart part state: %#v", part)
	}
	session, err := store.GetBurnUploadSession(bucket, key, uploadID)
	if err != nil {
		t.Fatal(err)
	}
	if session.NextPartNumber != 3 || session.BytesReceived != int64(len(part1)+len(part2)) {
		t.Fatalf("unexpected multipart session after later part upload: %#v", session)
	}
	segments, err := store.ListBurnObjectSegments(bucket, multipartSessionObjectKey(uploadID))
	if err != nil {
		t.Fatal(err)
	}
	if len(segments) != 3 {
		t.Fatalf("expected three shadow multipart segments after second part, got %d", len(segments))
	}
	if segments[2].SegmentIndex != 2 || segments[2].ByteOffset != int64(len(part1)) {
		t.Fatalf("unexpected third segment after second part: %#v", segments[2])
	}
}

func TestMultipartContinuousStreamDefersUnalignedTailUntilComplete(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "meta.db")
	store, err := meta.NewSqlMeta(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	const (
		bucket   = "DISC-A"
		key      = "multipart-tail.bin"
		uploadID = "upload-tail"
		jobID    = "job-tail"
	)
	part1 := bytes.Repeat([]byte("A"), backend.MinPartSize)
	part2 := bytes.Repeat([]byte("B"), 927000)
	digest1 := bbSegmentMD5Hex(part1)
	digest2 := bbSegmentMD5Hex(part2)
	etag1 := quotedETag(digest1)
	etag2 := quotedETag(digest2)
	totalSize := int64(len(part1) + len(part2))
	tailSize := len(part2) % 2048
	alignedPart2Size := len(part2) - tailSize

	if err := store.StoreBurnbridgeDiscInfo(&meta.BurnbridgeDiscInfoDocument{
		Bucket:                bucket,
		WritableState:         "Appendable",
		TotalCapacityBytes:    totalSize + 4096,
		FreeCapacityBytes:     totalSize + 4096,
		WritableCapacityBytes: totalSize + 4096,
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertBurnUploadSession(meta.BurnUploadSessionRecord{
		Bucket:         bucket,
		ObjectName:     key,
		UploadID:       uploadID,
		Kind:           meta.BurnUploadKindMultipart,
		State:          meta.BurnUploadStateWriting,
		MediaID:        bucket,
		RecorderJobID:  jobID,
		BytesReceived:  0,
		NextPartNumber: 1,
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.StoreAttribute(nil, bucket, key, multipartInitAttribute(uploadID), []byte(`{}`)); err != nil {
		t.Fatal(err)
	}

	stream := &testUploadObjectStream{
		ackQueue: []*burnbridgev1.UploadObjectAck{
			{
				JobId:             jobID,
				SegmentIndex:      0,
				ByteOffset:        0,
				ByteSize:          int64(len(part1)),
				DiscExtents:       []*burnbridgev1.DiscExtent{{DiscAddress: "9000000", FileSize: int64(len(part1))}},
				SegmentBurnResult: burnbridgev1.SegmentBurnResult_SEGMENT_BURN_RESULT_OK,
			},
			{
				JobId:             jobID,
				SegmentIndex:      1,
				ByteOffset:        int64(len(part1)),
				ByteSize:          int64(alignedPart2Size),
				DiscExtents:       []*burnbridgev1.DiscExtent{{DiscAddress: "9005242880", FileSize: int64(alignedPart2Size)}},
				SegmentBurnResult: burnbridgev1.SegmentBurnResult_SEGMENT_BURN_RESULT_OK,
			},
			{
				JobId:             jobID,
				SegmentIndex:      2,
				ByteOffset:        int64(len(part1) + alignedPart2Size),
				ByteSize:          int64(tailSize),
				UploadComplete:    true,
				BytesReceived:     totalSize,
				DiscExtents:       []*burnbridgev1.DiscExtent{{DiscAddress: "9006168576", FileSize: int64(tailSize)}},
				SegmentBurnResult: burnbridgev1.SegmentBurnResult_SEGMENT_BURN_RESULT_OK,
			},
		},
	}
	var commitReq *burnbridgev1.CommitJobRequest
	b := &BurnBridge{
		meta:           store,
		activeBucket:   bucket,
		volumeLabelRaw: bucket,
		udfLabel:       bucket,
		chunkSize:      backend.MinPartSize,
		putQueueSem:    make(chan struct{}, 4),
		grpc: testBurnBridgeClient{
			uploadObjectFn: func(context.Context, ...grpc.CallOption) (grpc.BidiStreamingClient[burnbridgev1.UploadObjectChunk, burnbridgev1.UploadObjectAck], error) {
				return stream, nil
			},
			commitJobFn: func(_ context.Context, req *burnbridgev1.CommitJobRequest, _ ...grpc.CallOption) (*burnbridgev1.CommitJobResponse, error) {
				commitReq = req
				return &burnbridgev1.CommitJobResponse{JobId: jobID, Status: "layout_persisted"}, nil
			},
		},
	}
	b.recordRecorderReadyState(&burnbridgev1.TestUnitReadyResponse{
		Ready:         true,
		VolumeLabel:   bucket,
		WritableState: "Appendable",
	})

	if _, err := b.UploadPart(context.Background(), &s3.UploadPartInput{
		Bucket:        ptr(bucket),
		Key:           ptr(key),
		UploadId:      ptr(uploadID),
		PartNumber:    ptr(int32(1)),
		Body:          bytes.NewReader(part1),
		ContentLength: ptr(int64(len(part1))),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := b.UploadPart(context.Background(), &s3.UploadPartInput{
		Bucket:        ptr(bucket),
		Key:           ptr(key),
		UploadId:      ptr(uploadID),
		PartNumber:    ptr(int32(2)),
		Body:          bytes.NewReader(part2),
		ContentLength: ptr(int64(len(part2))),
	}); err != nil {
		t.Fatal(err)
	}
	sendsBeforeComplete := len(stream.sendChunks)
	if sendsBeforeComplete < 2 {
		t.Fatalf("expected aligned payload sends before complete, got %d", sendsBeforeComplete)
	}
	if stream.sendChunks[sendsBeforeComplete-1].GetEof() {
		t.Fatal("did not expect EOF before CompleteMultipartUpload")
	}

	_, _, err = b.CompleteMultipartUpload(context.Background(), &s3.CompleteMultipartUploadInput{
		Bucket:   ptr(bucket),
		Key:      ptr(key),
		UploadId: ptr(uploadID),
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: []types.CompletedPart{
				{PartNumber: ptr(int32(1)), ETag: ptr(etag1)},
				{PartNumber: ptr(int32(2)), ETag: ptr(etag2)},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(stream.sendChunks) != sendsBeforeComplete+1 {
		t.Fatalf("expected CompleteMultipartUpload to send one deferred tail chunk, got before=%d after=%d", sendsBeforeComplete, len(stream.sendChunks))
	}
	tail := stream.sendChunks[len(stream.sendChunks)-1]
	if !tail.GetEof() || len(tail.GetData()) != tailSize {
		t.Fatalf("expected deferred tail EOF chunk, got eof=%v size=%d", tail.GetEof(), len(tail.GetData()))
	}
	if commitReq == nil || commitReq.FinalizeManifest == nil || len(commitReq.FinalizeManifest.Files) != 1 || len(commitReq.FinalizeManifest.Files[0].Segments) != 3 {
		t.Fatalf("expected finalize manifest to include deferred tail segment, got %#v", commitReq)
	}
}

func TestCompleteMultipartUploadCommitsMergedManifest(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "meta.db")
	store, err := meta.NewSqlMeta(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	const (
		bucket   = "DISC-A"
		key      = "multipart-final.bin"
		uploadID = "upload-003"
	)
	part1 := bytes.Repeat([]byte("A"), backend.MinPartSize)
	part2 := bytes.Repeat([]byte("B"), 2048)
	digest1 := bbSegmentMD5Hex(part1)
	digest2 := bbSegmentMD5Hex(part2)
	etag1 := quotedETag(digest1)
	etag2 := quotedETag(digest2)
	totalSize := int64(len(part1) + len(part2))

	if err := store.UpsertBurnUploadSession(meta.BurnUploadSessionRecord{
		Bucket:         bucket,
		ObjectName:     key,
		UploadID:       uploadID,
		Kind:           meta.BurnUploadKindMultipart,
		State:          meta.BurnUploadStateWriting,
		MediaID:        "DISC-A",
		RecorderJobID:  "job-final",
		ContentLength:  totalSize,
		BytesReceived:  totalSize,
		NextPartNumber: 3,
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertBurnUploadPart(meta.BurnUploadPartRecord{
		Bucket:        bucket,
		ObjectName:    key,
		UploadID:      uploadID,
		PartNumber:    1,
		StartOffset:   0,
		BytesReceived: int64(len(part1)),
		PartSize:      int64(len(part1)),
		ChecksumMD5:   digest1,
		ETag:          etag1,
		State:         meta.BurnUploadStateCompleted,
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertBurnUploadPart(meta.BurnUploadPartRecord{
		Bucket:        bucket,
		ObjectName:    key,
		UploadID:      uploadID,
		PartNumber:    2,
		StartOffset:   int64(len(part1)),
		BytesReceived: int64(len(part2)),
		PartSize:      int64(len(part2)),
		ChecksumMD5:   digest2,
		ETag:          etag2,
		State:         meta.BurnUploadStateCompleted,
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertBurnObjectSegment(bucket, multipartSessionObjectKey(uploadID), "DISC-A", 0, 0, int64(len(part1)), digest1, meta.BurnSegmentSucceeded, []meta.BurnDiscExtent{
		{DiscAddress: "8000000", FileSize: int64(len(part1))},
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertBurnObjectSegment(bucket, multipartSessionObjectKey(uploadID), "DISC-A", 1, int64(len(part1)), int64(len(part2)), digest2, meta.BurnSegmentSucceeded, []meta.BurnDiscExtent{
		{DiscAddress: "8001024", FileSize: int64(len(part2))},
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertBurnUploadSession(meta.BurnUploadSessionRecord{
		Bucket:         bucket,
		ObjectName:     key,
		UploadID:       "stale-upload",
		Kind:           meta.BurnUploadKindMultipart,
		State:          meta.BurnUploadStateFailed,
		MediaID:        "DISC-A",
		RecorderJobID:  "job-stale",
		ContentLength:  int64(len(part1)),
		BytesReceived:  int64(len(part1)),
		NextPartNumber: 2,
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertBurnUploadPart(meta.BurnUploadPartRecord{
		Bucket:        bucket,
		ObjectName:    key,
		UploadID:      "stale-upload",
		PartNumber:    1,
		StartOffset:   0,
		BytesReceived: int64(len(part1)),
		PartSize:      int64(len(part1)),
		ChecksumMD5:   digest1,
		ETag:          etag1,
		State:         meta.BurnUploadStateCompleted,
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertBurnObjectSegment(bucket, multipartSessionObjectKey("stale-upload"), "DISC-A", 0, 0, int64(len(part1)), digest1, meta.BurnSegmentSucceeded, []meta.BurnDiscExtent{
		{DiscAddress: "7000000", FileSize: int64(len(part1))},
	}); err != nil {
		t.Fatal(err)
	}

	b := &BurnBridge{
		meta:             store,
		chunkSize:        4096,
		cancelJobTimeout: time.Second,
		putQueueSem:      make(chan struct{}, 4),
		activeBucket:     bucket,
		volumeLabelRaw:   "DISC-A",
		udfLabel:         "DISC-A",
	}
	if err := b.storeMultipartInitState(bucket, key, uploadID, buildMultipartInitState(s3response.CreateMultipartUploadInput{
		Bucket:      ptr(bucket),
		Key:         ptr(key),
		ContentType: ptr("application/x-multipart-test"),
		Metadata:    map[string]string{"owner": "drive"},
	})); err != nil {
		t.Fatal(err)
	}
	if err := b.storeMultipartInitState(bucket, key, "stale-upload", buildMultipartInitState(s3response.CreateMultipartUploadInput{
		Bucket: ptr(bucket),
		Key:    ptr(key),
	})); err != nil {
		t.Fatal(err)
	}

	var commitReq *burnbridgev1.CommitJobRequest
	b.grpc = testBurnBridgeClient{
		commitJobFn: func(_ context.Context, req *burnbridgev1.CommitJobRequest, _ ...grpc.CallOption) (*burnbridgev1.CommitJobResponse, error) {
			commitReq = req
			return &burnbridgev1.CommitJobResponse{JobId: "job-final", Status: "layout_persisted"}, nil
		},
	}

	expectedETag, err := backend.GetMultipartMD5([]types.CompletedPart{
		{PartNumber: ptr(int32(1)), ETag: ptr(etag1)},
		{PartNumber: ptr(int32(2)), ETag: ptr(etag2)},
	})
	if err != nil {
		t.Fatal(err)
	}

	res, _, err := b.CompleteMultipartUpload(context.Background(), &s3.CompleteMultipartUploadInput{
		Bucket:   ptr(bucket),
		Key:      ptr(key),
		UploadId: ptr(uploadID),
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: []types.CompletedPart{
				{PartNumber: ptr(int32(1)), ETag: ptr(etag1)},
				{PartNumber: ptr(int32(2)), ETag: ptr(etag2)},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.ETag == nil || *res.ETag != expectedETag {
		t.Fatalf("unexpected complete result etag: %#v", res.ETag)
	}
	if commitReq == nil || commitReq.FinalizeManifest == nil || len(commitReq.FinalizeManifest.Files) != 1 {
		t.Fatalf("unexpected CommitJob request: %#v", commitReq)
	}
	if commitReq.GetCommittedEtag() != expectedETag {
		t.Fatalf("unexpected CommitJob committed etag: got=%q want=%q", commitReq.GetCommittedEtag(), expectedETag)
	}
	file := commitReq.FinalizeManifest.Files[0]
	if file.ObjectKey != key || file.FileSize != totalSize || len(file.Segments) != 2 {
		t.Fatalf("unexpected finalize file: %#v", file)
	}
	if file.Segments[0].ByteOffset != 0 || file.Segments[1].ByteOffset != int64(len(part1)) {
		t.Fatalf("unexpected merged segment offsets: %#v", file.Segments)
	}

	committedRec, err := store.GetBurnbridgeCommittedRecord(bucket, key)
	if err != nil {
		t.Fatal(err)
	}
	if committedRec.ETag != expectedETag || committedRec.ContentType != "application/x-multipart-test" || committedRec.Metadata["owner"] != "drive" {
		t.Fatalf("unexpected committed record: %#v", committedRec)
	}
	finalSegments, err := store.ListBurnObjectSegments(bucket, key)
	if err != nil {
		t.Fatal(err)
	}
	if len(finalSegments) != 2 || finalSegments[1].ByteOffset != int64(len(part1)) {
		t.Fatalf("unexpected persisted merged segments: %#v", finalSegments)
	}
	if _, err := store.GetBurnUploadSession(bucket, key, uploadID); err == nil {
		t.Fatal("expected multipart session cleanup after complete")
	}
	if _, err := store.GetBurnUploadPart(bucket, key, uploadID, 1); err == nil {
		t.Fatal("expected multipart part cleanup after complete")
	}
	if _, err := store.GetBurnUploadSession(bucket, key, "stale-upload"); err == nil {
		t.Fatal("expected stale multipart session cleanup after complete")
	}
	if _, err := store.GetBurnUploadPart(bucket, key, "stale-upload", 1); err == nil {
		t.Fatal("expected stale multipart part cleanup after complete")
	}
	if _, err := b.loadMultipartInitState(bucket, key, "stale-upload"); !errors.Is(err, meta.ErrNoSuchKey) {
		t.Fatalf("expected stale multipart init cleanup after complete, got %v", err)
	}
	mpMeta, err := b.loadMultipartObjectMetadata(bucket, key)
	if err != nil {
		t.Fatal(err)
	}
	if mpMeta.UploadID != uploadID || mpMeta.ETag != expectedETag || len(mpMeta.Parts) != 2 || mpMeta.Parts[1] != totalSize {
		t.Fatalf("unexpected multipart object metadata: %#v", mpMeta)
	}
	if shadowSegments, err := store.ListBurnObjectSegments(bucket, multipartSessionObjectKey(uploadID)); err != nil {
		t.Fatal(err)
	} else if len(shadowSegments) != 0 {
		t.Fatalf("expected shadow multipart segments to be cleaned up, got %#v", shadowSegments)
	}
	if shadowSegments, err := store.ListBurnObjectSegments(bucket, multipartSessionObjectKey("stale-upload")); err != nil {
		t.Fatal(err)
	} else if len(shadowSegments) != 0 {
		t.Fatalf("expected stale shadow multipart segments to be cleaned up, got %#v", shadowSegments)
	}
}

func TestMultipartSessionVisibleAcceptsActiveBucketMediaID(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "meta.db")
	store, err := meta.NewSqlMeta(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	const bucket = "e60502460000000026426a43"
	b := &BurnBridge{
		meta:           store,
		activeBucket:   bucket,
		volumeLabelRaw: "E60502460000000026426A43",
	}

	session := &meta.BurnUploadSessionRecord{
		Bucket:     bucket,
		ObjectName: "large.bin",
		UploadID:   "upload-001",
		Kind:       meta.BurnUploadKindMultipart,
		MediaID:    bucket,
	}

	if !b.multipartSessionVisibleOnCurrentMedia(bucket, session) {
		t.Fatal("expected multipart session with active bucket media id to be visible on current media")
	}
}

func TestIsLocalRecorderTarget(t *testing.T) {
	tests := []struct {
		name string
		addr string
		want bool
	}{
		{name: "localhost with port", addr: "localhost:50051", want: true},
		{name: "ipv4 loopback", addr: "127.0.0.1:50051", want: true},
		{name: "ipv6 loopback", addr: "[::1]:50051", want: true},
		{name: "all interfaces ipv4", addr: "0.0.0.0:50051", want: true},
		{name: "all interfaces ipv6", addr: "[::]:50051", want: true},
		{name: "hostname remote", addr: "recorder.example.com:50051", want: false},
		{name: "ipv4 remote", addr: "192.168.1.10:50051", want: false},
		{name: "ipv6 remote", addr: "[2001:db8::10]:50051", want: false},
		{name: "missing port localhost", addr: "localhost", want: true},
		{name: "empty", addr: "", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isLocalRecorderTarget(tt.addr); got != tt.want {
				t.Fatalf("isLocalRecorderTarget(%q) = %v, want %v", tt.addr, got, tt.want)
			}
		})
	}
}

func TestRecorderProbeAddress(t *testing.T) {
	tests := []struct {
		name string
		addr string
		want string
	}{
		{name: "loopback passthrough", addr: "127.0.0.1:50051", want: "127.0.0.1:50051"},
		{name: "localhost passthrough", addr: "localhost:50051", want: "localhost:50051"},
		{name: "wildcard ipv4 remapped", addr: "0.0.0.0:50051", want: "127.0.0.1:50051"},
		{name: "wildcard ipv6 remapped", addr: "[::]:50051", want: "[::1]:50051"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := recorderProbeAddress(tt.addr)
			if err != nil {
				t.Fatalf("recorderProbeAddress(%q) returned error: %v", tt.addr, err)
			}
			if got != tt.want {
				t.Fatalf("recorderProbeAddress(%q) = %q, want %q", tt.addr, got, tt.want)
			}
		})
	}
}

func TestRecorderEndpointReachable(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()

	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, err := ln.Accept()
		if err == nil && conn != nil {
			_ = conn.Close()
		}
	}()

	if !recorderEndpointReachable(ln.Addr().String(), time.Second) {
		t.Fatalf("expected reachable endpoint for %q", ln.Addr().String())
	}

	<-done
}

func TestMaxInt(t *testing.T) {
	if got := maxInt(3, 5); got != 5 {
		t.Fatalf("maxInt(3, 5) = %d, want 5", got)
	}
	if got := maxInt(7, 2); got != 7 {
		t.Fatalf("maxInt(7, 2) = %d, want 7", got)
	}
}

func ptr[T any](v T) *T { return &v }

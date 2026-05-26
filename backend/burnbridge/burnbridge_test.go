package burnbridge

import (
	"bytes"
	"context"
	"encoding/json"
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
	burnbridgev1 "github.com/versity/versitygw/backend/burnbridge/proto"
	"github.com/versity/versitygw/backend/meta"
	"github.com/versity/versitygw/s3response"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type testBurnBridgeClient struct {
	createJobFn           func(context.Context, *burnbridgev1.CreateJobRequest, ...grpc.CallOption) (*burnbridgev1.CreateJobResponse, error)
	uploadObjectFn        func(context.Context, ...grpc.CallOption) (grpc.BidiStreamingClient[burnbridgev1.UploadObjectChunk, burnbridgev1.UploadObjectAck], error)
	commitJobFn           func(context.Context, *burnbridgev1.CommitJobRequest, ...grpc.CallOption) (*burnbridgev1.CommitJobResponse, error)
	cancelJobFn           func(context.Context, *burnbridgev1.CancelJobRequest, ...grpc.CallOption) (*burnbridgev1.CancelJobResponse, error)
	registerPullSourceFn  func(context.Context, *burnbridgev1.RegisterS3ObjectPullSourceRequest, ...grpc.CallOption) (*burnbridgev1.RegisterS3ObjectPullSourceResponse, error)
	finalizeFn            func(context.Context, *burnbridgev1.FinalizeLayoutRequest, ...grpc.CallOption) (*burnbridgev1.FinalizeLayoutResponse, error)
	importedBucketStateFn func(context.Context, *burnbridgev1.GetImportedBucketStateRequest, ...grpc.CallOption) (*burnbridgev1.GetImportedBucketStateResponse, error)
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

func (testBurnBridgeClient) GetJobStatus(context.Context, *burnbridgev1.GetJobStatusRequest, ...grpc.CallOption) (*burnbridgev1.GetJobStatusResponse, error) {
	return &burnbridgev1.GetJobStatusResponse{}, nil
}

func (c testBurnBridgeClient) CancelJob(ctx context.Context, req *burnbridgev1.CancelJobRequest, opts ...grpc.CallOption) (*burnbridgev1.CancelJobResponse, error) {
	if c.cancelJobFn != nil {
		return c.cancelJobFn(ctx, req, opts...)
	}
	return &burnbridgev1.CancelJobResponse{}, nil
}

func (testBurnBridgeClient) ReadObject(context.Context, *burnbridgev1.ReadObjectRequest, ...grpc.CallOption) (grpc.ServerStreamingClient[burnbridgev1.ReadObjectChunk], error) {
	return nil, nil
}

func (c testBurnBridgeClient) RegisterS3ObjectPullSource(ctx context.Context, req *burnbridgev1.RegisterS3ObjectPullSourceRequest, opts ...grpc.CallOption) (*burnbridgev1.RegisterS3ObjectPullSourceResponse, error) {
	if c.registerPullSourceFn != nil {
		return c.registerPullSourceFn(ctx, req, opts...)
	}
	return &burnbridgev1.RegisterS3ObjectPullSourceResponse{}, nil
}

func (testBurnBridgeClient) TestUnitReady(context.Context, *burnbridgev1.TestUnitReadyRequest, ...grpc.CallOption) (*burnbridgev1.TestUnitReadyResponse, error) {
	return &burnbridgev1.TestUnitReadyResponse{Ready: true}, nil
}

func (testBurnBridgeClient) GetDiscInfo(context.Context, *burnbridgev1.GetDiscInfoRequest, ...grpc.CallOption) (*burnbridgev1.GetDiscInfoResponse, error) {
	panic("unexpected GetDiscInfo call")
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
	if b.activeBucket != "archive-20260523" {
		t.Fatalf("active bucket mismatch: %q", b.activeBucket)
	}
	if b.udfLabel != "ARCHIVE-20260523" {
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

	gotACLRaw, err := b.GetBucketAcl(context.Background(), &s3.GetBucketAclInput{Bucket: ptr("archive-20260523")})
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
	if len(userList.Buckets.Bucket) != 1 || userList.Buckets.Bucket[0].Name != "archive-20260523" {
		t.Fatalf("expected user bucket list to contain archive-20260523, got %+v", userList.Buckets.Bucket)
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
		activeBucket: "archive-20260523",
	}

	initialACL, err := json.Marshal(defaultBucketACL("drive"))
	if err != nil {
		t.Fatal(err)
	}
	if err := b.storeBucketACL("archive-20260523", initialACL); err != nil {
		t.Fatal(err)
	}

	if err := b.ChangeBucketOwner(context.Background(), "archive-20260523", "admin"); err != nil {
		t.Fatalf("ChangeBucketOwner returned error: %v", err)
	}

	gotACLRaw, err := b.GetBucketAcl(context.Background(), &s3.GetBucketAclInput{Bucket: ptr("archive-20260523")})
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
		activeBucket: "e60302480000000025891a00",
		udfLabel:     "E60302480000000025891A00",
	}

	res, err := b.ListBuckets(context.Background(), s3response.ListBucketsInput{
		Owner:      "drive",
		MaxBuckets: 1000,
	})
	if err != nil {
		t.Fatalf("ListBuckets returned error: %v", err)
	}
	if len(res.Buckets.Bucket) != 1 || res.Buckets.Bucket[0].Name != "e60302480000000025891a00" {
		t.Fatalf("expected auto-mapped bucket in result, got %+v", res.Buckets.Bucket)
	}

	raw, err := b.GetBucketAcl(context.Background(), &s3.GetBucketAclInput{Bucket: ptr("e60302480000000025891a00")})
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
	if err := store.StoreBurnbridgeCommitted(nil, "bucket1", "file.txt", rec); err != nil {
		t.Fatal(err)
	}

	b := &BurnBridge{
		meta:         store,
		grpc:         testBurnBridgeClient{},
		readMount:    t.TempDir(),
		activeBucket: "bucket1",
	}

	head, err := b.HeadObject(context.Background(), &s3.HeadObjectInput{
		Bucket: ptr("bucket1"),
		Key:    ptr("file.txt"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if head == nil || head.ContentLength == nil || *head.ContentLength != 1234 {
		t.Fatalf("unexpected content length: %#v", head)
	}

	list, err := b.ListObjects(context.Background(), &s3.ListObjectsInput{
		Bucket: ptr("bucket1"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(list.Contents) != 1 || list.Contents[0].Size == nil || *list.Contents[0].Size != 1234 {
		t.Fatalf("unexpected list output: %#v", list.Contents)
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

	b.activeBucket = "disc-a"
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
}

func TestInvokeFinalizeLayoutReusesSuccessfulTranscript(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "meta.db")
	store, err := meta.NewSqlMeta(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	payload, err := json.Marshal(meta.BurnbridgeFinalizeLayoutDocument{
		Bucket:         "bucket1",
		RecorderStatus: "finalized",
		CompletedAtUtc: time.Now().UTC().Format(time.RFC3339Nano),
		GrpcOK:         true,
		GrpcCode:       "OK",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.StoreBurnbridgeFinalizeLayoutJSON("bucket1", payload); err != nil {
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

	got, err := b.invokeFinalizeLayoutAgainstRecorder(context.Background(), "bucket1", false)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(payload) {
		t.Fatalf("expected cached finalize transcript to be reused")
	}
	if calls != 0 {
		t.Fatalf("expected no grpc finalize call, got %d", calls)
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
			results[idx], errs[idx] = b.loadOrFinalizeLayoutTranscript(context.Background(), "bucket1", false)
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

type testUploadObjectStream struct {
	sendChunks []*burnbridgev1.UploadObjectChunk
	ackQueue   []*burnbridgev1.UploadObjectAck
	recvIndex  int
}

func (s *testUploadObjectStream) Header() (metadata.MD, error) { return nil, nil }
func (s *testUploadObjectStream) Trailer() metadata.MD         { return nil }
func (s *testUploadObjectStream) CloseSend() error             { return nil }
func (s *testUploadObjectStream) Context() context.Context     { return context.Background() }
func (s *testUploadObjectStream) SendMsg(any) error            { return nil }
func (s *testUploadObjectStream) RecvMsg(any) error            { return nil }

func (s *testUploadObjectStream) Send(chunk *burnbridgev1.UploadObjectChunk) error {
	s.sendChunks = append(s.sendChunks, chunk)
	return nil
}

func (s *testUploadObjectStream) Recv() (*burnbridgev1.UploadObjectAck, error) {
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
		bucket = "disc-a"
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

func TestPutObjectComputesChecksumWhenTailAckOmitsFinalChecksum(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "meta.db")
	store, err := meta.NewSqlMeta(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	const (
		bucket = "disc-a"
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

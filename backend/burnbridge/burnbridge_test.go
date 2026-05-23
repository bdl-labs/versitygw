package burnbridge

import (
	"context"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/versity/versitygw/backend/meta"
	burnbridgev1 "github.com/versity/versitygw/backend/burnbridge/proto"
	"google.golang.org/grpc"
)

type testBurnBridgeClient struct{}

func (testBurnBridgeClient) CreateJob(context.Context, *burnbridgev1.CreateJobRequest, ...grpc.CallOption) (*burnbridgev1.CreateJobResponse, error) {
	panic("unexpected CreateJob call")
}

func (testBurnBridgeClient) GetVersion(context.Context, *burnbridgev1.GetVersionRequest, ...grpc.CallOption) (*burnbridgev1.GetVersionResponse, error) {
	return &burnbridgev1.GetVersionResponse{}, nil
}

func (testBurnBridgeClient) UploadObject(context.Context, ...grpc.CallOption) (grpc.BidiStreamingClient[burnbridgev1.UploadObjectChunk, burnbridgev1.UploadObjectAck], error) {
	return nil, nil
}

func (testBurnBridgeClient) CommitJob(context.Context, *burnbridgev1.CommitJobRequest, ...grpc.CallOption) (*burnbridgev1.CommitJobResponse, error) {
	panic("unexpected CommitJob call")
}

func (testBurnBridgeClient) GetJobStatus(context.Context, *burnbridgev1.GetJobStatusRequest, ...grpc.CallOption) (*burnbridgev1.GetJobStatusResponse, error) {
	return &burnbridgev1.GetJobStatusResponse{}, nil
}

func (testBurnBridgeClient) CancelJob(context.Context, *burnbridgev1.CancelJobRequest, ...grpc.CallOption) (*burnbridgev1.CancelJobResponse, error) {
	return &burnbridgev1.CancelJobResponse{}, nil
}

func (testBurnBridgeClient) ReadObject(context.Context, *burnbridgev1.ReadObjectRequest, ...grpc.CallOption) (grpc.ServerStreamingClient[burnbridgev1.ReadObjectChunk], error) {
	return nil, nil
}

func (testBurnBridgeClient) RegisterS3ObjectPullSource(context.Context, *burnbridgev1.RegisterS3ObjectPullSourceRequest, ...grpc.CallOption) (*burnbridgev1.RegisterS3ObjectPullSourceResponse, error) {
	panic("unexpected RegisterS3ObjectPullSource call")
}

func (testBurnBridgeClient) TestUnitReady(context.Context, *burnbridgev1.TestUnitReadyRequest, ...grpc.CallOption) (*burnbridgev1.TestUnitReadyResponse, error) {
	return &burnbridgev1.TestUnitReadyResponse{Ready: true}, nil
}

func (testBurnBridgeClient) GetDiscInfo(context.Context, *burnbridgev1.GetDiscInfoRequest, ...grpc.CallOption) (*burnbridgev1.GetDiscInfoResponse, error) {
	panic("unexpected GetDiscInfo call")
}

func (testBurnBridgeClient) FinalizeLayout(context.Context, *burnbridgev1.FinalizeLayoutRequest, ...grpc.CallOption) (*burnbridgev1.FinalizeLayoutResponse, error) {
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
		meta:        store,
		grpc:        testBurnBridgeClient{},
		readMount:   t.TempDir(),
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

func ptr[T any](v T) *T { return &v }

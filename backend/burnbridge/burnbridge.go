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
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/versity/versitygw/backend"
	burnbridgev1 "github.com/versity/versitygw/backend/burnbridge/proto"
	meta "github.com/versity/versitygw/backend/meta"
	"github.com/versity/versitygw/s3response"
	"google.golang.org/grpc"
)

// emptyQuotedMD5 is the S3-style quoted ETag for zero-length payload.
const emptyQuotedMD5 = "\"d41d8cd98f00b204e9800998ecf8427e\""

// Options configures the BurnBridge backend.
type Options struct {
	DBPath string

	GRPCAddr               string
	GRPCUseTLS             bool
	GRPCCAFile             string
	GRPCServerName         string
	GRPCInsecureSkipVerify bool
	GRPCSkipPing           bool

	UDFVolumeLabel string
	ChunkSize      int
}

type Backend struct {
	backend.BackendUnsupported
	meta      meta.SqlMeta
	grpc      burnbridgev1.BurnBridgeClient
	grpcConn  *grpc.ClientConn
	chunkSize int
	udfLabel  string
}

var _ backend.Backend = &Backend{}

// New constructs a BurnBridge backend using SQL metadata and a BurnBridge gRPC endpoint.
func New(opts Options) (*Backend, error) {
	if opts.DBPath == "" {
		return nil, fmt.Errorf("burnbridge: db path required")
	}
	if opts.GRPCAddr == "" {
		return nil, fmt.Errorf("burnbridge: grpc address required (e.g. 127.0.0.1:50051)")
	}
	if opts.ChunkSize <= 0 {
		opts.ChunkSize = 1 << 20
	}

	metaStore, err := meta.NewSqlMeta(opts.DBPath)
	if err != nil {
		return nil, err
	}

	dialCtx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	conn, err := dialBurnBridgeGRPC(dialCtx, opts.GRPCAddr, opts.GRPCUseTLS, opts.GRPCCAFile, opts.GRPCServerName, opts.GRPCInsecureSkipVerify)
	if err != nil {
		return nil, err
	}

	client := burnbridgev1.NewBurnBridgeClient(conn)
	if !opts.GRPCSkipPing {
		pingCtx, pingCancel := context.WithTimeout(dialCtx, 10*time.Second)
		pingErr := grpcConnectivityPing(pingCtx, client)
		pingCancel()
		if pingErr != nil {
			_ = conn.Close()
			return nil, fmt.Errorf("burnbridge grpc ping: %w", pingErr)
		}
	}

	return &Backend{
		meta:      metaStore,
		grpc:      client,
		grpcConn:  conn,
		chunkSize: opts.ChunkSize,
		udfLabel:  opts.UDFVolumeLabel,
	}, nil
}

// Close releases gRPC resources.
func (b *Backend) Close() error {
	if b.grpcConn == nil {
		return nil
	}
	err := b.grpcConn.Close()
	b.grpcConn = nil
	return err
}

func (b *Backend) String() string { return "BurnBridge" }

func (b *Backend) ListBuckets(context.Context, s3response.ListBucketsInput) (s3response.ListAllMyBucketsResult, error) {
	return s3response.ListAllMyBucketsResult{
		Buckets: s3response.ListAllMyBucketsList{
			Bucket: []s3response.ListAllMyBucketsEntry{{Name: "burn-jobs", CreationDate: time.Now()}},
		},
	}, nil
}

func (b *Backend) PutObject(ctx context.Context, input s3response.PutObjectInput) (s3response.PutObjectOutput, error) {
	if input.Bucket == nil || input.Key == nil {
		return s3response.PutObjectOutput{}, fmt.Errorf("bucket/key required")
	}

	bucket := *input.Bucket
	key := *input.Key

	metaSlice := objectMetadataProto(&input)
	var contentLen int64
	if input.ContentLength != nil {
		contentLen = *input.ContentLength
	}

	createResp, err := b.grpc.CreateJob(ctx, &burnbridgev1.CreateJobRequest{
		Bucket:        bucket,
		ObjectKey:     key,
		ContentLength: contentLen,
		Metadata:      metaSlice,
	})
	if err != nil {
		return s3response.PutObjectOutput{}, err
	}
	jobID := createResp.GetJobId()
	if jobID == "" {
		return s3response.PutObjectOutput{}, fmt.Errorf("burnbridge: empty job id from CreateJob")
	}

	var committed bool
	defer func() {
		if committed || jobID == "" {
			return
		}
		cctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = b.grpc.CancelJob(cctx, &burnbridgev1.CancelJobRequest{JobId: jobID})
	}()

	stream, err := b.grpc.UploadObject(ctx)
	if err != nil {
		return s3response.PutObjectOutput{}, err
	}

	var offset int64
	body := input.Body
	if body == nil {
		if err := stream.Send(&burnbridgev1.UploadObjectChunk{JobId: jobID, Offset: offset, Eof: true}); err != nil {
			return s3response.PutObjectOutput{}, err
		}
	} else {
		buf := make([]byte, b.chunkSize)
		for {
			n, readErr := body.Read(buf)
			if n > 0 {
				if err := stream.Send(&burnbridgev1.UploadObjectChunk{JobId: jobID, Offset: offset, Data: buf[:n]}); err != nil {
					return s3response.PutObjectOutput{}, err
				}
				offset += int64(n)
			}
			if readErr != nil {
				if readErr == io.EOF {
					break
				}
				return s3response.PutObjectOutput{}, readErr
			}
		}
		if err := stream.Send(&burnbridgev1.UploadObjectChunk{JobId: jobID, Offset: offset, Eof: true}); err != nil {
			return s3response.PutObjectOutput{}, err
		}
	}

	uploadResp, err := stream.CloseAndRecv()
	if err != nil {
		return s3response.PutObjectOutput{}, err
	}

	commitResp, err := b.grpc.CommitJob(ctx, &burnbridgev1.CommitJobRequest{
		JobId:          jobID,
		UdfVolumeLabel: b.udfLabel,
	})
	if err != nil {
		return s3response.PutObjectOutput{}, err
	}
	committed = true

	etag := quotedETag(uploadResp.GetChecksumMd5())
	checksumMD5 := uploadResp.GetChecksumMd5()

	if err := b.meta.StoreAttribute(nil, bucket, key, "burnbridge-job-id", []byte(jobID)); err != nil {
		return s3response.PutObjectOutput{}, err
	}
	if err := b.meta.StoreAttribute(nil, bucket, key, "burnbridge-status", []byte(commitResp.GetStatus())); err != nil {
		return s3response.PutObjectOutput{}, err
	}
	if err := b.meta.StoreAttribute(nil, bucket, key, "burnbridge-etag", []byte(etag)); err != nil {
		return s3response.PutObjectOutput{}, err
	}

	if len(input.Metadata) > 0 {
		for mk, mv := range input.Metadata {
			if err := b.meta.StoreAttribute(nil, bucket, key, mk, []byte(mv)); err != nil {
				return s3response.PutObjectOutput{}, err
			}
		}
	}

	out := s3response.PutObjectOutput{
		ETag: etag,
		Size: &offset,
	}
	if checksumMD5 != "" {
		out.ChecksumMD5 = &checksumMD5
	}
	return out, nil
}

func objectMetadataProto(input *s3response.PutObjectInput) []*burnbridgev1.ObjectMetadata {
	var md []*burnbridgev1.ObjectMetadata
	for k, v := range input.Metadata {
		md = append(md, &burnbridgev1.ObjectMetadata{Key: k, Value: v})
	}
	if input.ContentType != nil && *input.ContentType != "" {
		md = append(md, &burnbridgev1.ObjectMetadata{Key: "content-type", Value: *input.ContentType})
	}
	if input.ContentEncoding != nil && *input.ContentEncoding != "" {
		md = append(md, &burnbridgev1.ObjectMetadata{Key: "content-encoding", Value: *input.ContentEncoding})
	}
	if input.ContentDisposition != nil && *input.ContentDisposition != "" {
		md = append(md, &burnbridgev1.ObjectMetadata{Key: "content-disposition", Value: *input.ContentDisposition})
	}
	if input.CacheControl != nil && *input.CacheControl != "" {
		md = append(md, &burnbridgev1.ObjectMetadata{Key: "cache-control", Value: *input.CacheControl})
	}
	return md
}

func quotedETag(md5Hex string) string {
	h := strings.TrimSpace(strings.ToLower(md5Hex))
	if h == "" {
		return emptyQuotedMD5
	}
	h = strings.TrimPrefix(strings.TrimSuffix(h, `"`), `"`)
	return `"` + h + `"`
}

func (b *Backend) objectQuotedETag(bucket, key string) string {
	v, err := b.meta.RetrieveAttribute(nil, bucket, key, "burnbridge-etag")
	if err != nil || len(v) == 0 {
		return emptyQuotedMD5
	}
	return quotedETag(string(v))
}

func (b *Backend) HeadObject(_ context.Context, input *s3.HeadObjectInput) (*s3.HeadObjectOutput, error) {
	if input == nil || input.Bucket == nil || input.Key == nil {
		return nil, fmt.Errorf("bucket/key required")
	}
	bucket := *input.Bucket
	key := *input.Key

	attrs, err := b.meta.ListAttributes(bucket, key)
	if err != nil {
		return nil, err
	}
	md := map[string]string{}
	for _, a := range attrs {
		if strings.HasPrefix(a, "burnbridge-") {
			continue
		}
		v, err := b.meta.RetrieveAttribute(nil, bucket, key, a)
		if err != nil {
			continue
		}
		md[a] = string(v)
	}
	etag := b.objectQuotedETag(bucket, key)
	etagCopy := etag
	return &s3.HeadObjectOutput{
		Metadata:     md,
		ETag:         &etagCopy,
		LastModified: backend.GetTimePtr(time.Now()),
	}, nil
}

func (b *Backend) GetObject(_ context.Context, input *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	if input == nil || input.Bucket == nil || input.Key == nil {
		return nil, fmt.Errorf("bucket/key required")
	}
	bucket := *input.Bucket
	key := *input.Key

	attrs, err := b.meta.ListAttributes(bucket, key)
	if err != nil {
		return nil, err
	}
	md := map[string]string{}
	for _, a := range attrs {
		if strings.HasPrefix(a, "burnbridge-") {
			continue
		}
		v, err := b.meta.RetrieveAttribute(nil, bucket, key, a)
		if err != nil {
			continue
		}
		md[a] = string(v)
	}
	etag := b.objectQuotedETag(bucket, key)
	etagCopy := etag
	return &s3.GetObjectOutput{
		Body:         io.NopCloser(bytes.NewReader(nil)),
		Metadata:     md,
		ETag:         &etagCopy,
		LastModified: backend.GetTimePtr(time.Now()),
	}, nil
}

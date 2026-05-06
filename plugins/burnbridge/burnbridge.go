package main

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/versity/versitygw/backend"
	"github.com/versity/versitygw/plugins"
	"github.com/versity/versitygw/s3response"
)

// Backend is the exported plugin entry point.
var Backend plugins.BackendPlugin = &burnPlugin{}

type burnPlugin struct{}

type burnConfig struct {
	DBPath string `json:"db_path"`
}

func (p *burnPlugin) New(config string) (backend.Backend, error) {
	cfg := burnConfig{DBPath: "./burnbridge-meta.db"}
	if config != "" {
		b, err := os.ReadFile(config)
		if err != nil {
			return nil, fmt.Errorf("read config: %w", err)
		}
		if err := json.Unmarshal(b, &cfg); err != nil {
			return nil, fmt.Errorf("parse config: %w", err)
		}
	}
	if cfg.DBPath == "" {
		cfg.DBPath = "./burnbridge-meta.db"
	}
	if err := os.MkdirAll(filepath.Dir(cfg.DBPath), 0o755); err != nil && filepath.Dir(cfg.DBPath) != "." {
		return nil, fmt.Errorf("create db directory: %w", err)
	}
	metaStore, err := NewSqlMeta(cfg.DBPath)
	if err != nil {
		return nil, err
	}
	return newBurnBackend(metaStore), nil
}

type burnBackend struct {
	backend.BackendUnsupported
	meta      SqlMeta
	emptyETag string
}

func newBurnBackend(metaStore SqlMeta) *burnBackend {
	sum := md5.Sum([]byte{})
	return &burnBackend{meta: metaStore, emptyETag: hex.EncodeToString(sum[:])}
}

func (b *burnBackend) String() string { return "BurnBridge" }

func (b *burnBackend) ListBuckets(context.Context, s3response.ListBucketsInput) (s3response.ListAllMyBucketsResult, error) {
	return s3response.ListAllMyBucketsResult{Buckets: s3response.ListAllMyBucketsList{Bucket: []s3response.ListAllMyBucketsEntry{{Name: "burn-jobs", CreationDate: time.Now()}}}}, nil
}

func (b *burnBackend) PutObject(_ context.Context, input s3response.PutObjectInput) (s3response.PutObjectOutput, error) {
	if input.Bucket == nil || input.Key == nil {
		return s3response.PutObjectOutput{}, fmt.Errorf("bucket/key required")
	}
	if input.Body != nil {
		if _, err := io.Copy(io.Discard, input.Body); err != nil {
			return s3response.PutObjectOutput{}, err
		}
	}
	if len(input.Metadata) > 0 {
		for k, v := range input.Metadata {
			if err := b.meta.StoreAttribute(nil, *input.Bucket, *input.Key, k, []byte(v)); err != nil {
				return s3response.PutObjectOutput{}, err
			}
		}
	}
	if err := b.meta.UpsertBurnJob(BurnJob{JobID: fmt.Sprintf("%s/%s", *input.Bucket, *input.Key), Bucket: *input.Bucket, ObjectName: *input.Key, Status: "uploaded"}); err != nil {
		return s3response.PutObjectOutput{}, err
	}
	return s3response.PutObjectOutput{ETag: b.emptyETag}, nil
}

func (b *burnBackend) HeadObject(_ context.Context, input *s3.HeadObjectInput) (*s3.HeadObjectOutput, error) {
	if input == nil || input.Bucket == nil || input.Key == nil {
		return nil, fmt.Errorf("bucket/key required")
	}
	attrs, err := b.meta.ListAttributes(*input.Bucket, *input.Key)
	if err != nil {
		return nil, err
	}
	md := map[string]string{}
	for _, a := range attrs {
		v, err := b.meta.RetrieveAttribute(nil, *input.Bucket, *input.Key, a)
		if err != nil {
			continue
		}
		md[a] = string(v)
	}
	return &s3.HeadObjectOutput{Metadata: md, ETag: &b.emptyETag, LastModified: backend.GetTimePtr(time.Now())}, nil
}

func (b *burnBackend) GetObject(_ context.Context, input *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	if input == nil || input.Bucket == nil || input.Key == nil {
		return nil, fmt.Errorf("bucket/key required")
	}
	attrs, err := b.meta.ListAttributes(*input.Bucket, *input.Key)
	if err != nil {
		return nil, err
	}
	md := map[string]string{}
	for _, a := range attrs {
		v, err := b.meta.RetrieveAttribute(nil, *input.Bucket, *input.Key, a)
		if err != nil {
			continue
		}
		md[a] = string(v)
	}
	return &s3.GetObjectOutput{
		Body:         io.NopCloser(bytes.NewReader(nil)),
		Metadata:     md,
		ETag:         &b.emptyETag,
		LastModified: backend.GetTimePtr(time.Now()),
	}, nil
}

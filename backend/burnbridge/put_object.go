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
	"log/slog"
	"time"

	burnbridgev1 "github.com/versity/versitygw/backend/burnbridge/proto"
	meta "github.com/versity/versitygw/backend/meta"
	"github.com/versity/versitygw/s3err"
	"github.com/versity/versitygw/s3response"
)

func (b *BurnBridge) PutObject(ctx context.Context, input s3response.PutObjectInput) (s3response.PutObjectOutput, error) {
	if input.Bucket == nil || input.Key == nil {
		return s3response.PutObjectOutput{}, fmt.Errorf("bucket/key required")
	}

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
		// Recorder receives object bytes only; gateway keeps a minimal index row in SQLite (no S3 user metadata).
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

	commitResp, err := b.grpc.CommitJob(ctx, &burnbridgev1.CommitJobRequest{
		JobId:          jobID,
		UdfVolumeLabel: b.udfLabel,
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

	out := s3response.PutObjectOutput{
		ETag: etag,
		Size: &offset,
	}
	if checksumMD5 != "" {
		out.ChecksumMD5 = &checksumMD5
	}
	return out, nil
}

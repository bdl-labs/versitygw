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
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"

	burnbridgev1 "github.com/versity/versitygw/backend/burnbridge/proto"
	meta "github.com/versity/versitygw/backend/meta"
	"google.golang.org/grpc"
)

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

func (b *BurnBridge) burnMaybeInvalidateSegments(bucket, key string, segmentIdx int, digest string) error {
	if segmentIdx != 0 {
		return nil
	}
	prev, err := b.meta.GetBurnObjectSegment(bucket, key, 0)
	if errors.Is(err, meta.ErrNoSuchKey) {
		return nil
	}
	if err != nil {
		return err
	}
	if prev.ChecksumMD5 != digest {
		return b.meta.DeleteBurnObjectSegments(bucket, key)
	}
	return nil
}

func (b *BurnBridge) burnShouldSkipSegment(bucket, key string, segmentIdx int, digest string, offset, segLen int64) (bool, error) {
	stored, err := b.meta.GetBurnObjectSegment(bucket, key, segmentIdx)
	if errors.Is(err, meta.ErrNoSuchKey) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if stored.ChecksumMD5 != digest || !stored.Burned() || stored.ByteSize != segLen || stored.ByteOffset != offset {
		return false, nil
	}
	return true, nil
}

func (b *BurnBridge) recvSegmentUploadAck(stream grpc.BidiStreamingClient[burnbridgev1.UploadObjectChunk, burnbridgev1.UploadObjectAck],
	jobID, bucket, key string, segmentIdx int, offset, segLen int64, digest string) error {
	ack, err := stream.Recv()
	if err != nil {
		return err
	}
	if ack.GetUploadComplete() {
		_ = b.meta.UpsertBurnObjectSegment(bucket, key, segmentIdx, offset, segLen, digest, meta.BurnSegmentFailed, nil)
		return fmt.Errorf("burnbridge: unexpected upload_complete ack before segment %d finished", segmentIdx)
	}
	switch ack.GetSegmentBurnResult() {
	case burnbridgev1.SegmentBurnResult_SEGMENT_BURN_RESULT_FAILED:
		_ = b.meta.UpsertBurnObjectSegment(bucket, key, segmentIdx, offset, segLen, digest, meta.BurnSegmentFailed, nil)
		msg := strings.TrimSpace(ack.GetSegmentBurnError())
		if msg == "" {
			return fmt.Errorf("burnbridge: recorder reported segment %d burn failed", segmentIdx)
		}
		return fmt.Errorf("burnbridge: recorder reported segment %d burn failed: %s", segmentIdx, msg)
	case burnbridgev1.SegmentBurnResult_SEGMENT_BURN_RESULT_UNSPECIFIED,
		burnbridgev1.SegmentBurnResult_SEGMENT_BURN_RESULT_OK:
	default:
		_ = b.meta.UpsertBurnObjectSegment(bucket, key, segmentIdx, offset, segLen, digest, meta.BurnSegmentFailed, nil)
		return fmt.Errorf("burnbridge: unknown segment_burn_result %v for segment %d", ack.GetSegmentBurnResult(), segmentIdx)
	}
	if j := ack.GetJobId(); j != "" && j != jobID {
		_ = b.meta.UpsertBurnObjectSegment(bucket, key, segmentIdx, offset, segLen, digest, meta.BurnSegmentFailed, nil)
		return fmt.Errorf("burnbridge: ack job_id mismatch: got %q want %q", j, jobID)
	}
	if ack.GetSegmentIndex() != int32(segmentIdx) {
		_ = b.meta.UpsertBurnObjectSegment(bucket, key, segmentIdx, offset, segLen, digest, meta.BurnSegmentFailed, nil)
		return fmt.Errorf("burnbridge: segment_index mismatch: got %d want %d", ack.GetSegmentIndex(), segmentIdx)
	}
	if ack.GetByteOffset() != offset || ack.GetByteSize() != segLen {
		_ = b.meta.UpsertBurnObjectSegment(bucket, key, segmentIdx, offset, segLen, digest, meta.BurnSegmentFailed, nil)
		return fmt.Errorf("burnbridge: ack byte range mismatch: got offset=%d size=%d want offset=%d size=%d",
			ack.GetByteOffset(), ack.GetByteSize(), offset, segLen)
	}
	extents := bbProtoDiscExtentsToMeta(ack.GetDiscExtents())
	return b.meta.UpsertBurnObjectSegment(bucket, key, segmentIdx, offset, segLen, digest, meta.BurnSegmentSucceeded, extents)
}

// grpcUploadObjectStream implements PutObject → BurnBridge bidi stream: gateway sends UploadObjectChunk;
// recorder sends UploadObjectAck after each logical segment (with disc_extents) and a final upload_complete ack.
// SQLite segment rows: pending before payload send, succeeded after each valid segment ack, failed on recorder/IO errors.
func (b *BurnBridge) grpcUploadObjectStream(ctx context.Context, jobID, bucket, key string, body io.Reader, _ int64) (int64, *burnbridgev1.UploadObjectAck, error) {
	stream, err := b.grpc.UploadObject(ctx)
	if err != nil {
		return 0, nil, err
	}

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
		if err := b.burnMaybeInvalidateSegments(bucket, key, segmentIdx, digest); err != nil {
			return offset, nil, err
		}

		skip, err := b.burnShouldSkipSegment(bucket, key, segmentIdx, digest, offset, int64(len(chunk)))
		if err != nil {
			return offset, nil, err
		}

		if skip {
			slog.Warn("burnbridge: skipping chunk already recorded as burned (client retry); not re-sending payload",
				"bucket", bucket, "key", key, "segment", segmentIdx, "offset", offset, "size", len(chunk), "md5", digest)
			ch := &burnbridgev1.UploadObjectChunk{
				JobId: jobID, Offset: offset, Eof: false,
				ReusedBurnedBytes: int64(len(chunk)),
			}
			if err := stream.Send(ch); err != nil {
				return offset, nil, err
			}
		} else {
			if err := b.meta.UpsertBurnObjectSegment(bucket, key, segmentIdx, offset, int64(len(chunk)), digest, meta.BurnSegmentPending, nil); err != nil {
				return offset, nil, err
			}
			for i := 0; i < len(chunk); {
				end := i + burnbridgeUploadMaxDataPerFrame
				if end > len(chunk) {
					end = len(chunk)
				}
				part := chunk[i:end]
				if err := stream.Send(&burnbridgev1.UploadObjectChunk{JobId: jobID, Offset: offset + int64(i), Data: part, Eof: false}); err != nil {
					return offset, nil, err
				}
				i = end
			}
		}

		if err := b.recvSegmentUploadAck(stream, jobID, bucket, key, segmentIdx, offset, int64(len(chunk)), digest); err != nil {
			if !skip {
				_ = b.meta.UpsertBurnObjectSegment(bucket, key, segmentIdx, offset, int64(len(chunk)), digest, meta.BurnSegmentFailed, nil)
			}
			return offset, nil, err
		}

		offset += int64(len(chunk))
		segmentIdx++
		if errRead == io.ErrUnexpectedEOF {
			break
		}
	}

	if err := sendEOF(); err != nil {
		return offset, nil, err
	}
	if err := stream.CloseSend(); err != nil {
		return offset, nil, err
	}
	final, err := stream.Recv()
	if err != nil {
		return offset, nil, err
	}
	if !final.GetUploadComplete() {
		return offset, nil, fmt.Errorf("burnbridge: expected upload_complete on final ack")
	}
	slog.Info("burnbridge: object stream finished; final ack received",
		"bucket", bucket, "key", key, "jobId", jobID, "bytes", offset)
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

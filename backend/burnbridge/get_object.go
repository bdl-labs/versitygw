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
	"errors"
	"fmt"
	"io"
	"os"
	"syscall"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/versity/versitygw/backend"
	burnbridgev1 "github.com/versity/versitygw/backend/burnbridge/proto"
	meta "github.com/versity/versitygw/backend/meta"
	"github.com/versity/versitygw/s3err"
)

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

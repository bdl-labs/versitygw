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
	"errors"
	"fmt"
	"os"
	"syscall"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/versity/versitygw/backend"
	meta "github.com/versity/versitygw/backend/meta"
	"github.com/versity/versitygw/s3err"
)

func (b *BurnBridge) HeadObject(ctx context.Context, input *s3.HeadObjectInput) (*s3.HeadObjectOutput, error) {
	if input == nil || input.Bucket == nil || input.Key == nil {
		return nil, fmt.Errorf("bucket/key required")
	}
	bucket := *input.Bucket
	key := *input.Key
	if !b.burnbridgeBucketExists(bucket) {
		return nil, s3err.GetAPIError(s3err.ErrNoSuchBucket)
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
		clen := int64(len(raw))
		etag := quotedMD5Bytes(raw)
		ct := discInfoContentType
		lm := discInfoLastModifiedFromJSON(raw)
		return &s3.HeadObjectOutput{
			ContentType:   &ct,
			ContentLength: &clen,
			ETag:          &etag,
			LastModified:  backend.GetTimePtr(lm),
		}, nil
	}

	summary, err := b.meta.GetCommittedObjectSummary(bucket, key)
	if err != nil {
		if errors.Is(err, meta.ErrNoSuchKey) {
			return nil, s3err.GetAPIError(s3err.ErrNoSuchKey)
		}
		return nil, err
	}

	etagCopy := summary.ETag
	if etagCopy == "" {
		etagCopy = emptyQuotedMD5
	}
	clen := summary.Size
	lm := summary.LastModified

	if b.readMount != "" {
		objPath, pathErr := bbSafeObjectPath(b.readMount, bucket, key)
		if pathErr != nil {
			return nil, pathErr
		}
		fi, err := os.Stat(objPath)
		if err == nil && !fi.IsDir() {
			// Volume reflects a completed burn; prefer on-media size and mtime.
			clen = fi.Size()
			lm = fi.ModTime().UTC()
		} else if err != nil && !errors.Is(err, os.ErrNotExist) && !errors.Is(err, syscall.ENOTDIR) {
			return nil, err
		}
		// Missing path: treat as committed-but-not-yet-visible on mount (in-flight burn or not remounted).
		// Keep clen/lm from SQLite so Head stays consistent with List.
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

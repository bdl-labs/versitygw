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

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/versity/versitygw/backend"
	"github.com/versity/versitygw/s3err"
	"github.com/versity/versitygw/s3response"
)

// ListObjects lists object keys that have completed PutObject (burnbridge committed JSON in metadata).
func (b *BurnBridge) ListObjects(ctx context.Context, input *s3.ListObjectsInput) (s3response.ListObjectsResult, error) {
	if input == nil || input.Bucket == nil {
		return s3response.ListObjectsResult{}, s3err.GetAPIError(s3err.ErrInvalidBucketName)
	}
	bucket := *input.Bucket
	fsys, byKey, err := b.prepareCommittedListing(ctx, bucket)
	if err != nil {
		return s3response.ListObjectsResult{}, err
	}
	prefix := backend.GetStringFromPtr(input.Prefix)
	delim := backend.GetStringFromPtr(input.Delimiter)
	marker := backend.GetStringFromPtr(input.Marker)
	maxkeys := listDefaultMaxKeys
	if input.MaxKeys != nil {
		maxkeys = *input.MaxKeys
	}
	if maxkeys == 0 {
		isFalse := false
		return s3response.ListObjectsResult{
			IsTruncated:    &isFalse,
			MaxKeys:        &maxkeys,
			Name:           &bucket,
			Prefix:         backend.GetPtrFromString(prefix),
			Marker:         backend.GetPtrFromString(marker),
			Delimiter:      backend.GetPtrFromString(delim),
			CommonPrefixes: []types.CommonPrefix{},
		}, nil
	}
	results, err := backend.Walk(ctx, fsys, prefix, delim, marker, maxkeys, b.walkObjectMeta(bucket, byKey), nil)
	if err != nil {
		return s3response.ListObjectsResult{}, fmt.Errorf("list objects walk: %w", err)
	}
	return s3response.ListObjectsResult{
		CommonPrefixes: results.CommonPrefixes,
		Contents:       results.Objects,
		Delimiter:      backend.GetPtrFromString(delim),
		IsTruncated:    &results.Truncated,
		Marker:         backend.GetPtrFromString(marker),
		MaxKeys:        &maxkeys,
		Name:           &bucket,
		NextMarker:     backend.GetPtrFromString(results.NextMarker),
		Prefix:         backend.GetPtrFromString(prefix),
	}, nil
}

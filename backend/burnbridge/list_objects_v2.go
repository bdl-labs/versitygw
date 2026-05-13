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

// ListObjectsV2 lists object keys that have completed PutObject (burnbridge committed JSON in metadata).
func (b *BurnBridge) ListObjectsV2(ctx context.Context, input *s3.ListObjectsV2Input) (s3response.ListObjectsV2Result, error) {
	if input == nil || input.Bucket == nil {
		return s3response.ListObjectsV2Result{}, s3err.GetAPIError(s3err.ErrInvalidBucketName)
	}
	bucket := *input.Bucket
	fsys, byKey, err := b.prepareCommittedListing(ctx, bucket)
	if err != nil {
		return s3response.ListObjectsV2Result{}, err
	}
	prefix := backend.GetStringFromPtr(input.Prefix)
	delim := backend.GetStringFromPtr(input.Delimiter)
	marker := ""
	if input.ContinuationToken != nil {
		marker = *input.ContinuationToken
	}
	if input.StartAfter != nil && *input.StartAfter > marker {
		marker = *input.StartAfter
	}
	maxkeys := listDefaultMaxKeys
	if input.MaxKeys != nil {
		maxkeys = *input.MaxKeys
	}
	startAfterVal, contTok := listObjectsV2RequestTokens(input)
	if maxkeys == 0 {
		isFalse := false
		return s3response.ListObjectsV2Result{
			IsTruncated:       &isFalse,
			MaxKeys:           &maxkeys,
			Name:              &bucket,
			Prefix:            backend.GetPtrFromString(prefix),
			Delimiter:         backend.GetPtrFromString(delim),
			StartAfter:        backend.GetPtrFromString(startAfterVal),
			ContinuationToken: backend.GetPtrFromString(contTok),
			CommonPrefixes:    []types.CommonPrefix{},
		}, nil
	}
	results, err := backend.Walk(ctx, fsys, prefix, delim, marker, maxkeys, b.walkObjectMeta(bucket, byKey), nil)
	if err != nil {
		return s3response.ListObjectsV2Result{}, fmt.Errorf("list objects v2 walk: %w", err)
	}
	count := int32(len(results.Objects))
	return s3response.ListObjectsV2Result{
		CommonPrefixes:        results.CommonPrefixes,
		Contents:              results.Objects,
		IsTruncated:           &results.Truncated,
		MaxKeys:               &maxkeys,
		Name:                  &bucket,
		KeyCount:              &count,
		Delimiter:             backend.GetPtrFromString(delim),
		ContinuationToken:     backend.GetPtrFromString(contTok),
		NextContinuationToken: backend.GetPtrFromString(results.NextMarker),
		Prefix:                backend.GetPtrFromString(prefix),
		StartAfter:            backend.GetPtrFromString(startAfterVal),
	}, nil
}

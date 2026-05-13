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
	"github.com/versity/versitygw/s3err"
)

// DeleteObject is rejected: BurnBridge maps to write-once read-many optical storage.
func (b *BurnBridge) DeleteObject(_ context.Context, input *s3.DeleteObjectInput) (*s3.DeleteObjectOutput, error) {
	if input == nil || input.Bucket == nil || input.Key == nil {
		return nil, fmt.Errorf("bucket/key required")
	}
	if !b.burnbridgeBucketExists(*input.Bucket) {
		return nil, s3err.GetAPIError(s3err.ErrNoSuchBucket)
	}
	return nil, burnbridgeWORMNoDelete
}

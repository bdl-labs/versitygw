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
	"github.com/versity/versitygw/s3err"
	"github.com/versity/versitygw/s3response"
)

// DeleteObjects reports MethodNotAllowed for every key (WORM).
func (b *BurnBridge) DeleteObjects(_ context.Context, input *s3.DeleteObjectsInput) (s3response.DeleteResult, error) {
	if input == nil || input.Bucket == nil || input.Delete == nil {
		return s3response.DeleteResult{}, fmt.Errorf("bucket/delete payload required")
	}
	if !b.burnbridgeBucketExists(*input.Bucket) {
		return s3response.DeleteResult{}, s3err.GetAPIError(s3err.ErrNoSuchBucket)
	}
	code := burnbridgeWORMNoDelete.Code
	msg := burnbridgeWORMNoDelete.Description
	errs := make([]types.Error, 0, len(input.Delete.Objects))
	for _, obj := range input.Delete.Objects {
		if obj.Key == nil {
			continue
		}
		errs = append(errs, types.Error{
			Key:     obj.Key,
			Code:    &code,
			Message: &msg,
		})
	}
	return s3response.DeleteResult{Error: errs}, nil
}

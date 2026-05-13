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
	"fmt"

	"github.com/versity/versitygw/backend"
	"github.com/versity/versitygw/s3err"
)

func parseCommittedGetRange(objSize int64, rangeHdr string) (startOffset, length int64, contentRange *string, err error) {
	startOffset = 0
	length = objSize
	if rangeHdr == "" {
		return
	}
	start, lgth, isValid, perr := backend.ParseObjectRange(objSize, rangeHdr)
	if perr != nil || !isValid {
		return 0, 0, nil, s3err.GetAPIError(s3err.ErrInvalidRange)
	}
	startOffset, length = start, lgth
	if objSize > 0 {
		contentRange = backend.GetPtrFromString(fmt.Sprintf("bytes %d-%d/%d", startOffset, startOffset+length-1, objSize))
	}
	return
}

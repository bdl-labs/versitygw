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
	"net/http"

	"github.com/versity/versitygw/s3err"
)

const (
	// emptyQuotedMD5 is the S3-style quoted ETag for zero-length payload.
	emptyQuotedMD5 = "\"d41d8cd98f00b204e9800998ecf8427e\""

	// burnbridgeDefaultContentType is the only Content-Type returned for committed objects (no per-object metadata in SQLite).
	burnbridgeDefaultContentType = "application/octet-stream"

	discInfoContentType = "application/json"

	// objectLockShards serializes GetObject (and per-key coordination with Put) for (bucket,key).
	objectLockShards = 256

	// burnbridgeUploadMaxDataPerFrame caps gRPC UploadObjectChunk.Data size (typical 4MiB recv limit).
	burnbridgeUploadMaxDataPerFrame = 3 << 20

	defaultChunkSizeBytes = 1 << 20

	listDefaultMaxKeys int32 = 1000
)

// burnbridgeWORMNoDelete is returned for delete operations on WORM optical media.
var burnbridgeWORMNoDelete = s3err.APIError{
	Code:           "MethodNotAllowed",
	Description:    "BurnBridge optical media is WORM: object deletion is not supported.",
	HTTPStatusCode: http.StatusMethodNotAllowed,
}

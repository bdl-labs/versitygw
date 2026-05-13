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
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	burnbridgev1 "github.com/versity/versitygw/backend/burnbridge/proto"
	"github.com/versity/versitygw/s3err"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// grpcObjectReadCloser streams exactly byteCount bytes from ReadObject (recorder-side byte stream from offset).
type grpcObjectReadCloser struct {
	stream grpc.ServerStreamingClient[burnbridgev1.ReadObjectChunk]
	left   int64
	buf    []byte
	off    int
	closed bool
}

func (g *grpcObjectReadCloser) Read(p []byte) (n int, err error) {
	if g.closed {
		return 0, io.EOF
	}
	if g.left == 0 {
		g.shutdown()
		return 0, io.EOF
	}
	for n < len(p) {
		if g.off >= len(g.buf) {
			msg, err := g.stream.Recv()
			if errors.Is(err, io.EOF) {
				if g.left > 0 {
					g.shutdown()
					return n, io.ErrUnexpectedEOF
				}
				g.shutdown()
				return n, io.EOF
			}
			if err != nil {
				g.shutdown()
				return n, err
			}
			g.buf = msg.GetData()
			g.off = 0
			continue
		}
		c := copy(p[n:], g.buf[g.off:])
		if int64(c) > g.left {
			c = int(g.left)
		}
		g.off += c
		n += c
		g.left -= int64(c)
		if g.left == 0 {
			return n, nil
		}
	}
	return n, nil
}

func (g *grpcObjectReadCloser) shutdown() {
	if g.closed {
		return
	}
	g.closed = true
	_ = g.stream.CloseSend()
}

func (g *grpcObjectReadCloser) Close() error {
	g.shutdown()
	return nil
}

func mapReadFallbackError(err error) error {
	if err == nil {
		return nil
	}
	st, ok := status.FromError(err)
	if !ok {
		return err
	}
	switch st.Code() {
	case codes.Unimplemented:
		return s3err.APIError{
			Code: "BurnbridgeMediaNotVisible",
			Description: "Object metadata is committed but the file is not yet visible under the configured read mount and the recorder does not implement ReadObject (or cannot serve this object yet). " +
				"Ensure the BurnBridge server supports ReadObject and still holds the object bytes.",
			HTTPStatusCode: http.StatusServiceUnavailable,
		}
	case codes.NotFound:
		return s3err.GetAPIError(s3err.ErrNoSuchKey)
	default:
		return err
	}
}

// bbSafeObjectPath returns an absolute path under readMount for bucket/key, rejecting ".." traversal.
func bbSafeObjectPath(mountRoot, bucket, key string) (string, error) {
	mountRoot = filepath.Clean(mountRoot)
	if mountRoot == "" || mountRoot == "." {
		return "", fmt.Errorf("burnbridge: read mount path invalid")
	}
	rm, err := filepath.Abs(mountRoot)
	if err != nil {
		return "", err
	}
	rel := filepath.FromSlash(strings.TrimPrefix(key, "/"))
	if rel == "" || rel == "." {
		return "", fmt.Errorf("burnbridge: empty object key")
	}
	full := filepath.Join(rm, bucket, rel)
	rp, err := filepath.Abs(full)
	if err != nil {
		return "", err
	}
	relOut, err := filepath.Rel(rm, rp)
	if err != nil || relOut == ".." || strings.HasPrefix(relOut, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("burnbridge: object path escapes read mount")
	}
	return rp, nil
}

func (b *BurnBridge) openCommittedObjectFile(bucket, key string) (*os.File, os.FileInfo, error) {
	if b.readMount == "" {
		return nil, nil, errors.New("burnbridge: read mount not configured")
	}
	objPath, err := bbSafeObjectPath(b.readMount, bucket, key)
	if err != nil {
		return nil, nil, err
	}
	f, err := os.Open(objPath)
	if err != nil {
		return nil, nil, err
	}
	fi, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, nil, err
	}
	if fi.IsDir() {
		_ = f.Close()
		return nil, nil, syscall.EISDIR
	}
	return f, fi, nil
}

func mapOpenError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, os.ErrNotExist) || errors.Is(err, syscall.ENOTDIR) {
		return s3err.GetAPIError(s3err.ErrNoSuchKey)
	}
	if errors.Is(err, syscall.EISDIR) {
		return s3err.GetAPIError(s3err.ErrNoSuchKey)
	}
	if errors.Is(err, syscall.ENAMETOOLONG) {
		return s3err.GetAPIError(s3err.ErrKeyTooLong)
	}
	return err
}

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
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"time"

	burnbridgev1 "github.com/versity/versitygw/backend/burnbridge/proto"
	"github.com/versity/versitygw/s3err"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func dialBurnBridgeGRPC(ctx context.Context, grpcReadyTimeout time.Duration, addr string, useTLS bool, caFile, serverName string, insecureSkipVerify bool) (*grpc.ClientConn, error) {
	var opts []grpc.DialOption
	if useTLS {
		tlsCfg := &tls.Config{MinVersion: tls.VersionTLS12}
		if insecureSkipVerify {
			tlsCfg.InsecureSkipVerify = true
		} else {
			var pool *x509.CertPool
			var err error
			if caFile != "" {
				pem, rerr := os.ReadFile(caFile)
				if rerr != nil {
					return nil, fmt.Errorf("read grpc ca file: %w", rerr)
				}
				pool = x509.NewCertPool()
				if !pool.AppendCertsFromPEM(pem) {
					return nil, fmt.Errorf("parse grpc ca pem")
				}
			} else {
				pool, err = x509.SystemCertPool()
				if err != nil {
					pool = x509.NewCertPool()
				}
			}
			tlsCfg.RootCAs = pool
		}
		if serverName == "" {
			host := addr
			if h, _, err := net.SplitHostPort(addr); err == nil {
				host = h
			}
			tlsCfg.ServerName = host
		} else {
			tlsCfg.ServerName = serverName
		}
		opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg)))
	} else {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	conn, err := grpc.NewClient(addr, opts...)
	if err != nil {
		return nil, fmt.Errorf("grpc new client: %w", err)
	}
	// NewClient is lazy by default; proactively start connection so readiness wait can progress.
	conn.Connect()

	waitCtx, cancel := context.WithTimeout(ctx, grpcReadyTimeout)
	defer cancel()
	for {
		state := conn.GetState()
		if state == connectivity.Ready {
			return conn, nil
		}
		if !conn.WaitForStateChange(waitCtx, state) {
			_ = conn.Close()
			return nil, fmt.Errorf("grpc wait ready %s: %w", addr, waitCtx.Err())
		}
	}
}

// grpcConnectivityPing issues a lightweight RPC so we know the server speaks BurnBridge.
func grpcConnectivityPing(ctx context.Context, client burnbridgev1.BurnBridgeClient) error {
	_, err := client.GetJobStatus(ctx, &burnbridgev1.GetJobStatusRequest{JobId: "__versitygw_ping__"})
	if err == nil {
		return nil
	}
	st, ok := status.FromError(err)
	if !ok {
		return err
	}
	switch st.Code() {
	case codes.Unavailable, codes.DeadlineExceeded:
		return err
	default:
		// Any other code means the server responded to the method.
		return nil
	}
}

// grpcObjectReadCloser streams exactly byteCount bytes from ReadObject (recorder-side byte stream from offset).
type grpcObjectReadCloser struct {
	stream grpc.ServerStreamingClient[burnbridgev1.ReadObjectChunk]
	cancel context.CancelFunc
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
	if g.cancel != nil {
		g.cancel()
		g.cancel = nil
	}
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

func isGRPCUnimplemented(err error) bool {
	st, ok := status.FromError(err)
	return ok && st.Code() == codes.Unimplemented
}

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
	"fmt"
	"net"
	"os"
	"time"

	burnbridgev1 "github.com/versity/versitygw/backend/burnbridge/proto"
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

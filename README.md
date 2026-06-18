# The Versity S3 Gateway:<br/>A High-Performance S3 Translation Service

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="https://github.com/versity/versitygw/blob/assets/assets/logo-white.svg">
  <source media="(prefers-color-scheme: light)" srcset="https://github.com/versity/versitygw/blob/assets/assets/logo.svg">
  <a href="https://www.versity.com"><img alt="Versity Software logo image." src="https://github.com/versity/versitygw/blob/assets/assets/logo.svg"></a>
</picture>

 [![Apache V2 License](https://img.shields.io/badge/license-Apache%20V2-blue.svg)](https://github.com/versity/versitygw/blob/main/LICENSE) [![Go Report Card](https://goreportcard.com/badge/github.com/versity/versitygw)](https://goreportcard.com/report/github.com/versity/versitygw) [![Go Reference](https://pkg.go.dev/badge/github.com/versity/versitygw.svg)](https://pkg.go.dev/github.com/versity/versitygw)

### Binary release builds
Download [latest release](https://github.com/versity/versitygw/releases)
 | Linux/amd64 | Linux/arm64 | MacOS/amd64 | MacOS/arm64 | BSD/amd64 | BSD/arm64 |
 |:-----------:|:-----------:|:-----------:|:-----------:|:---------:|:---------:|
 |    ✔️    |  ✔️  |   ✔️   |  ✔️   |  ✔️   |  ✔️   |

### Use Cases
* Expose the BurnBridge optical archive workflow through an S3-compatible API.
* Upload and download objects through the gateway while recorder work is coordinated by the bridge backend.
* Use the built-in admin API and WebUI around the bridge deployment.

### WebGUI
Get more details about the new (optional) WebGUI management/explorer here: [https://github.com/versity/versitygw/wiki/WebGUI](https://github.com/versity/versitygw/wiki/WebGUI)

![admin-explorer](https://github.com/user-attachments/assets/e99db171-2c72-4d0f-8c8d-480a56e1c8a1)

### Static Website Hosting
Serve S3 buckets as static websites with index documents, custom error pages, and routing rules.
Enable a separate website endpoint with `--website :8090 --website-domain example.com` for virtual-host style routing (`blog.example.com` serves bucket `blog`, `example.com` serves bucket `example.com`).
When `--website-domain` is omitted, catch-all mode is used: the full hostname becomes the bucket name (name your buckets as FQDNs, e.g. `blog.example.com`).
See [Global Options](https://github.com/versity/versitygw/wiki/Global-Options) for all `--website-*` flags.

### News
Check out latest wiki articles: [https://github.com/versity/versitygw/wiki/Articles](https://github.com/versity/versitygw/wiki/Articles)

### Mailing List
Keep up to date with latest gateway announcements by signing up to the [versitygw mailing list](https://www.versity.com/products/versitygw#signup).

### Documentation
See project [documentation](https://github.com/versity/versitygw/wiki) on the wiki.

### Need help?
Ask questions in the [community discussions](https://github.com/versity/versitygw/discussions).
<br>
Contact [Versity Sales](https://www.versity.com/contact/) to discuss enterprise support.

### Overview
Versity Gateway, a simple to use tool for seamless inline translation between AWS S3 object commands and storage systems. The Versity Gateway bridges the gap between S3-reliant applications and other storage systems, enabling enhanced compatibility and integration while offering exceptional scalability.

The server translates incoming S3 API requests and transforms them into equivalent operations to the backend service. By leveraging this gateway server, applications can interact with the S3-compatible API on top of already existing storage systems. This project enables leveraging existing infrastructure investments while seamlessly integrating with S3-compatible systems, offering increased flexibility and compatibility in managing data storage.

The Versity Gateway is focused on performance and simplicity for the BurnBridge optical archive deployment.

The gateway is completely stateless. Multiple Versity Gateway instances may be deployed in a cluster to increase aggregate throughput. The Versity Gateway’s stateless architecture allows any request to be serviced by any gateway thereby distributing workloads and enhancing performance. Load balancers may be used to evenly distribute requests across the cluster of gateways for optimal performance.

The S3 HTTP(S) server and routing is implemented using the [Fiber](https://gofiber.io) web framework.  This framework is actively developed with a focus on performance.  S3 API compatibility leverages the official [aws-sdk-go-v2](https://github.com/aws/aws-sdk-go-v2) whenever possible for maximum service compatibility with AWS S3.

## Getting Started
See the [Quickstart](https://github.com/versity/versitygw/wiki/Quickstart) documentation.

### Run the gateway with BurnBridge backend:

```
mkdir /tmp/vgw
ROOT_ACCESS_KEY="testuser" ROOT_SECRET_KEY="secret" ./versitygw --port :10000 --iam-dir /tmp/vgw burnbridge --db-path /tmp/vgw/burnbridge-meta.sqlite --grpc-addr 127.0.0.1:50051
```
This will enable an S3 server on the current host listening on port 10000 and connecting to the recorder bridge gRPC service at `127.0.0.1:50051`. The `--iam-dir` option enables simple JSON flat file accounts for testing.

To get the usage output, run the following:

```
./versitygw --help
```

The command format is

```
versitygw [global options] command [command options] [arguments...]
```
The [global options](https://github.com/versity/versitygw/wiki/Global-Options) are specified before the backend type and the backend options are specified after.

### Testing & Production Readiness

VersityGW is **battle-tested and production-ready**. Every pull request must pass our comprehensive test suite before it can be reviewed or merged. All code reviews are done by at least one human in the loop. LLMs may be used to augment the review process, but are never the sole reviewer or decision maker. See [Testing](https://github.com/versity/versitygw/wiki/Testing) for high level testing documentation.

#### Comprehensive Test Coverage

Our multi-layered testing strategy includes:

- **Go Unit Test Files** - Extensive unit tests with race detection and code coverage analysis covering core functionality, edge cases, and error handling.
- **Integration Test Scripts** - Real-world scenario testing for the S3-compatible gateway and bridge workflow.
- **Functional/Regression Tests** - End-to-end SDK tests validating complete workflows including full-flow operations and IAM functionality populated with regression tests as issues are addressed.
- **Static Analysis** - Static Analysis checks using [staticcheck](https://staticcheck.dev).
- **System Tests** - Protocol-level validation using industry-standard S3 clients:
  - AWS CLI - Official AWS command-line tools
  - s3cmd - Popular S3 client
  - Direct REST API testing with curl for request/response validation
- **Security Testing** - Both HTTP and HTTPS configurations tested. Vulnerability scanning with govulncheck. And regular dependency updates with dependabot.
- **Compatibility Testing** - Bridge workflow scenarios, static bucket modes, and various authentication methods.

### Run the gateway in Docker

Use the published image like the native binary by passing CLI arguments:

```bash
docker run --rm versity/versitygw:latest --version
```

See [Docker](https://github.com/versity/versitygw/wiki/Docker) for more
documentation for running within Docker.

### Run on Kubernetes

A Helm chart is provided to easily run Versity in Kubernetes environments:

```sh
helm install versitygw oci://ghcr.io/versity/versitygw/charts/versitygw
```

Please refer to the [chart's README](./chart/README.md) for more information and configuration parameters.

### BurnBridge backend (local development)

For building, regenerating `burnbridge` protobuf stubs, CLI flags, and architecture notes when continuing work on another machine, see [doc/BURNBRIDGE_DEVELOPMENT.md](./doc/BURNBRIDGE_DEVELOPMENT.md).

Quick local start example (BurnServer on `127.0.0.1:50051`):

```powershell
.\versitygw.exe --port :10000 --access admin --secret admin123456 burnbridge --db-path ".\burnbridge-meta.db" --grpc-addr "127.0.0.1:50051" --grpc-dial-timeout 120s --grpc-ready-timeout 90s --grpc-ping-timeout 60s
```

Environment-variable variant:

```powershell
$env:VGW_ACCESS="admin"
$env:VGW_SECRET="admin123456"
$env:VGW_BURNBRIDGE_DB_PATH="D:\BRS\versitygw\burnbridge-meta.db"
$env:VGW_BURNBRIDGE_GRPC_ADDR="127.0.0.1:50051"
$env:VGW_BURNBRIDGE_GRPC_DIAL_TIMEOUT="120s"
$env:VGW_BURNBRIDGE_GRPC_READY_TIMEOUT="90s"
$env:VGW_BURNBRIDGE_GRPC_PING_TIMEOUT="60s"

.\versitygw.exe --port :10000 burnbridge
```

***

#### Versity gives you clarity and control over your archival storage, so you can allocate more resources to your core mission.

### Contact
![versity logo](https://www.versity.com/wp-content/uploads/2022/12/cropped-android-chrome-512x512-1-32x32.png)
info@versity.com <br />
+1 844 726 8826

### @versitysoftware
[![linkedin](https://github.com/versity/versitygw/blob/assets/assets/linkedin.jpg)](https://www.linkedin.com/company/versity/) &nbsp;
[![twitter](https://github.com/versity/versitygw/blob/assets/assets/twitter.jpg)](https://twitter.com/VersitySoftware) &nbsp;
[![facebook](https://github.com/versity/versitygw/blob/assets/assets/facebook.jpg)](https://www.facebook.com/versitysoftware) &nbsp;
[![instagram](https://github.com/versity/versitygw/blob/assets/assets/instagram.jpg)](https://www.instagram.com/versitysoftware/) &nbsp;

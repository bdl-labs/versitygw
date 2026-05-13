# BurnBridge 本地 / 换机开发说明

本文档便于在新机器上恢复环境并继续开发 **BurnBridge**（S3 → gRPC 刻录服务）相关代码。更长的产品与里程碑说明见 [plan/burn-bridge-plugin-dev-plan.md](plan/burn-bridge-plugin-dev-plan.md)，任务台账见 [plan/burn-bridge-task-ledger.csv](plan/burn-bridge-task-ledger.csv)。

## 环境与前置条件

| 项 | 说明 |
|----|------|
| **Go** | 与仓库 `go.mod` 一致（当前 `go 1.25.0`）。 |
| **构建** | 仓库根目录：`go build -o versitygw ./cmd/versitygw` 或 `make`（见根目录 `Makefile`）。 |
| **protoc** | 修改 `backend/burnbridge/proto/burnbridge.proto` 后需重新生成 Go 代码。 |
| **插件** | `protoc-gen-go`、`protoc-gen-go-grpc` 需在 `PATH` 中（通常 `go install google.golang.org/protobuf/cmd/protoc-gen-go@latest` 与 `go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest`）。 |

**Windows 提示**：若未把官方 `protoc` 安装进系统 PATH，可任选其一：

1. **临时/全路径**：若曾把 [protobuf 发布包](https://github.com/protocolbuffers/protobuf/releases) 解压到本机临时目录，常见为 **`%TEMP%\protoc\bin\protoc.exe`**（可用 `protoc --version` 确认）。生成时在仓库根目录用该全路径代替 `protoc`。
2. **加入 PATH（当前用户）**：将上述 `bin` 目录（以及 **`%GOPATH%\bin`**，以便找到两个 `protoc-gen-*`）加入用户环境变量 `Path`，新开终端后可直接运行 `make proto-burnbridge` 或下面的 `protoc` 命令。

### 重新生成 gRPC / protobuf 存根

```bash
# 若已安装 protoc（推荐）
make proto-burnbridge
```

等价命令（Windows / 无 make 时可直接运行）：

```bash
protoc -I=backend/burnbridge/proto \
  --go_out=backend/burnbridge/proto --go_opt=paths=source_relative \
  --go-grpc_out=backend/burnbridge/proto --go-grpc_opt=paths=source_relative \
  backend/burnbridge/proto/burnbridge.proto
```

生成物：`backend/burnbridge/proto/burnbridge.pb.go`、`burnbridge_grpc.pb.go`。**提交代码时若改了 proto，请一并提交生成文件**，避免他人换机后缺桩无法编译。

## 启动 BurnBridge 网关

子命令在 `cmd/versitygw/burnbridge.go`。示例（需本地已有刻录端 gRPC 服务）：

```bash
# 全局参数 + burnbridge 子命令参数；具体见 ./versitygw burnbridge --help
./versitygw --port :10000 burnbridge \
  --db-path ./burnbridge-meta.db \
  --grpc-addr 127.0.0.1:50051 \
  --read-mount /path/to/mounted/disc/root
```

**启动顺序（刻录端契约）**：TCP/gRPC 连通后，网关**立刻**调用 **`TestUnitReady`（无请求体字段）**，用于探测当前装载的光盘；刻录端应返回 **`ready=true`** 与 **`volume_label`**（如 UDF 卷标）。网关将卷标**净化**为符合 S3 命名规则的**单一桶名**，本会话内所有 `ListBuckets` / `HeadBucket` / 对象 API 均使用该桶名（**不再固定为 `burn-jobs`**）。若未就绪、无卷标或 RPC 为 `Unimplemented`，**`burnbridge.New` 直接报错**，网关不会完成启动。卷标探测成功后，仍会按配置执行 **`GetJobStatus` ping**（除非 `--grpc-skip-ping`）。

常用标志与环境变量（节选，以 `--help` 为准）：

| 标志 | 环境变量 | 含义 |
|------|----------|------|
| `--db-path` | `VGW_BURNBRIDGE_DB_PATH` | SQLite 元数据路径（含 `burnbridge-committed`、片段表等）。 |
| `--grpc-addr` | `VGW_BURNBRIDGE_GRPC_ADDR` | 刻录服务地址。 |
| `--read-mount` | `VGW_BURNBRIDGE_READ_MOUNT` | 已挂载读盘路径；对象路径为 `{mount}/{bucket}/{key}`。 |
| `--grpc-tls` / `--grpc-ca` / `--grpc-insecure-skip-verify` | 对应 `VGW_BURNBRIDGE_*` | gRPC TLS。 |
| `--grpc-chunk-size` | `VGW_BURNBRIDGE_GRPC_CHUNK_SIZE` | 每段逻辑读取缓冲 / 上传帧大小相关，默认约 1MiB。 |
| `--put-object-timeout` | `VGW_BURNBRIDGE_PUT_OBJECT_TIMEOUT` | 慢速刻录时可加大 Put 整链路超时；`0` 表示主要跟请求上下文。 |

## 代码布局（换分支后快速定位）

| 路径 | 作用 |
|------|------|
| `backend/burnbridge/proto/burnbridge.proto` | 与刻录服务的 RPC 契约（`UploadObject`、`ReadObject`、`CommitJob` 等）。 |
| `backend/burnbridge/proto/*.pb.go` | `protoc` 生成代码，勿手改。 |
| `backend/burnbridge/burnbridge.go` | 主后端：`PutObject` 流、片段状态、`GetObject` / `HeadObject`、ReadObject 回退等。 |
| `backend/burnbridge/grpc.go` | gRPC 拨号、TLS、启动探测等。 |
| `backend/meta/sqlmeta.go` | SQLite：`BurnbridgeCommittedRecord`、片段表 `burnbridge_object_segments`、`burn_state`（pending / succeeded / failed）。 |
| `cmd/versitygw/burnbridge.go` | CLI 与 `burnbridge.Options` 装配。 |

## 行为要点（实现与刻录端约定）

1. **上传**：`CreateJob` → 可选 **`RegisterS3ObjectPullSource`**（配置 `--recorder-s3-endpoint` 等时由网关下发 S3 拉流参数）→ `UploadObject` 双向流 → `CommitJob`；成功后写入 `burnbridge-committed` 元数据。
2. **片段状态（SQLite）**：`BurnSegmentPending` / `Succeeded` / `Failed`；成功段可配合 **`reused_burned_bytes`** 做重试时 skip（网关发一帧告诉刻录端区间已在媒体，由刻录端推进逻辑指针并回 ack）。
3. **Ack 扩展**：`UploadObjectAck.segment_burn_result` / `segment_burn_error` 表示刻录端对该逻辑片段的成功或失败；网关会据此更新 `burn_state`。
4. **读对象**：优先读 `--read-mount` 下文件；若元数据已提交但文件尚未出现，则对已提交的桶键走 gRPC **`ReadObject`**（按 `bucket` + `object_key` + `offset` + `length` 流式取字节，不依赖 `job_id`）。刻录端需实现 `ReadObject`。
5. **S3 错误**：例如媒体未就绪且 `ReadObject` 未实现时可能返回 `503` / `BurnbridgeMediaNotVisible`（见 `burnbridge.go` 中 `mapReadFallbackError`）。
6. **Put 串行（全局）**：任意两个对象的 `PutObject` 不能重叠（`putSerialMu`），适合单机单刻录流道。同一对象的 `GetObject` 仍用分片锁与 `Put`/`Get` 互斥；`Get` 与**其他 key** 的 `Put` 仍可并发（若业务要求连这也禁止，可再为 `GetObject` 加同一全局锁）。
7. **刻录单元就绪与桶名**：启动时通过空 **`TestUnitReady`** 请求获取 **`volume_label`** 并映射为 S3 **唯一桶名**。运行中网关在每个 **`PutObject` / `GetObject` / `HeadObject` / `ListObjects` / `ListObjectsV2`** 前会再次调用 **`TestUnitReady`**；未就绪返回 **503**、`BurnbridgeUnitNotReady`。**`HeadBucket`** 使用同一检查。运行若遇 **`TestUnitReady` `Unimplemented`**（旧刻录端），会打日志并**放行**该次检查（启动阶段则**不允许** `Unimplemented`，必须实现本 RPC）。
8. **SQLite**：元数据按 **桶名** 分区；若此前使用固定桶名 **`burn-jobs`**，切换到「卷标即桶名」后，旧库中的行不会自动出现在新桶名下，需自行迁移或沿用新库。

## 换机检查清单

1. 安装匹配版本的 **Go**，`git clone` 本仓库。
2. `go build ./cmd/versitygw` 确认能通过。
3. 若你拉到的提交修改过 **proto** 但未包含生成文件，在本地执行 **`make proto-burnbridge`** 后再编译。
4. 准备 **SQLite 路径** 与 **刻录 gRPC 地址**；可选配置 **读挂载** 做 GetObject 本地读。
5. 刻录端 (.NET 等) 需与当前 `burnbridge.proto` 一致并实现所用电文（含 `ReadObject`、片段 ack 与 `segment_burn_result`）。

## 测试

```bash
go test ./...
```

BurnBridge 包当前可能无单测文件；修改核心逻辑后至少保证全仓 `./...` 通过编译与相关包的测试。

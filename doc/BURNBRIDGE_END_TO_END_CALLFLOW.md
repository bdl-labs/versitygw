# BurnBridge 端到端调用关系与数据流（网关 + 刻录端）

本文档基于当前代码实现梳理对象上传全链路，覆盖：

- 网关侧：分片规则、任务下发、任务状态、元数据落盘、恢复逻辑
- 刻录侧：任务接收、分片写入、状态回传、UDF 布局组织
- 关键参数与一致性要求

适用代码基线：

- `versitygw/backend/burnbridge/burnbridge.go`
- `versitygw/backend/meta/sqlmeta.go`
- `primoburner-net/samples/BurnServer/Services/BurnBridgeService.cs`
- `primoburner-net/samples/block-device/Core/Burner/Orchestration.cs`

---

## 1. 总体架构

调用路径：

1. S3 客户端发起 `PUT /{bucket}/{key}`
2. 网关 `BurnBridge.PutObject` 接管请求
3. 网关通过 gRPC 调用 BurnServer：
   - `CreateJob`
   - `UploadObject`（双向流）
   - `CommitJob`
4. BurnServer 驱动 `block-device`：
   - `StartStreamWriteSession`
   - `AppendChunkToSession`
   - `CompleteStreamWriteSession`
   - `FinalizeStreamWriteSession`
5. 网关把提交结果写入 SQLite committed 元数据，返回 S3 `PutObjectOutput`

---

## 2. 网关侧（versitygw）详细流程

### 2.1 PutObject 主流程

入口：`BurnBridge.PutObject`

核心步骤：

1. 参数与桶校验
   - 校验 bucket/key
   - 校验 bucket 是否等于当前活动光盘映射桶
2. 设备就绪检查
   - `requireRecorderReady` -> `TestUnitReady`
3. 并发控制
   - 全局串行锁 `putSerialMu`（不同对象 Put 不重叠）
   - 对象级锁分片（同 key 的 Put/Get 互斥）
4. 下发任务
   - 调用 `CreateJob(bucket,key,contentLength)` 获得 `jobId`
   - 可选下发 `RegisterS3ObjectPullSource`（启用 recorder 直拉 S3）
5. 上传对象流
   - `grpcUploadObjectStream(ctx, jobId, bucket, key, body, contentLen)`
6. 构建 finalize manifest
   - `buildFinalizeManifest(bucket,key,offset)`，基于 SQLite 分片记录
   - 若无可用 extents，返回 `nil`（fallback，不携带 manifest）
7. 提交任务
   - `CommitJob(jobId, udfVolumeLabel, finalizeManifest)`
8. 落盘 committed 元数据
   - `StoreBurnbridgeCommitted(bucket,key,record)`
9. 返回 ETag/Size/Checksum

失败处理：

- 上传/提交失败时，defer 触发 best-effort `CancelJob(jobId)`
- 失败分片会被写入 `BurnSegmentFailed`

---

### 2.2 分片规则与分片大小

在 `grpcUploadObjectStream` 中：

1. 逻辑分片大小：`b.chunkSize`（默认 1 MiB，来源 `--grpc-chunk-size`）
2. 物理 gRPC 帧大小上限：`burnbridgeUploadMaxDataPerFrame = 3 MiB`
   - 单个逻辑分片如果大于 3 MiB，会拆成多个 gRPC frame
3. 尾分片判定：
   - `io.ReadFull` 返回 `io.ErrUnexpectedEOF` 视为尾分片
   - 尾分片最后一帧 `Eof=true`
4. 空体对象：
   - 直接发送 `Eof=true` 空 chunk 并等待最终 ack

结论：  
**分片大小 = 网关 chunkSize；传输帧大小 <= 3MiB；尾分片通过 Eof 标识。**

---

### 2.3 任务状态监控（网关）

网关侧监控来源：

1. gRPC 实时 ack（`UploadObjectAck`）
   - `segment_index / byte_offset / byte_size`
   - `segment_burn_result`（OK/FAILED）
   - `segment_burn_error`
2. 上传统计日志
   - `segments_total / skipped / replayed / trimmed / skip_hit_rate_pct / elapsed_ms`
3. 任务最终状态
   - `CommitJobResponse.status`
   - committed record 持久化成功即视为 S3 层成功

---

### 2.4 元数据记录（SQLite）

#### A. 分片级表：`burnbridge_object_segments`

关键字段：

- `(bucket, object_name, segment_index)` 复合主键
- `byte_offset / byte_size / checksum_md5`
- `burn_state`：
  - `0` Pending
  - `1` Succeeded
  - `2` Failed
- `disc_extents`（JSON）

关键操作：

- `UpsertBurnObjectSegment`：写 Pending/Succeeded/Failed
- `ListBurnObjectSegments`：按 `segment_index` 顺序读出构建 manifest
- `DeleteBurnObjectSegmentsFromWithCount`：删除尾部脏段
- `DeleteBurnObjectSegments`：首段哈希变化时清空旧对象分片

#### B. 对象级 committed 元数据

- 存储在 metadata entries 的 `burnbridge-committed` 快照
- 供 `HeadObject`/`ListObjects`/`GetObject` 元数据路径使用

---

### 2.5 恢复与重试逻辑（网关）

当前实现是“快照 + 判定”模式：

1. 上传开始时加载对象全部分片快照到内存
2. 每个分片计算 MD5，进行 skip 判定：
   - 前一分片必须已 burned
   - 当前分片 checksum/offset/size 与已成功记录一致
3. 命中 skip：
   - 不重发 payload，只发 `ReusedBurnedBytes`
4. 未命中：
   - 写 Pending，发送 payload，等待 ack 后写 Succeeded/Failed
5. 上传完成后删除多余尾段（trim）

---

## 3. 刻录侧（BurnServer + block-device）详细流程

### 3.1 任务接收与排队

入口：`BurnBridgeService.CreateJob`

关键行为：

1. 生成 `jobId`
2. 解析 JobSettings（deviceIndex/relativePath/tempDiscId/volumeLabel/conflict）
3. 写入 `ActiveJobs`
4. 若当前无运行任务：立即 `StartRuntimeSession`
5. 否则进入队列（状态 `Queued`）

状态机：`Queued -> Running -> Completed | Failed | Cancelled`

---

### 3.2 会话启动（接收任务）

`StartRuntimeSession` 调用：

- `BurnerApp.StartStreamWriteSession(deviceIndex,tempDiscId)`
  - `PrepareDevice(closeTray: true)`
  - `Burner.StartStreamWriteSession(tempDiscId)`
    - 介质校验（append 模式）
    - 预加载现有布局（追加场景）
    - 打开 BlockDevice，启动后台 writer 线程

---

### 3.3 分片接收与刻录写入

入口：`BurnBridgeService.UploadObject`（双向流）

每个 chunk 处理：

1. 校验 job 存在且运行中
2. 校验偏移连续：`chunk.Offset == runtime.BytesReceived`
3. 调用 `app.AppendChunkToSession(relativePath, payload, chunk.Eof)`
4. 更新 `runtime.BytesReceived`、`runtime.UploadCompleted`
5. 回传 `UploadObjectAck`：
   - `segmentIndex / byteOffset / byteSize`
   - `uploadComplete`
   - `segmentBurnResult=Ok/Failed`

block-device 写入机制：

- `AppendChunkToSession` 入队 chunk
- 后台 writer 线程消费
- 非 EOF chunk 强制满足 2048 和传输块对齐
- EOF chunk 允许尾部补齐后落盘
- 每次落盘记录文件 `Extents`（`discAddress`,`logicalBytes`）

---

### 3.4 任务状态监控（刻录端）

监控维度：

1. `Jobs` 字典（对外 `GetJobStatus`）
2. `JobMessages`（失败原因）
3. `CompletedCommits`（Commit 幂等重放）
4. 日志：
   - Create/Upload/Commit/Cancel 全链路 job_id

---

### 3.5 文件分片数据记录（刻录端）

记录位置：

- 运行时内存：`StreamWriteSession.Files`（`BurnFileInfo` 列表）
- 每个文件包含 `Extents`（一个或多个 `FileExtent`）

来源：

- 上传流实时写入时由 block-device writer 记录 extents
- 若网关提供 `FinalizeManifest` 且可用，`ReplaceStreamSessionFiles` 用 manifest 覆盖
- manifest 不可用时保留 runtime extents（fallback）

---

### 3.6 UDF 文件布局组织（刻录端）

提交入口：`CommitJob`

顺序：

1. `CompleteStreamWriteSession`：结束写流，关闭 BlockDevice 写入阶段
2. 可选 `ReplaceStreamSessionFiles(finalizeFiles)`：使用网关 manifest
3. `FinalizeStreamWriteSession(volumeLabel, conflict, closeDisc)`
   - `LoadExistingLayoutOrCreateEmpty()` 读取已有布局
   - `AddFileNode` 把本次文件按路径合并到目录树
   - `FinalizeDisc(closeTrack=true, closeSession=true, closeDisc=请求参数)`

追加语义：

- `closeDisc=false`：允许后续继续追加
- `closeDisc=true`：最终封盘

---

## 4. 参数统一建议（必须对齐）

至少对齐以下参数：

1. gRPC 地址
   - 网关 `--grpc-addr` 与 BurnServer `applicationUrl` 必须一致
2. 上传 chunk 大小
   - 网关 `grpc-chunk-size`
   - BurnServer `BurnBridge:GrpcChunkSize`
   - 且必须满足 `BlocksPerTransfer * SectorSizeBytes` 的倍数
3. block-device 运行参数
   - `SectorSizeBytes / BlocksPerTransfer / SessionCacheCapacityBytes`
   - 通过 BurnServer 启动注入环境变量统一
4. read mount
   - 网关与 BurnServer 统一读取 `OpticalArchive:ReadMountPath`（若启用 ReadObject 挂载读）

---

## 5. 已知风险点与优化方向

1. `UploadObjectAck` 当前未稳定回传 `DiscExtents`  
   - 现有方案：网关支持 manifest fallback
   - 建议：后续补齐 ack extents，恢复强一致 manifest 路径

2. BurnServer 进程占用 DLL 导致构建失败  
   - 建议：统一重启脚本/调试流程，避免锁文件影响发布节奏

3. 运行中配置漂移  
   - 建议：启动时打印“网关互通参数 + BlockDeviceRuntime 生效参数”并做一致性告警

---

## 6. 时序图（简化）

1. Client -> Gateway: `PUT bucket/key`
2. Gateway -> BurnServer: `CreateJob`
3. Gateway <-> BurnServer: `UploadObject(stream chunk/ack)`
4. Gateway -> SQLite: `segment pending/succeeded/failed`
5. Gateway -> BurnServer: `CommitJob(manifest or nil)`
6. BurnServer -> block-device: `Complete + Finalize(UDF merge)`
7. Gateway -> SQLite: `burnbridge-committed`
8. Gateway -> Client: `ETag/Size`

---

## 7. 排障优先顺序（推荐）

1. 看 BurnServer 启动日志参数摘要（Chunk + BlockDeviceRuntime + GatewayInterop）
2. 看网关 `upload recovery stats`（是否全 replay / 有无 skip）
3. 看 `CommitJob` 是否使用 manifest 还是 fallback
4. 看 BurnServer `JobMessages` 与 `GetJobStatus`
5. 看 SQLite `burnbridge_object_segments` 与 `burnbridge-committed`


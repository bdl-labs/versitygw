# VersityGW Burn Bridge 后端集成开发计划（PrimoBurner BlockDevice 流式刻录）

## 1. 项目背景与目标

### 1.1 背景
当前目标是将 VersityGW 作为 S3 网关前端，通过内置 BurnBridge 后端将对象数据桥接到 .NET Core gRPC 刻录服务，最终由 PrimoBurner SDK 的 BlockDevice 能力实现光盘流式刻录。

目标数据链路：

`S3 Client -> VersityGW -> Burn Bridge Backend(Go) -> gRPC Service(.NET Core) -> PrimoBurner BlockDevice -> Optical Drive`

### 1.2 目标
- 在不修改 VersityGW 核心 S3 API 行为的前提下，以内置后端方式接入刻录能力。
- 支持基于 BlockDevice 的流式写入路径，避免全量落盘后再刻录。
- 满足基础 S3 上传与状态查询能力，并提供可追踪、可恢复、可观测的任务体系。
- 建立可执行的任务台账，支持周/日粒度跟踪。

### 1.3 范围
**In Scope**
- Go 内置后端开发（实现 `backend.Backend`，通过 `versitygw burnbridge` 子命令加载）。
- S3 写入路径（PutObject、Multipart Upload）到 gRPC 流式传输映射。
- .NET gRPC Burn Service 协议对接（以流式 chunk 传输 + commit 模式）。
- 刻录任务状态模型与状态查询接口。
- 失败重试、取消、超时、幂等策略。
- 端到端测试与上线方案。

**Out of Scope（当前阶段）**
- 全量 S3 特性（例如完整 ACL/Tagging/Lock 的 100% 兼容）。
- 多品牌刻录 SDK 统一抽象层。
- 跨地域分布式调度。

---

## 2. 技术方案总览

### 2.1 VersityGW 后端接入点
- 使用 `versitygw burnbridge --db-path <path>` 启动内置 BurnBridge 后端。
- 后端入口位于 `backend/burnbridge`，通过 `cmd/versitygw/burnbridge.go` 完成参数解析与初始化。

### 2.2 关键设计
1. **流式上传优先**
   - `PutObject` 直接将 request body 以 chunk 方式发送到 gRPC 双向/客户端流。
   - 减少磁盘缓冲，仅在异常恢复时落盘。

2. **异步刻录任务模型**
   - S3 上传完成后触发 `CommitBurnJob`。
   - 刻录在后端任务引擎异步执行，状态可查询（Queued/Writing/Finalizing/Done/Failed/Canceled）。

3. **幂等与恢复**
   - 使用 `bucket+key+uploadId(+partNo)` 生成幂等键。
   - 服务崩溃恢复后可通过 gRPC `GetJobStatus` 回补状态。

4. **流控策略**
   - 后端侧配置 chunk size、窗口大小、发送超时。
   - gRPC 服务返回 backpressure 信号（ACK/窗口额度）。

5. **错误映射**
   - gRPC 错误码与 PrimoBurner 异常映射到 S3 友好错误（如 5xx/4xx）。

---

## 3. 里程碑计划（建议 8 周）

| 里程碑 | 周期 | 目标 | 交付物 | 通过标准 |
|---|---|---|---|---|
| M1 需求冻结与架构评审 | W1 | 明确功能边界、状态模型、接口契约 | 需求文档、架构图、proto 草案 | 评审通过，风险清单完成 |
| M2 后端骨架+协议联通 | W2 | 内置后端可启动，gRPC 基础调用可用 | backend skeleton、healthcheck、proto v1 | `versitygw burnbridge` 启动成功，health OK |
| M3 PutObject 流式链路 | W3-W4 | 单对象上传流式写入可刻录 | PutObject 实现、流式传输、任务状态 | 1GB 连续上传成功率≥99% |
| M4 Multipart 链路 | W5 | 分片上传/合并/提交刻录可用 | MPU API 实现、幂等合并策略 | 10GB 分片上传完成并刻录成功 |
| M5 稳定性与恢复 | W6 | 重试、超时、取消、崩溃恢复 | 恢复机制、重试策略、异常处理 | 故障注入场景通过率≥95% |
| M6 可观测性与验收 | W7-W8 | 指标、日志、告警、验收报告 | metrics、dashboard、Runbook、UAT报告 | UAT通过，进入灰度 |

---

## 4. 详细开发任务表（WBS）

> 说明：优先级 P0/P1/P2；复杂度 S/M/L；估时为人日。

| 编号 | 模块 | 任务 | 输出 | 依赖 | 优先级 | 复杂度 | 估时 | 负责人 | 状态 |
|---|---|---|---|---|---|---|---:|---|---|
| T-001 | 项目管理 | 需求澄清会与验收标准定义 | 需求规格说明 v1 | 无 | P0 | S | 1 | PM/架构 | 未开始 |
| T-002 | 架构 | E2E 架构设计评审 | 架构设计文档 | T-001 | P0 | M | 1.5 | 架构 | 未开始 |
| T-003 | 协议 | 定义 gRPC proto v1（Create/Stream/Commit/Status/Cancel） | proto 文件 | T-002 | P0 | M | 2 | Go+.NET | 未开始 |
| T-004 | 工程化 | 代码仓结构与 Makefile/CI 初版 | 构建脚本、CI流程 | T-002 | P0 | S | 1 | DevOps | 未开始 |
| T-005 | 后端框架 | 实现 burnbridge 命令参数与后端初始化 | 后端可启动 | T-004 | P0 | S | 1.5 | Go | 已完成 |
| T-006 | 后端框架 | Backend 基类（嵌入 BackendUnsupported） | 基础后端对象 | T-005 | P0 | S | 1 | Go | 已完成 |
| T-007 | gRPC Client | 连接池/TLS/鉴权/重连策略 | grpc client manager | T-003,T-005 | P0 | M | 2 | Go | 未开始 |
| T-008 | PutObject | 流式 chunk 上传实现（含 CRC/MD5 可选） | PutObject 主链路 | T-006,T-007 | P0 | L | 4 | Go | 未开始 |
| T-009 | PutObject | Commit 与状态回写（ETag/metadata） | 上传完成语义 | T-008 | P0 | M | 2 | Go | 未开始 |
| T-010 | Multipart | CreateMultipartUpload 与 upload session | MPU 会话管理 | T-006,T-007 | P0 | M | 2 | Go | 未开始 |
| T-011 | Multipart | UploadPart 流式分片传输 | 分片传输能力 | T-010 | P0 | L | 3 | Go | 未开始 |
| T-012 | Multipart | CompleteMultipartUpload -> CommitBurnJob | 分片完成语义 | T-011 | P0 | M | 2 | Go | 未开始 |
| T-013 | 状态查询 | HeadObject/GetObjectAttributes 状态映射 | 状态可读 | T-009,T-012 | P1 | M | 2 | Go | 未开始 |
| T-014 | 列举能力 | ListObjectsV2 最小可用实现（任务目录视图） | 列举能力 | T-013 | P2 | M | 2 | Go | 未开始 |
| T-015 | 错误处理 | gRPC/PrimoBurner 异常到 S3 错误映射表 | 错误映射实现 | T-008 | P0 | M | 2 | Go+.NET | 未开始 |
| T-016 | 幂等 | 幂等键与重复请求处理 | 幂等模块 | T-008,T-011 | P0 | M | 2 | Go | 未开始 |
| T-017 | 恢复能力 | 服务重启后任务状态恢复 | recovery 机制 | T-016 | P1 | L | 3 | Go | 未开始 |
| T-018 | .NET 服务 | Burn Engine 封装（PrimoBurner BlockDevice） | burn executor | T-003 | P0 | L | 5 | .NET | 未开始 |
| T-019 | .NET 服务 | 任务状态机与队列调度 | job scheduler | T-018 | P0 | L | 4 | .NET | 未开始 |
| T-020 | .NET 服务 | 流控与 backpressure 协议实现 | 流控机制 | T-018 | P0 | M | 3 | .NET | 未开始 |
| T-021 | 可观测性 | 结构化日志、traceId 贯穿 | 日志规范 | T-007 | P1 | S | 1.5 | Go+.NET | 未开始 |
| T-022 | 可观测性 | metrics: 吞吐/延迟/失败率/队列深度 | 指标埋点 | T-021 | P1 | M | 2 | Go+.NET | 未开始 |
| T-023 | 测试 | 单元测试（Go/.NET） | test cases | T-008~T-020 | P0 | L | 5 | QA+Dev | 未开始 |
| T-024 | 测试 | 集成测试（S3 client->光驱模拟） | 集成报告 | T-023 | P0 | M | 3 | QA | 未开始 |
| T-025 | 测试 | 故障注入（断网/超时/介质错误） | 故障测试报告 | T-024 | P1 | M | 3 | QA/SRE | 未开始 |
| T-026 | 发布 | 灰度发布与回滚脚本 | 发布方案 | T-024 | P1 | S | 1.5 | SRE | 未开始 |
| T-027 | 文档 | 运维手册与故障 Runbook | Runbook | T-025 | P1 | S | 1.5 | SRE | 未开始 |
| T-028 | 验收 | UAT 与上线评审 | 验收报告 | T-026,T-027 | P0 | S | 1 | PM/业务 | 未开始 |

---

## 5. 开发任务台账（可持续跟踪模板）

> 文件建议每周更新一次，并在每日站会中更新“状态/阻塞项/下步动作”。

### 5.1 台账字段说明
- **状态**：未开始 / 进行中 / 阻塞 / 已完成 / 已取消
- **进度%**：0~100
- **风险等级**：低/中/高
- **阻塞原因**：依赖未完成、环境问题、需求变更等
- **下一步**：可执行的下一动作（最小粒度）

### 5.2 台账主表（初始）

| 任务ID | 任务名称 | 负责人 | 开始日期 | 计划完成 | 实际完成 | 状态 | 进度% | 风险 | 阻塞原因 | 下一步动作 | 备注 |
|---|---|---|---|---|---|---|---:|---|---|---|---|
| T-001 | 需求澄清与验收标准定义 | PM | 2026-05-11 | 2026-05-12 |  | 未开始 | 0 | 中 |  | 组织需求评审会议 |  |
| T-002 | 架构评审 | 架构 | 2026-05-12 | 2026-05-14 |  | 未开始 | 0 | 中 | 依赖 T-001 | 输出 E2E 架构图 |  |
| T-003 | proto v1 定义 | Go/.NET | 2026-05-13 | 2026-05-16 |  | 未开始 | 0 | 高 | 依赖 T-002 | 对齐流控字段与状态码 |  |
| T-005 | 后端入口与配置 | Go | 2026-05-15 | 2026-05-16 |  | 已完成 | 100 | 低 |  | 持续补齐更多 S3 接口 | 内置后端入口与配置已可用 |
| T-008 | PutObject 流式上传 | Go | 2026-05-17 | 2026-05-22 |  | 未开始 | 0 | 高 | 依赖 grpc client | 先打通 100MB 样例链路 |  |
| T-011 | UploadPart 分片流式 | Go | 2026-05-23 | 2026-05-27 |  | 未开始 | 0 | 中 | 依赖 T-010 | 完成 part 粒度重试 |  |
| T-018 | Burn Engine 封装 | .NET | 2026-05-15 | 2026-05-23 |  | 未开始 | 0 | 高 | PrimoBurner 细节待验证 | 先实现 mock BlockDevice |  |
| T-019 | 任务队列与状态机 | .NET | 2026-05-24 | 2026-05-29 |  | 未开始 | 0 | 中 | 依赖 T-018 | 定义状态迁移图 |  |
| T-023 | 单元测试 | QA/Dev | 2026-05-30 | 2026-06-05 |  | 未开始 | 0 | 中 | 依赖主链路完成 | 先补接口级 UT |  |
| T-024 | 集成测试 | QA | 2026-06-06 | 2026-06-10 |  | 未开始 | 0 | 高 | 依赖测试环境 | 准备测试介质与驱动器 |  |

---

## 6. 风险台账（初版）

| 风险ID | 描述 | 概率 | 影响 | 等级 | 应对策略 | 责任人 | 状态 |
|---|---|---|---|---|---|---|---|
| R-001 | BurnBridge 后端与主程序版本兼容性导致运行异常 | 中 | 高 | 高 | 固定 Go 版本+构建镜像+CI 强校验 | Go负责人 | 打开 |
| R-002 | BlockDevice 在长时间流式写入下出现吞吐波动 | 中 | 高 | 高 | 加入流控窗口、队列缓冲、动态 chunk 调优 | .NET负责人 | 打开 |
| R-003 | 光盘介质错误/驱动不稳定导致失败率上升 | 中 | 高 | 高 | 重试与介质检测、失败隔离、告警 | SRE/.NET | 打开 |
| R-004 | S3 语义与异步刻录语义不一致引发客户端误解 | 中 | 中 | 中 | 明确 API 文档，定义完成语义与状态查询 | 架构/PM | 打开 |
| R-005 | 故障恢复不完善导致任务丢失或重复刻录 | 低 | 高 | 中 | 幂等键+持久化任务日志+恢复扫描 | Go/.NET | 打开 |

---

## 7. 质量与验收标准

### 7.1 功能验收
- 支持 PutObject 上传并触发刻录任务。
- 支持 Multipart 上传并完成刻录提交。
- 支持任务状态查询与失败原因回传。

### 7.2 性能指标（建议初始门槛）
- 单文件 1GB 上传成功率 >= 99%。
- 端到端成功任务中位时延满足业务阈值（由业务定义）。
- 长稳压测 8 小时无内存泄漏级别问题。

### 7.3 稳定性指标
- 故障注入（断连、超时、介质异常）通过率 >= 95%。
- 服务重启后任务可恢复率 >= 99%。

---

## 8. 迭代节奏与例会机制
- **每日站会（15min）**：更新任务台账状态、阻塞项、当日计划。
- **每周评审（60min）**：里程碑进展、风险复盘、范围调整。
- **阶段评审（里程碑结束）**：演示、验收、下阶段目标确认。

---

## 9. 文件与维护建议
- 本计划文件：`doc/plan/burn-bridge-plugin-dev-plan.md`（建议后续重命名为 `burn-bridge-backend-dev-plan.md`）
- 建议新增：
  - `doc/plan/burn-bridge-weekly-status.md`（周报）
  - `doc/plan/burn-bridge-risk-log.md`（风险动态）
  - `doc/plan/burn-bridge-change-log.md`（需求变更记录）

> 建议由 PM 维护“计划与台账”，技术负责人维护“风险与技术决策记录（ADR）”。

# idkv

Rust 动态文档与批量 Lookup 引擎，提供嵌入式 API、HTTP 服务及一主一从异步复制。WAL、LSM、SST、MVCC、索引、恢复和合并均由本项目实现，不依赖 MongoDB、RocksDB、redb 或其他数据库引擎。

GitHub 项目名为 `idkv`。为保留现有集成兼容性，Rust 包及服务命令仍为 `tenant-kv`，库导入名为 `tenant_kv`。

每个租户拥有独立 UUID 目录、目录锁、提交序号、WAL、Manifest、SST、索引和缓存实例。一个租户内的集合与索引使用同一套 LSM。租户名称和文档中的 `tenant` 字段不会参与文件路径或权限判断。

当前为可运行版本。测试证据、性能测量及尚未完成的生产资格验证分别记录在各版本报告中。WAL/SST 格式目前为 v1；v0.8 根注册表为 v4，旧版 Store 拒绝打开；复制协议为 3，主从需同时升级。升级前应备份，步骤与兼容性见 [中间件集成契约](docs/middleware.md)。

针对 75e2938 的复查已修复复制导出在副本重启后不能释放、保留 WAL 不推进及管理响应重新打开租户的问题，并收紧一致性等待和在线索引背压边界。实际重启对比、并发负载、故障验证及剩余限制见 [复查修复报告](docs/review-followup-20261106.md)。

后续[导出生命周期修复](docs/export-lifecycle-20261106.md)去掉了一小时总期限，将检查点准备和目录清理移出导出锁，清理失败由后台重试，并增加复制初始化/重建计数，便于发现日志窗口不足导致的反复初始化。

[复制安全修复](docs/replication-safety-20261106.md)进一步阻止不确定 WAL 位置参与计划切换，避免活动句柄导致反复下载整库，并补齐准备任务 panic、保留 WAL 缺失及恢复配额竞态的处理。报告包含真实 fsync EIO 后切换的分叉复现与修复对照。

v0.10.1 优化客户端读取路由与后台维护：一秒路由缓存、并发发现合并、正文/索引排队读取取消，以及写锁外磁盘用量扫描。缓存不改变最低读取位置、游标绑定或写入确认语义。使用见[读取控制](docs/read-controls.md)，前后对比与验证范围见 [v0.10.1 报告](docs/v0.10.1-validation.md)。

v0.10 增加[请求级读取控制和完整主从读取客户端](docs/read-controls.md)：共享截止时间、扫描上限、禁止全扫描、协作式取消，以及普通/批量/发布查询的游标节点绑定。Query JSON、DSL 语法、持久格式和复制协议不变。新客户端要求节点确认读取控制能力，先升级主从。可运行示例为 [read_controls](examples/read_controls.rs)，验证范围见 [v0.10 报告](docs/v0.10-validation.md)。

v0.9.1 完善主键范围定位、二级索引 IN 多段查询和安全的稀疏唯一索引范围读取；非唯一批量 Lookup 复用 SST 扫描状态及已解压块。HTTP、DSL、持久格式与复制协议保持不变。使用边界见 [API 查询契约](docs/api.md)，前后测量与回归范围见 [v0.9.1 验证报告](docs/v0.9.1-validation.md)。

v0.9 增加[通用 KV 组合能力](docs/kv-capabilities.md)：只检查不修改的条件批次、跨集合同快照 read_batch，以及不依赖 Lookup 的不可变数据集发布和按版本 Get/MultiGet/Query。业务层保留 Schema、来源规则和同步决策。持久格式及复制协议不变，现有身份接口保持兼容。回归、实际 HTTP 与兼容边界见 [v0.9 验证报告](docs/v0.9-validation.md)。

v0.8.1 修复时钟回拨下过期重试可能再次获准、小字节预算导致历史回收不前进、副本达到前台磁盘门槛后无法应用删除记录的问题；同时优化多文件租户恢复时的文件归属检查。格式与复制协议保持 v0.8 不变，验证结果和范围见 [v0.8.1 验证报告](docs/v0.8.1-validation.md)。

v0.8 增加[租户级统一历史保留](docs/retention.md)：一个 retention_seconds 控制旧发布、回执和时态知识的回收。当前状态不失效；旧业务状态从被覆盖后计时，回执从提交后计时。安全重试凭据、时态边界基线和分批 WAL 回收保护快照与复制。发布请求 ID 与响应实际版本分离；调用方需按升级说明适配。原始 92 项回归、真实旧版升级及持续回收结果见 [v0.8 验证报告](docs/v0.8-validation.md)。

v0.7 增加带历史的 CAS、普通写入持久化重试、稳定 subject、不变 Lookup 发布版本、主从客户端和就绪/排空生命周期；同时支持独立可开关的 [Prometheus 指标、OTLP 链路与日志](docs/telemetry.md)。使用与升级见 [中间件集成](docs/middleware.md)，运行证据与限制见 [v0.7 验证报告](docs/v0.7-validation.md)。

v0.6.1 优化复制 WAL 顺序读取、内存表记录插入及固定结构的时态解码，保留原有持久化和查询语义。前后对比、资源边界及运行验证见 [v0.6.1 优化报告](docs/v0.6.1-validation.md)。

v0.6.0 增加 [时态身份第一阶段](docs/identity.md)：受管集合、自动/手动有效区间、永久知识版本、条件更新、幂等收据和基础解析；同时优化长扫描与复制追赶期间 Lookup，修复无变更事务提前确认 staged 数据的问题。使用 [Rust 示例](examples/identity.rs) 或 [HTTP 示例](scripts/identity_smoke.py)，性能及稳定性证据见 [v0.6 验证报告](docs/v0.6-validation.md)。启用时态写入前需升级全部主从节点。

v0.5.2 修复租户删除结果不确定时仍能确认写入、接任节点配置不匹配时提前停写两个严重问题；主键 Get/MultiGet 增加有界、快照感知的集合身份缓存，减少重复元信息读取。协议和持久化格式不变。复现、前后测量及故障验证见 [v0.5.2 性能与稳定性报告](docs/v0.5.2-validation.md)。

v0.5.1 优化复制追赶、整页 WAL 写入、响应内存计费和从库查询缓存；修复单租户生命周期阻塞拖住其他租户同步的问题，补齐从库写入统计。该轮测量见 [v0.5.1 性能与稳定性报告](docs/v0.5.1-validation.md)。

v0.5 增加 [传统主从复制与切换](docs/replication.md)：检查点初始化、原子 WAL 增量、断线追赶、从库只读、持久停写、双向计划切换及显式故障升主。默认配置一主一从，异步确认本地持久化；不包含半同步或自动选主。实际故障测试范围见 [v0.5 验证报告](docs/v0.5-validation.md)。

v0.4 修复并发游标重复续页和 MultiGet 响应计费遗漏；优化热点历史版本、事务点读、二进制排序及完整文档 Top-K 延迟解码，并缩短刷盘轮转占用的排他锁时间。新增多租户持续负载下的批次原子性、索引一致性及重开验证。前后对比和压力测试范围见 [v0.4 性能与稳定性报告](docs/v0.4-validation.md)。

v0.3 增加 [DSL 与准备语句](docs/dsl.md)，HTTP 入口为 `POST /v1/tenants/{tenant}/dsl`。同时优化 SST 点读、二进制字段过滤、`in` 集合判断、批量 Lookup 和快照感知的查询元信息缓存。实际前后对比、并发负载和适用边界见 [v0.3 验证报告](docs/dsl-validation.md)。

v0.2 增加 [OpenDAL S3 备份](docs/backup.md)：本地存储为主、异步检查点备份、SST 增量复用、完整性校验、租户恢复、新机器导入和引用保留清理。配置入口为 `--backup-config`，示例见 [backup-s3.json](examples/backup-s3.json)。该版本运行证据见 [备份验证报告](docs/backup-validation.md)；原性能数据是此前基线，不能代表开启备份后的测量。

## 构建与运行

GitHub 仓库提交源代码、测试、文档和验证记录，使用以下命令从源码构建。Linux 验证二进制动态链接 glibc，构建哈希记录于对应验证报告，不提交到源码仓库。本次不创建 GitHub Release。

需要 Rust 1.90+ 和 Unix 文件系统；Linux 为已验证平台。运行目录必须支持文件锁、原子重命名及文件/目录 `fsync`。

```sh
cargo build --locked --release --bin tenant-kv --bin tenant-kv-restore
cargo run --locked --example embedded -- /var/tmp/tenant-kv-demo
cargo run --locked --example dsl -- /var/tmp/tenant-kv-dsl-demo
cargo run --locked --example identity -- /var/tmp/tenant-kv-identity-demo
cargo run --locked --example kv_capabilities -- /var/tmp/tenant-kv-capabilities-demo
cargo run --locked --example read_controls -- /var/tmp/tenant-kv-controlled-reads
cargo test --locked --features failpoints -- --test-threads=1
```

HTTP 服务使用预先配置的 Bearer token 哈希。创建至少 16 字节的随机 token，将原文交给调用方，仅将 SHA-256 写入权限配置；不要使用示例值作为生产凭证。

```sh
openssl rand -hex 32 > token.txt
cat token.txt | target/release/tenant-kv token-hash
```

将输出填入 `auth.json`：

```json
{"tokens":[{"sha256":"填写上一步得到的64位哈希","subject":"operator","tenant":null,"role":"admin"}]}
```

```sh
target/release/tenant-kv serve --root /var/tmp/tenant-kv-data --auth auth.json --listen 127.0.0.1:8080
```

`tenant: null` 允许全局管理员及专用 `replication` 凭证；复制凭证不能调用普通业务接口。租户凭证必须绑定租户 UUID，角色为 `read`、`write`、`identity_write`、`identity_correct` 或 `admin`。租户身份和路由最低权限在读取请求正文、打开引擎之前校验；DSL 的命令读写权限在正文解码后、打开引擎之前检查，完整语法解析后再复核。默认监听回环地址；需要远程访问时，在服务前配置 TLS。权限文件在启动时加载，修改后需重启。

执行完整 HTTP 示例：

```sh
python3 scripts/http_smoke.py http://127.0.0.1:8080 token.txt
python3 scripts/dsl_smoke.py http://127.0.0.1:8080 token.txt
```

该脚本创建自己的示例租户，演示集合、写入、索引、Lookup、查询、备份和恢复，并删除创建的租户。备份保留在服务的数据目录中。

执行一主一从及切换示例（目录必须不存在，脚本结束会关闭两边）：

```sh
python3 scripts/replication_demo.py --binary target/release/tenant-kv --root /var/tmp/tenant-kv-pair-demo
```

长期部署通过 `--replication-config` 指定 [主库](examples/replication-primary.json) / [从库](examples/replication-replica.json) 配置。完整凭证、切换及故障隔离步骤见复制文档。

## 已实现能力

- 动态 JSON、精确整数、嵌套 JSON Pointer、对象与数组投影；`_key` 主键和 `_rev` 条件更新。
- 单集合原子写批次、串行写入协调、group commit、持久化后确认、未知提交结果隔离。
- 单字段、复合、稀疏唯一和覆盖索引；在线快照构建与持久化增量追平。
- 专用 Get、按块分组的 MultiGet、固定 Lookup 计划、批量结果顺序恢复。
- 布尔过滤、严格类型比较、多字段排序、有界 Top-K、快照游标分页、执行计划与实际读取统计。
- 七命令 DSL、值参数绑定、可复用准备语句、分片语法缓存、带位置的语法错误；原有 JSON API 保持可用。
- 带校验和的分段 WAL、压缩 SST、分区索引与 Bloom Filter、分层合并、快照版本保留。
- 按需打开、空闲关闭、租户配额、共享且有界的维护线程池与轮询 I/O 队列。
- 租户检查点、备份、恢复切换、完整性检查、删除及崩溃恢复。
- OpenDAL S3/fs 远端备份、持久任务与中断恢复、同租户文件增量复用、SHA-256 校验、保留清理和离线灾难恢复工具。
- 一主一从异步复制、检查点重建、源日志保留、从库只读、持久切换状态及按历史与序号等待写后读。
- `/v1` JSON HTTP API、角色授权、OpenAPI 3.1、资源与存储错误分类。

不包含 SQL、聚合、全文、CIDR、多数派共识、自动故障转移、跨租户查询或事务。异步故障升主可能丢失尚未复制的成功写入，必须先在外部隔离旧主。单进程或共享磁盘故障仍可能影响多个租户。

## 文档与源码

| 内容 | 位置 |
|---|---|
| 架构、持久化协议与格式 | [docs/format.md](docs/format.md) |
| 接口、查询和 Lookup 语义 | [docs/api.md](docs/api.md) |
| DSL 语法、HTTP/Rust 用法和预算 | [docs/dsl.md](docs/dsl.md) |
| v0.3 DSL 与性能优化的运行证据 | [docs/dsl-validation.md](docs/dsl-validation.md) |
| v0.4 性能与稳定性、缺陷复现及回归 | [docs/v0.4-validation.md](docs/v0.4-validation.md) |
| 一主一从部署、切换与故障恢复 | [docs/replication.md](docs/replication.md) |
| v0.5 回归、双进程与切换故障验证 | [docs/v0.5-validation.md](docs/v0.5-validation.md) |
| v0.5.1 复制优化、资源边界与运行验证 | [docs/v0.5.1-validation.md](docs/v0.5.1-validation.md) |
| v0.5.2 严重缺陷修复、主键读取优化与运行验证 | [docs/v0.5.2-validation.md](docs/v0.5.2-validation.md) |
| 配额、备份恢复和故障处理 | [docs/operations.md](docs/operations.md) |
| 主节点丢失、外部隔离、故障升主、重建与备份恢复演练 | [docs/recovery-drill.md](docs/recovery-drill.md) |
| 当前版本并行负载、磁盘不足、WAL I/O 故障与重启资格验证 | [docs/production-qualification.md](docs/production-qualification.md) |
| 后台备份进度同步与轻租户写入尾延迟优化 | [docs/tail-latency-optimization.md](docs/tail-latency-optimization.md) |
| OpenDAL 配置、备份协议与恢复 | [docs/backup.md](docs/backup.md) |
| v0.2 备份实现验证 | [docs/backup-validation.md](docs/backup-validation.md) |
| 实际测试结果、复现命令与限制 | [docs/validation.md](docs/validation.md) |
| 嵌入式示例 | [examples/embedded.rs](examples/embedded.rs) |
| DSL 完整示例 | [examples/dsl.rs](examples/dsl.rs) |
| 原生读路径前后对比 / DSL 开销基准 | [examples/read_perf.rs](examples/read_perf.rs)、[examples/dsl_bench.rs](examples/dsl_bench.rs) |
| 热点历史版本及非索引排序基准 | [examples/deep_perf.rs](examples/deep_perf.rs) |
| 基准装载及混合负载 | [examples/bench.rs](examples/bench.rs) |
| 多租户运行验证 | [examples/soak.rs](examples/soak.rs) |
| 主从追赶与轻租户延迟对比 | [examples/replication_perf.rs](examples/replication_perf.rs) |
| 原始测试与测量结果 | [bench-results/](bench-results/) |
| OpenAPI 3.1 文件 | [openapi.json](openapi.json) |
| 租户管理、文档与索引 | `src/tenant/` |
| 存储、提交、读取与合并 | `src/storage/` |
| 查询规划与执行 | `src/query/` |
| DSL 解析、绑定和缓存 | `src/dsl/` |
| 复制协议、网络服务与持久节点角色 | `src/replication/` |
| HTTP 与 OpenAPI | `src/http.rs`、`src/http/openapi.rs` |

可通过 `--config config.json` 覆盖 `Options` 的任意配置字段；未指定字段保留默认值。配置边界及预算含义见运维文档。

针对外部报告的逐项核实、并发隔离与数据安全修复及回归边界，见[报告核实与修复](docs/review-remediation-20261106.md)。

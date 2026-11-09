# 主体迁移与 GC 的百万行规模验证（2026-11-09）

基线为 `9f5c657`。本轮审查迁移和回收的扫描成本、批次推进、历史保留边界、资源竞争与重启续建，新增可选执行的 `identity::subject_stress::million_history`；常规测试不会自动生成百万行数据。

## 确认并修复的问题

1. **迁移与 link 回收逐行重建范围扫描器，重复读取同一 SST 数据块。** 每次 gc_prefix(..., 1) 都重新建立多个表游标；顺序扫描不向数据块缓存准入，因此同行附近的下一条记录又需要一次块读取。修复后每个事务复用一个流式扫描器，仍逐行解码、按字节/时间/物理条数停止，只在跨越已回收尾部时重新定位。原有 gc_prefix 的事务内修改叠加语义保留；流式接口仅用于不改写尚未访问源键的维护路径。
2. **历史 GC 被固定在每轮最多 16 行，落后于迁移与 link 回收。** 对照组约 239.9 秒仅清理 283,010 行历史，link 已清理约 109.9 万行。改为默认最多 64 条物理记录，并在下一条之前检查 10ms 协作预算；批次/字节上限不足仍缩小事务，删除和游标一起提交。不会借用客户端的查询超时使整步反复回滚。

把本轮回归放到原实现运行，明确出现两项失败：

- 257 行、相同 5 次迁移提交：原实现产生 **275 个 I/O 调度任务**；修复后 **22 个**，减少约 **92%**。用 I/O 次数保护重复读取回归，不用容易抖动的墙钟时间作断言。红/绿运行的编译优化级别不同，不用耗时推导速度倍数。
- 一个标识已完成迁移、另一个仍在迁移：原实现 3 轮 GC 后，第一个标识仍有 128 行；修复后回收到应保留的 67 行，第二个标识继续保留 128 行。正/负前驱基线和查询结果同时核对。

## 环境与数据

OrbStack Ubuntu 25.10 / Linux aarch64，Rust 1.99.1，普通用户 UID 501。存储和请求预算使用默认值，idle_seconds 延长至 3600，避免试验中的持有句柄被无关空闲策略关闭。

规模验证使用 release 优化，构建时关闭 LTO、codegen-units=16；这是缩短验证构建时间的设置，不是产品默认发布配置。共享机器上同时存在其他编译及测试，操作系统页缓存未强制清空；以下数字是执行记录，不作为冷盘性能、发布 SLO 或严格受控的延迟因果对比。

数据是**旧存储形态夹具**：通过真实 WAL 事务写入历史正文，再 flush/compact 到真实 SST，不用内存 mock，不伪造迁移进度。它不等同于让旧版二进制执行一百万次 API 写入。每个租户有 32 个反复变化的标识、两个主体；成员会转移，并包含空时间线，检验缺席的前驱基线。查询核对知识头和保留边界两个知识版本，不声称逐个查询了全部中间版本。

## 默认调度：8 个租户合计百万行

每租户 125,000 行，合计 **1,000,000 行**。迁移使用正常的 100ms 协调调度和最多 4 个维护 worker。

- 在累计 **50,169** 行、**502,776** 行处分别 SIGKILL；恢复时断言持久进度不回退，且 ready 状态不丢失。第一段使用原实现，之后使用修复版。
- 剩余迁移、GC 和随后 30 秒的空闲维护阶段，4 个大租户和 1 个无旧历史的轻租户持续发起前台写入，并立即读取刚写的值。前期对照/探索阶段每写线程最多 1000 次，后期改为持续到维护阶段结束；因此不将两者的整体写入延迟冒充相同负载的修复前后对比。
- 迁移最终全部 ready，正向 resolve 与完整分页反查一致；此时核对 **23,598** 条已落账成功写入。
- GC 在运行约 **120.5 秒**时再次 SIGKILL，恢复后继续历史游标、link 游标和计数。恢复段约 **141.2 秒**完成两种 GC、归属核对及最终 flush/compact；两段合计约 **261.7 秒**，不含进程重启间隔。
- 每租户最终恰好 **289 行历史、353 条 link**；合计 **2,312 行历史、2,824 条 link**。历史窗口 257 行加每标识一个前驱，满足夹具预期；空关系基线也保留。
- GC 后、30 秒同样前台写入及另一次独立重启校验，累计核对 **64,175 条完整落账的成功写入**，值全部匹配。迁移/GC 均没有发生进度回退、挂死或观测到维护错误。显式记录拒绝计数的 GC 恢复段和空闲段均为 0。
- 每秒采样的进程 RSS 峰值约 **300.6 MiB**，不是内核记录的精确瞬时峰值。

成功回执另存客户端账本并 fsync；进程可能死在账本写到一半，重启只丢弃没有完成换行的最后一个记录，不丢弃任何完整记录，不修改数据库 WAL。未完整落账和终止瞬间的请求属于结果未知，不计入上述已确认数；可能存在额外的成功数据库提交。

### 前台延迟仍受共享磁盘影响

下表来自修复版的持续写入阶段，单位 ms。p99 按成功样本排序取第 99 百分位；读取是刚写入值的立即点读，主要命中内存，不能代表冷 SST 读取。

| 阶段 | 租户 | 成功样本 | 写入 p50 / p99 | 点读 p99 |
|---|---|---:|---:|---:|
| 迁移剩余段 | 大租户合计 | 14,374 | 4.569 / 95.979 | 0.029 |
| 迁移剩余段 | 轻租户 | 3,627 | 4.598 / 73.944 | 0.020 |
| GC 两段 | 大租户合计 | 28,529 | 4.269 / 104.033 | 0.029 |
| GC 两段 | 轻租户 | 7,193 | 4.198 / 143.705 | 0.022 |
| GC 后 30 秒 | 大租户合计 | 3,886 | 4.003 / 19.757 | 0.034 |
| GC 后 30 秒 | 轻租户 | 969 | 4.126 / 21.992 | 0.035 |

重复块读取和低 GC 吞吐已经修复；这些数据**不证明前台尾延迟隔离已经解决**。10ms 只限制事务内可协作的扫描工作，不能抢占 fsync、compaction 或底层共享设备。持续大规模维护期间仍需观察轻租户延迟并按业务 SLO 控制维护窗口。

## 单租户独占百万行：饱和驱动与恢复

另建独立目录，将 **1,000,000 行**全部放入一个租户，与一个无旧历史的轻租户持续并发写入、立即点读。两组场景使用独立目录，同一场景重启继续原目录。本场景启用测试专用 burst 驱动：在默认维护 worker 外，每 1ms 直接调用已有维护流程，以检验更大单库和高频维护竞争；它验证算法、预算和恢复，**不是默认 100ms 调度的吞吐测试**。

- 在 **507,475 行**处 SIGKILL，第一段约 70.0 秒；重新打开同一目录，断言进度不回退，再约 106.1 秒完成迁移与归属/回执核对。此时完整账本含 **11,142** 条成功写入。
- GC 约 **254.5 秒**完成，包括最终归属核对、flush/compact 和读取核对；恰好保留 **289 行历史、353 条 link**。
- 两次迁移进程和 GC 过程的显式前台拒绝计数均为 **0**，采样进度无维护错误。每秒采样 RSS 峰值约 **195.8 MiB**。
- GC 后另起进程验证，共 **27,500** 条已落账成功写入全部匹配，两个保留知识版本的正反向结果一致。

两组独立数据合计核对 **91,675** 条完整落账成功写入。资格场景共执行 4 次 SIGKILL：8 租户迁移两次、GC 一次，单租户迁移一次；每次均重新打开原目录续建。

## 可重复执行

先构建可选测试，在 OrbStack Linux 中运行；ROOT 必须是新目录。下面省略日志重定向，实际执行时将 stdout/stderr 保存在 /var/tmp：

```sh
export CARGO_TARGET_DIR=/var/tmp/idkv-rust199-target-20261109
export TMPDIR=/var/tmp/idkv-rust199-tmp-20261109
export CARGO_PROFILE_RELEASE_LTO=false
export CARGO_PROFILE_RELEASE_CODEGEN_UNITS=16
cargo test --locked --release --all-features --lib identity::subject_stress::million_history --no-run

export TKV_SUBJECT_STRESS_ROOT=/var/tmp/idkv-subject-scale-new
TKV_SUBJECT_STRESS_STAGE=seed cargo test --locked --release --all-features --lib identity::subject_stress::million_history -- --ignored --exact --nocapture
TKV_SUBJECT_STRESS_STAGE=run TKV_SUBJECT_KILL_AFTER=500000 cargo test --locked --release --all-features --lib identity::subject_stress::million_history -- --ignored --exact --nocapture
# 上一步按要求 SIGKILL，预期非零退出；不再设置 KILL_AFTER，继续原目录。
TKV_SUBJECT_STRESS_STAGE=run cargo test --locked --release --all-features --lib identity::subject_stress::million_history -- --ignored --exact --nocapture
TKV_SUBJECT_STRESS_STAGE=gc cargo test --locked --release --all-features --lib identity::subject_stress::million_history -- --ignored --exact --nocapture
TKV_SUBJECT_STRESS_STAGE=idle cargo test --locked --release --all-features --lib identity::subject_stress::million_history -- --ignored --exact --nocapture
TKV_SUBJECT_STRESS_STAGE=verify cargo test --locked --release --all-features --lib identity::subject_stress::million_history -- --ignored --exact --nocapture
```

seed 可设 TKV_SUBJECT_STRESS_TENANTS=1，将同样的一百万行放进一个租户。TKV_SUBJECT_STRESS_BURST=1 会增加直接调用现有维护接口的测试驱动，省去 100ms 调度等待，适合单库饱和度与恢复验证；它不是服务配置，也不代表默认调度的生产升级耗时。

stdout 为逐行 JSON；持久 progress.json 和 scale.json 沿用带 TKJ1 校验头的内部元数据格式，应使用 storage::read_json 读取，不应直接当作裸 JSON 解析。重启时若 stderr 提示丢弃客户端账本的最后一个未完成记录，完整记录仍逐一核对。

常规 `cargo test --locked --release --all-features --no-fail-fast -- --test-threads=4` **294 项通过**，另有本规模测试 1 项默认忽略。两项性能回归在原实现失败、修复版全部 7 项主体回归通过；最终 Clippy（-D warnings）、格式和 diff 检查通过。

Linux 原始日志前缀：/var/tmp/idkv-subject-scale- 和 /var/tmp/idkv-subject-single-；大文件未提交进仓库。未执行真实设备掉电/介质错误、满盘、跨节点复制或百万不同标识的实验，不将 SIGKILL 解释为这些故障的等价验证。

# 主体索引迁移与回收优化验证（2026-11-09）

本记录之后的百万行历史、多租户并发、强制终止和扫描 I/O 放大修复，见[规模验证记录](subject-maintenance-scale-validation-20261109.md)。下文保留本阶段原始结果。

本轮接续 `bc2e3ab`，针对旧数据迁移、历史 GC、反向 link 回收、批量容量和 history 时间过滤的反馈。此前主体查询验证记录保留原环境和测量，本记录说明本轮增量。

## 确认与处理

| 反馈 | 确认结果与处理 |
|---|---|
| 每次只迁移一行，迁移期间历史 GC 全部等待 | 已复现旧实现 32 次维护提交仍未完成 257 行迁移。改成每步最多 64 行源记录及 64 次 link 写入，另有 1 MiB 输入和 10ms 协作预算；历史 GC 可先处理已完整迁移的标识。仍在迁移的标识保留必要历史，避免丢失负向关系。 |
| link 回收每步四行、重复扫描 | 改成最多 64 行的小事务；保留正/负前驱基线，跳过已经回收的旧尾部。水位不变且回收完成后不反复写状态。 |
| 内部记录使原业务批次超限 | 实测成立。保留展开记录的硬上限，避免放大 WAL 帧和原子提交内存；补充开销公式、升级说明及成功/拒绝的原子性回归。 |
| history 的 recorded_from 不提前结束 | 已复现旧实现扫描预算耗尽。现在遇到第一条更早的事件即结束；已过期但正文尚未物理回收的事件，也可根据固定时间字段结束，不必解码用户属性。 |
| 启动强制升级注册表 v6 | 属于排除旧写入器的兼容边界，保留。补充首次 v4/v5 升级必须有可写空间的说明；健康 v6 根不会仅因格式检查再次升级。 |

10ms 和输入字节数是协作预算，不能中断已经开始的单条记录读取、编码、操作系统 I/O 或 fsync；单条大记录仍允许推进。遇到准备阶段的预算不足会缩小批次；WAL 追加或同步后的错误不会触发整步自动重做。旧迁移游标和未完成 link 的偏移仍持久化，较小的客户端查询预算不会阻止后台重建。

新增持久迁移进度 `ready`、`processed_history_rows`、`written_links`、`gc_scanned_rows`、`gc_removed_rows`，通过租户 retention 状态读取。进度随成功事务保存，重启继续累计；旧状态的新增计数从本版开始观测，不追溯之前的工作。status 和 Prometheus 使用本节点维护完成后的缓存，不为抓指标打开冷租户或扫描磁盘；这些指标是已观测工作量，不是全量数据计数或完成百分比。

## 工具链和环境

- OrbStack Ubuntu 25.10，Linux aarch64，普通用户 UID 501；运行验证全部在 Linux 完成。
- `rustc 1.99.1 (58cf8f9c3 2026-10-13)`，`cargo 1.99.1 (5f94df478 2026-08-27)`。版本来自 [Rust 官方最新稳定版发布页](https://blog.rust-lang.org/2026/10/15/Rust-1.99.1/)。
- `rust-toolchain.toml` 固定 1.99.1，Cargo 最低编译器声明与 README 同步。macOS 同样安装该版本，用于格式检查；依赖锁文件未改变。

## 实际验证

修复前使用旧工具链运行同一行为回归，257 行迁移在 32 步内未完成，带 recorded_from 的小扫描预算查询失败。修复后，先将这 257 行旧历史刷入 SST，再进行迁移，**5 次提交完成，约 48.6ms**；核对历史知识版本的正反向归属、持久计数和重启后的状态。该数值是本机小样本，不是百万行升级耗时或生产性能承诺；前后编译器和存储夹具不同，不据此计算速度倍数。

其他回归覆盖：

- 两个标识各 128 个历史版本：迁移进入第二个标识时，第一个标识已能进行历史 GC，第二个仍保留所需记录；迁移和 link 回收完成后，比较不同知识版本的正向查询与反查，包括主体已经离开的负向基线。
- 重启续建、迁移游标之前的并发纠错、`query_ms=1` / `max_query_bytes=1`、`max_batch_operations=2`，缩小维护批次后仍能完成，计数不因失败重试而重复。
- 默认 1000 条展开限制下，同一主体绑定全新短标识：未启用 retention 的 199 项成功、200 项拒绝；启用后的 110 项成功、111 项拒绝。拒绝不推进 seq；核对全部 309 个成功标识、311 个被拒标识及 310 条主体事件，并核对幂等重试不新增事件。
- history 按精确的最后一次记录时间过滤，在扫描上限 8、页大小 2 时返回唯一事件、最多扫描两行、游标为空；把较早事件标成已到期但保留正文后重复验证。
- 回收完成且水位不变时，重复维护不增加提交序号；分页失败释放临时内存，旧游标仍可续用。

运行命令：

```sh
export CARGO_TARGET_DIR=/var/tmp/idkv-rust199-target-20261109
export TMPDIR=/var/tmp/idkv-rust199-tmp-20261109
cargo test --locked --all-features --no-fail-fast -- --test-threads=4
cargo test --locked --all-features --lib identity::subject_tests -- --nocapture --test-threads=1
cargo test --locked --all-features --test identity --test http --test middleware -- --test-threads=4
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo fmt --all -- --check
git diff --check
```

**全量 294 项通过。** 随后补充 SST 迁移夹具和过期正文过滤边界，受影响的主体回归 7 项及 identity/HTTP/middleware 集成测试 41 项再次通过。最终 Clippy（warnings 视为错误）、格式及 diff 检查通过。全量回归与最后一轮定向验证的执行顺序在此明确区分，未将定向验证计作第二次全量。

原始日志保留在 Linux `/var/tmp/`：`idkv-subject-batch-red-20261109.log`、`idkv-subject-rust199-all-20261109.log`、`idkv-subject-rust199-final-20261109.log`、`idkv-subject-rust199-reads-20261109.log`、`idkv-subject-clippy199-delivery-20261109.log`，未将批量日志加入仓库。

本轮未进行百万行、多租户长期升级 soak、真实磁盘满升级或设备故障演练。recorded_to 仍为顺序过滤；已经被物理删除的事件正文不能用其时间字段提前结束，仍受已有分页和扫描预算约束。未引入新的时间索引。

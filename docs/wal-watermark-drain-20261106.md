# WAL 同步水位与复制排空期限（2026-11-06）

后续变更（2026-11-08）：下文为历史验证结果；当前最后一个 WAL 的水位外坏帧恢复规则及其限制见 [WAL 尾部恢复验证](wal-tail-recovery-20261108.md)。

基点为 163ce1bc62e923b4a20c51b6c8e2841b4ff54022。无条件重写 WAL、长期句柄让排空无限等待、位置可见后读取仍短暂 BUSY、冷导出追平后长期留存均已从源码确认。前三项修复了行为边界，最后一项改为追平后尝试关闭。版本仍为 0.10.1，WAL/SST 字节格式与复制协议 3 不变。

## 同步水位

健康引擎最后一个引用释放时，先同步 Manifest 引用的未刷盘 WAL，再以已有原子元数据格式持久保存 WAL-SYNC：tenant、generation、WAL UUID 与有效长度。写入器已隔离或恢复只读时不会建立新水位。正常事务只追加 WAL，后续写入会超过旧水位；因此旧水位不能为新的未知尾部背书。

打开始终校验 WAL 身份、帧 CRC 与序号。只有水位匹配完整文件长度且没有残缺尾部时，才跳过重写；内容未变化的后续关闭也不重复同步和发布水位。水位缺失、校验失败、身份不匹配、尾部增长或不完整时，仍走新 inode 普通写入、fsync、rename、目录同步的恢复路径。水位不是绕过数据校验的信任捷径。

新增/扩展回归覆盖：跨 Store 重开两次保持 inode；删除水位后改用新 inode；恢复输出写完及发布后的两个 abort 点；未知 WAL 尾部阻止切换及恢复后核对。首次从旧版本打开没有水位的租户仍需恢复重建。异常退出但 WAL 与已有持久水位完全一致时可直接复用；新增尾部仍需重建。

## 满盘只读访问

若 WAL 已校验，但新文件重建所需空间不足或系统返回 ENOSPC，则保留已回放内容打开只读实例。不会将其他 I/O 错误、完整 CRC 损坏或内存配额错误当作满盘而忽略。残缺尾部不写回原文件，且所有修改入口均拒绝。

- 普通 get/query/index/lookup/check 可读取；context.writable=false，租户状态 stats.recovery_read_only=true。
- 写入、flush/compaction/GC 修改、复制日志应用与导出、复制位置确认及计划切换返回 RESOURCE_EXHAUSTED。只读回放可能包含先前未知提交，不能据此宣布持久追平或升主。
- 重用当前只读实例，避免每秒再次复制。释放空间和旧引用后 close/reopen，完成持久恢复后恢复可写；不会在仍持有旧引用时偷偷换库。
- 未变化的 Store 注册表和备份 TARGET 不再在启动时重复发布，保留格式迁移、生命周期恢复和备份目标变更校验。新建或尚需发布的控制面事务仍需要可写空间。

真实 ENOSPC 使用独立 256 MiB ext4 loop 卷，按普通用户可用空间填到 0；未卸载或删除填充文件之前完成重开与读取，随后才释放空间。本轮对照验证旧版冷打开与 Store 重启均报 IO_ERROR。新版同时验证健康关闭的非空 WAL 可在满盘时正常打开、异常退出且尾部增长的 WAL 在满盘重启后可只读访问、拒绝写入和 fence、连续读取不重复写盘，再释放空间恢复写入。数据按确认日志逐个核对正文与 _rev。

## 有界排空与读取入口发布

下载前和安装前分别设立安装闸门，每次读者排空等待最多 10 秒；1 秒后仍会撤销快照、清掉缓存游标。长期保留的 TenantHandle/Engine/Snapshot 引用不能由系统强制析构，但超时将释放闸门，旧句柄上的普通读取和新的 tenant() 都可恢复访问旧库。初始化按现有每租户 100 ms 至 30 s 指数退避重试，失败不继续永久占用初始化名额。

initialization_errors 按租户 UUID 保存最近的初始化失败原因，直到成功或该租户/来源移除。waiting_for_readers 仍表示当前排空等待。关闭闸门时唤醒 Engine 进度等待；wait_context 与 wait_replication 立即检查排空状态，返回 BUSY 并释放调用方的临时引用，不再等满 5 秒。缓存游标被撤销后仍需重新查询。

检查点位置发布与清除 installing 在同一把 slot 锁内完成，不依赖后台任务稍后 Drop。回归特意让安装 guard 继续存活，验证安装返回后已有位置可见且 tenant() 可读；集成测试继续覆盖两个持续短读租户、缓存游标、长期读者下另一租户初始化，并让下载后的等待实际超过 10 秒，确认超时后旧数据可读且随后可以成功安装。

10 秒限制的是读者排空等待，不能中断已经进入的系统 I/O、检查点本地复制或原子发布。超时后读取仍可能旧于主库，客户端要求新鲜度时应继续用最低位置等待并处理 BUSY/REPLICA_LAG。

## 冷导出生命周期

由日志导出冷打开的实例会记录其运行身份；多页追赶期间复用，在追平或日志读取失败后尝试关闭。原本已打开的实例不受影响，并发请求更新 last_used 或持有引用时也不会被关闭。仍有活动句柄或 WAL 租约引用时，交给默认 300 秒空闲回收；不能保证所有冷导出立即释放资源。

## 验证结果

验证环境为 OrbStack 原生 Linux AArch64、普通用户 livesite，Rust 1.90；没有把 Linux 结果等同于 macOS F_FULLFSYNC 性能。本轮没有运行真实设备写回故障加掉电试验。

最终 tenant-kv SHA-256 为 `e4f552a15bd1970759073be81c4be4f7811fe7a276786819b68eeed27832f3aa`，restore 为 `19160efc5a0d6e4da535fd3c275fbefbf8a243af1f236bf1c25a557c3e40a160`；对照构建为上一版的 `17256d1438dd82a06ca97540a752ed2c497b99bb1eebe2ebe88b85f43890fb20`。

完整 **146 项测试**通过，最后以 `cargo test --features failpoints -- --test-threads=1` 运行整套；并发回归内部的线程/异步任务仍按原样并发。最终只读轮询生命周期补充验证了 4 个 manager 回归及全目标 Clippy（failpoints、-D warnings）、格式和发布构建，见 [final-validation.log](../bench-results/wal-watermark-drain-20261106/final-validation.log)、[readonly-poll-check.log](../bench-results/wal-watermark-drain-20261106/readonly-poll-check.log)。

一次并行全量运行在恢复测试中出现临时 Store 目录锁 BUSY，保留于 [final-validation-first.log](../bench-results/wal-watermark-drain-20261106/final-validation-first.log)；随后整套串行通过，带 strace 的 11 项并行恢复复测也通过，见 [lock-check.log](../bench-results/wal-watermark-drain-20261106/lock-check.log)。这次间歇失败未定位根因，不能声称已消除所有测试波动。

同一 [wal-open.py](../bench-results/wal-watermark-drain-20261106/wal-open.py)，两侧各用全新 Store、默认 16 MiB 内存表，生成 **5,611,769 字节 WAL、1,200 个确认文档**，顺序执行 7 次关闭/打开。计时包含本机 HTTP 开销，OS 页缓存已热。

| 中位数，ms | 163ce1b | 本次修复 |
|---|---:|---:|
| 健康打开 | 5.973 | 1.441 |
| 关闭 | 0.495 | 0.320 |
| 关闭加打开完整周期 | 6.494 | 1.820 |

健康打开约快 **4.15 倍**，完整周期约快 **3.57 倍**。新版第一次关闭需要建立同步水位，耗时 **3.521 ms**，已经计入 7 次样本；后续内容未变化时不再重复同步。旧版每次换 inode，新版 7 次保持同一个 inode，文档及 _rev 哈希两侧相同。见 [perf/before/result.json](../bench-results/wal-watermark-drain-20261106/perf/before/result.json)、[perf/after/result.json](../bench-results/wal-watermark-drain-20261106/perf/after/result.json)。这不是 macOS 性能对照，也没有测量 1,000 租户的重启耗时。

最终构建的满盘实测：健康租户 **1,200 条**、异常退出租户 **1,208 条**在释放填充文件之前均可读取且零差异；后一租户为只读，状态轮询后仍保持打开。拒绝写入和 fence，连续 1.4 秒状态轮询/读取的进程 write_bytes 增量为 **0**。释放空间重开后仍为 1,208 条；重试之前被拒的操作后为 1,216 条，无重复或缺失。见 [full-disk/after/result.json](../bench-results/wal-watermark-drain-20261106/full-disk/after/result.json)，对照失败见 [full-disk/before/result.json](../bench-results/wal-watermark-drain-20261106/full-disk/before/result.json)。

ENOSPC、WAL write EIO、WAL fsync EIO 三种前台故障各保留 **2,000 条**既有确认数据，共 **6,000 条无丢失**；恢复并按原操作 ID 重试后各为 2,008 条。fsync EIO 下的 8 条未知结果可见，另两种为 0 条。前台故障驱动先完成 flush/compaction，再注入，以免后台维护先失败而使前台直接进入只读；后者返回 RESOURCE_EXHAUSTED 是正常拒绝，并非 COMMIT_UNKNOWN。本轮没有把这组定向故障称为后台并发故障注入。持续 fsync 故障时切换被拒，修复后两端 seq 9、109 条数据一致。见 [faults/result.json](../bench-results/wal-watermark-drain-20261106/faults/result.json)、[switch/result.json](../bench-results/wal-watermark-drain-20261106/switch/result.json)。

最终构建再运行 30 秒基线、120 秒持续写入/flush/compaction/复制/备份/GC 并发，重启后再运行 30 秒。以下是同一构建的不同阶段，不能当作修复前后尾延迟对照。

| 阶段 | 轻租户写 P99，ms | 重租户写 P99，ms | 已校验备份 | 五项后台工作共同推进的采样区间 |
|---|---:|---:|---:|---:|
| 基线 | 11.788 | 16.892 | 0 | 0 |
| 并发 | 18.897 | 22.907 | 3 | 13 |
| 重启后 | 29.102 | 34.708 | 0 | 5 |

共 **10,966 个确认批次、167,080 次文档操作**。主从在重启前后及最终状态、独立 Store 导入的最终备份，正文与 _rev 均零差异。最终重租户 20,000 条、轻租户 200 条，恢复备份 20,000 条；重启到可写且从库追平 **107.766 ms**。业务调用最终错误为 0，包含 **572 次 BUSY 内部重试**的延迟；GC 回收 7,483 条记录并排空到期项，中断备份最终完成且作业文件 CRC 已核对。后台并发时轻租户 P99 仍比基线上升约 60%，本轮没有证明所有后台尾延迟问题都已消除。见 [load/result.json](../bench-results/wal-watermark-drain-20261106/load/result.json)、[interrupted-job-final.json](../bench-results/wal-watermark-drain-20261106/load/interrupted-job-final.json)。

源码与环境指纹、原始确认日志、延迟样本和结果见 [证据目录](../bench-results/wal-watermark-drain-20261106/)。[validate-evidence.py](../bench-results/wal-watermark-drain-20261106/validate-evidence.py) 重算计数、P99 和后台重叠区间，交叉核对数据哈希及源码指纹；结果见 [summary.json](../bench-results/wal-watermark-drain-20261106/summary.json)。本轮覆盖定向回归、满盘读取及短时压力，不替代 macOS 实测、长期运行或真实硬件写回故障资格验证。

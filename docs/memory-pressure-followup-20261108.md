# 内存压力二次复查（2026-11-08）

基线：`78e6bfe75dadfeed0f091539929603cfdffaccab`。逐项核对本轮六条反馈；前三项先在基线实现上添加测试，均复现失败。修复与运行验证使用 OrbStack Ubuntu AArch64，数据放在 `/var/tmp` 原生文件系统。

## 结论与改动

| 反馈 | 核实及处理 |
|---|---|
| 缓存随刷盘压力长期关闭，包括索引块 | 属实。用其他预留维持 3/4 以上的占用，触发后再降到 7/8 以下；基线对同一 key 第二次读取仍读取两个块。现在缓存与刷盘各有独立状态，重复读取为 **0 次块读取**；缓存压力仍高时也保留小索引工作集，重复读取降为 **1 次数据块读取、1 次索引缓存命中**。 |
| 普通刷盘没有占住检查到的空间 | 属实。只够两个刷盘工作区时，基线四次准入均成功。现在准入实际计费预留，只有两个普通刷盘可进入；分配从各自预留支取并归还，前台不能借用。测试同时核对租户配额失败退款、发布后元数据继续计费、未用预留归还及读取不能继承权限。 |
| 临时 View 克隆排除了繁忙租户 | 属实。基线在短读/临时 View 存活时返回零候选字节。改为检查引用当前 memtable、且已持有至少 1 秒的租约；不按 Arc 引用总数排除。真实事务暂停在求值阶段时可以完成压力刷盘，恢复事务后新读看到新值、原快照仍读到旧值。 |
| 协调线程持 slot 锁扫描缓存 | 源码确认，未把推测的延迟尖峰写成实测结论。现在只在锁内复制 core 引用，淘汰和候选检查在锁外进行。每轮所有租户共享 512 次条目检查、8 MiB 淘汰额度，轮转扫描起点；测试验证额度耗尽即停止。最后一个不可分割块可使字节量略超额。 |
| 副本回放仍因显式 flush 的 BUSY 重取整页 | 属实。预先刷盘改为非阻塞的 flush_if_needed；解码内存不足而必须刷盘时保留已下载页面，在 100 ms 窗口内重试 BUSY。测试先确认直接 flush 被许可拒绝，25 ms 后释放许可便能继续；持续占用时有界返回 BUSY，已确认数据不变。不重试 COMMIT_UNKNOWN。 |
| 合并使用另一套压力刷盘门槛 | 源码确认。合并中只在 write_buffer_full 时插入普通刷盘；低于正常阈值的压力回收统一交给协调器，保留最大候选和动态最小回收量策略。原来的合并中途刷盘测试及真实混合负载继续通过。 |

缓存退出点为 `7/8 - min(普通容量 / 64, 4 MiB)`，不再为退出刷盘压力而清到 3/4。索引保留最多每租户 1 MiB、每 shard 128 KiB，并受自身缓存容量限制；达到普通全局容量 15/16 或实际分配失败时，这个保留策略也可让步，不能突破硬上限。`cache_active` 指标解释为什么刷盘仍处于压力状态而缓存已恢复。

普通刷盘的预留在 memory_bytes 中计费，但不是提前分配同等大小的物理缓冲区；临时缓冲可反复支取，不双重计费。超过预留的额外工作集仍可能遇到资源拒绝。快照的 1 秒判断是回收收益的保守判断，不保证刚创建的快照马上释放，也不会强制作废长快照。详见[运维说明](operations.md)、[指标说明](telemetry.md)、[复制说明](replication.md)。

## 验证结果

- 完整 `cargo test --locked --features failpoints --lib --tests -- --test-threads=1`：**232 项通过**，其中 103 项库测试、129 项集成测试。
- 随后稳定小配额测试的准备阶段，单例连续 **10 次通过**，并再次执行 **31 项复制集成测试全部通过**。Clippy 全目标、failpoints、`-D warnings` 和格式检查通过。
- 同一 HTTP 缓存负载：64 MiB 全局、16 MiB memtable、每租户 8 MiB 缓存；**3,704 次确认写入**，轻租户 120 次小写入新增刷盘和合并输出均为 **0**。强杀未关闭实例后，**3,585 条最终文档及修订号全部一致**。采样保留约 52.20–52.35 MB 计费内存，上轮同负载约 45.56–45.71 MB；保留了更多缓存，不以清到旧低水位来解决压力。轻租户 P99 当次为 15.193 ms。
- 原来的零缓存低内存探针：**5,000 次确认写入**；2 次 RESOURCE_EXHAUSTED 有界重试后恢复。在线与重启后各 **2,501 条最终文档全部一致**。不依靠降低 memtable 阈值。
- 混合演练：5,000 条重租户、200 条轻租户文档；20 秒基线、70 秒写入/合并/复制/备份/GC 同时运行、25 秒双节点强杀重启后负载。混合和恢复阶段分别有 **5/3 个后台进展重叠区间**、**4/1 次在线备份校验**。主从重启前后及最终各 **5,200 条当前文档全部一致**，最终备份离线恢复的 **5,000 条文档一致**，GC 回收 4,686 条记录。各前台操作无最终失败；管理/复制等辅助请求有 235 次 BUSY 退避。
- 真实 ext4 满盘、WAL append EIO、WAL fsync EIO：每例故障前 **2,000 条已确认数据零丢失**；未知结果安全重试后各 **2,008 条全部一致**。EIO 为限定测试路径的系统调用注入，不等同于真实设备故障或整机断电。

这些任务共享 VM，延迟数字只是本次观测，不作为隔离的前后性能百分比或 SLA。块读取统计是引擎文件读取，可能由操作系统页缓存服务，不能称为实际设备 I/O 次数。

## 小配额用例的时序依赖

开发中的首次完整测试在 `an_oversized_log_page_rebuilds_instead_of_retrying_forever` 源库造数后的 flush 报租户配额不足，尚未进入复制断言。随后在独立、未修改的 `78e6bfe` 源码上重复原用例，第 4 次在同一位置复现同一错误。它不能归因于本次工作区预留。

该用例验证超大日志页的检查点回退，并非验证源库 3 MiB 配额的突发容量。准备阶段现每 32 条显式刷盘，仍写入全部 300 条记录，保留全部 WAL、相同的从库配额，以及重新初始化次数和每条修订号断言。修改后连续 10 次及完整复制套件通过。没有通过扩大从库配额、缩小日志页面或删除回退断言来规避目标问题；也不声称修复了任意极小配额下的突发写入容量限制。

## 证据

- [三个基线回归失败](../bench-results/memory-pressure-followup-20261108/baseline-regressions.log)、[完整测试](../bench-results/memory-pressure-followup-20261108/final-tests.log)、[复制重复验证](../bench-results/memory-pressure-followup-20261108/replication-repeat.log)
- [开发中小配额失败](../bench-results/memory-pressure-followup-20261108/small-quota-first-failure.log)、[旧提交重复复现](../bench-results/memory-pressure-followup-20261108/small-quota-baseline-repeat.log)
- [缓存 HTTP](../bench-results/memory-pressure-followup-20261108/cache-http.log)、[零缓存 HTTP](../bench-results/memory-pressure-followup-20261108/http-progress.log)、[混合负载](../bench-results/memory-pressure-followup-20261108/mixed.log)、[磁盘故障](../bench-results/memory-pressure-followup-20261108/faults.log)
- [结果与确认日志哈希](../bench-results/memory-pressure-followup-20261108/summary.json)、[源码及二进制对应关系](../bench-results/memory-pressure-followup-20261108/source-provenance.json)、[Clippy](../bench-results/memory-pressure-followup-20261108/clippy-final.log)、[清理结果](../bench-results/memory-pressure-followup-20261108/cleanup.json)

发布二进制 SHA-256：`f79041a964c77871c7db46daae58e50df99606786d42f4ebfe3fdd3a3ec59354`。生产源码与该二进制对应；构建后唯一的代码文件变动是上述测试准备阶段，哈希差异单独记录。测试服务、认证文件、注入库、挂载和 loop 设备已清理，原始数据保留。未进行长期 soak、macOS 运行验证或真实断电测试。

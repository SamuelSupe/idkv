# 读流量、冷追赶与 WAL 恢复修复（2026-11-06）

后续修正：本报告的无条件 WAL 重写和无期限排空已由 [同步水位与排空期限修复](wal-watermark-drain-20261106.md) 替代。以下保留当时构建和测量结果。

基点为 31b575f6a68e58e80816524accd43f3bb09e15d2。持续短读的安装饥饿、逐页冷重开、隔离引擎无法空闲关闭和打开 I/O 错误频繁重试均属实；索引写入背压仍是契约，补齐测试重试和文档。WAL 恢复不再仅重试旧 inode 的 fsync。版本仍为 0.10.1，WAL/SST 字节格式和复制协议 3 不变，部署按构建哈希区分。

## 安装闸门与短读饥饿

下载前的排空阶段、下载验证后的安装阶段分别取得 slot 安装闸门。新 tenant() 返回 BUSY；旧句柄上的新普通操作也不能绕过入口。已有请求可以收尾，不再被源源不断的新短请求顶住。闸门不覆盖整个远端下载过程，成功、错误、取消及展开都由 RAII 释放。

等待 1 秒后撤销该引擎快照并释放缓存游标，续页返回 CURSOR_EXPIRED；索引恢复和普通维护不再为正在排空的引擎增加新引用。复制安装成功时缓存已验证、已发布的检查点序号，状态读取不需要在闸门尚未释放时重新打开引擎。相同配额的目录同步允许继续，配额变更仍在安装期间返回 BUSY。

新增回归同时覆盖两个热租户：两组线程以 20 ms 的短引用持续重叠，一租户还保留分页游标；保持读流量运行时，两租户在 8 秒限期内完成重建，观测到闸门 BUSY，最终全文及 _rev 与主库一致，旧游标失效。它不依赖等待客户端恰好同时空闲。

系统不能强制析构嵌入式调用方长期保留的 TenantHandle、直接 Engine 或 Snapshot Arc；调用方仍需释放这些引用。撤销快照使继续使用失效，但不会替调用方释放内存。已进入的 OS I/O 仍按原合作式取消边界处理，不能保证任意故障下固定时间排空。

## 冷日志追赶

冷打开后，如果日志页还没有追平就保留引擎，后续页复用它。原本已经打开的实例不因日志读取而被强制关闭；多页追赶结束后交给空闲回收。单次尾页/空页的临时打开仍可关闭。单元回归验证多页期间 runtime_id 不变。

使用同一 [cold-export.py](../bench-results/read-drain-recovery-20261106/cold-export.py)、分别全新 Store，16 个并发写入者生成 12,032 个唯一文档；将源停写后读出固定 WAL。WAL 为 **12,681,898 字节、12,033 帧、48 页**。每种模式连续三次，验证每次导出帧的顺序和完整日志哈希，并逐个核对所有已确认文档与 _rev。

| 本机 HTTP 中位数，ms | 31b575f | 本次修复 |
|---|---:|---:|
| 已打开租户 | 53.197 | 51.868 |
| 冷租户追赶 | 535.869 | 73.418 |

最终构建冷追赶约快 **7.30 倍**，包含 HTTP、JSON/base64 处理及首次恢复。OS 页缓存已热；为固定未刷盘 WAL，两侧都使用 64 MiB 内存表而非默认 16 MiB。不能把绝对耗时直接等同于用户的嵌入式 47 页测量，也不是生产 SLO。两侧各自三轮日志哈希不变、12,032 条已确认数据无差异；不同运行的并发提交次序及集合 UUID 不要求相同。

## 写回错误后的恢复

原保证过强：Linux errseq 的新观察者可能不再看到已经被观察的旧错误；文件系统研究也报告了 fsync 写回错误后页面被标为 clean 的情况。读取仍在缓存中的内容，再同步旧 inode，不能作为重新落盘的充分证据。[Linux errseq 文档](https://docs.kernel.org/core-api/errseq.html)、[USENIX ATC 2020 原始研究](https://www.usenix.org/conference/atc20/presentation/rebello)。

恢复路径现在先校验帧和连续序号，再通过用户态 read/write 把有效 WAL 前缀写入新文件，fsync 新文件，原子替换原路径并同步目录，全部成功后才发布恢复序号。保留原 WAL 身份和有效内容，结果未知的完整帧不被盲目丢弃；不使用 reflink/copy_file_range 来保留旧物理 extent。新文件初始权限为 0600，发布前继承原权限。未完成的同名恢复暂存可在下一次恢复中安全重做；恢复最多需要约一个 WAL 文件大小的额外空间，失败不退回旧 inode fsync。

这一路径适用于同一进程和进程重启后的打开，不依赖内存中是否还记得隔离标志。完整帧 CRC 错误、活跃 WAL 或 SST 缺失仍拒绝恢复；无法从已被内核丢弃/损坏的内容中凭空重建数据。

验证分三个层次：

- inode 回归确认有效 WAL 被重写到新 inode、正文/_rev 不变，遗留暂存被处理；恢复新文件写完及发布之后的两个进程 abort 点，重开后已确认尾部仍正确。
- [materialization.py](../bench-results/read-drain-recovery-20261106/materialization.py) 在冷打开时只使 WAL 写入失败、允许 fsync。旧版返回成功，新版必须返回 IO_ERROR；移除故障后两者的 100 条已确认文档均正确。这证明新版确实重新发起写入，不是第二次同步旧文件。
- 沿用真实进程 fsync EIO 后计划切换演练：持续故障时 fence 拒绝，修复后两端均为 seq 9、109 条文档且全文/_rev 一致。

上述 LD_PRELOAD 是系统调用边界测试，不模拟内核清脏页状态；本轮没有执行真实块设备写回错误加主机掉电试验。新的实现及研究依据不能替代这种硬件/内核故障资格验证。

## 自动恢复、错误缓存和索引背压

COMMIT_UNKNOWN 统一标记恢复需求。新请求不复用隔离引擎、不更新其 last_used；入口关闭、快照撤销，旧引用释放后后台立即关闭，不再等待 300 秒空闲。后续请求可重开并按新路径恢复。持续故障时打开的 IO_ERROR 缓存 1 秒，避免最快 10 ms 的状态轮询反复回放整份 WAL；故障修复后自动重试，手动 close 可提前清除。CORRUPTION 和发布不确定的 UNAVAILABLE 保持原隔离策略。

回归验证持续重试不续命、无需 /close 即关闭并换运行身份；另用真实文件权限错误验证缓存命中，修复权限后在缓存期满自动恢复。普通写入仍在内存表/L0 背压时返回 RESOURCE_EXHAUSTED；原在线建索引并发写测试改为有界重试此错误，继续验证最终索引结果与并发更新一致。客户端需要原操作 ID/重试上下文，不能将未知提交当作可盲目重试的背压。

隔离后的下一次请求可能在用户事务执行前因恢复失败返回 IO_ERROR，而不是再次返回 COMMIT_UNKNOWN。磁盘故障驱动据此接受这两种拒绝，仍严格要求没有成功确认、不产生被拒绝操作的数据。

## 最终验证

OrbStack 原生 Linux AArch64、普通用户 livesite、Rust 1.90：完整 **145 项测试**、全目标 Clippy（failpoints、-D warnings）、格式及发布构建通过，见 [validation.log](../bench-results/read-drain-recovery-20261106/validation.log)。初次验证记录了安装期间相同配额同步/位置查询误报 BUSY，分别修正为无变化配额可继续及发布后可信位置缓存；第一轮 Clippy 自动解引用警告修正后最终检查通过。过程保留于 initial-validation.log、targeted.log 和 validation-first.log。

最终构建 SHA-256：tenant-kv 为 `17256d1438dd82a06ca97540a752ed2c497b99bb1eebe2ebe88b85f43890fb20`，tenant-kv-restore 为 `c8aedee44c76d033a539d203082eb8009e8fe37195c4abea07314511415529c3`。对照版 tenant-kv 为 `168a8c97359e8ecab16bb1c20030eef3099c20e496027897cb2d09e6f1ae272c`。最终格式、Clippy、构建及二进制指纹见 [final-check.log](../bench-results/read-drain-recovery-20261106/final-check.log)；下列运行证据均来自最终构建。源码、驱动和环境指纹见 [source-provenance.json](../bench-results/read-drain-recovery-20261106/source-provenance.json)、[environment.json](../bench-results/read-drain-recovery-20261106/load/environment.json)。

在最终构建上再运行 30 秒基线、120 秒持续写入/合并/复制/备份/GC 并发、重启后 30 秒恢复负载。以下为同次运行不同阶段，不能视作修复前后延迟对照；修复前后的相同负载对照仅为上面的冷日志追赶实验。

| 阶段 | 轻租户写 P99，ms | 重租户写 P99，ms | 已验证备份数 | 五项后台工作共同推进的采样区间 |
|---|---:|---:|---:|---:|
| 基线 | 9.971 | 24.818 | 0 | 0 |
| 并发 | 26.719 | 27.094 | 4 | 13 |
| 重启后 | 30.266 | 25.367 | 0 | 5 |

轻租户并发阶段 P99 约为基线的 2.68 倍，重启后短阶段仍有 30.266 ms 尾延迟；本轮没有证明所有后台任务引起的尾延迟都已消除。业务调用最终错误为 0，内部准入重试包括 BUSY 547 次、RESOURCE_EXHAUSTED 76 次，延迟统计包含这些重试。共记录 **10,925 个确认批次、166,450 次文档操作**；主从在重启前后及最终状态的正文、_rev 均无差异。最终重租户 20,000 条、轻租户 200 条；独立 Store 导入最终备份后 20,000 条也完全一致。重启到可写且从库追平为 **64.357 ms**。GC 回收 7,452 条记录并排空到期项；重启时中断的备份最终完成，归档校验了其作业文件 CRC。见 [load/result.json](../bench-results/read-drain-recovery-20261106/load/result.json)、[interrupted-job-final.json](../bench-results/read-drain-recovery-20261106/load/interrupted-job-final.json)。

磁盘不足使用独立 ext4 loop 卷填满；WAL write/fsync EIO 使用已有注入器。三种故障各保留全部 2,000 条既有确认数据，合计 **6,000 条无丢失**；修复故障并重启后到安全重试分别为 36.600、39.784、38.199 ms。fsync EIO 的 8 条未知结果重开后可见，write EIO 和 ENOSPC 为 0 条；按原操作 ID 重试后均为 2,008 条且哈希一致。见 [faults/result.json](../bench-results/read-drain-recovery-20261106/faults/result.json)。

原始确认日志、延迟样本、指标及故障结果保存在 [证据目录](../bench-results/read-drain-recovery-20261106/)。[validate-evidence.py](../bench-results/read-drain-recovery-20261106/validate-evidence.py) 重算计数、P99、后台重叠区间并交叉核对数据哈希及源码指纹，汇总见 [summary.json](../bench-results/read-drain-recovery-20261106/summary.json)。本轮是原生 Linux 的定向回归与短时压力验证，不替代长期运行及真实硬件故障资格验证。

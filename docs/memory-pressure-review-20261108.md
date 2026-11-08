# 内存压力修复复查（2026-11-08）

后续对缓存准入、刷盘预留和繁忙租户选择的修正见[二次复查](memory-pressure-followup-20261108.md)；本文保留当时的验证记录。

基线为 `258125f5ccffa2d0139f650aeea4c88a0ef2c55a`。本轮核对外部报告的五项反馈，先在未修改的实现上加入行为测试，复现后修复。运行环境为 OrbStack Ubuntu AArch64，运行数据放在 `/var/tmp` 原生文件系统。

## 逐项结论

| 反馈 | 核实结果与处理 |
|---|---|
| 缓存维持压力、微小 memtable 反复刷盘 | 属实。真实 SST 读取预热缓存后，12 次小写入新增 2,568 字节刷盘输出。修复后同一测试新增输出为 0，缓存空间被回收。增加 7/8 进入、3/4 退出的压力迟滞；优先淘汰缓存，再选择较大的可回收 memtable。 |
| 回收锁 BUSY 中断维护 | 属实。保持另一个刷盘许可时，原实现的 maintain 返回 BUSY。自动刷盘现在跳过竞争并继续维护；许可释放后可以刷盘。有充足普通容量的显式刷盘在另一租户持有回收许可时也能完成。真正需要保护容量的显式 flush 仍可返回 BUSY，保留有界重试语义。 |
| 按通用 BUSY 跳过输出清理 | 当前路径未发现已生成 SST 后返回 BUSY 的实际泄漏，属于防御性问题。已把许可申请放到产生输出之前；许可竞争直接处理，后续失败统一走原有清理，不再依赖错误码猜测是否生成过文件。 |
| 快照固定内存，刷盘无回收收益 | 属实。原测试中仍持有快照也新增了 748 字节刷盘输出。压力候选现在排除已固定的 memtable；释放快照后可刷盘。同时增加保护容量实际占用指标与告警建议。正常阈值刷盘、显式检查点仍保持原契约。 |
| 复制普通可用预算减少 | 是上次保护容量机制的容量取舍，不是数据正确性问题。本轮明确记录在复制文档与提交说明。报告中的 512 KiB 是文档大小，该测试有 16 个覆盖索引，不能表述为单个 512 KiB 日志页超限。 |

后台每轮最多额外选择一个压力刷盘候选，并优先处理它；最小回收量随打开租户数降低，避免固定 2 MiB 门槛再次卡住大量小租户。新增测试覆盖两个大租户加轻租户、64 个小租户加轻租户，确保跳过固定快照、保留轻租户小写入，并选出能够推进的租户。原来的维护队列公平性测试保留。

这仍是有界回收策略。选择之后才创建的快照、调用方保留的读取结果或大量固定元数据可能继续占用内存；不强制作废这些引用，也不突破全局或租户上限。具体策略、显式 BUSY 语义及指标见[运维说明](operations.md)和[遥测说明](telemetry.md)。

## HTTP 极端场景

使用[同一缓存负载脚本](../bench-results/memory-pressure-review-20261108/cache-pressure.py)：64 MiB 全局、64 MiB 租户、16 MiB memtable、8 MiB 租户缓存；14 个负载租户各写入 256 条 16 KiB 文档，检查点后读取预热，再对轻租户执行 120 次间隔小写入。

- 基线完成 3,584 次种子写入，但缓存预热阶段的读取因全局内存不足，持续重试 30 秒仍失败，未进入轻租户阶段。这次基线运行不作为完整吞吐或延迟对比。
- 修复版完成所有读取和 **3,704 次写入确认**，写入没有重试错误。轻租户 120 次写入的额外 flush_bytes 和 compaction_write_bytes 均为 **0**。
- 预热后采样为 52,343,012 字节，压力标记为 1；随后采样约 45.56–45.71 MB，没有依靠刷出小 SST 维持进展。JSON/Prometheus 新增容量字段均核对通过。
- 未关闭租户就 SIGKILL 服务，重启后逐条核对 **3,585 条最终文档及修订号，零差异**。120 次轻租户写入更新同一个键，因此最终文档数小于确认次数。
- 轻租户写入 P50/P95/P99 为 5.672/10.267/67.271 ms。测试与其他验证共用 VM，只是观测值，不是隔离的性能基准或 SLA。

继续运行原来的零缓存、9 租户低内存[安全重试探针](../bench-results/memory-pressure-fix-20261108/http-progress.py)，不降低 16 MiB memtable 阈值：完成 2,500 次大文档写入和 2,500 次轻租户写入；1 次 RESPONSE_UNAVAILABLE 使用相同操作 ID/history 恢复，无最终失败。在线及重启后各核对 2,501 条最终文档，零差异。该探针核对时会逐租户关闭；真正未关闭实例的崩溃恢复由前述缓存场景覆盖。

## 回归与故障恢复

最终源码执行 `cargo test --locked --features failpoints --lib --tests -- --test-threads=1`，**225 项全部通过**（96 项库测试、129 项集成测试），见[完整日志](../bench-results/memory-pressure-review-20261108/final-tests.log)。定向覆盖缓存回收、快照固定、许可竞争、并行普通刷盘、迟滞与计费、候选选择、原低内存写入进展，以及原有复制边界回退和已确认数据核对。`cargo clippy --locked --features failpoints --all-targets -- -D warnings` 通过。

发布二进制重新执行原故障演练：256 MiB ext4 测试卷真实写满、WAL append EIO、WAL fsync EIO。每例故障前 **2,000 条已确认文档全部保留**；8 条结果未知写入在安全重试前分别可见 0/0/8 条，使用原操作 ID 重试后各 **2,008 条全部一致**。EIO 是限定测试目录的系统调用注入，不代表真实设备写回失败或整机掉电。

## 证据与范围

- [基线三个回归失败](../bench-results/memory-pressure-review-20261108/baseline-regressions.log)、[修复后定向测试](../bench-results/memory-pressure-review-20261108/targeted.log)、[原低内存回归](../bench-results/memory-pressure-review-20261108/original-regression.log)
- [HTTP 缓存基线失败](../bench-results/memory-pressure-review-20261108/cache-baseline.log)、[修复版缓存结果](../bench-results/memory-pressure-review-20261108/cache-fixed-result.json)、[原安全重试探针](../bench-results/memory-pressure-review-20261108/http-progress.log)
- [磁盘故障结果](../bench-results/memory-pressure-review-20261108/faults.log)、[结果与确认日志哈希](../bench-results/memory-pressure-review-20261108/summary.json)
- [源码和二进制对应关系](../bench-results/memory-pressure-review-20261108/source-provenance.json)、[Clippy](../bench-results/memory-pressure-review-20261108/clippy.log)、[清理结果](../bench-results/memory-pressure-review-20261108/cleanup.json)

服务二进制 SHA-256 为 `43e3aedeb94ef118bd284108ed02d985c3ecfd7c08750ee07cc07d561ba0c967`。测试进程、认证文件、注入库、挂载和 loop 设备均已清理，原始数据保留在证据所列路径。本轮未进行长期 soak、macOS 运行验证或真实整机断电测试。

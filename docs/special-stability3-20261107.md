# 备份排空边界审查（2026-11-07）

在上一轮 210 项测试的未提交修复上继续检查后台任务与在线管理交叉的场景，确认并修复一类引用生命周期错误，涉及远端备份准备和本地同步备份校验。没有发现或宣称这些场景造成了确认数据丢失。证据位于 [special-stability3-20261107](../bench-results/special-stability3-20261107/)。

## 已确认的问题

远端备份在检查点复制后继续持有在线 TenantHandle，直到私有副本校验、文件枚举、限速 SHA-256 和 manifest.local 发布结束。关闭和改配额要求在线句柄在一秒内排空，因此即使在线引擎已经不参与处理，慢速哈希仍让这些管理操作持续返回 BUSY。备份越大、限速越低，额外阻塞越久。

真实 HTTP 服务使用 1500×4 KiB 文档、备份限速 1 MiB/s：等私有引擎完成校验并发布 WAL-SYNC 后，在哈希阶段执行关闭和增加内存配额，两者各等待约一秒后返回 BUSY。备份约 6.1 MiB；在线数据和健康租户可读。新增回归同样在修复前因排空超时失败。

本地同步备份也在私有副本 Engine::open/verify 期间持有在线句柄。用限定到实验根下 backups/.../sst 的 Linux pread 延迟，暂停私有副本读取，在线文件未延迟；关闭和增加配额仍分别在约一秒后返回 BUSY。该场景是实际服务上的受控 I/O 延迟，不能称为真实设备故障。

这一引用还会阻止 WAL 故障后的自动关闭和重开。在远端备份哈希阶段，对在线 WAL fsync 注入一次 EIO，第一次写入按契约返回 COMMIT_UNKNOWN；解除注入后，原版在两秒观察窗内 83 次重试仍全部返回 COMMIT_UNKNOWN，备份哈希仍在进行。修复版在哈希继续运行时约 5 ms 内完成原操作 ID 的安全重试。这是同一引用错误的故障恢复影响，不是第二种持久化格式缺陷。

## 修复

两条路径在检查点复制完成后立即释放在线句柄。远端备份保存捕获时的 TenantInfo，后续校验、哈希和清单使用这一份信息；本地备份同时捕获原预算，继续对私有引擎计费。没有重新读取可能变化的在线元信息，也没有取消资源计费或改变 WAL、SST、注册表、复制协议。

新增回归延迟哈希读取，在私有检查点完成校验后关闭在线租户、增加配额，随后让备份完成，再重开 Store 核对确认修订与正文。它保护的是“私有副本处理不能继续占有在线引擎”的行为边界。

## 最终验证

完整 **211 项测试通过**：lib 84、backup 11、DSL 7、engine 28、HTTP 13、identity 5、middleware 14、recovery 14、reliability 4、replication 31。相比上一轮新增一项引用生命周期回归；其余 210 项继续通过。Clippy（all-targets、failpoints、`-D warnings`）、格式检查和 release 服务/离线恢复程序构建通过。源码相对上一轮仅改动 `src/backup/jobs.rs` 和 `src/tenant/manager/lifecycle.rs`；此前修复保留。

最终服务二进制 SHA-256 为 `fdf23674ec94c85d35d829c8e39ad258d12c4fbf9f892cde1cfc89f9f4173106`。原脚本在独立数据根上前后运行，最终对照如下。耗时为单次操作记录，不能作为 P99 或整体吞吐性能结论。

| 场景 | 原版 | 修复版 |
|---|---|---|
| 远端哈希期间 close | 1003.4 ms 后 BUSY | 3.4 ms 成功 |
| 远端哈希期间增加配额 | 1003.8 ms 后 BUSY | 4.8 ms 成功 |
| 本地副本读取暂停时 close | 1001.4 ms 后 BUSY | 1.9 ms 成功 |
| 本地副本读取暂停时增加配额 | 1005.5 ms 后 BUSY | 2.6 ms 成功 |
| 哈希期间 WAL fsync EIO，解除后安全重试 | 两秒内 83 次 COMMIT_UNKNOWN，未恢复 | 哈希仍活跃，约 5.0 ms 成功 |

故障对照在两秒观察后等待备份结束才再试成功；原始日志的约 12.3 秒不是测得的最早可恢复时间。修复版使用同一操作 ID、相同正文及 history 安全重试，随后 1501 条确认文档在重启前后一致，备份 verify 也通过。该备份固定于故障前，其恢复点不含故障后确认的第 1501 条；该场景验证在线恢复、备份校验和重启，实际恢复备份在另两个场景执行。

普通远端和本地备份均在在线增加配额后完成校验与实际恢复，再强制终止并重启核对；恢复后保留 256 MiB 新配额，未被备份中的 128 MiB 旧值覆盖。最终三场景共 **12,642 次文档状态核对，差异为零**；这是重复核对次数，不是不同文档数量。另一个健康租户在延迟、故障及重启前后均可读，确认数据无差异。

主要证据：[来源与源码哈希](../bench-results/special-stability3-20261107/source-provenance.json)、[全量测试](../bench-results/special-stability3-20261107/full-tests.log)、[远端对照](../bench-results/special-stability3-20261107/remote-proof-before/result.json)及[复测](../bench-results/special-stability3-20261107/remote-proof-after/result.json)、[本地对照](../bench-results/special-stability3-20261107/local-before/result.json)及[复测](../bench-results/special-stability3-20261107/local-after/result.json)、[WAL 故障对照](../bench-results/special-stability3-20261107/wal-before/result.json)及[复测](../bench-results/special-stability3-20261107/wal-after/result.json)。

## 运行范围和限制

后端运行使用 OrbStack Ubuntu Linux aarch64，编译目录和测试 TMPDIR 为原生 Btrfs。远端备份目标使用 OpenDAL fs；不是实际 S3 验证。本地延迟 shim 只暂停独立实验目录下私有 SST 的 pread，不修改内容、不延迟在线 SST。所有实例和数据目录均为本轮独立创建。

检查点复制阶段、真实系统 I/O/锁等待以及其他活动引用仍可能导致排空超时。备份的共享预算保持计费，缩配额仍可能因在用内存被拒；本地同步备份继续占用该租户维护名额。没有新增真实掉电、设备写回错误、长期 soak 或大规模性能测试。本轮没有提交或推送 GitHub。

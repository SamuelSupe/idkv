# 备份控制元信息边界审查（2026-11-07）

在上一轮 211 项测试对应的未提交修复上，继续审查后台任务控制文件、跨租户状态查询与 I/O 故障的交叉场景。确认并修复任务身份丢失、全局锁阻塞和 forget 重试三类问题，未复现确认数据损坏。原始证据位于 [special-stability4-20261107](../bench-results/special-stability4-20261107/)。

## 实测发现

**排队发布错误可导致服务无法重启。** 原 submit 在 atomic_json 成功返回后才登记内存任务。重命名已经完成、父目录 fsync 返回 EIO 时，磁盘有 Queued 任务，内存列表却为空；新提交再创建第二个任务。强制终止并重启后，BackupService 报 `CORRUPTION: multiple active backup jobs for tenant`，配置了备份的整个服务退出。停止加载备份服务后，1500 条原确认文档及健康租户仍可读，说明这是启动可用性故障。

**结束状态发布错误也可产生重复活动任务。** 远端备份已完成，写本地 Completed 临时文件时 fsync 出错，磁盘仍为 Uploading。原工作者把内存任务改成 Interrupted 后释放租户，允许新建第二个任务；重启同样因两个未结束的持久任务退出。不能仅修提交入口的重命名窗口。

**慢速元信息写盘拖住其他租户。** 原全局备份状态 mutex 跨越文件写入和 fsync。暂停一个任务的 Checkpointing 元信息 fsync，再从两个租户轮询状态，所有查询都卡在同一把锁。真实 HTTP 服务以执行池 8、每租户 4 的合法配置验证：8 个轮询占满执行池，第三个健康租户点读返回 RESOURCE_EXHAUSTED/HTTP worker concurrency；/health 仍返回正常。后者不能代表业务路径可用。

**forget 无法从目录同步错误中重试。** 终态任务文件已 unlink，父目录 fsync 出错后，内存行还在；再次 forget 因 ENOENT 持续失败，直到重启。限定目录的实际 EIO 注入复现了该状态。

## 修复及约束

提交先在内存预留任务行、租户与容量，再发布同一任务文件；发布失败保留 Interrupted 和租户身份，不允许另建第二个持久任务。原任务可安全重试支持的操作，或检查后 forget。结束状态发布失败也保留身份，等待同样的处置；restore/import 的禁止 retry 规则不变。

使用已有按租户锁串行提交、重试、工作者和 forget，工作者持有到结束状态发布和清理完成。全局状态锁只保护内存读写，不跨文件操作；结束状态在内存发布与释放租户锁时一起生效，避免看到 Completed 后又被清理中的旧工作者挡住。forget 在控制文件已不存在时继续同步目录和删除内存状态。

没有改变持久格式、复制协议或任务结构。可见的 Queued 行在提交仍进行时可能尚未落盘；POST 202 仍只在排队文件成功发布后返回。未知发布不能根据内存状态推断磁盘结果，重启依实际持久状态恢复。已经由旧版产生的多个未结束任务仍保守拒绝启动，避免自动挑选恢复操作造成覆盖；可先停止加载备份服务读取在线数据，再检查和处理控制任务。

## 最终验证

最终 **213 项测试通过**：lib 85、backup 11、DSL 7、engine 28、HTTP 13、identity 5、middleware 14、recovery 15、reliability 4、replication 31。新增两项回归函数，覆盖慢更新期间状态可读、文件已删除后的 forget，以及排队/结束两个发布窗口的同任务重试和重启。Clippy（all-targets、failpoints、`-D warnings`）、格式检查和 release 服务/离线恢复程序构建均通过。

最终服务二进制 SHA-256 为 `e9fb9657f3ee523f096ab6220a160cc36571a610b90d13d6f2015f645cae3a2c`。独立根目录上的真实服务前后对照如下：

| 场景 | 原版 | 修复版 |
|---|---|---|
| 排队文件重命名后目录同步 EIO | 任务列表为空，再次新建获准；重启退出 | Interrupted 原任务可见，新建返回 BUSY；原 ID 重试完成，正常重启 |
| Completed 临时文件同步 EIO | Interrupted 后另建获准；重启退出 | 保留原任务身份，原 ID 重试完成，正常重启 |
| 慢阶段同步 + 两租户状态轮询 | 8 个查询未完成，健康租户读 429 | 8 个查询均完成，健康租户点读成功（单次 0.21 ms） |
| forget 文件删除后目录同步 EIO | 重试持续 ENOENT，内存行留下 | 重试成功，内存行删除；远端备份保留 |

毫秒值只是本轮单次样本，不是 P99 或生产性能结论。四个修复版场景共 **6520 次文档状态核对，差异为零**；这是重复检查次数，不是不同文档数量。发布故障场景各确认 1500 条业务文档及健康租户 20 条；隔离场景三租户各 20 条，forget 场景 100 条。按原 ACK 账本核对文档键、修订及载荷，所有场景均实际重启。原版两次启动失败时，停止加载备份服务后也核对了确认数据，未将控制任务损坏误报为业务数据损坏。

最终源码相对上一轮仅涉及两个生产模块和既有故障探针/恢复测试。首次完整检查后补齐了结束状态发布失败的所有权处理，再对最终源码完整检查；中间日志和哈希单独保留，不作为最终证据。实验进程、凭证、故障控制文件和共享库均已清理，归档不含业务数据库或密钥。

主要证据：[源码及来源](../bench-results/special-stability4-20261107/source-provenance.json)、[全量测试](../bench-results/special-stability4-20261107/full-tests.log)、[排队对照](../bench-results/special-stability4-20261107/admission-before/result.json)及[复测](../bench-results/special-stability4-20261107/admission-after/result.json)、[结束状态对照](../bench-results/special-stability4-20261107/terminal-before/result.json)及[复测](../bench-results/special-stability4-20261107/terminal-after/result.json)、[隔离对照](../bench-results/special-stability4-20261107/isolation-before/result.json)及[复测](../bench-results/special-stability4-20261107/isolation-after/result.json)、[清理对照](../bench-results/special-stability4-20261107/forget-before/result.json)及[复测](../bench-results/special-stability4-20261107/forget-after/result.json)。

## 范围

后端运行使用 OrbStack Ubuntu Linux aarch64，测试 TMPDIR 和编译目录为原生 Btrfs。故障限定在本轮独立根的 control/backup-jobs：目录 fsync EIO 在重命名/unlink 之后触发，终态临时文件 fsync EIO 在重命名前触发，慢速阶段写盘通过受控暂停验证。数据 WAL/SST 没有被该 shim 修改，备份后端为 OpenDAL fs。这些结果不代表真实设备写回错误、掉电、实际 S3 或长期 soak。本轮保留此前工作树改动，没有提交或推送 GitHub。

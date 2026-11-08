# 0214ec3 租约与检查点复查

本轮核对该提交的四项反馈。前两项均已复现；第三项没有独立复现出错误数据，但统一了故障标记与租约失效的同步边界；第四项属于恢复契约，补齐测试和文档。

| 反馈 | 处理 |
| --- | --- |
| 弱引用升级仍可能延长 Engine 生命周期 | `LogRetention` 改为持有独立 `Arc<RetentionState>`，不引用 Engine。有效性检查、续期及释放只访问内存状态，不会成为 Engine 的最后持有者或执行析构 I/O。健康租约仍由 manager 的 `replication_pinned()` 检查保护。 |
| 完成并同步的检查点被末尾检查撤销 | 移除发布和目录同步之后的源端故障检查。复制中及发布前保留协作取消；取消代码使用现有 `CANCELLED`，实际 flush 的不确定提交仍保留 `COMMIT_UNKNOWN`。 |
| 故障标记与租约映射分开读取 | 设置恢复标记、清空全部旧租约、创建/续期租约共用映射锁；有效性和 pinned 检查只读取这份映射。故障后不能插入或续活租约，Engine 析构也撤销遗留租约。 |
| 恢复后旧追赶租约丢失 | 保留该设计。旧租约不迁移到新 Engine；日志仍在正常保留窗口时可以继续增量，所需 WAL 被回收才返回 `LOG_GONE` 并重新初始化，并非每次恢复都必然全量重传。 |

完整检查点与有效复制会话是两个条件：本地备份不会因源端后来的故障而被追溯取消；复制导出仍必须有有效 WAL 租约。源引擎恢复后，旧下载会话继续按过期流程退出。

## 回归证据

`replication_lease_operations_do_not_acquire_engine_ownership` 在租约映射锁被占用时分别调用 valid、renew 和 Drop，观察管理器关闭所依赖的 Engine 所有权不变量。原实现三次均得到强引用数 2，预期为 1，测试失败；修复后均为 1。测试同时保留一个租约跨越 Engine 析构，要求它失效、不能续活，并立即重开数据库核对已确认记录。这是受控锁竞争测试，没有把导出锁的实际阻塞延迟作为已测量结果。

扩展 `replication_exports_do_not_keep_uncertain_engines_alive` 的追赶分支：故障后不能新建有效租约；重开后旧日志请求可以读到仍保留的确认数据，但不能恢复旧 pin；再写入并刷盘两个 700 KiB 事务，超过 1 MiB 正常保留窗口，旧请求必须返回 `LOG_GONE`，新检查点和 fence 位置仍准确。复制中取消测试继续验证最多再复制一个 256 KiB 块，并核对重开后的完整 2 MiB 记录。

检查点完成边界使用真实 HTTP 服务及独立测试根目录，在 OrbStack Linux / Btrfs 上运行。测试 shim 先执行检查点父目录的最终 fsync，再暂停返回；随后只对该根目录内 WAL 注入一次 fsync EIO。原 `0214ec3` 二进制确实返回 `COMMIT_UNKNOWN` 并删除已经完整发布的本地备份。脚本按成功 ACK 账本核对文档、修订、载荷、索引、multi_get、lookup 及存储校验，故障写入不记入成功账本。

修复版在相同故障时序下返回成功。在线库恢复写入后及强杀重启后，分别核对故障租户 100 条、对照租户 10 条文档，差异均为零；再恢复保留下来的备份，按检查点之前的 ACK 水位核对 100 条文档，强杀重启后再核对一次，均一致。备份之后的写入不属于该恢复点。

复用了上一轮的四个真实服务驱动：活动导出、暂停私有 SST 哈希、暂停正常副本的检查点响应，以及健康下载/追赶租约跨越 idle 超时。四项全部通过；前三项移除 EIO 后的业务写入错误数均为零，哈希及传输仍暂停时源端已恢复写入。正常副本场景完成双节点数据核对、两端强杀重启及计划切换。健康租约仍阻止空闲回收，release 后可以关闭，重开后数据一致。没有将本轮单次运行耗时作为性能基准或生产恢复时限。

最终 **216 项测试全部通过**（lib 88，其余集成测试 128）；Clippy（all-targets、failpoints、`-D warnings`）、格式检查及 release 构建通过。验证前后 112 个 Rust/Cargo 文件哈希一致。release 服务 SHA-256 为 `3ae4a3d1b6fec42bc37b4045efe79c65b358d2584f77a04fe0e560f9c1f4b451`；原版对照为上一轮归档的 `0214ec3` release，SHA-256 为 `67dc2958814917b4d584abb88052c205a372a0880198976d1e83c353ee5840fa`。

复现命令（输出目录必须不存在）：

```sh
python3 bench-results/lease-review-20261108/checkpoint-completion-probe.py \
  /path/to/tenant-kv /var/tmp/idkv-checkpoint-completion
# 重现 0214ec3 的旧错误时加 --expect-before-bug
```

源码未改变磁盘格式或复制协议。故障注入覆盖系统调用 EIO 和进程强杀重启，未模拟物理设备掉电或长时间生产负载。取消仍不能打断已进入内核的阻塞 I/O。

证据目录：[lease-review-20261108](../bench-results/lease-review-20261108/)。原版失败断言见 [lease-before.log](../bench-results/lease-review-20261108/lease-before.log)，原版检查点误取消见 [checkpoint-before.log](../bench-results/lease-review-20261108/checkpoint-before.log)。最终 [检查结果](../bench-results/lease-review-20261108/checks.json)、[测试日志](../bench-results/lease-review-20261108/tests.log)、[源码哈希](../bench-results/lease-review-20261108/tested-source.json)、[真实服务结果](../bench-results/lease-review-20261108/runtime.json)、[ACK 账本](../bench-results/lease-review-20261108/acknowledgements.jsonl) 随本轮保存。每个运行场景的原始服务日志及数据目录保留在 `/var/tmp/idkv-lease-review-runtime-20261108`；旧版完成边界对照位于 `/var/tmp/idkv-lease-review-completion-before-20261108`。实验进程、认证文件、故障控制文件和共享库已清理。

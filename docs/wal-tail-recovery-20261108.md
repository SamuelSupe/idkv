# WAL 尾部完整长度损坏恢复（2026-11-08）

基线为 `5600a4ea0aca5f0d13e9ec7f5966cb2782f294f1`。报告中的打开失败已复现：在包含已确认文档的 WAL 后追加 8 KiB 全零字节，重开返回 `CORRUPTION: WAL batch header checksum`。底层多帧回归同样在完整长度的坏帧处失败。修复前日志见 [底层复现](../bench-results/wal-tail-recovery-20261108/baseline-unit.log)和 [Store 复现](../bench-results/wal-tail-recovery-20261108/baseline-integration.log)。

## 恢复边界

- 只有 Manifest 中最后一个 WAL，在有效 WAL-SYNC 长度之外，允许将残缺帧、坏魔数、帧头或载荷 CRC 失败作为不确定尾部。没有该 WAL 的有效水位时，从文件头后的帧区间应用此规则。
- 在第一处坏帧之前停止，后面即便还有完整有效帧，也整个丢弃；不通过查找魔数跳过事务。保留前缀仍使用普通写入建立新 inode，完成 fsync、原子替换及目录同步后才对外发布恢复位置。
- 水位范围内的校验错误仍为 `CORRUPTION`。本次也补上已同步区间被截短的保护：即使恰好截到完整帧边界或只剩文件头，也不得当作合法尾部恢复。
- 非尾 WAL、文件身份错误、合法校验帧内的非法结构、序号缺口以及真实读取 I/O 错误不适用截尾规则。网络复制的收包及日志导出校验不放宽。
- 新 inode 重建遇到 ENOSPC 时沿用只读恢复，原文件不改写，不能发布为可切换的持久位置。本轮未重新执行满盘资格验证。

WAL-SYNC **只在健康关闭时更新，并非每次 fsync 或客户端确认水位**。因此“水位之外”不等于“从未确认”。当水位缺失、无效或陈旧时，已确认帧后续发生的介质损坏，与未确认追加的撕裂写可能相同；本策略选择截去坏帧起的后缀，不能承诺任意介质损坏下零确认数据丢失。需要更强判别能力时必须增加独立持久提交证据，当前修复没有引入额外的逐提交同步。故障后的数据完整性仍须结合备份、副本和业务确认记录判断。

## 实际验证

运行环境为 OrbStack Ubuntu、Linux `7.2.12-orbstack-00394-gbecdd6130068`、AArch64；测试数据使用 Linux `/var/tmp`，不是 macOS 共享文件系统。证据目录为 [wal-tail-recovery-20261108](../bench-results/wal-tail-recovery-20261108/)。

- 底层 36 种组合：6 种损坏 × 尾/非尾 WAL × 无水位/前缀水位/覆盖坏帧的水位。含损坏后还有有效事务帧，检查恢复内容、物理文件长度及错误路径不修改原文件。[运行日志](../bench-results/wal-tail-recovery-20261108/wal-tests.log)
- Store 回归分别保留和删除 WAL-SYNC，追加 8 KiB 全零尾部。核对已确认正文与 `_rev`、已有 SST 数据、恢复后继续写入及再次重开。
- 已同步数据分别遭遇帧头 CRC、载荷 CRC、头部截短、载荷截短和整个帧缺失，均要求拒绝打开且不改原 WAL；其他租户仍可用。
- 扩展现有子进程 abort 测试，新增完整长度的全零帧头、头部 CRC 错误和载荷 CRC 错误，覆盖文档/索引原子性及身份历史/操作收据原子性；原有 append、fsync、轮转和 Manifest 故障点继续运行。

完整 `cargo test --locked --features failpoints --lib --tests -- --test-threads=1` 通过 **234 项**（104 项库测试、130 项集成测试，含 16 项恢复和 31 项复制测试）。`cargo clippy --locked --features failpoints --all-targets -- -D warnings`、格式检查和 `git diff --check` 通过。[完整测试日志](../bench-results/wal-tail-recovery-20261108/tests.log)、[静态检查](../bench-results/wal-tail-recovery-20261108/clippy.log)、[结果摘要](../bench-results/wal-tail-recovery-20261108/summary.json)及 [源码指纹与环境](../bench-results/wal-tail-recovery-20261108/source-provenance.json)。

上述损坏由文件内容修改和写入后进程 abort 构造，用于确定性模拟掉电后的磁盘镜像；没有做真实设备断电、任意介质损坏或页回写顺序实验。

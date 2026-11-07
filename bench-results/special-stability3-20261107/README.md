# 备份排空边界证据

对应 [审查与修复报告](../../docs/special-stability3-20261107.md)。基线为上一轮已验证的 210 项测试版本；当前工作树原有改动均保留。

- `backup-regression-before.log` / `backup-regression-after.log`：新增行为回归先失败、修复后通过。私有检查点校验结束后，受控延迟哈希，此时关闭及增加配额必须不再等待在线句柄。
- `backup-drain.py`：真实 HTTP 服务、1500×4 KiB 文档、1 MiB/s 备份限速，在复制完成后的哈希阶段执行 close/增加配额，然后完成 verify、恢复及重启，按 ACK 模型核对数据。
- `local-backup-drain.py` / `checkpoint-read-delay.c`：延迟本次实验 backups/.../sst 下的 pread；在线 SST 不延迟。在私有副本读取暂停期间执行管理操作及前台读取，解除后完成备份、恢复和重启核对。
- `remote-before` / `remote-after`、`local-before` / `local-after`：相同脚本、独立数据根的原始结果、确认账本及服务日志。脚本的 expect-busy 只控制对照版本的预期结果。
- `remote-proof-before` / `remote-proof-after`：最终远端探针等待私有 WAL-SYNC 水位，精确确认校验结束再执行管理操作；之前的远端运行使用 CURRENT 加短暂等待，作为补充记录保留。
- `backup-wal-recovery.py`、`wal-before` / `wal-after`：哈希期间使用现有 `scripts/wal_io_fault.c` 注入 WAL fsync EIO，解除后以原操作 ID 重试；原版两秒内无法恢复，修复版在哈希期间恢复。随后核对数据、备份 verify 及重启。
- `full-tests.log`、`test-summary.json`、`clippy.log`、`format.log`、`release-build.log`：最终源码的完整检查。
- `validated-source.json`、`source-provenance.json`、`binaries.log`：源码、脚本和固定二进制来源。

运行需 OrbStack/Linux、Python 和已有 GCC；没有安装新测试框架。C interposer 以 `-U_FORTIFY_SOURCE` 定义 libc pread 入口，仅供此实验，生产构建选项不变。初始本地探针曾在暂停备份时调用包含 `/check` 的完整 verify helper，触发既有每租户维护上限；最终版本在暂停期间只比较普通读取，解除延迟后再完整核对。该拒绝没有作为新缺陷报告。

归档不含密钥、认证文件、共享库、业务数据库或二进制。原始 JSON、ACK 和日志保留，较长日志压缩。受控延迟、进程强制终止和 OpenDAL fs 结果不代表真实硬件掉电、真实 S3 或长期生产运行。当前修复没有提交或推送。

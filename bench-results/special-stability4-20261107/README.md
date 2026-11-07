# 备份控制文件故障证据

对应 [审查与修复报告](../../docs/special-stability4-20261107.md)。基线是上一轮 211 项测试版本，保留此前未提交修复。

- `backup-metadata-io.c`：仅对本次实验 control/backup-jobs 注入一次目录 fsync EIO、终态临时文件 fsync EIO，或暂停 Checkpointing 文件同步；不修改 WAL/SST。
- `metadata-recovery.py`：排队发布后的目录同步失败；`--terminal` 覆盖结束状态写入失败。原版再次新建任务后无法带备份服务重启；修复版保留原任务、拒绝另建、以原 ID 重试完成并重启。
- `metadata-isolation.py`：执行池 8、每租户 4，暂停一条后台元信息 fsync，两个租户各四个状态查询；比较第三租户点读是否受到执行池耗尽影响，解除后核对所有确认数据并重启。
- `forget-recovery.py`：终态文件 unlink 后目录同步失败，比较原进程中的 forget 重试能否完成；远端备份必须仍存在。
- `metadata-unit.log`：受控暂停阶段更新时状态读取可完成，模拟文件已移除后的 forget 可继续。
- `admission-regression.log`：早期排队窗口回归；最终全量测试还覆盖结束状态发布失败。
- `full-tests.log`、`validated-source.json`：最终完整检查与源码；`full-tests-initial.log` 和 `validated-source-initial.json` 是补齐结束状态处理之前的中间检查，不能作为最终源码证据。
- `clippy.log`、`format.log`、`release-build.log`、`binaries.log`：最终构建检查及固定二进制。

使用 OrbStack Ubuntu 原生 Btrfs target/TMPDIR、实际 HTTP 服务和 OpenDAL fs。CLI 的 expect-broken/expect-blocked 只控制对照版本的预期结果。C 代码由已有 GCC 构建，没有安装新的测试框架。原始 JSON、ACK 账本和日志保留，较长日志压缩；认证文件、共享库和业务数据库不归档。

故障为限定路径的 syscall 注入，重启通过进程强制终止实现，不代表真实设备写回故障、掉电、真实 S3 或长期 soak。源码、探针、二进制来源及清理结果见 source-provenance.json、cleanup.json。本轮没有提交或推送。

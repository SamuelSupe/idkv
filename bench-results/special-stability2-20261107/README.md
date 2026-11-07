# 特殊恢复边界证据

对应 [审查与修复报告](../../docs/special-stability2-20261107.md)。基线为上一轮已验证的 209 项测试版本，最终源码在 `validated-source.json` 固定；此前未提交修复均保留。

- `legacy-regression-before.log`：延迟升级回归在修复前失败。
- `capabilities-regression-before.log`：HTTP 实际格式契约在修复前报告 4/5 不一致。
- `registry-after.log`：初次延迟升级修复后，三项注册表回归通过；最终完整检查以 `full-tests.log` 为准。
- `full-tests.log`、`test-summary.json`：最终 210 项通过。
- `clippy.log`、`format.log`、`release-build.log`、`binaries.log`：最终构建检查和固定二进制。
- `upgrade-before` / `upgrade-after`：同一 `full-disk-upgrade.py`，实际 ext4 满盘，独立根及旧版对照；含能力接口、OpenAPI、确认数据核对。
- `pressure-before` / `pressure-after`：同一 `recovery-pressure.py`，未封口 WAL 在字节/inode 耗尽后的只读服务、资源释放、写入恢复和再次重启。
- `compat-after`：未修改的上一轮 v5 兼容探针，旧读者拒绝及新程序重开。

脚本复用仓库 `scripts/qualification.py` 的真实 HTTP 服务和 ACK 模型，运行需 Linux、独立 loop 文件系统工具和无需交互的 sudo。全量测试用 OrbStack Ubuntu 原生 Btrfs TMPDIR；这些不是 macOS 或默认 tmpfs 的结果。原始 JSON、ACK 日志及服务日志保留，较长日志压缩；认证文件、业务数据库、磁盘镜像及二进制不归档。

复现与复测参数、哈希和清理状态见 `source-provenance.json`、`cleanup.json`。尚未覆盖真实设备错误/掉电、长期 soak 或真实 S3，当前改动没有提交或推送。

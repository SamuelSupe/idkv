# ab7a0ec 复查验证证据

逐项结论与完整验证边界见 [报告](../../docs/ab7a0ec-review-20261107.md)。基线 `ab7a0ecfa6077b3d3b3a030749bd2b5be9187dc8`。

- `full-tests.log`：最终完整 202 项测试，含五项新增行为回归。
- `clippy.log`、`fmt-check.log`、`release-build.log`、`openapi-verification.log`：最终检查。成功的格式检查日志为空。
- `initial-lib-tests.log`：初轮新增测试响应形状断言错误的诊断，已修正；不混入最终结果。
- `control-results.json`、`control-test-input.json`、`control-source-provenance.json`、`control-*.log`：原版独立源码上五项预期行为失败及输入证明，不是编译失败。
- `qualification-before/`、`qualification-fixed/`：同脚本同负载 10/45/60 秒的原版/修复版结果、ACK 模型、采样、事件、延迟原始数组与节点日志。
- `performance-comparison.json`：全部阶段、全部操作的分位数、吞吐及重试，不只展示有利样本。
- `faults/`：独立 ext4 卷 ENOSPC、WAL write/fsync EIO 的确认模型和恢复记录。
- `protocol/`、`probe-http-protocol.py`：b206985、ab7a0ec、修复版的实际 h2c/HTTP1 探测与确认数据核对。
- `hyper-transport-features.json`：本轮原/现锁定特性图；更早 b206985 的特性证据在上一轮同名文件。
- `runtime-summary.json`：72,400 次资格文档状态核对、6000 条故障前确认数据、900 次协议文档状态核对的汇总。
- `shipping-source.json`、`binaries.json`、`source-provenance.json`：最终源码、固定 release 二进制、验证命令与脚本未修改证明。
- `archived-files.json`、`archive-results.py`：归档运行文件的哈希与可重复提取规则；不含数据库目录、loop 镜像、认证文件或二进制。
- `verify-provenance.py`：在本轮原生 target/运行文件仍存在时重做来源校验；不是通用 CI 脚本。
- `cleanup-verification.json`：本轮服务进程、挂载卷和 loop 映射已清理。

运行目录分别是 `/var/tmp/idkv-ab7a0ec-qualification-before-20261107`、`/var/tmp/idkv-ab7a0ec-qualification-fixed-20261107`、`/var/tmp/idkv-ab7a0ec-faults-20261107`、`/var/tmp/idkv-ab7a0ec-protocol-20261107`，均位于 OrbStack Ubuntu 内。宿主机同名 `/var/tmp` 不是这些目录。资格和故障脚本命令见 `source-provenance.json`，每次重跑应使用新的 root，避免覆盖既有证据。

这些记录产生于提交前，GitHub 交付以包含本目录的 Git 提交为准。EIO 为 syscall 注入，备份为 OpenDAL fs 后端，短测不等于长期性能资格。

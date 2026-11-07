# cd83246 两项反馈的验证证据

结论与限制见 [修复报告](../../docs/cd83246-review-20261107.md)。

- `full-tests.log`：最终完整 204 项测试，包括两项新增行为回归。
- `additional-regression.log`、`additional-regression.json`：追加的正常可写状态中断回归通过，移除该测试函数可逐字重建完整测试时的源码；累计覆盖 205 个不同测试。
- `clippy.log`、`fmt-check.log`、`release-build.log`：最终检查；成功格式检查日志为空。
- `tmpfs-initial/`：早期默认 Linux tmpfs 上的记录，不作为最终磁盘恢复验证。其中 `initial-focused-tests.log` 是 seed 超过单批操作上限的诊断，已分批且未放宽配置；`focused-tests.log` 之后还补了 20 ms 截止时间断言。顶层 full-tests 和原版对照都已在 Btrfs 重跑。
- `control-*.log`、`control-results.json`：原版 cd83246 上三项预期行为失败，未把编译失败计为复现。
- `control-test-input.json`：最初注入原版的两个测试函数；第三项原文在 additional-regression.json。
- `control-source-provenance.json`：原/现对照源码哈希，唯一变化在 admission 测试模块，生产前缀及 Cargo 文件不变。
- `full-test-source.json`、`validated-source.json`、`source-provenance.json`：完整测试和追加测试后的源文件哈希、工具链、命令和二进制来源。
- `native-filesystem.log`：最终测试临时目录的文件系统为 Btrfs。
- `initial-btrfs-full-tests.log`：旧复制退避测试过早注入状态的时序失败，已修正等待条件；baseline-existing-backoff-*.log 保留原版三次单独通过的记录。

修复目标为 `/var/tmp/idkv-cd83246-review-target-20261107`，测试 TMPDIR 为 `/var/tmp/idkv-cd83246-test-tmp-20261107`。对照源码为 `/var/tmp/idkv-cd83246-control-source-20261107`，对照目标为 `/var/tmp/idkv-cd83246-control-target-20261107`，测试 TMPDIR 为 `/var/tmp/idkv-cd83246-control-test-tmp-20261107`。路径均位于 OrbStack Ubuntu 内，宿主同名目录不是这些路径。最终测试数据库落在原生 Btrfs。

新回归对 close/配额后的 4096 个文档状态及 4096 个索引结果核对已确认主键、修订和载荷；删除另按管理契约验证。冷准入回归以容量 1 覆盖满池边界，包含全局两类锁竞争和读取截止时间。没有新运行长期负载、硬件故障或尾延迟基准。

# 特殊场景稳定性审查证据

结论及测试边界见 [报告](../../docs/special-stability-audit-20261107.md)。审查版本是 cd83246 加当前未提交修复，不等同于单独的 GitHub main。

- `source-provenance.json`：审查源码、独立副本哈希、生产前缀未修改证明及 release 来源。
- `probes-manager.rs`、`probes-resources.rs`、`probes-background.rs`：注入独立副本测试模块的完整探针输入。
- `run-probes.py`：在已准备的独立源码中运行四个探针。它们断言安全不变量，当前版本出现失败是发现问题，不代表已经修复。
- `probe-results.json`、`audit_*.log`：三类问题、四个场景的观测记录。配额最终实验使用 128 MiB→64 MiB，日志在 counter-repeat 文件中。
- `counter-repeat-results.json`、`counter-repeat-*.log`：三次实际配额规模下的并发计费失败。
- `initial-probe-results.json`、`counter-mini-initial.log`、`probes-resources-mini-initial.rs.txt`：初步使用小计数值的探针，保留原文，不将其当成最终配额规模实验。
- `qualification/`、`runtime-summary.json`、`qualification-files.json`：实际 HTTP、主从、后台任务、进程终止、离线恢复及 36,200 次确认状态核对。
- `binaries.log`、`setup.log`、`qualification-driver.log`：固定二进制、Btrfs 环境及运行阶段日志。

独立源码位于 OrbStack 内 `/var/tmp/idkv-special-audit-source-20261107`，目标为 `/var/tmp/idkv-special-audit-target-20261107`，测试 TMPDIR 为原生 Btrfs `/var/tmp/idkv-special-audit-tmp-20261107`。先复制当前 Cargo/src/tests，记录原始哈希，再把探针追加到 manager/resources 的 cfg(test) 模块；没有修改原函数来制造故障。Cargo 关闭增量编译，使用独立 target，并清理本包缓存以隔离源码根。

实际服务使用另外固定的 `/var/tmp/idkv-special-audit-bin-20261107`，资格运行目录为 `/var/tmp/idkv-special-audit-qualification-20261107`。宿主机同名 `/var/tmp` 不是这些路径。归档不包含认证文件、数据库目录或二进制。复制元数据夹具模拟历史累积；Counter 探针验证预算而非 RSS；自动后台索引场景没有手工恢复调用，具体限制见报告。

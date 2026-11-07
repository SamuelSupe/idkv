# 第五轮核实与修复证据

结论及逐项解释见 [修复报告](../../docs/deep-remediation5-20261107.md)。所有最终运行使用 `shipping-binaries.json` 中的二进制，与 `shipping-source.json` 固定的源码一致；完整来源见 `source-provenance.json`。

| 最终证据 | 内容 |
|---|---|
| `shipping-full-tests.log` | 185 项测试通过，包含 failpoints 崩溃恢复 |
| `shipping-clippy.log`、`shipping-fmt.log`、`shipping-release-build.log` | 静态检查及构建 |
| `shipping-openapi-verification.log` | 最终 CLI 与仓库 OpenAPI JSON 相等 |
| `native/`、`shipping-native-driver.log` | 三组原始场景，各运行基线和最终版本 |
| `shipping-verification.log`、`verify-evidence.py` | 独立重建确认模型，674 条确认、16 阶段、1156 次读取、零差异 |
| `qualification-fixed/` | 最终版本相同混合负载、主从重启、GC 和备份恢复 |
| `qualification-before-repeat/`、`qualification-before/` | 相同负载的最近及首次基线；未删去较慢基线 |
| `performance-comparison.json` | 最近基线与最终版本的吞吐、P50/P95/P99 |
| `faults/`、`shipping-faults-driver.log` | ENOSPC、WAL write EIO、WAL fsync EIO；每组保留 2000 条原确认记录 |

`profile-before/`、`profile-fixed/` 与 `fsync-profile-summary.json` 是单独的 tracing 定位证据；其候选二进制为中间版本，含 tracing 开销，不能作为最终性能成绩。

`*-pre-tail-fix/`、`performance-before-tail-fix.json` 和其他未带 `shipping-` 的构建/运行日志保留中间诊断过程，不属于最终验收。`transport-handler-before.log` 记录已修复的长处理器误超时回归。`shipping-incremental-compiler-ice.log` 是旧增量缓存触发的 Rust 1.90 编译器错误；最终验收改用独立 target 并关闭增量编译后通过。

没有归档数据库、备份实体、磁盘镜像、可执行文件或临时凭证。相应进程和 loop 挂载已清理；原生临时目录路径记录在来源文件中。新的原始场景可用 `validate-native.py` 重跑；原有资格脚本未修改。

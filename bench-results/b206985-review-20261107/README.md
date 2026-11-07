# b206985 review 核实与修复证据

逐项结论及性能风险见[修复报告](../../docs/b206985-review-20261107.md)。验证记录在提交前固定，后续发布以 Git 历史为准。

| 证据 | 含义 |
|---|---|
| `control-results.json`、`control-*.log` | 八个行为场景在独立 b206985 源码上实际失败 |
| `control-test-input.json`、`control-source-provenance.json` | 对照测试输入、测试专用适配及生产逻辑保持基线的核对 |
| `full-tests.log` | 新增 12 项，整套 197 项通过 |
| `retention-tests.log`、`backup-lock-test.log`、`migration-versions-test.log` | lint 调整后相关测试及加强的多版本分页边界 |
| `clippy.log`、`fmt-check.log`、`release-build.log` | 静态检查及构建 |
| `release-binary-recheck.log` | 加强测试后两个运行资格二进制字节仍完全一致 |
| `openapi-verification.log` | 最终 CLI OpenAPI JSON 与仓库文件一致 |
| `connections/`、`connections-driver.log` | 空闲、半截头部及 keep-alive 三组连接耗尽，基线/修复实际服务对照 |
| `qualification-before/`、`qualification-fixed/` | 原有脚本相同 10/45/10 秒阶段，含主从重启、GC、备份恢复和确认模型 |
| `qualification-repeat-before/`、`qualification-repeat-fixed/` | 成对复测，将恢复阶段延长至 60 秒，避免只依赖十秒样本 |
| `performance-comparison.json`、`performance-repeat-comparison.json` | 吞吐及 P50/P95/P99 原始对比；没有取得普遍尾延迟改善证据 |
| `light-write-outliers.json` | 第一轮轻租户最慢确认，保留原时间戳和延迟 |
| `faults/`、`faults-driver.log` | ENOSPC、限定 WAL write/fsync EIO，每组保留 2000 条原确认记录 |
| `source-provenance.json`、`binaries.json` | 源码、脚本、环境、二进制 SHA-256 及验证版本关系 |

`initial-*`、`io-priority-test-compile.log`、`lib-tests.log` 和 `http-tests.log` 为中间诊断或首批验证，不替代最终验收。初轮低内存测试拒绝、缺少测试导入及 lint 诊断保留，以便核对过程。

`validate-connections.py` 重用仓库已有 Lab 与确认模型，无额外测试框架。原有资格和磁盘故障脚本未改动。目录不包含数据库、备份实体、可执行文件、磁盘镜像或临时认证文件。测试进程及 loop 挂载已清理。

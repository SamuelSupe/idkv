# 特殊稳定性问题修复证据

结论、兼容性及剩余边界见 [修复报告](../../docs/special-stability-fix-20261107.md)。保留原审查目录，不覆盖修复前证据。

- `validated-source.json`：完整 209 项测试、Clippy、格式检查和 release 的最终源文件哈希。
- `full-tests.log`、`clippy.log`、`fmt-check.log`、`release-build.log`：完整验证；成功格式日志为空。
- `registry-tests.log`：旧 v4 满额历史迁移、UUID 保护和新格式容量预检的实际记录。
- `original-probes-input.json`、`probe-source.json`：原自动后台及计费探针原文，与审查输入一致；独立源码仅追加测试。
- `original-probe-results.json`、`original-*.log`：同一个引擎自动恢复，以及三次原计费并发探针通过。
- `qualification/`、`faults/`、`runtime-summary.json`：真实 HTTP、重启、备份离线恢复、ENOSPC 和限定 WAL EIO 的数据核对。
- `performance-comparison.json`：全部阶段/操作的前后结果，含普通阶段 P99 上升，未只挑选有利数据。
- `compat/`、`check-v5-compat.py`：真实旧/新二进制的 v5 拒绝、历史状态、容量指标及 200 次文档状态核对。
- `binaries.log`、`archived-files.json`、`source-provenance.json`：固定二进制、归档哈希和来源记录。

主要 target 是 `/var/tmp/idkv-special-fix-target-20261107`，测试 TMPDIR 为 `/var/tmp/idkv-special-fix-tmp-20261107`。原探针副本在 `/var/tmp/idkv-special-fix-probe-source-20261107`，有独立 target 和 TMPDIR。实际服务从 `/var/tmp/idkv-special-fix-bin-20261107` 启动，不覆盖运行文件；qualification、faults、compat 各使用独立新目录。路径均在 OrbStack Ubuntu 内，宿主同名路径不是这些目录。

根格式为 v5，旧读者拒绝；WAL/SST 与复制协议不变。历史表示压缩仍有 16 MiB 容量上限。验证不代表无限周转容量、真实硬件掉电或长期尾延迟 SLA。修改尚未提交。

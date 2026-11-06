# OpenDAL 备份验证 — v0.2.0，2026-11-05

本报告记录实现与实际运行证据。原 v0.1 百万/千万条性能基线保留在 [validation.md](validation.md)，未作为开启备份后的性能结论。

本页为 v0.2 历史报告。v0.3 的完整测试含这 8 项备份回归；新增 DSL、优化和运行结果见 [dsl-validation.md](dsl-validation.md)。本页 S3 模拟器场景未计入 v0.3 的重新运行范围。

## 环境

- OrbStack Ubuntu Linux AArch64，Rust/Cargo 1.90.0。实际业务暂存位于 VM `/var/tmp` 的 Btrfs 磁盘；Cargo 编译产物使用 `/tmp`。
- OpenDAL 固定为 0.55.0，启用 s3、fs、blocking；存储内核仍为自研 WAL/LSM/SST。
- S3 兼容运行测试使用本机 Moto Server 5.1.14，独立测试 bucket、合成凭证、回环 HTTP endpoint。未连接真实 AWS，也未部署云资源。
- 实际运行随包提供的 Linux release 程序及离线导入工具，未启用 failpoints。宿主 macOS 因未接受 Xcode 许可，未运行原生编译；没有修改许可状态。

## 自动化验证

Linux 上实际执行 34 个集成测试全部通过：19 个引擎、8 个备份、2 个 HTTP、5 个恢复测试。静态检查 Clippy `-D warnings` 和格式检查通过；静态检查与运行结果分别记录。

备份新增行为验证：

| 场景 | 已验证结果 |
|---|---|
| 连续两次检查点备份 | 复用已有对象；第二次上传字节减少 |
| 保留清理 | 旧备份被移除，共享 SST 保留且可校验恢复；其他租户文件不受影响 |
| 正文与覆盖索引恢复 | 回退后按索引查询与原值一致 |
| 损坏对象、越界 manifest 路径 | 校验和恢复失败；在线文档不被替换；清理遇损坏中止 |
| 备份目标故障、备份大小超限 | 任务失败，本地后续提交正常；修复目标后可重试 |
| 上传 manifest 后 / COMPLETED 后崩溃 | 重启续接同一任务，只发布完整备份 |
| 备份时并发提交、刷盘、合并 | 恢复结果精确对应 checkpoint_seq，一个提交边界 |
| 恢复切换后崩溃 | 重启不重放 restore，不覆盖重启后写入 |
| 新 Store 导入 / journal 发布与目录移入后崩溃 | 保留原租户身份，重启完成已发布导入；同 UUID 再导入拒绝 |
| 当前租户 CURRENT 损坏无法打开 | 仍可从远端验证并恢复 |
| HTTP 权限 | read/write 不能管理或列举备份；租户 admin 不能越租户；跨租户任务 UUID 不泄露状态 |
| 忘记任务 | 清理本地任务状态后远端备份仍可见 |

完整日志：[opendal-tests.log](../bench-results/opendal-tests.log)、[opendal-clippy.log](../bench-results/opendal-clippy.log)、[opendal-build.log](../bench-results/opendal-build.log)。

```sh
cargo test --locked --features failpoints -- --test-threads=1
cargo clippy --locked --all-targets --features failpoints -- -D warnings
cargo fmt --check
cargo build --locked --release --bin tenant-kv --bin tenant-kv-restore
```

## S3 兼容端到端运行

`remote_backup` 示例装载 2000 条包含 8192 字节伪随机 payload 的文档，产生约 16.7 MB 备份，以触发 multipart。通过 OpenDAL 实际发送 S3 HTTP 请求，而非用 fs 替代 S3 调用。执行首次备份、增量备份、逐文件 verify、修改后恢复、保留最近一份、再次校验。

第二份备份复用约 16.68 MB，只有约 0.8 KB 的新 WAL/Manifest 元数据需要上传；准确字节数记录在 [opendal-s3-smoke.json](../bench-results/opendal-s3-smoke.json)，不是大规模吞吐或延迟基准。

随后删除本地源库，从该远端备份运行随包离线导入工具，在新根目录恢复原 UUID。使用随包 HTTP 服务读取首尾文档、校验完整 SST；完整 HTTP 脚本继续执行本地原接口及新增远端备份/校验/恢复/清理/任务删除流程，并核对恢复后的覆盖索引 Lookup。

证据：

- [S3 示例结果](../bench-results/opendal-s3-smoke.json)
- [离线导入结果](../bench-results/opendal-s3-import.json)
- [HTTP 工作流](../bench-results/opendal-http-smoke.json)
- [运行摘要与 multipart 请求统计](../bench-results/opendal-runtime.json)
- [S3 模拟服务 HTTP 日志](../bench-results/opendal-s3-requests.log)

OpenAPI 3.1 共 32 条路径、29 个 schema，使用 openapi-spec-validator 0.9.0 对实际服务返回的规范完成验证，见 [opendal-openapi.json](../bench-results/opendal-openapi.json)。

## 未验证与边界

- 未连接 AWS S3、真实 MinIO 或其他厂商对象存储；Moto 不代表 IAM、KMS、Object Lock、限流、跨区复制或供应商兼容性已验收。生产启用前应在目标 bucket 执行同样的备份/校验/恢复流程。
- 未验证真实掉电、设备持久化失败或文件系统谎报 fsync；进程崩溃测试不能替代这些场景。
- 未完成 10–100 GB 备份、长时间断网、长期保留容量和开启备份时的 P95/P99/RSS 测量，不承诺性能零下降。上传不在前台读写路径，但本地检查点复制、校验、哈希仍消耗 CPU 与磁盘。
- 没有连续 WAL 归档/PITR、复制/高可用、远端懒加载、跨主机同租户仓库锁或内置定时调度。单文件 multipart 中断从头上传；文件级重用与任务恢复已验证。
- 备份 bucket、密钥权限、凭证和授权配置需要部署方管理。离线导入不恢复服务权限文件，使用目标配额。

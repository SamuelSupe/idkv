# 第四轮稳定性修复与验证（2026-11-07）

修复[第四轮审查](deep-audit4-20261107.md)确认的两项问题。基线为 `81c46c6bcff79f04fd08ee13cdf922dbaa72a40f` 加上第三轮尚未提交的修复；本轮保留这些改动。运行证据位于 [deep-remediation4-20261107](../bench-results/deep-remediation4-20261107/)。

## 修复行为

### 小磁盘配额下合并无法推进

取消合并开始时额外要求 `3 × 所选 SST 总大小 + 64 KiB` 的准入方式。合并先预留 64 KiB 元数据发布空间，再在每次 SST 输出写入前按实际字节申请同一个租户磁盘预算，包括表头、压缩数据、索引和页脚；较大的 Manifest 追加预留，单文件层级提升也预留发布元数据。普通写入和合并继续共享原子磁盘计数，没有提高租户配额或取消维护限额。

实际输出触及配额时，合并失败并清理尚未发布的文件，原 Manifest、最新数据和快照仍可读。Manifest 发布结果不确定时仍保留恢复所需文件，并将引擎标记为 `COMMIT_UNKNOWN`。持久化格式及合并选取方式保持兼容。刷盘仍使用既有保守预留；这次修复消除的是原复现场景中不必要的多倍输入预留，并不保证所有容量组合都能完成合并。

### fs 备份空目录阻塞列表和清理

剪除旧备份时，删除完成标记和清单后，同时移除已经清空的 UUID 目录。有效完成备份仍最多列出 4096 份，旧空目录不再占用这个数量上限；列表保持只读，总扫描边界为 200000 项。

prune 在读取备份清单前有界回收旧空目录，每次最多扫描 200000 项、删除 4096 个空目录。先回收再列举，避免历史目录过多时清理操作被自己的列表上限永久挡住；可以重复 prune 分批推进。回收限定当前租户的 UUID 目录，确认没有子项后使用非递归删除，不清理未完成但非空的快照。有效完成清单损坏仍然中止对象 GC，保留引用检查不变。

相关行为已同步到[运维](operations.md)、[存储格式](format.md)和[备份](backup.md)文档。

## 回归测试

新增四项回归，覆盖实际可观察行为及资源边界：

1. 原 16 MiB 磁盘配额下，1000 条随机 4 KiB 记录加两轮乱序覆盖更新，持续刷盘和合并后重新打开，正文及编码在值内的提交序号一致。
2. 实际合并输出超过 8 MiB 配额时返回 `RESOURCE_EXHAUSTED`，原 CURRENT 和 SST 文件集合不变，没有未发布文件残留；旧快照和最新已确认数据均完整可读。
3. 真实创建两份备份并剪除一份，旧 UUID 目录消失；加入 4097 个旧空目录后仍能列举，并用两次 prune 分批回收。第一次仅回收 4096 个，非空未完成快照和保留备份均不受影响。
4. 4097 份有效完成备份仍触发数量上限，不能因忽略空历史而绕过资源限制。

在独立源码副本、独立 Cargo target 中恢复本轮三个产品文件的旧实现，两项正向业务回归均失败；新实现通过，见 [before-tests.json](../bench-results/deep-remediation4-20261107/before-tests.json)。该副本的产品源码与第三轮最终指纹一致，没有使用混合版本二进制作为对照。

## 测试与构建结果

OrbStack Ubuntu Linux aarch64，原生 `/var/tmp` Btrfs；Rust 1.90。当前工作区使用专属 target：

```sh
CARGO_TARGET_DIR=/var/tmp/idkv-remediation4-target-20261107 TMPDIR=/var/tmp cargo test --locked --features failpoints -- --test-threads=1
CARGO_TARGET_DIR=/var/tmp/idkv-remediation4-target-20261107 TMPDIR=/var/tmp cargo clippy --locked --all-targets --features failpoints -- -D warnings
cargo fmt -- --check
CARGO_TARGET_DIR=/var/tmp/idkv-remediation4-target-20261107 TMPDIR=/var/tmp cargo build --locked --release --bins --features failpoints
```

全量 **168 项通过、0 失败**；Clippy、fmt 和发布构建通过。全量完成后仅追加了备份回归中“第一次回收 4096 个”的中间断言，随后重新运行两项备份回归和 Clippy，均通过；产品代码没有继续变动。见 [full-tests.log](../bench-results/deep-remediation4-20261107/full-tests.log)、[backup-batch-boundary.log](../bench-results/deep-remediation4-20261107/backup-batch-boundary.log)、[clippy-final.log](../bench-results/deep-remediation4-20261107/clippy-final.log) 和 [release-build.log](../bench-results/deep-remediation4-20261107/release-build.log)。

## 原场景与已确认数据核对

使用未加诊断改动的发布二进制，通过真实 HTTP 服务运行三个场景。合并参数与原报告相同：全局内存 128 MiB、租户内存 64 MiB、磁盘 16 MiB、memtable 256 KiB、cache 0、target_sst 4 MiB、level_base 64 MiB、l0_trigger 4、l0_stop 12。

| 场景 | 结果 |
|---|---|
| 旧版本制造停顿，再只升级二进制 | 1000 条初始记录和 553 次更新已确认后，连续 30 次写入重试失败，重启仍失败；升级后原 16 MiB 配额完成两轮各 1000 次更新、删除、强制合并及重启。升级前后先核对已有状态，每轮更新后再次核对，没有用后续覆盖写掩盖旧 ACK 丢失 |
| 修复版从空库运行同一负载 | 同配额完成初始写入、两轮更新、删除、合并、check 和重启；本次没有写入准入重试，1000 条最终状态一致 |
| 旧版本产生备份空历史，再升级 | 两份真实备份加一次 prune 留下空目录，按同样形态加速扩充至 4097 个目录、仅一份有效备份；旧列表和 prune 均失败。升级后 GET 列出一份且不删除目录；prune 回收到一个目录，随后五次更新、备份和 prune 的目录数始终为一 |

升级与空库两个合并场景，以 5 ms 目标间隔分别采样 3519 / 3131 次，租户目录观测峰值为 **10763098 / 9632207 B**，均低于原 16777216 B 配额。这是离散目录采样，不是文件系统物理分配的精确峰值；实际配额边界另由超额输出回归保护。此次升级在健康检查返回后，先核对全部 1000 条已有状态再完成第一笔新写，合计约 196 ms；它包含核对耗时，不作为纯恢复时间或性能提升结论。

每个成功写入响应保存键、修订号、生成参数和正文 SHA-256；删除保存键及修订号。独立核对器从 ACK 日志的相应前缀重建每个阶段的预期状态，再核对归档读取。最终结果：

- **6209 条已确认操作记录、17 个检查阶段、11400 次文档读取，内容及修订号差异为 0。** 两个合并租户与备份源库共核对 2200 条最终文档。
- 升级前保留的旧备份离线导入新节点，核对全部 200 条初始状态；五轮更新及 prune/GC 后，该已导入节点仍保持原状态。
- 最新备份离线导入另一个新节点，核对全部 200 条最新状态，并与源库中止重启后的状态一致。两次离线导入合计核对 400 条文档。
- 两个合并数据库再次打开，分别确认已成功删除的探测键返回 `NOT_FOUND`，check 成功。

见 [native-result.json](../bench-results/deep-remediation4-20261107/native-result.json)、[verification.log](../bench-results/deep-remediation4-20261107/verification.log)、[deletion-verification.json](../bench-results/deep-remediation4-20261107/deletion-verification.json)，以及 [native/](../bench-results/deep-remediation4-20261107/native/) 下的 ACK、阶段指纹及离线导入输出。对同一键的覆盖写按各阶段最新已确认版本核对，不声称保留全部历史版本。空目录场景采用加速夹具，没有执行 4097 个完整备份周期。

最终服务 SHA-256：`658da6e593bffe34fb2d44a5f17b510588dab15d59f965b6997098f91ad88309`；恢复工具 SHA-256：`c3e436d5c83031c6ab062f7d210a85d32d6d3acc13b187babf06c9b04e546452`。完整来源见 [source-provenance.json](../bench-results/deep-remediation4-20261107/source-provenance.json)。

## 重现和范围

提供保留的第三轮基线二进制及本轮发布二进制；固定版目录同时包含 `tenant-kv-restore`。root 必须是尚不存在的新目录：

```sh
python3 -B bench-results/deep-remediation4-20261107/validate-native.py --root /var/tmp/idkv-remediation4-new --binary /path/to/fixed/tenant-kv --before-binary /path/to/baseline/tenant-kv
python3 -B bench-results/deep-remediation4-20261107/recheck-deletes.py --root /var/tmp/idkv-remediation4-new --binary /path/to/fixed/tenant-kv
python3 -B bench-results/deep-remediation4-20261107/verify-evidence.py
```

前两条操作新 root 下的数据库，结束时停止自身进程并删除测试凭证。最后一条重算仓库内已归档证据；新运行可按归档时的结构复制结果后独立重算。本轮先完成一次连续写入验证，随后加强阶段核对，最终以第二次运行作为上述主要证据；首次结果保留在 `native-initial/`。备份回归初次实现与删除核对初次驱动的诊断日志也保留，但不计为通过验收。

本轮没有重跑长期五路并行性能资格负载，没有测试 macOS、S3 实际服务、真实设备掉电或内核写回故障。修复针对已复现的两项故障；真实容量不足、保留中的旧快照、刷盘预算和物理磁盘故障仍可能触发资源或 I/O 错误。

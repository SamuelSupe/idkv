# 复制活性和恢复发布语义复查（2026-11-09）

五项反馈均确认存在。保留工作树中已有的身份查询等改动，本次修复集中于复制请求截止、恢复发布结果和离线修复归档。

| 反馈 | 修复和运行证据 |
|---|---|
| 慢日志页没有完整额度，永远不追平也不初始化 | 修复前，真实四租户 HTTP 回归中，状态查询耗掉部分周期、成功正文持续超出剩余额度，10 秒内该租户仍不推进。保留 150 秒整轮时限和轮转；同一租户连续 3 次成功正文被较短的周期截止切断后，回退到分块检查点。成功完成请求或未收到成功正文会清零计数。修复后该场景恰好初始化一次并追平，全部租户的已确认文档及 revision 与源端一致。 |
| 已发布后最终报告写失败被误报为修复失败 | 最终报告写失败只产生 report_warning，stdout 保持 published 或 published_cleanup_pending，report_persisted=false，退出成功。故障注入验证磁盘报告仍在 publishing_tenant、stdout 已明确发布，重开后核对 history、epoch、已确认文档与完整归档。 |
| 普通恢复因清理失败被标记 UNAVAILABLE | 共享发布函数返回“已发布，附清理 warning”，收集旧目录和删除旧目录都属于这个阶段。普通恢复及副本安装成功返回，repair 保留 published_cleanup_pending。两个阶段分别注入失败，恢复进程内立即读回原备份并确认新写入；重启后再次核对新写入 revision。 |
| 失败的归档复制留下占位目录，阻止重试 | 复用已有 preparation Drop 清理尚未准备完成的归档；完整归档持久化后转为保留，供发布失败时诊断。验证复制阶段注入错误，以及普通 Linux 用户对源子目录的真实读权限拒绝：失败后不留新归档、不更换 epoch/history、不改原 WAL，同一路径修正故障后可成功重试。已有归档不覆盖，创建失败包含路径与检查提示。 |
| 清理 pending 只有 stdout 字段 | repair CLI 同时把 cleanup warning 和最终报告 warning 打到 stderr；相关 CLI 故障回归检查成功退出、结构化 stdout 与 stderr warning。 |

## 验证环境与命令

OrbStack Ubuntu 25.10 ARM64，普通用户 UID 501，Rust/Cargo 1.90.0。使用共享工作目录，测试、HTTP 服务和 CLI 子进程在 Linux 执行。

```sh
CARGO_TARGET_DIR=/var/tmp/idkv-p01-target-20261108 \
TMPDIR=/var/tmp/idkv-p01-tmp-20261108 \
cargo test --all-features --no-fail-fast -- --test-threads=4
cargo test --all-features --test engine -- --test-threads=4
cargo clippy --all-targets --all-features -- -D warnings
cargo fmt --all -- --check
git diff --check
```

定向验证：reviewc3b 的复制/CLI 回归通过，recovery 集成测试 26 项通过。首轮全量运行 289 项通过，唯一失败是旧用例要求“清理失败后隔离租户”，与本次修正的语义相反。更新该用例为检查恢复成功、立即确认新写入、重启后核对原备份和新写入 revision 后，重跑 engine 全部 33 项通过。当前 290 项测试均已执行通过；其余目标在首轮完整运行中全部通过，最终未再次重复无关目标。Clippy、fmt 和 diff 检查通过。

原始日志位于 Linux `/var/tmp/idkv-review-liveness-red-20261109.log`、`/var/tmp/idkv-review-followup-20261109.log`、`/var/tmp/idkv-review-recovery-20261109.log`、`/var/tmp/idkv-review-all-20261109.log` 和 `/var/tmp/idkv-review-engine-final-20261109.log`。

活性回归使用缩短的时限：状态查询 250 ms、单轮 500 ms、慢日志正文 800 ms；未实际等待每次 120/150 秒。等待响应头及首次短预算仍不触发初始化，计划升主的既有总期限回归仍通过。检查点回退沿用原有初始化名额和退避，不能保证任意低吞吐或持续故障链路都能完成复制。

普通错误会尽力移除不完整归档；进程崩溃、只读文件系统或目录本身无法删除时仍可能留下目录，下一次创建会明确报出路径，管理员应检查后处理。发布前的 journal/目录切换结果不确定仍会隔离租户，未把这类错误降为 warning。发布后最终报告无法确认持久化时，磁盘上的阶段不能单独作为重跑截尾的依据。

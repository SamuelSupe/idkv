# 第三轮稳定性修复与验证（2026-11-07）

基线 `81c46c6bcff79f04fd08ee13cdf922dbaa72a40f`，修复[第三轮审查](deep-audit3-20261107.md)确认的两项可用性问题，并保护相关的配额缩容边界。运行证据位于 [deep-remediation3-20261107](../bench-results/deep-remediation3-20261107/)。

## 修复

1. **刷盘后正文不可读。** SST 顶层目录、索引块及数据块分别计费输入/解压缓冲和解码结果，不再按正文原始长度统一预留十二倍内存。记录分配前检查数量、值长度和前缀压缩键展开长度，覆盖键缓冲容量、记录及 Arc 开销；解码后释放输入预算，保留结果及缓存继续计费。没有通过漏计前缀展开、取消预算或提高租户配额来绕过限制；持久格式不变。
2. **复制误用默认配额。** 检查点校验使用源端实际租户配额；已有租户重建时复用该租户的计费器，在线实例和临时校验不能各自独占一份完整配额。节点全局预算继续生效。离线灾备 import 仍使用目标节点默认配额。
3. **缩容回归保护。** 降低 SST 解码预留后，只验证存储能够打开已不足以保证 Get 可用。缩容预检增加当前文档扫描，复用 Get 的 JSON 解码预算计算；不能容纳点读的新配额会被拒绝，原配额保持。该附加扫描受 snapshot_seconds 期限约束，失败不发布新配额。

## 回归与负向对照

新增四项有意义的回归：同配额大文档在刷盘、合并、重开后仍可读写；节点默认配额低于实际租户配额时完成复制初始化；已有租户校验不能绕开在线资源占用；前缀压缩长键的实际展开不能突破预算且读取结束后预算释放。另扩展既有缩容测试，覆盖未刷盘的 700/400 KiB 文档和已刷盘的 700 KiB 文档，验证拒绝缩容后完整内容、修订号及原配额在重开后保留。

独立基线构建中，两项业务回归均失败。只应用 SST 修复、保留旧复制预算时，默认 1 MiB / 实际 16 MiB 的复制回归仍失败，证明测试没有被第一个修复掩盖。只应用 SST 修复而保留旧缩容预检时，会错误接受 2 MiB 配额，缩容保护测试也失败。四个预期失败及命令见 [before-tests.json](../bench-results/deep-remediation3-20261107/before-tests.json)。

## 最终验证

OrbStack Ubuntu Linux aarch64，原生 `/var/tmp` Btrfs，Rust 1.90。最终代码运行：

```sh
CARGO_TARGET_DIR=/var/tmp/idkv-remediation3-target-20261107 TMPDIR=/var/tmp cargo test --locked --features failpoints -- --test-threads=1
CARGO_TARGET_DIR=/var/tmp/idkv-remediation3-target-20261107 TMPDIR=/var/tmp cargo clippy --locked --all-targets --features failpoints -- -D warnings
cargo fmt -- --check
CARGO_TARGET_DIR=/var/tmp/idkv-remediation3-target-20261107 TMPDIR=/var/tmp cargo build --locked --release --bins --features failpoints
```

全量 **164 项通过、0 失败**，包含备份、恢复、HTTP、复制和切换；Clippy、fmt 和发布构建通过。分别见 [full-tests.log](../bench-results/deep-remediation3-20261107/full-tests.log)、[clippy.log](../bench-results/deep-remediation3-20261107/clippy.log) 和 [release-build.log](../bench-results/deep-remediation3-20261107/release-build.log)。

发布二进制通过真实 HTTP 进程完成以下五个场景：

| 场景 | 结果 |
|---|---|
| 旧二进制生成不可读 SST，中止后只更换二进制，保持原 8 MiB 配额 | 完整读取原已确认 700 KiB 文档，内容 SHA-256 和 `_rev` 相同，check 成功 |
| 8 MiB 租户，可压缩 700 KiB 文档，后台自动刷盘 | 连续 25 次读取成功；不安全缩容被拒，原配额不变；合并、check、中止重启后完整一致 |
| 8 MiB 租户，随机 700 KiB 文档，后台自动刷盘 | 连续 24 次读取成功；不安全缩容被拒；合并、check、中止重启后完整一致 |
| 节点默认 8 MiB、租户实际 16 MiB，从库首次初始化 | 一次初始化，零失败，源端与副本正文和修订号一致 |
| 节点默认 1 MiB、租户实际 16 MiB，首次初始化及已有租户重建 | 两阶段各一次初始化、零失败；停从库期间确认三次更新，耗尽 1 MiB 日志窗口后重建至 seq 5，最新已确认状态一致 |

两种默认配额下，启动从库至追平且初始化工作者退出分别约 81.0 / 86.8 ms。结果包括初始化任务结束状态，不仅检查位置已发布。计时用于有界验收，不作为吞吐或尾延迟的版本收益结论。五个场景的已确认状态差异均为 0；对同一键的覆盖写按最新已确认版本核对。

真实进程驱动、逐场景摘要及原始输出分别见 [memory-validation.py](../bench-results/deep-remediation3-20261107/memory-validation.py)、[native-result.json](../bench-results/deep-remediation3-20261107/native-result.json)、[native-driver.log](../bench-results/deep-remediation3-20261107/native-driver.log)。驱动结束后已停止自身进程并删除测试凭证。

最终服务二进制 SHA-256：`f193543e4595867be1305a8967d9e879574e120b2ffdc453aaed32789a6fc18b`。源码及两个发布二进制的指纹见 [source-provenance.json](../bench-results/deep-remediation3-20261107/source-provenance.json)。

## 复现与范围

发布构建后，提供保留的基线二进制；root 必须是尚不存在的新目录：

```sh
python3 -B bench-results/deep-remediation3-20261107/memory-validation.py --root /var/tmp/idkv-remediation-new --binary /path/to/fixed/tenant-kv --before-binary /path/to/baseline/tenant-kv
```

初期复现副本与当前检出共用 Cargo target，造成产物复用，相关 `mixed-target-full-tests.log` 等诊断日志不计入最终验收。随后为基线和修复代码分别使用独立 target，并清除其产品产物；最终全量和原生进程验证未复用其他源码版本的产品二进制。

本轮没有重跑长期五路并行性能资格验证，也不包含 macOS、真实设备掉电及内核写回故障。资源持续不足仍可能拒绝读取或初始化；本轮验证的是合法的小内存配额下两项确定性故障的修复，没有宣称取消资源边界。

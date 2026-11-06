# 验证报告 — 2026-11-05

本页保留 v0.1 的基线运行与性能结果。v0.2 OpenDAL 备份的回归测试、S3 兼容验证和未测边界单独记录于 [backup-validation.md](backup-validation.md)，不得将本页基准视为开启备份后的性能结果。

当前 v0.10.1 的持续并行负载、磁盘不足、WAL I/O 故障与重启资格检查另见[生产资格验证](production-qualification.md)；主节点故障升主与旧节点重建见[恢复演练](recovery-drill.md)。这些独立运行不把本页历史基线升级为当前版本的百万/千万条性能资格。

v0.3 DSL 与读取优化的测试和前后对比见 [dsl-validation.md](dsl-validation.md)。本页百万/千万条结果未在 v0.3 重跑，保留为历史基线。

本报告记录已执行的检查和实际测量，不将初始性能目标或未运行场景视为验收通过。源码、复现程序、原始 JSON/日志与本报告一并交付。

## 平台与边界

- OrbStack Ubuntu，Linux aarch64，Rust/Cargo 1.90.0；宿主为 Apple CPU。VM 可见 18 个逻辑 CPU，约 31 GiB 内存。
- 单租户性能及多租户运行命令使用 `taskset -c 0-7` 和 systemd `CPUQuota=800%`；热缓存测量 `MemoryMax=16G`，单租户引擎预算 4 GiB。
- 数据位于 Linux `/var/tmp` 的 Btrfs `/dev/vdb1`，不是 `/tmp` 的 tmpfs。实际宿主 NVMe 型号及设备持久化语义未验证，不能等同裸机 NVMe 成绩。
- 超内存验证单独设置进程/cgroup 上限 3 GiB、引擎预算 512 MiB。宿主及虚拟存储缓存仍可能存在。
- 发布程序为 Linux AArch64 ELF，release 构建，未开启 `failpoints`。macOS 原生编译未完成：本机 Xcode 许可尚未接受；没有修改宿主许可状态。其他架构未做运行验证。

环境记录见 [environment.json](../bench-results/environment.json)。

## 正确性、隔离与故障恢复

Linux 实际执行 25 个集成测试：19 个引擎测试、1 个 HTTP 工作流测试、5 个恢复测试，全部通过。`cargo clippy --all-targets --features failpoints -- -D warnings` 通过。这里的 Clippy 是静态检查；运行验证由测试和 TCP 示例提供。

覆盖的行为包括：

- 随机 CRUD 与参考模型对照，刷盘、合并、删除及重启后结果一致；范围、降序、负数和排序同键的参考结果比较。
- 大整数、整数/浮点不同值域、缺失/null、JSON Pointer、数组及对象覆盖投影。
- 批次内唯一冲突、条件版本冲突、并发提交、正文与索引一致性；批次响应超限在提交前拒绝。
- 在线建索引期间写入和删除、失败唯一索引修复、覆盖索引更新、非唯一 Lookup 上限及只取首条。
- 分页快照在并发修改和合并期间保持一致；快照过期、跨租户游标拒绝、冷租户关闭与运行代次更新。
- 同键/同索引值的租户物理隔离，损坏或错误归属文件拒绝、其他租户继续可用；备份恢复和删除不修改其他租户。
- 句柄/快照比 Store 活得更久时仍持有所有权锁；单独打开的引擎阻止管理者删除该目录。
- 删除 GC 继续推进，同时保留旧快照及重新创建的集合；其他租户操作不会清理尚未发布的恢复暂存目录。
- 资源配额、扫描限额、返回限额、HTTP 权限及解析前内存准入，包括大量 JSON 数组元素输入。

子进程中止覆盖 10 个位置：WAL 不完整头、不完整负载、追加完成、同步完成、Manifest 发布、SST 同步、合并输出同步、索引发布，以及恢复时旧目录移出/新目录移入。另有 3 个可重试/错误注入位置：未发布 SST、WAL 轮换创建后、恢复 journal 发布后。最后一种验证该租户被禁止重新打开，重启 Store 后按 journal 恢复。

这些用例验证已确认数据保留、批次与索引原子性、尾部截断规则和恢复状态机；没有模拟真实设备掉电或文件系统谎报 fsync 成功。

实际 TCP 服务也通过 CRUD/CAS → 索引 → Lookup → 查询 → 备份恢复 → 完整性检查 → 删除工作流。嵌入式示例通过。打包后的 Linux 程序也独立启动并通过同一 TCP 工作流，见 [发布程序验证](../bench-results/package-smoke.json)。OpenAPI 3.1 含 25 个路径、26 个 schema，418 处内部引用全部解析成功；这不是完整的第三方 OpenAPI 规范认证。

证据：[tests.log](../bench-results/tests.log)、[clippy.log](../bench-results/clippy.log)、[HTTP 工作流](../bench-results/http-smoke.json)、[嵌入式结果](../bench-results/embedded.json)。

## 单租户混合负载

数据为 100 万/1000 万条，文档含 1024 字节 payload 及键、IP、env、score、owner 等字段，因此完整文档略大于 1 KiB。每个集合 3 个索引。热缓存 Get/Lookup 使用固定的 1000 个分散键；索引查询返回至多 100 条覆盖字段。

每种模式持续约 15 秒，16 个并发读线程，同时以 20 条原子批次持续更新约 1000 条文档/秒，后台刷盘和合并开启。QPS 为接口调用数，Lookup 一次包含 1000 个输入。它衡量该热点工作集，不能外推为整个数据库全热或任意查询性能。

| 记录数 | 场景 | P95 ms | P99 ms | 调用/秒 | 更新文档/秒 |
|---|---|---:|---:|---:|---:|
| 100 万 | Get | 0.071 | 0.169 | 1,085,630 | 1000.3 |
| 100 万 | 覆盖索引查询，100 条 | 2.359 | 2.934 | 10,366 | 1001.0 |
| 100 万 | Lookup，1000 键 | 13.810 | 18.472 | 2,089 | 1000.9 |
| 1000 万 | Get | 0.070 | 0.167 | 1,101,451 | 1000.4 |
| 1000 万 | 覆盖索引查询，100 条 | 2.628 | 3.273 | 9,320 | 999.8 |
| 1000 万 | Lookup，1000 键 | 15.957 | 20.455 | 1,747 | 1000.7 |

三项初始 P99 目标在上述热缓存混合负载中均达到。千万条最终 Lookup 样本：1909 次缓存命中、161 个候选文件、0 次块读取、0 次正文回读；原始结果记录了实际而非估算的计数。

千万条三个阶段累计：WAL 82,239,744 B、刷盘 36,747,521 B、合并输出 83,337,149 B、输入文档 54,948,300 B，对应观测窗口写放大约 **3.68×**。这是含 3 个索引、未强制等待所有合并结束的短窗口值，不代表稳态写放大。累计写锁等待 257.75 ms；I/O 排队等待是所有作业等待时间之和，不能当作请求延迟。热测量采样 RSS 约 198–298 MiB，并非峰值 RSS。

千万条装载在开发过程中分段续载，不作为一次完整、可比的装载吞吐成绩。百万装载期间存在构建任务，也不报告孤立写入吞吐。

原始结果：[百万条](../bench-results/million.jsonl)、[千万条](../bench-results/ten-million.jsonl)。

## 数据大于可用内存与冷启动探测

数据库文件约 11,643,230,586 B（11.64 GB），大于 3 GiB cgroup 内存上限的三倍。以 512 MiB 引擎预算完成 SST 校验和全量逻辑文档扫描：**10,000,000 条**，总耗时 74.47 秒。引擎采样内存 278,522,475 B，RSS 267,452 KiB；扫描读取 843,059 个块，处理 10,015,520 个可见/历史候选条目。检查和扫描合计时间不能当作单纯扫描吞吐。

探测前只针对本任务数据文件执行 fsync 和 `posix_fadvise(DONTNEED)`，没有清空系统全局缓存。下列场景顺序执行，属于冷启动后逐渐变热的混合缓存探测；不是每个场景都处于完全冷缓存，更不是物理设备冷读。

| 场景 | 样本数 | P95 ms | P99 ms | 累计块读取 | 累计正文回读 |
|---|---:|---:|---:|---:|---:|
| random_get | 5000 | 0.101 | 0.195 | 未采集 | 未采集 |
| missing_get | 5000 | 0.001 | 0.001 | 未采集 | 未采集 |
| random_lookup_1000 | 20 | 67.660 | 67.660 | 22244 | 0 |
| body_lookup_1000 | 20 | 136.351 | 136.351 | 53009 | 20000 |
| missing_lookup_1000 | 20 | 0.794 | 0.794 | 0 | 0 |

Get 接口未导出逐调用统计，表中明确记为未采集。Lookup 探测只有 20 个批次，P95/P99 接近最大样本，仅用于暴露随机磁盘访问与正文回读成本，不能据此承诺尾延迟 SLA。未命中键位于现有键范围之外，此结果不代表所有 Bloom Filter 未命中分布。

证据：[完整校验](../bench-results/disk-verify.jsonl)、[探测结果](../bench-results/disk-probes.jsonl)、[定向缓存回收](../bench-results/probe-cache-eviction.json)。

## 多租户短时运行

8 个活跃租户，每租户 1000 条可压缩的约 1 KiB payload 文档、3 个索引。2 个热点写者不限速更新，16 个读取线程每次读取后休息 1 ms；另有 32 个冷租户创建、自动空闲关闭及重开。全局引擎预算 512 MiB，每租户 64 MiB。持续运行 120 秒后，显式刷盘/合并并校验所有活跃租户。

- 热点租户分别更新 430,840 与 432,780 条文档，没有请求失败或配额拒绝；热点读线程 P99 6.935–7.053 ms。
- 其余 6 个租户的读线程 P99 0.015–0.017 ms；这个轻读热点集不包含随机磁盘负载。
- 引擎计量峰值内存 34,438,582 B（32.84 MiB），没有突破全局预算。32 个冷实例均观察到关闭，重开后数据校验通过；重开 P99 0.101 ms。
- 收尾后各活跃租户都为 L1 六个文件、L0 无积压，维护错误为空、没有遗留活动快照。

下表是各租户累计 I/O 作业份额及等待，不是磁盘带宽份额，也不是等额配额承诺。轻租户大部分读取命中缓存，提交到 I/O 队列的工作显著更少。

| 租户 | 完成 I/O 作业 | 作业份额 | 累计 I/O 排队 ms | 累计写锁等待 ms |
|---|---:|---:|---:|---:|
| active-0 | 172,845 | 48.60% | 2863.27 | 12831.83 |
| active-1 | 173,542 | 48.80% | 2822.62 | 12313.40 |
| active-2 | 1,546 | 0.43% | 23.64 | 5.91 |
| active-3 | 1,543 | 0.43% | 12.59 | 0.03 |
| active-4 | 1,541 | 0.43% | 320.05 | 0.00 |
| active-5 | 1,541 | 0.43% | 10.67 | 7.11 |
| active-6 | 1,539 | 0.43% | 29.18 | 0.00 |
| active-7 | 1,539 | 0.43% | 327.34 | 0.00 |

原始数据：[multi-tenant.jsonl](../bench-results/multi-tenant.jsonl)。配额触发后的其他租户可用性由集成测试覆盖；此运行未触发背压。120 秒不足以验证数日运行、数千冷租户或大数据量多租户公平性。

## HTTP 独立测量

真实 loopback HTTP/1.1 keep-alive，Python 标准库客户端，16 个并发连接、共 320,000 次请求，持续 13.76 秒。读取一个含 1024 字节 payload 的热点文档，无并发更新：吞吐 23,250 次/秒，P95 **1.800 ms**，P99 **2.673 ms**。

此独立探测包含客户端调度、JSON 编解码、认证及 loopback 网络开销；HTTP 服务使用 VM 默认 CPU 配置，没有沿用嵌入式混合负载的 8 核配额。不能与上表相减得出服务纯开销，也不代表服务吞吐上限。结果见 [http-bench.json](../bench-results/http-bench.json)。

## 复现

在 Linux 项目目录运行；OrbStack 场景在命令前使用 `orb -m ubuntu`。磁盘数据目录不要使用 tmpfs。

```sh
cargo test --locked --features failpoints -- --test-threads=1
cargo clippy --locked --all-targets --features failpoints -- -D warnings
cargo build --locked --release --bin tenant-kv --examples

target/release/examples/bench load /var/tmp/tkv-million 1000000 1024
target/release/examples/bench measure /var/tmp/tkv-million 15
target/release/examples/bench load /var/tmp/tkv-ten-million 10000000 1024
target/release/examples/bench measure /var/tmp/tkv-ten-million 15

python3 scripts/evict_benchmark_cache.py /var/tmp/tkv-ten-million
sudo systemd-run --scope -p MemoryMax=3G -p CPUQuota=800% \
  env TKV_BENCH_MEMORY=536870912 taskset -c 0-7 \
  target/release/examples/bench verify /var/tmp/tkv-ten-million

target/release/examples/bench probe /var/tmp/tkv-ten-million
target/release/examples/soak /var/tmp/tkv-soak 120
python3 scripts/http_smoke.py http://127.0.0.1:8080 token.txt
python3 scripts/http_bench.py http://127.0.0.1:8080 token.txt
```

对热缓存 `load/measure` 和多租户命令也可加 `sudo systemd-run --scope -p MemoryMax=16G -p CPUQuota=800% taskset -c 0-7`；冷启动 probe 使用与 verify 相同的 3 GiB / 512 MiB 设置。`probe` 顺序测量多个场景，需要每次重测前按所需缓存条件回收本任务数据文件缓存。

## 尚未验证的验收项

真实掉电、设备级 torn write、fsync/磁盘满的系统级故障注入、100 GB 规模、不同索引数量/选择率的完整矩阵、数千冷租户、多租户同时大规模恢复与建索引、24 小时以上稳态、跨平台运行与升级迁移，均未完成专项验证。读写正确性、隔离与资源上限已有实现和对应边界测试，但不能把上述短时结果表述为生产环境长期有界运行已经验收。

后续报告核实与并发修复的当前工作区验证见[2026-11-06 报告修复记录](review-remediation-20261106.md)。上述历史性能及资格结果对应各自当时的构建，不能直接代替后续修复后的性能结论。

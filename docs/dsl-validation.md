# DSL 与读取优化验证 — v0.3.0，2026-11-05

本页保留 v0.3 的历史测量。v0.4 的稳定性修复、持续负载与新一轮前后对比见 [v0.4-validation.md](v0.4-validation.md)。

本次实现七命令 DSL、参数绑定、租户语法缓存和可复用准备语句，并优化原生点读、批量 Lookup、标量过滤和查询元信息读取。所有数据仍落入原有租户独立 WAL/SST；未引入数据库依赖，也未改变持久格式。

## 正确性与接口

OrbStack Linux 上实际执行 **43 项测试，全部通过**：19 项引擎、8 项备份、7 项 DSL、3 项 HTTP、5 项恢复，以及 1 项 SST 点读单元测试。Clippy 全目标/failpoints 模式 `-D warnings`、格式检查通过。测试与静态检查分别记录于 [dsl-tests.log](../bench-results/dsl-tests.log)、[dsl-clippy.log](../bench-results/dsl-clippy.log)。

新增的行为覆盖包括：

- 所有 DSL 命令、条件版本、唯一/覆盖索引、原生接口结果一致性；参数值不能变成可执行语法，复合语句解析失败不产生部分写入。
- 大整数、整数/浮点分离、负零、缺失/null、Unicode/NUL、对象/数组字段及复杂布尔条件；编译过滤结果与参考比较函数逐项核对。
- 字段/查询参数变化、跨租户游标、缓存授权、DDL 失效、集合删除重建，以及旧快照不能使用新索引目录。
- 准备语句缓存上限及租户分离；大参数重复引用在展开前拒绝，错误后释放预留内存；2,000 条确定性随机 UTF-8 文本解析无 panic。
- 单键的 300 个历史版本跨越多个 SST 数据块和索引分区，含删除标记；多个快照序号均得到正确可见版本。
- 既有原子提交、随机 CRUD、在线索引、并发分页、崩溃恢复、物理隔离、备份/恢复与配额测试继续通过。

实际启动随包 Linux release 服务，通过 TCP 运行 [dsl_smoke.py](../scripts/dsl_smoke.py) 以及原生 [http_smoke.py](../scripts/http_smoke.py)，包含 CRUD/CAS、覆盖 Lookup、查询、分页、错误位置、本地备份恢复、完整性检查与删除。Rust [dsl.rs](../examples/dsl.rs) 也已实际执行。OpenAPI 3.1 使用 openapi-spec-validator 0.9.0 校验通过，包含 33 个路径、30 个 schema、版本 0.3.0。原始结果见 `bench-results/dsl-*-smoke.json`、[dsl-example.json](../bench-results/dsl-example.json)、[dsl-openapi-validation.json](../bench-results/dsl-openapi-validation.json)。

这轮没有重新执行真实/模拟 S3 端到端流程；8 项 OpenDAL 备份回归已重跑。v0.2 的 S3 模拟器证据保留在 [backup-validation.md](backup-validation.md)。

## 测量方法

- OrbStack Ubuntu Linux AArch64，Rust/Cargo 1.90.0，release 构建，未开启 failpoints。VM 可见 18 CPU、约 31 GiB 内存；基准绑定 CPU 0–7，**本次未设置 cgroup 内存上限**。引擎默认全局 4 GiB、租户 512 MiB 预算。
- 数据在 `/var/tmp` Btrfs 虚拟磁盘；无真实掉电或裸机 NVMe 资格验证。宿主 macOS 原生编译仍因 Xcode 许可未接受而未执行，没有更改许可。
- 每库 10,000 条文档，含 512 字节 payload 和元字段；一个 `ip` 唯一覆盖索引（include owner）、一个 owners Lookup。点读与唯一查询反复命中同一键；批量 Lookup 为 1,000 个固定不同键，另测 1,000 次重复键。
- 装载后刷盘、合并。每模式先预热 3 次；点读和 Lookup 热块缓存，扫描不主动准入普通数据块，但 OS 页缓存已热，**不属于冷盘测量**。
- 原生前后对比将同一份 [read_perf.rs](../examples/read_perf.rs) 放入已交付 v0.2.0 源码和 v0.3.0 源码，分别构建独立可执行文件。执行 3 轮，按旧/新、新/旧、旧/新交替顺序；表格为各轮分位数/吞吐的中位数，倍数按 P50 计算。
- 原生每模式目标 2 秒，至少 10 次、最多 200,000 次调用。极快的 Get 及新版唯一查询因达到次数上限会提前结束；实际调用数和时长均保存在原始 JSONL，不能把所有样本描述成完整 2 秒。
- DSL 开销测试独立运行：单线程无更新、16 读取线程加目标 1,000 文档/秒更新，每模式 2 秒；一轮依次测 native/prepared/cached/parse_each。写入按每批 20 条持久提交，后台维护开启。模式间有不同的更新/维护进度，属于短时诊断，不是长期稳定性或严格随机对照。

环境、二进制哈希和相同基准源码哈希见 [dsl-environment.json](../bench-results/dsl-environment.json)。

## 原生接口前后对比

延迟单位为 **µs/调用**；Lookup 一次调用包含 1,000 个输入。`in` 为 256 项成员列表，255 项不匹配、一项匹配 team-1，扫描 10,000 条、返回 313 条。

| 场景 | v0.2 P50 | v0.3 P50 | v0.2 P95 / P99 | v0.3 P95 / P99 | P50 倍数 |
|---|---:|---:|---:|---:|---:|
| 热点 Get | 1.250 | 0.917 | 1.500 / 1.750 | 1.166 / 1.209 | 1.36× |
| 唯一覆盖索引查询 | 21.625 | 1.459 | 29.625 / 37.250 | 1.750 / 1.959 | 14.82× |
| 256 项 in 扫描 | 98,100.129 | 11,110.103 | 101,449.926 / 102,320.345 | 18,832.824 / 22,641.498 | 8.83× |
| 1,000 不同键 Lookup | 1,147.752 | 982.126 | 1,203.294 / 1,239.169 | 1,036.418 / 1,099.918 | 1.17× |
| 1,000 重复键 Lookup | 248.501 | 237.293 | 270.917 / 288.917 | 254.334 / 270.834 | 1.05× |

相应调用吞吐中位数：Get 750,000 → 1,009,830 次/秒；唯一查询 52,198 → 645,486；in 扫描 10.27 → 82.82；不同键 Lookup 872.78 → 1,017.51 批/秒；重复键 Lookup 4,007.92 → 4,205.61 批/秒。分位数与吞吐分别取中位数，不能直接用其中一个的倒数替代另一个。

优化与证据的对应关系：

| 改动 | 作用及观测 |
|---|---|
| 独立 SST/LSM Get 路径 | 避免单键包装为批量键和映射；保留历史版本/墓碑语义 |
| 快照感知的目录缓存 | 避免每次唯一查询重新读取集合和索引定义；DDL/旧快照回归保护正确性 |
| 预编译字段与常量，IN HashSet，直接读取二进制标量 | 前后都扫描 10,000 条、读取 499 块、解压 7,106,053 字节；收益主要来自减少 CPU 和临时分配，并非少读数据 |
| 已排序输入快路、结果大小复用、少克隆 | 不同键 Lookup 前后均无正文回读、热数据块读取为 0，扫描 1,000 项；重复键只扫描 1 项 |

覆盖查询前后同样只扫描 1 个索引条目且无正文回读。这里的 query.stats 反映执行路径数据读取，不包含所有元信息准备成本；Get 公共 API 不返回 ReadStats，基准中它的默认零统计不代表一次调用完全没有读取。

汇总见 [dsl-native-comparison.json](../bench-results/dsl-native-comparison.json)，逐轮结果为 `bench-results/dsl-native-{before,after}-{0,1,2}.jsonl`，另有进程资源 JSON。

## DSL 开销与并发

`native` 为原生 Rust API，`prepared` 为保留 Arc 的 execute，`cached` 为 execute_dsl 缓存命中，`parse_each` 每次重新解析。下面均为嵌入式调用，**不包含 HTTP、网络或响应 JSON 序列化时间**。

### 1 个读取线程，无更新

| 场景 | 入口 | 调用/秒 | P50 µs | P95 µs | P99 µs |
|---|---|---:|---:|---:|---:|
| Get | native | 961,737 | 0.959 | 1.167 | 1.209 |
| Get | prepared | 939,991 | 1.000 | 1.209 | 1.250 |
| Get | cached | 922,649 | 1.000 | 1.250 | 1.292 |
| Get | parse_each | 879,581 | 1.042 | 1.292 | 1.334 |
| 唯一覆盖查询 | native | 630,277 | 1.500 | 1.750 | 1.792 |
| 唯一覆盖查询 | prepared | 613,476 | 1.542 | 1.792 | 1.875 |
| 唯一覆盖查询 | cached | 601,093 | 1.583 | 1.833 | 1.875 |
| 唯一覆盖查询 | parse_each | 546,702 | 1.750 | 2.000 | 2.833 |
| 1,000 键 Lookup | native | 973 | 1,019.044 | 1,092.752 | 1,151.544 |
| 1,000 键 Lookup | prepared | 964 | 1,027.252 | 1,094.253 | 1,146.377 |
| 1,000 键 Lookup | cached | 971 | 1,020.793 | 1,083.294 | 1,114.586 |
| 1,000 键 Lookup | parse_each | 975 | 1,016.835 | 1,077.377 | 1,114.835 |

### 16 个读取线程，目标 1,000 文档/秒更新

| 场景 | 入口 | 调用/秒 | P50 µs | P95 µs | P99 µs |
|---|---|---:|---:|---:|---:|
| Get | native | 1,526,232 | 2.125 | 49.542 | 98.334 |
| Get | prepared | 1,397,391 | 2.500 | 54.542 | 105.876 |
| Get | cached | 1,320,118 | 2.750 | 59.542 | 116.834 |
| Get | parse_each | 1,264,716 | 2.708 | 61.958 | 127.834 |
| 唯一覆盖查询 | native | 1,055,111 | 3.375 | 74.500 | 144.958 |
| 唯一覆盖查询 | prepared | 979,973 | 3.709 | 76.375 | 143.875 |
| 唯一覆盖查询 | cached | 944,915 | 3.834 | 82.209 | 160.084 |
| 唯一覆盖查询 | parse_each | 927,824 | 4.000 | 89.001 | 193.875 |
| 1,000 键 Lookup | native | 2,257 | 6,610.721 | 11,505.773 | 16,232.824 |
| 1,000 键 Lookup | prepared | 2,259 | 6,602.888 | 11,162.188 | 16,192.656 |
| 1,000 键 Lookup | cached | 2,275 | 6,601.096 | 11,699.606 | 16,481.490 |
| 1,000 键 Lookup | parse_each | 2,244 | 6,657.555 | 11,094.063 | 15,893.198 |


单线程点读只有约 1 µs 基线，几十至几百纳秒的绑定/查缓存成本就会形成可见百分比；这不是零成本语法层。高频嵌入式调用宜保留准备语句。HTTP 自动缓存可避免重复解析，但此次只验证 HTTP 行为，未给出 HTTP 性能数字。

原始数据见 [单线程 JSONL](../bench-results/dsl-1-threads.jsonl) 和 [16 线程 JSONL](../bench-results/dsl-16-threads.jsonl)。并发阶段实测更新速率 989.1–1018.9 文档/秒；同一写入线程贯穿所有模式，采样窗口边界与预热会造成小幅偏差。

进程峰值 RSS：单线程 46.2 MiB、16 线程 98.5 MiB（Linux wait4 ru_maxrss，包含基准程序保存的延迟样本）。并发各阶段结束时引擎计费内存最大 35.9 MiB；这是阶段采样最大值，不是连续采样的瞬时峰值。当前 AST 缓存只保留 3 项，计费 18,560 字节；语句重复执行未造成条目持续增长。

混合进程末次采样：累计 1210 次提交、1212 次 fsync，写锁累计等待 20.98 ms；调度完成 7720 项任务，累计排队 676.44 ms，采样时排队数 0。WAL 21,595,134 字节、刷盘 1,431,352 字节、正文 16,445,092 字节，按当前计数估算写放大 1.40×。本窗口未发生重写式合并（compaction_read_bytes/write_bytes 均为 0），结束时 L0 为 6 文件、L1 为 5 文件，且没有维护错误；因此该写放大不包含尚未完成的合并成本，也不能作为长期合并压力验证。每阶段 levels、维护状态、写锁、I/O 和调度计数保存在 JSONL 的 tenant_state 中。

## 复现

在 Linux 环境内构建与执行，OrbStack 用户先进入对应 Linux 机器；数据放 `/var/tmp`，避免将数据库放在 tmpfs：

```sh
cargo test --locked --features failpoints -- --test-threads=1
cargo clippy --locked --all-targets --features failpoints -- -D warnings
cargo fmt -- --check
cargo build --locked --release --bin tenant-kv --bin tenant-kv-restore --example read_perf --example dsl_bench --example dsl
taskset -c 0-7 target/release/examples/read_perf /var/tmp/tkv-read-current 10000 2
```

旧版需从 v0.2.0 归档取源码，只加入同一 `examples/read_perf.rs`，使用独立 target 目录构建并另用一个新数据库。两版数据库均由相同程序装载，交替各跑三轮。不要用已装载不同行数的目录复用 `read_perf`，它只在首次运行时装载数据。

DSL 基准读取 read_perf 创建的库。数据库关闭后复制两份，分别运行，避免单线程测试留下状态影响并发测试：

```sh
cp -a /var/tmp/tkv-read-current /var/tmp/tkv-dsl-one
cp -a /var/tmp/tkv-read-current /var/tmp/tkv-dsl-mixed
taskset -c 0-7 target/release/examples/dsl_bench /var/tmp/tkv-dsl-one 1 2 0
taskset -c 0-7 target/release/examples/dsl_bench /var/tmp/tkv-dsl-mixed 16 2 1000
```

## 尚未验证

这轮是 10,000 条、一个索引、热点/固定输入的短时基准；没有重新运行百万/千万条、超内存冷盘、三索引混合更新、大量冷租户或长期多租户公平性测试。没有测量读锁的分项等待分布；写锁累计等待和调度统计按当前指标记录。没有真实 S3、设备掉电、故障磁盘或其他 CPU 架构资格验证。历史规模测试保留在 [validation.md](validation.md)，不得冒充 v0.3 的实测结果，也不能据此宣称生产环境性能不下降。

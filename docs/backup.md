# OpenDAL 远端备份

本地 WAL、SST 和索引仍是在线读写的数据源。文档提交只等待本地 WAL 持久化；S3 不进入 Get、MultiGet、Lookup 或提交确认路径。备份由 OpenDAL 0.55.0 访问 S3，也提供 fs 后端用于开发与故障测试。该版本支持项目的 Rust 1.90 工具链，依赖锁定在 Cargo.lock。

v0.5 的 [主从复制](replication.md) 与此备份能力独立。启用复制的节点可以创建/验证备份，但普通 restore/import 会拒绝，需在新的 Store 恢复后重新建立复制关系。共享同一 S3 前缀时只配置一个创建/清理执行者；主从切换不会自动交接备份任务、权限配置或 S3 凭证。

## 一致性与布局

备份是一个租户的完整可恢复检查点，上传采用文件级增量：不变 SST 在同一租户、同一持久 generation 内复用 SHA-256 对象。每份 manifest 都列出完整文件集合，恢复不需要依次回放前几份备份。合并产生的新 SST 需要重新上传；这不是文档级增量。

```text
本地 root/
  control/backup-jobs/LOCK, TARGET, <job>.json  # 管理状态，不含业务文档
  backup-work/<job>/checkpoint/               # 每个任务独立业务暂存
  backup-work/<job>/manifest.local

远端 Operator root/
  tenants/<tenant-uuid>/
    objects/<database-generation>/<sha256>
    snapshots/<backup-uuid>/manifest.json
    snapshots/<backup-uuid>/COMPLETED
```

对象不会跨租户复用，所有路径由内部 UUID、固定文件名和摘要产生。HTTP 调用方不能提供 bucket、endpoint、对象路径或凭证。普通文档的 `tenant` 字段不会改变备份位置。共享 S3 bucket 的前缀提供对象命名隔离，基础设施权限由部署方的 bucket/IAM 策略管理。

创建流程：

1. 刷盘，持锁捕获持久 Manifest 和 SST 文件描述符，固定 `checkpoint_seq`。
2. 在锁外复制检查点；即使合并移除了源文件名，已打开的描述符仍可完成复制。文件复制使用共享 I/O 队列，不建立硬链接。
3. 打开暂存引擎校验文件归属、提交序号、SST 内容，并计算各文件 SHA-256。
4. 持久化本地任务 manifest，上传不存在的对象，全部完成后条件创建远端 manifest。
5. 最后条件创建 `COMPLETED`，其中保存 manifest 的 SHA-256，再将本地任务标记为 completed。

检查点复制完成后即释放在线租户句柄，后续校验、哈希和上传只依赖私有副本及捕获的 TenantInfo；本地同步备份也在校验副本前释放在线句柄。因此，较慢的副本校验或限速哈希不再单独阻止 close 或增加配额。复制阶段、其他请求/快照和调用方保留的句柄仍可能导致一秒排空超时；后台任务的共享内存计费继续生效，缩配额仍须满足在用预算。本地备份仍占用本租户维护名额，校验请求等其他维护操作不能同时运行。

释放在线引用也允许哈希期间出现 COMMIT_UNKNOWN 的引擎自动关闭和重开；故障解除后的重试仍必须使用原操作 ID 和 history 核对未知结果。副本处理不改变备份固定时刻，之后才确认的写入不属于这份备份的恢复点。

只有带有效 COMPLETED 的备份可见。`POST` 返回 202 仅表示已持久记录任务，不能据此宣称备份成功。若远端成功而本地状态未写完，重启时核对完成标记，不重复创建备份。已持久化检查点的上传中断会重用原检查点及已存在对象；单个未完成 multipart 文件从头重传，不保留分片续传令牌。检查点尚未完成时，重启会重新获取检查点，因此 202 时还没有固定备份序号。

上传按块计算 SHA-256，与 manifest 比较。S3 默认请求 CRC32C 传输校验，且对象带摘要 metadata；已有 S3 对象用长度与摘要 metadata 判断复用。没有 metadata 的后端读取完整对象验证。显式 verify 和所有恢复都会完整读取并计算 SHA-256，不把 ETag 当作文件摘要。摘要用于损坏检测，不是对有写权限的恶意备份仓库的签名认证。

## 启用

```sh
cargo build --locked --release --bin tenant-kv --bin tenant-kv-restore --bin tenant-kv-repair
target/release/tenant-kv serve --root /var/lib/tenant-kv \
  --auth auth.json --backup-config examples/backup-s3.json
```

先修改 [S3 配置](../examples/backup-s3.json) 的 bucket、region 和 root。bucket 应预先创建，服务不会创建 bucket。使用 AWS 环境凭证或部署环境中的身份凭证链，避免把访问密钥写入配置或请求正文。兼容 S3 服务可在 `options` 中设置 `endpoint`；必须实际支持条件创建、multipart、列举、metadata 和所选校验算法，不能只依据“S3 兼容”字样认定可用。若后端不接受 CRC32C，可显式将 `checksum_algorithm` 改为该版本支持的 `md5`，并执行完整 verify 验证。

运行身份需要目标前缀的读、写、列举及 multipart 操作权限；prune 需要删除权限。需要 KMS 时还必须配置对应密钥权限。错误输出不回显底层签名 URL 和凭证配置。加密、跨区域复制、Object Lock、版本保留由 bucket 策略管理，本实现不自动修改这些策略。

[fs 配置](../examples/backup-fs.json) 的 root 必须与数据库根目录互不包含；fs 适合开发或独立挂载的备份目标。同一磁盘上的另一个目录不能抵御整盘丢失。

一个 Store 只能启动一个 BackupService。持久 TARGET 指纹防止重启时将旧任务发送到另一 bucket/root/endpoint；凭证轮换不改变指纹。切换目标需要停止服务、处理未完成任务并归档 `control/backup-jobs` 与 `backup-work`，不能直接改目标继续旧任务。

## HTTP 工作流

所有远端备份和任务接口（包括 GET）要求该租户 admin 或全局 admin。沿用现有 Bearer 授权，库 API 属于可信管理员入口。

以下 `T=/v1/tenants/<tenant-uuid>`：

| 方法与路径 | 契约 |
|---|---|
| `POST T/remote-backups` | 空正文或 `{}`，返回 202 BackupJob |
| `GET T/remote-backups` | 完整已发布备份列表，按检查点时间降序 |
| `POST T/remote-backups/<backup>/verify` | 202；逐文件读取验证 SHA-256 |
| `POST T/remote-backups/<backup>/restore` | 202；下载、校验后替换当前租户 |
| `POST T/remote-backups/prune` | `{"keep_last":7}`，202；保留最近 7 份并回收无引用对象 |
| `GET T/backup-jobs` | 本地该租户任务列表 |
| `GET T/backup-jobs/<job>` | 状态、进度、结果、错误 |
| `POST T/backup-jobs/<job>/retry` | 重试 failed/interrupted 的 create、verify、prune |
| `DELETE T/backup-jobs/<job>` | 忘记终态任务并删除其本地暂存；不删除远端备份 |

```sh
curl -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{}' "$BASE/v1/tenants/$TENANT/remote-backups"
# 使用返回的 id 轮询。backup 是备份 UUID，id 是任务 UUID，二者不同。
curl -H "Authorization: Bearer $TOKEN" \
  "$BASE/v1/tenants/$TENANT/backup-jobs/$JOB"
curl -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{}' "$BASE/v1/tenants/$TENANT/remote-backups/$BACKUP/verify"
```

状态为 queued、checkpointing、uploading、verifying、restoring、pruning、completed、failed、interrupted。进度的 uploaded_bytes、reused_bytes、processed_files 按当前尝试计数，重启恢复/显式重试时清零。completed 的 create 结果包含 checkpoint_seq、checkpoint_ms、文件数和总字节。checkpoint_ms 是以当前时间为基准、按租户持久递增的检查点排序水位。时钟回拨时取上一次水位加一；初始化水位时读取已有远端清单，因此 restart、forget 和 prune 不会使排序回退。它不是精确墙钟时间，也不是按时间回放 WAL 的索引。旧版本已经错序的历史备份不能自动推断真实先后，应人工确认保留范围。

排队或结束状态写盘失败时，该任务在当前进程可显示 interrupted，并保留该租户的任务身份；错误不证明控制文件未落盘。先查看任务列表和远端完成状态，再以原任务 ID 重试 create/verify/prune，或显式 forget 后新建；restore/import 继续禁止 retry，必须检查实际数据后处理。此期间同租户新建备份操作返回 BUSY，其他租户不受这份任务所有权限制。重启依据实际持久状态恢复，不应反复新建任务来处理未知发布结果。

任务元信息写入、目录同步及 forget 清理在全局状态锁外执行，同租户提交、重试、工作者和 forget 按租户串行。状态查询可在慢速写盘期间返回上一次已发布状态；Completed 在清理结束并释放租户所有权时一起对外可见。已删除任务文件但目录同步失败时，再次 forget 可继续完成；远端备份仍保留。

任务阶段、检查点清单和终态结果持久化后才发布；逐文件进度只更新内存，在下一阶段或终态时一同保存，避免每个文件的计数更新都同步磁盘。通过任务 HTTP/API 查看实时进度，不依赖内部任务 JSON 文件的计数实时变化。续传根据检查点清单、已上传对象和完成标记恢复，重启后仍按原规则重置本次进度。实现与验证见[后台备份进度与写入尾延迟优化](tail-latency-optimization.md)。

同租户一次仅执行一个远端操作。活动任务、目录锁或未释放的句柄可导致 BUSY；备份未配置为 UNAVAILABLE。后台故障反映在任务的 error 中，不能仅看轮询 HTTP 200。远端错误使用 BACKUP_STORAGE_ERROR，资源限制为 RESOURCE_EXHAUSTED，内容校验失败为 CORRUPTION。

## 恢复与新机器导入

从完整备份恢复到新 Store 的实际流程、检查点后成功写入的损失核对及恢复计时，见[故障恢复演练](recovery-drill.md)。该演练的 fs 仓库保存在同一虚拟磁盘，只验证两个在线进程不可用后的恢复路径，不覆盖整盘丢失或真实云 S3。

现有租户恢复只接受同 UUID、同持久 generation；不依赖打开当前引擎，因此可以恢复 CURRENT/Manifest 损坏的租户。先下载至私有目录，验证长度、SHA-256、文件白名单、租户归属、SST 内容和 checkpoint_seq。再复用 Store 恢复 journal，校验后发布目录切换。发布重新检查当前注册状态、配额和独占锁；下载期间租户被删除会使恢复失败。发布前的校验失败不替换在线目录。

恢复会回退到指定检查点，包括舍弃之后的写入。调用方必须停该租户新流量并释放句柄和快照；否则最终切换可能返回 BUSY。失败后检查任务原因，再提交新恢复任务。恢复/导入结束后清理下载工作区，失败或终态发布异常也会尝试清理；启动时回收旧的终态恢复/导入暂存，已发布的生命周期 journal 目录不受此清理影响。清理 I/O 失败会记录日志，仍可使用 forget 或重启重试。已开始的 restore/import 在重启后标记 interrupted，不自动重放，也不允许通过 retry 重放；目录 journal 会完成已经发布的切换。检查实际数据库后再决定后续操作。启动扫描只在内存中归一化任务状态，不强制重写所有未完成任务文件。任务在执行副作用前持久化执行阶段；满盘导致阶段或终态无法持久化时，该任务在当前进程显示 interrupted，不阻断数据服务启动。释放空间后可显式 retry 非恢复任务；若未持久化终态，进程再次重启会重新评估原任务。已进入恢复/导入阶段的任务每次重启均保持 interrupted，不能自动重放。此行为不豁免损坏任务文件、首次创建控制目录、格式迁移或生命周期发布所需的写入空间。

新机器丢失整个本地根目录时，用离线导入工具，目标 Store 中不得出现过该租户 UUID；已删除 UUID 也不复用：

```sh
target/release/tenant-kv-restore /var/lib/tenant-kv-new \
  examples/backup-s3.json "$TENANT" "$BACKUP"
```

工具持有 Store 独占锁，保留原 UUID、持久 generation、集合、索引和 Lookup 定义，使用目标 Options 默认内存/磁盘配额。同名或同 UUID 注册冲突会拒绝。导入校验完成后持久记录生命周期 journal，重启可完成中断的目录发布。可在 Rust API 中使用 `BackupOperation::Import` 配合自定义 Options；为避免 URL 身份与未注册租户混淆，HTTP 不提供新租户导入接口。凭证和服务端 auth 配置不属于业务备份，需另行管理。导入后更换运行代次，旧缓存/游标不复用。

切换机器前必须停止源机器对同一租户备份命名空间的操作。本版本没有跨机器仓库锁，不支持两个控制器同时备份、恢复或 GC 同一个 tenant 前缀；本地锁不能替代分布式锁。不同租户使用各自独立对象。

## 保留与资源

本地同步 `/backup` 的总额、64 份数量上限、物理空闲空间保护及删除接口见[运维文档](operations.md#备份恢复与迁移)；它们与下述远端对象和任务暂存预算分别计费。

prune 流式验证全部完成清单，保留最新 keep_last 份摘要，再撤销其余完成标记及元数据，不受列表 4096 份上限限制。保留对象引用先压缩为至多 8 MiB 的 SHA-256 摘要集，通常只需读一次保留清单；超过约 262144 条原始引用后回退到每 4096 个候选重读清单，维持内存边界。坏清单使 prune 停止，不猜测其未知引用；管理员可显式删除该备份元数据后重试。

空目录回收不删除未完成但非空的快照。选择保留集合时任何完成清单损坏都会中止；对象删除前根据已验证的保留引用摘要过滤；超出摘要缓存上限时，每批重读保留清单，不能把损坏当成空引用。多个备份共享的 SST 只在最后一个保留引用消失后删除。没有完成标记的中断上传对象也可在宽限期后回收；重试会从保留的本地检查点重新上传缺失对象。

默认保留数量由调用方 `keep_last` 明确指定，没有自动定时任务。可由外部调度器定期提交备份与 prune，并检查任务结果。不要对所有 objects 配置简单按对象年龄删除的 Lifecycle，否则旧 SST 即使仍被新备份引用也会被删除。可单独配置未完成 multipart 上传回收；启用 S3 Versioning 后，prune 不保证物理删除非当前版本，需要另配版本保留策略。Object Lock 可能阻止清理，任务会明确失败。已删除租户的远端备份仍保留；永久清除整个租户前缀是独立的管理员操作。

| 配置 | 默认值与含义 |
|---|---|
| concurrency | 2；全局最多 8 个备份工作线程，每租户串行 |
| max_jobs | 256；含终态持久任务，最多 4096。每租户最多 min(64, ceil(max_jobs / 2)) 份，达到任一上限需 forget；降低 max_jobs 只限制新任务，原有至多 4096 份仍可启动、查询和忘记 |
| max_backup_bytes | 100 GiB；单份备份上限 |
| max_staging_bytes | 200 GiB + 1 MiB；全局暂存预算，独立于在线租户数据配额 |
| reserve_free_bytes | 1 GiB；检查点/恢复开始前保留空间检查 |
| bytes_per_second | 50 MiB/s；全局流式哈希/传输速率，每块计费 |
| timeout_seconds | 30 秒；OpenDAL 单操作超时，瞬态错误最多重试 3 次 |
| job_timeout_seconds | 24 小时；在流式处理和 GC 边界协作检查 |
| gc_grace_seconds | 3600 秒；无引用远端对象清理宽限期 |

暂存按活跃预留与实际占用的较大者计费，失败工作区的未覆盖部分继续计入，forget 可释放。创建仍按 max_backup_bytes 预留，每租户上限为 max(max_staging_bytes / 2, max_backup_bytes)。Restore/Import 预留 2×实际备份字节+1 MiB，其租户上限改为 max(max_staging_bytes / 2, 2×max_backup_bytes+1 MiB)，全局预算仍严格生效。默认全局预算为 200 GiB+1 MiB，可为最大 100 GiB 备份预留恢复空间；已有暂存或物理空闲不足仍会拒绝。

每个活动工作任务在全局与租户预算中预留 48 MiB，额外的引擎校验缓存/目录仍按引擎预算收费。OpenDAL/TLS/运行时及分配器开销并非精确 RSS 上限。流式读取使用 1 MiB 块、8 MiB 写缓冲和单文件有界 multipart 并发；网络 I/O 不持有在线写入锁。检查点本地复制走共享 I/O 调度，尚未实现独立的低优先级磁盘带宽保证，因此备份期间性能影响必须测量，不能承诺零下降。

本地检查点复制每个 256 KiB 块检查在线引擎是否需要 WAL 恢复；检测到故障时以 CANCELLED 中止复制并释放在线句柄，让业务重开恢复。取消不能中断已进入的系统 I/O。该错误表示复制被取消；准备阶段若 flush 自身的持久化结果不确定，仍可能返回 COMMIT_UNKNOWN。复制后的最后一次取消检查通过后，使用已捕获的持久 Manifest 发布检查点；发布和目录同步已完成时，源端随后发生的 WAL 故障不会追溯取消这份备份。故障解除后可按任务状态重试远端 create，或重新发起本地备份。

创建失败的检查点保留以支持同一备份 ID 重试，但同时占用本租户与全局暂存预算，forget 可释放；不可重试的恢复/导入暂存不再永久保留。每租户最多一个活动任务，终态任务也计入每租户任务数量上限，避免一个租户耗尽全部历史任务槽位。升级前已经积累到全局上限的创建任务记录和暂存不会被自动删除，管理员应使用 forget 释放。

单清单最多 20000 文件/4 MiB 元信息，列表接口最多扫描 200000 项、返回 4096 份完整快照；prune 单独流式扫描，选择摘要最多 keep_last 份，对象候选最多 4096 个，不再受总引用和对象数的硬性拒绝边界限制。每次 prune 最多回收 4096 个旧空目录，其余可在后续运行处理。磁盘余量检查及暂存准入不能代替文件系统容量隔离，应同时监控磁盘空间。

RPO 取决于最近完成备份的 checkpoint_seq 与之后的提交量，不承诺秒级或零数据丢失。S3 不可用时，本地业务仍可读写，备份任务失败/积压。RTO 包含下载、完整校验、本地复制、切换和预热时间。本版本不实现持续 WAL 归档、PITR、复制、高可用切换、远端懒加载或云端备份调度器。

完整可运行示例见 [remote_backup.rs](../examples/remote_backup.rs)，执行会创建独立示例租户并保留最后一份远端备份：

```sh
cargo run --locked --release --example remote_backup -- \
  /var/tmp/tenant-kv-backup-demo examples/backup-s3.json 2000
```

服务启用备份后，可运行包含远端任务的完整 HTTP 示例：

```sh
python3 scripts/http_smoke.py http://127.0.0.1:8080 token.txt --remote-backups
```

脚本创建并删除自己的示例租户，保留一份远端备份，并忘记本地终态任务；请使用测试仓库。

v0.6 的 `identity_temporal` 数据与普通文档一样纳入检查点、远端对象校验和恢复，不需要单独备份知识头。恢复只保证恢复点所在历史分支；此后生成的知识和幂等收据可能不存在。查询指定缺失的知识版本返回 `KNOWLEDGE_UNAVAILABLE`；重新执行已丢失收据的操作属于新提交。详细契约见 [identity.md](identity.md)。

## 租户保留与备份保留

v0.8 租户 retention_seconds 及回收任务、凭据、截止时间均进入检查点备份。在线历史到期不会修改已完成备份；备份保留清理仍独立配置。恢复不重置原截止时间，数据历史标识变化会拒绝缺失回执的旧写重试。详见 [统一保留](retention.md)。

本地 backup、delete_backup、本地 restore 和租户删除按租户串行；不同租户不共用备份锁。检查点与完整校验期间只持本租户备份锁，同时共享有界 I/O、物理磁盘余量和 HTTP 维护总额。

删除租户在持久化删除状态后清理 `backups/<tenant UUID>/` 本地副本，启动时也清理已登记删除租户的历史残留；健康租户及无法确认归属的目录保留。远端备份仍遵循上文的独立保留/永久清除契约。

上传在读取、校验、写入或 close 失败时显式调用 OpenDAL abort；失败的 abort 记入日志。fs 原子上传临时文件按 Store 根目录隔离，持有本 Store 备份管理锁后在启动时清理残留，不清理其他 Store 的活动上传。SIGKILL、掉电或持续不可达的 S3 无法保证客户端执行 abort，bucket 仍应配置 `AbortIncompleteMultipartUpload` 生命周期规则；prune 只管理已完成对象，不能替代 multipart 清理。旧版未分命名空间的 fs 临时文件需要停写后人工处理。

暂存目录计费允许并发删除使条目消失（NotFound 计零），其他 I/O 错误和符号链接仍拒绝。活动预留继续计费，不因扫描瞬间没有看到文件而放宽预算。

备份时钟在每次服务启动后首次为租户创建时与远端完成清单核对，即使本地 CLOCK 已存在，也能修复旧盘镜像带回的较低水位。缺失或损坏本地时钟从远端重建；损坏或缺失的已完成清单记错误并跳过时钟计算，降低 max_backup_bytes 不限制旧清单的列表、时钟和 prune 元数据读取，实际恢复/verify 仍检查当前配置。清单读取、verify、restore/import 和列表不以本机墙钟判断备份完整性。只有新检查点的排序水位忽略超过本机墙钟 366 天的异常值，并记录诊断；大幅 RTC/NTP 跳变后重新核对远端。prune 若见到这样的时间先返回 BACKUP_CLOCK_UNSAFE，恢复仍可用；应校正时钟或人工核对后再清理，不能用 Delete 作为恢复的前置条件。仓库仍只支持单 Store 写入者，不提供远端多写者协调。

`POST /v1/tenants/{tenant}/remote-backups/{backup}/delete`（空正文，租户 admin）提交显式删除任务，Rust 对应 `BackupOperation::Delete { backup }`。它先移除 COMPLETED，再移除 manifest，无需解析坏清单，重复执行幂等；对象由后续 prune 根据剩余清单引用回收。不要删除尚未核对价值的损坏备份。

本地备份 rename 后目录同步失败返回 BACKUP_COMMIT_UNKNOWN，并在诊断中给出备份 UUID。该备份可能已存在，应先检查或使用已有 delete_backup 删除该 ID，不把报错当成“没有备份”。max_jobs=1 只有一个任务名额，无法提供多租户容量隔离；生产多租户应配置至少 2 并定期 forget 终态任务。

新增 Delete 任务使用新的持久 operation 变体。旧二进制不保证读取新版任务记录；如需降级，应先完成并 forget 新类型任务，保持仓库单写入者约束。


配置要求 `max_staging_bytes >= 2×max_backup_bytes+1 MiB`，否则启动返回明确的配置错误；旧的 100/200 GiB 配置应补足 1 MiB。默认容量能承载一份最大恢复，此时不能承诺另一租户同时创建；若需一份最大恢复加一份最大创建，应至少配置 `3×max_backup_bytes+1 MiB`，并提供相应物理空间。单租户串行和全局容量限制继续生效。

每进程每租户首次创建时全量核对远端清单是保留的恢复成本，用来发现旧磁盘镜像带回的 CLOCK；后续创建复用本地水位。prune 的两遍扫描要求仓库遵守单 Store 写入者契约，外部程序不得并行写入清单/对象。显式 Delete 对不存在 ID 返回成功属于幂等删除契约。

异常时间导致 prune 返回 BACKUP_CLOCK_UNSAFE 时，服务日志记录 tenant/job UUID，并增加 `/metrics` 的 `tkv_backup_prune_clock_unsafe_total`。建议按 [遥测说明](telemetry.md) 对增长告警，避免保留策略长期停止而无人察觉；该计数不改变备份的可恢复性。

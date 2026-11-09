# DSL v1 使用与执行契约

v0.10 提供[请求级读取控制](read-controls.md)：读取截止时间、扫描预算、禁止全扫描和取消。现有请求正文与 DSL 语法保持不变。

当前提供九个命令：`from`、`explain`、`get`、`mget`、`lookup`、`resolve`、`resolvemany`、`put`、`delete`。查询和写入直接复用数据库原生接口，JSON API 保持可用。集合、索引、Lookup 定义和备份仍由管理 API 创建；DSL 没有管理语句、SQL 兼容层、JOIN、聚合、UDF 或脚本执行。

## 一个完整例子

v0.9.1 的主键范围、二级索引 IN 与批量 Lookup 优化直接作用于既有 DSL 命令；语法和返回契约保持不变。定位规则、展开上限和分页边界见 [API 查询契约](api.md)。

先运行已有的建库示例，或通过管理 API 创建 `assets` 集合、`ip` 唯一覆盖索引及 `asset_owner` Lookup 定义。可直接运行下面两个随附例子：

```sh
cargo run --locked --example dsl -- /var/tmp/tenant-kv-dsl-demo
python3 scripts/dsl_smoke.py http://127.0.0.1:8080 token.txt
```

Rust 示例会创建演示租户、集合、索引和 Lookup，并输出查询与匹配结果；重复运行会替换相同演示文档。HTTP 脚本自行创建和删除测试租户。

```text
put assets {"_key":"gateway-1","ip":"10.0.0.1","env":"prod","score":95,"owner":"platform","service":"edge"} if_rev 0

get assets "gateway-1"

mget assets ["gateway-1", "missing", "gateway-1"]

from assets
| where env == "prod" and score >= 80
| sort score desc
| fields _key, owner, service
| limit 100

lookup assets using asset_owner with [{"ip":"10.0.0.1"}, {"ip":"10.0.0.2"}]

explain from assets | where ip == "10.0.0.1" | fields owner, service
```

上面每段是一条独立请求；不能把整个代码块作为一次请求提交。`put` 为整文档替换，输入不得带 `_rev`；`if_rev 0` 表示仅新增，省略时无条件写入。更新和删除可使用读取结果中的版本，但正数 if_rev 必须在 HTTP/DSL Request 的 `write_options.expected_history` 中同时传入原读取历史。Rust 使用 `Prepared::execute_with` 或 `Request.write_options`：

```text
put assets $document if_rev $revision
delete assets $key if_rev $revision
```

写入成功仍须等待原生 WAL 持久化；DSL 不改变原子性、唯一性和提交结果未知的处理方式。

v0.5 的从库可执行读取和 Lookup，`put` / `delete` 返回 READ_ONLY。v0.7 使用响应 history/rev 作为 `X-TKV-Min-History`、`X-TKV-Min-Seq` 只读请求头；升主或重新跟随改变历史后，旧游标失效，CAS 版本应重新读取。详见 [主从复制](replication.md)。

## HTTP 与参数绑定

`POST /v1/tenants/{tenant_uuid}/dsl`，使用既有 Bearer token。请求接受 `statement`、`params`、`cursor`，写语句还可携带 `write_options`：

```json
{
  "statement": "from assets | where env == $env and score >= $min | sort score desc | fields _key, owner, service | limit $count",
  "params": {"env": "prod", "min": 80, "count": 100}
}
```

返回与相应原生接口相同的 JSON 结构：查询包含 `documents`、`cursor`、`snapshot_seq`、`plan`、`stats`；Lookup 包含 `matches` 和统计；写入返回 `WriteResult`。DSL `get` 未命中返回 JSON `null`，`mget` 未命中位置为 `null`。原有 HTTP 文档 GET 的未命中行为仍为 404。

参数只替换一个完整 JSON 值，不拼接源码。参数值中的引号、管道、分号和关键字都只是数据。参数集合必须与语句引用的名字完全一致，缺少或多余参数均拒绝；同一参数可以多次引用。集合名、字段名、运算符和 Lookup 名不能用参数替换。JSON 内部也不插值：`{"owner": "$name"}` 保存字面字符串 `$name`，需要动态文档时使用 `put assets $document`。

租户由已授权 URL 上下文确定，DSL 没有 `use tenant`。读取命令需要 `read` 权限；`put/delete` 需要 `write`。HTTP 在打开引擎前检查命令权限，解析完成后再次按命令类型校验；准备语句缓存不缓存授权结果。

## 时态身份匹配

```json
{
  "statement": "resolvemany with $request",
  "params": {
    "request": {
      "known_at": "latest",
      "items": [
        {"identifier":{"kind":"account","namespace":"crm/prod","value":"10086"},"valid_at":"2026-11-05T10:30:00Z"},
        {"identifier":{"kind":"account","namespace":"crm/prod","value":"10086"},"valid_at":"2026-11-05T11:00:00Z"}
      ]
    }
  }
}
```

`resolve with $request` 接收单条 `/identity/resolve` 的正文；`resolvemany with $request` 接收 `/identity/resolve_many` 的正文。都要求 read 权限，不指定内部集合名，返回与原生 API 相同的 JSON。批量共享一个快照和知识版本、保持输入顺序及重复项，未匹配的 binding 为 null；错误时整批失败。完整语义见[身份 API](identity.md)。

两个命令均不接受 cursor、write_options 写入条件或后续管道。参数只作为值绑定；批量列表建议放 params，避免 64 KiB 语句上限。准备语句不缓存知识头、参数或快照，同一 Prepared 再次执行 latest 会读取新的头。固定解释时由请求明确传入知识 UUID。`Prepared::execute_read` 与原生解析共用 TenantReader 的截止时间、取消及扫描预算。

## Rust 准备语句

已经取得授权的 `TenantHandle` 后：

```rust
use serde_json::json;
use tenant_kv::dsl::Output;

let prepared = tenant.prepare_dsl(
    "from assets | where ip == $ip | fields owner, service"
)?;
let bindings = json!({"ip": "10.0.0.1"});
let Output::Query(result) = prepared.execute(
    &tenant, bindings.as_object().unwrap(), None
)? else { unreachable!() };
println!("{:?}", result.documents);
```

保留 `Arc<Prepared>` 并重复 `execute` 可省去文本解析及缓存查找。`TenantHandle::execute_dsl(&Request)` 自动使用租户缓存。`Prepared::parse` 适合由嵌入式调用方自行管理的准备语句，它的 AST 分配由调用方承担；需要计入引擎预算时使用 `prepare_dsl`。

准备语句只保留语法和参数位置，不保留参数值、租户身份、索引 generation、结果或快照。可信 Rust 调用方可以把语法交给另一个已授权句柄执行；实际数据访问始终以传入句柄为准，不存在从语句切换租户的能力。

## 语法

```text
query   := "from" collection ["| where" expression]
           ["| sort" field ["asc" | "desc"] {"," field ["asc" | "desc"]}]
           ["| fields" field {"," field}]
           ["| limit" value]
explain := "explain" query
get     := "get" collection value
mget    := "mget" collection value
lookup  := "lookup" collection "using" lookup_name "with" value
resolve := "resolve" "with" value
resolvemany := "resolvemany" "with" value
put     := "put" collection value ["if_rev" value]
delete  := "delete" collection value ["if_rev" value]
value   := JSON_literal | "$" parameter_name
```

管道阶段必须按 `where → sort → fields → limit` 顺序出现，每项最多一次、均可省略。默认 `limit 100`，必须为正整数且不超过配置的分页上限。默认按 `_key ASC` 排序；指定的排序未包含 `_key` 时，在末尾追加 `_key ASC` 消歧。省略 `fields` 返回完整文档。

关键字小写且区分大小写。裸标识符匹配 `[A-Za-z_][A-Za-z0-9_-]*`。集合和 Lookup 名也可使用 JSON 字符串，如 `from "资产"`。值必须是 JSON，字符串使用双引号。JSON 外的 `#` 开始行注释。分号、多条语句、字符串插值和赋值不属于语法。

字段 `owner.team` 等价于 `/owner/team`。特殊字符字段及数组下标使用 `field("/pointer")`：

```text
from assets | where field("/tags/0") == "prod" | fields field("/a~1b"), field("/设备")
```

`/a~1b` 指字段 `a/b`；JSON Pointer 的 `~0` 表示 `~`。字段名与 `not/exists` 等表达式关键字冲突时也使用 `field()`。根路径不支持。

| 表达式 | 含义 |
|---|---|
| `score == 80` / `score != 80` | 严格类型相等 / 不等 |
| `score > 80`、`>=`、`<`、`<=` | 相同标量类型内范围比较 |
| `env in ["prod", "staging"]` | 严格类型集合成员；右侧必须为数组 |
| `exists(owner)` | 字段存在，显式 null 也存在 |
| `not exists(owner)` | 字段缺失 |
| `owner == null` | 仅显式 null，不匹配缺失 |
| `a == 1 and (b == 2 or not exists(c))` | 布尔组合及括号 |

优先级为比较/存在性、`not`、`and`、`or`。`!=` 是相等的否定，因此也可匹配缺失字段。数值沿用原生严格语义：`1` 与 `1.0` 不相等，`"1"` 不转数字；支持精确 i64/u64 和有限 f64。对象与数组可写入、读取和投影，比较操作数必须为标量，`in` 的元素也必须为标量；对象/数组不能作为普通比较值。

## 分页、Lookup 与诊断

下一页提交相同语句、相同参数并添加返回的 `cursor`。指纹根据绑定后的查询计算，因此只改空白不会改变查询；改变过滤值、字段、排序或 limit 会拒绝游标。`from`、`ids` 和 `history` 接受 cursor。游标绑定租户、集合、运行代次和快照，默认从第一页起 60 秒，到期/重启/恢复后失效；延续成功会消费旧 token。

v0.4 保证同一个 token 在并发续页时最多成功消费一次，其余请求返回 CURSOR_EXPIRED；超预算等执行失败不提前消费 token。

`lookup ... with` 接受对象数组，每个对象的结构与文档匹配字段一致。输入缺字段视为未匹配；结果与输入同序，重复输入保留重复结果；一次批次使用同一快照。最大匹配数及取首条行为由已有 Lookup 定义决定。DSL 不隐式把结果合并进事件。

`explain from ...` 只规划，不运行查询；实际读块、正文回读、解压字节与扫描条目数在查询和 Lookup 的 `stats` 中。语法错误返回 `INVALID_ARGUMENT` 及位置：

```json
{"error":{"code":"INVALID_ARGUMENT","message":"...","location":{"byte_offset":25,"line":1,"column":26}}}
```

`byte_offset` 从 0 开始，行和字符列从 1 开始。绑定、配额和存储错误未必包含源码位置，错误码与原生 API 一致。

## 预算与性能

- 每条语句最多 64 KiB，128 个不同参数，128 个过滤节点；过滤递归深度 16、JSON 参数/字面值深度 64。写入文档还受原生文档深度 32 等限制。
- 最多 8 个显式排序字段、64 个投影字段，单个 `in` 最多 1,000 个标量；批量输入、结果大小、扫描条数和时间继续受原生 `Options` 限制，默认批量 1,000 项、结果 16 MiB、扫描 100,000 项、5 秒。
- 参数编码总量不超过 `max_batch_bytes`。同一大参数多次引用产生的展开内存先计算、先预留，超过 `max_query_bytes` 或租户/全局配额即拒绝，不先克隆再检查。
- 租户 AST 缓存使用 8 个分片、读锁命中、FIFO 淘汰，合计最多 256 项/4 MiB 计费容量；单条过大的语句执行但不进入缓存。外部继续持有的准备语句淘汰后仍计费到最后引用释放。
- `get/mget/lookup` 直接使用专用读路径；查询复用规划器。SST 点读避免批量查找脚手架，MultiGet 复用已排序唯一输入，覆盖索引避免回读正文。
- 过滤字段路径和常量每次查询编译一次；`in` 使用编码值 HashSet；逐行直接从二进制文档取标量，避免字段名和 JSON 中间值分配。
- 集合及索引元信息使用提交序号校验的缓存；DDL 失效，旧分页快照读取其自己的目录版本。普通文档写入不反复重建元信息。

缓存上限和计费口径见 [运维文档](operations.md)，实际对比与适用范围见 [v0.3 验证报告](dsl-validation.md)。语法缓存不承诺免除物理规划，也没有结果缓存。

v0.6 的 `identity_temporal` 是受管集合，DSL 可以按租户权限读取其审计文档，但 `put`、`delete` 会返回 `MANAGED_COLLECTION`。时态修正使用 [身份 API](identity.md)，以保证有效区间、知识历史与幂等收据同批更新；DSL 尚未增加时态语法。

启用租户统一保留后，DSL 写命令的 `write_options` 还需携带首次提交前取得的 `retry_token`；operation_id、history、token 与原语句和参数一同保存，重试不得刷新。语法不变，过期行为见 [retention.md](retention.md)。

### 主体查询：ids / history

```json
{"statement":"ids with $request","params":{"request":{"entity_id":"<UUID>","valid_at":"2026-11-09T10:00:00Z","known_at":"latest","limit":100}}}
```

```json
{"statement":"history with $request","params":{"request":{"entity_id":"<UUID>","recorded_from":"2026-11-01T00:00:00Z","limit":100}}}
```

两者都是只读命令，不带集合名，返回与原生 `/identity/ids`、`/identity/history` 相同的结果。下一页重复 statement、params，在 DSL 请求顶层传 `cursor`；参数内 cursor 被拒绝。prepared 只缓存语法，知识头在首次执行时固定。游标隔离、保留期、空页和预算语义见[主体查询](identity.md#主体反查和变更历史)。

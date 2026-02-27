# dbtool 演示说明

## 1. 编译与运行方式

在项目根目录（gin1）下：

```powershell
cd d:\go\go_src\gin\gin1
go build -buildvcs=false ./dbtool
.\dbtool.exe -config .\dbtool\config.example.json
```

或直接运行（不生成 exe）：

```powershell
go run -buildvcs=false ./dbtool -config .\dbtool\config.example.json
```

---

## 2. 演示一：旧版配置（单文件 source + target + tables）

沿用原来的 `config.example.json`，格式不变：

- `source` / `target`：直接写驱动和 DSN
- `tables`：自定义表清单，每项可配置 `source_table`、`target_table`、`columns`、`batch_size`、`auto_create` 等

**命令：**

```powershell
go run -buildvcs=false ./dbtool -config .\dbtool\config.example.json
```

如需先看会同步哪些表、不真正写库，可加 `-dry-run`：

```powershell
go run -buildvcs=false ./dbtool -config .\dbtool\config.example.json -dry-run
```

---

## 3. 演示二：新版配置（数据源 + 表清单分离）

### 3.1 数据源与自定义表清单

使用 `config.tables.example.json`：

- `sources`：命名数据源（如 `src`、`dst`），每个包含 `driver`、`dsn`
- `sync`：指定本次同步用哪个源、哪个目标（`source` / `target` 填 `sources` 的 key）
- `table_list.from_source`：`false` 表示使用自定义表清单
- `table_list.list`：表配置数组，和旧版 `tables` 含义一致

**命令：**

```powershell
go run -buildvcs=false ./dbtool -config .\dbtool\config.tables.example.json
```

先把示例里的 `sources.src.dsn`、`sources.dst.dsn` 改成你自己的 MySQL / Postgres 连接串再运行。

### 3.2 从源库全量拉取表清单

使用 `config.sources.example.json`：

- 同样用 `sources` + `sync` 指定源和目标
- `table_list.from_source`：`true` 表示表清单从**源库**查出来，而不是手写
- `table_list.schema`：可选，指定 schema（MySQL 库名、Postgres/MSSQL/Oracle 的 schema）
- `table_list.include` / `table_list.exclude`：可选，正则过滤表名
- `table_list.defaults`：可选，给“从源拉出来的每张表”统一用的默认配置（如 `batch_size`、`auto_create`）

**仅列出源库表名（不执行同步）：**

```powershell
go run -buildvcs=false ./dbtool -config .\dbtool\config.sources.example.json -list-tables
```

会连接配置里的**源库**，查出表名并打印；若配置了 `include`/`exclude`，会先过滤再输出。

**真正执行同步（全量表从源同步到目标）：**

```powershell
go run -buildvcs=false ./dbtool -config .\dbtool\config.sources.example.json
```

同样建议先改好 `sources` 里的 DSN，必要时设置 `table_list.exclude`（如 `["^tmp_"]`）排除不需要的表。

---

## 4. 演示三：源库为 MSSQL / Oracle

### 4.1 依赖

MSSQL、Oracle 驱动已在 `dbtool/go.mod` 中声明，在 `dbtool` 目录执行：

```powershell
cd d:\go\go_src\gin\gin1\dbtool
go mod tidy
```

### 4.2 MSSQL 作为源

配置中 `driver` 填 `sqlserver` 或 `mssql`，`dsn` 示例：

```json
"sources": {
  "mssql_src": {
    "driver": "sqlserver",
    "dsn": "sqlserver://用户名:密码@host:1433?database=数据库名"
  }
}
```

在 `sync` 里把 `source` 设为 `mssql_src`，表清单用自定义 `table_list.list` 或 `from_source: true`（会查当前 schema 的表）。  
若需指定 schema，可设 `table_list.schema`（如 `dbo`）。

**仅列表：**

```powershell
go run -buildvcs=false ./dbtool -config .\dbtool\config.sources.example.json -list-tables
```

（需把示例里的 `sync.source` 改成 `mssql_src`，并确保 `config.sources.example.json` 里已有 `mssql_src` 的 DSN。）

### 4.3 Oracle 作为源

配置中 `driver` 填 `oracle`，`dsn` 示例（go-ora）：

```json
"oracle_src": {
  "driver": "oracle",
  "dsn": "oracle://用户名:密码@host:1521/service_name"
}
```

同样通过 `sync.source` 指定该数据源，用 `table_list.list` 或 `from_source: true`。  
`table_list.schema` 可填 Oracle 的 owner（大写），不填则查当前用户表。

**仅列表：**

```powershell
go run -buildvcs=false ./dbtool -config .\dbtool\config.sources.example.json -list-tables
```

（需在配置里把 `sync.source` 改为 `oracle_src` 并写好 `oracle_src` 的 DSN。）

---

## 5. 小结

| 能力           | 说明 |
|----------------|------|
| 旧版配置       | `source` + `target` + `tables`，直接 `-config` 指定该 json 即可 |
| 新版数据源     | `sources`（命名）+ `sync`（选 source/target） |
| 自定义表清单   | `table_list.from_source: false` + `table_list.list` |
| 从源库拉表清单 | `table_list.from_source: true`，可选 schema、include、exclude、defaults |
| 仅列源库表     | `-config xxx -list-tables` |
| 源库类型       | MySQL、Postgres、SQLite、MSSQL（sqlserver）、Oracle（oracle） |

所有带 `-config` 的用法都可加 `-dry-run` 做“只读试跑”，不写入目标库。

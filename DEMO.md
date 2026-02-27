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
- `table_list.defaults`：可选，给"从源拉出来的每张表"统一用的默认配置（如 `batch_size`、`auto_create`）

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

## 5. 演示四：全量表复制

使用 `config.full.example.json`：

- `table_list.from_source`：`true` 表示从源库自动获取所有表
- `table_list.defaults`：为所有表设置默认配置（如 `batch_size`、`auto_create`）
- `table_list.include` / `table_list.exclude`：可选，用正则表达式过滤表名

**命令：**

```powershell
go run -buildvcs=false ./dbtool -config .\dbtool\config.full.example.json
```

**仅列出源库表名（不执行同步）：**

```powershell
go run -buildvcs=false ./dbtool -config .\dbtool\config.full.example.json -list-tables
```

**使用场景：**
- 需要复制整个数据库的所有表
- 不想手动列出每个表
- 需要对所有表应用相同的配置（如批量大小、自动建表）

---

## 6. 演示五：自定义表复制（支持 SELECT 查询和字段映射）

使用 `config.custom.example.json`：

### 6.1 使用自定义 SELECT 查询（优先级最高）

当配置了 `select_sql` 字段时，程序会直接使用该查询，忽略 `table`、`columns`、`where`、`incremental_key`、`since`、`until` 等字段。

**示例：**

```json
{
  "source_table": "users",
  "target_table": "users_import",
  "select_sql": "SELECT id, username, email, created_at FROM users WHERE status = 'active' ORDER BY id",
  "batch_size": 5000,
  "auto_create": true
}
```

**使用场景：**
- 需要复杂的查询条件（如 JOIN、子查询）
- 需要指定特定的字段
- 需要使用聚合函数（如 SUM、COUNT）
- 需要排序数据

### 6.2 使用表名 + 字段映射

**示例：**

```json
{
  "source_table": "orders",
  "target_table": "orders_import",
  "where": "status = 'completed' AND amount > 100",
  "batch_size": 1000,
  "auto_create": true,
  "columns": [
    { "source": "id", "target": "order_id" },
    { "source": "user_id", "target": "customer_id" },
    { "source": "amount", "target": "total_amount", "target_type": "DECIMAL(10,2)" },
    { "source": "status", "target": "order_status", "target_type": "VARCHAR(50)" },
    { "source": "created_at", "target": "order_date", "target_type": "TIMESTAMP" }
  ]
}
```

**使用场景：**
- 需要重命名字段
- 需要指定部分字段
- 需要修改字段类型
- 需要简单的 WHERE 条件

### 6.3 使用表名 + 字段映射 + 增量同步

**示例：**

```json
{
  "source_table": "products",
  "target_table": "products_sync",
  "incremental_key": "updated_at",
  "since": "2024-01-01 00:00:00",
  "until": "2024-12-31 23:59:59",
  "batch_size": 2000,
  "auto_create": true,
  "columns": [
    { "source": "id", "target": "product_id" },
    { "source": "name", "target": "product_name", "target_type": "VARCHAR(255)" },
    { "source": "price", "target": "price", "target_type": "DECIMAL(10,2)" },
    { "source": "stock", "target": "stock_quantity", "target_type": "INT" },
    { "source": "updated_at", "target": "last_updated", "target_type": "TIMESTAMP" }
  ]
}
```

**使用场景：**
- 需要增量同步数据
- 需要按时间范围同步
- 需要重命名字段

### 6.4 使用自定义 SELECT 查询（复杂查询）

**示例：**

```json
{
  "source_table": "orders",
  "target_table": "order_summary",
  "select_sql": "SELECT o.id, u.username, o.amount, o.status, o.created_at FROM orders o LEFT JOIN users u ON o.user_id = u.id WHERE o.created_at >= '2024-01-01'",
  "batch_size": 3000,
  "auto_create": true
}
```

**使用场景：**
- 需要关联多张表（JOIN）
- 需要复杂的查询条件
- 需要从关联表获取数据

### 6.5 使用自定义 SELECT 查询（聚合统计）

**示例：**

```json
{
  "source_table": "orders",
  "target_table": "daily_stats",
  "select_sql": "SELECT DATE(created_at) as stat_date, COUNT(*) as order_count, SUM(amount) as total_amount FROM orders WHERE created_at >= '2024-01-01' GROUP BY DATE(created_at) ORDER BY stat_date",
  "batch_size": 1000,
  "auto_create": true
}
```

**使用场景：**
- 需要生成统计报表
- 需要聚合数据
- 需要数据汇总

### 6.6 命令行模式支持 select_sql

除了配置文件，命令行模式也支持 `select_sql`：

```powershell
go run -buildvcs=false ./dbtool `
  -source-driver mysql `
  -source-dsn "root:password@tcp(localhost:3306)/testdb" `
  -target-driver postgres `
  -target-dsn "postgres://test:password@localhost:5432/testdb?sslmode=disable" `
  -select-sql "SELECT id, username, email FROM users WHERE status = 'active'" `
  -batch 1000
```

**命令：**

```powershell
go run -buildvcs=false ./dbtool -config .\dbtool\config.custom.example.json
```

---

## 7. 演示六：全量表复制+自定义表混合模式

使用 `config.mixed.example.json`：

- `table_list.from_source`：`true` 表示从源库自动获取所有表
- `table_list.defaults`：为大部分表设置默认配置
- `table_list.list`：为特殊表设置自定义配置（会覆盖 defaults）
- `table_list.exclude`：排除不需要的表（正则表达式）
- **`list` 优先原则**：在 `list` 中配置的表不受 `exclude` 影响

**工作原理：**

1. 程序从源库获取所有表名
2. 对于在 `table_list.list` 中配置的表，**直接使用自定义配置**（不受 `exclude` 影响）
3. 对于不在 `list` 中的表，应用 `include`/`exclude` 过滤规则
4. 通过过滤的表使用 `defaults` 配置

**使用场景：**
- 需要复制整个数据库的大部分表
- 但某些表需要特殊处理（如使用自定义 SELECT、字段映射、增量同步等）
- 被排除的表仍可通过 `list` 自定义配置来处理
- 不想为每个表都写配置，只想为特殊表单独配置

**示例 1：排除大部分表，只复制特定几张表**

```json
{
  "table_list": {
    "from_source": true,
    "exclude": [".*"],
    "list": [
      {
        "source_table": "users",
        "select_sql": "SELECT id, username, email FROM users WHERE status = 'active'"
      },
      {
        "source_table": "orders",
        "batch_size": 500
      }
    ]
  }
}
```

**示例 2：复制所有表，但某些表使用自定义配置**

```json
{
  "table_list": {
    "from_source": true,
    "defaults": {
      "batch_size": 10000,
      "auto_create": true
    },
    "list": [
      {
        "source_table": "users",
        "where": "status = 'active'"
      },
      {
        "source_table": "logs",
        "batch_size": 500,
        "where": "created_at > '2024-01-01'"
      }
    ]
  }
}
```

**示例 3：排除特定表，但被排除的表通过自定义配置处理**

```json
{
  "table_list": {
    "from_source": true,
    "exclude": ["temp_.*", "backup_.*"],
    "defaults": {
      "batch_size": 10000,
      "auto_create": true
    },
    "list": [
      {
        "source_table": "temp_users",
        "select_sql": "SELECT * FROM temp_users WHERE is_valid = 1"
      },
      {
        "source_table": "backup_orders",
        "batch_size": 100
      }
    ]
  }
}
```

**命令：**

```powershell
go run -buildvcs=false ./dbtool -config .\dbtool\config.mixed.example.json
```

**执行结果：**

- 大部分表使用 `defaults` 配置（批量大小 10000，自动建表）
- `users` 表使用自定义 SELECT 查询
- `orders` 表使用字段映射 + WHERE 条件
- 以 `tmp_` 和 `temp_` 开头的表被排除
- **但在 `list` 中配置的表不受 `exclude` 影响，仍会被处理**

---

## 8. 小结

| 能力           | 说明 |
|----------------|------|
| 旧版配置       | `source` + `target` + `tables`，直接 `-config` 指定该 json 即可 |
| 新版数据源     | `sources`（命名）+ `sync`（选 source/target） |
| 自定义表清单   | `table_list.from_source: false` + `table_list.list` |
| 从源库拉表清单 | `table_list.from_source: true`，可选 schema、include、exclude、defaults |
| 全量表复制     | `table_list.from_source: true` + `table_list.defaults`，自动获取所有表 |
| 全量+自定义混合 | `table_list.from_source: true` + `defaults` + `list`，`list` 中的表不受 `exclude` 影响，可灵活组合 |
| 自定义 SELECT   | `select_sql` 字段，支持复杂查询、JOIN、聚合等 |
| 字段映射       | `columns` 数组，支持重命名、类型转换 |
| 增量同步       | `incremental_key` + `since` + `until`，按时间或ID增量同步 |
| 迁移耗时汇总   | 每张表显示开始时间、结束时间、总耗时（秒/分钟） |
| 数据核对       | 迁移后对比源表和目标表记录数，检测数据差异 |
| 总体数据核对汇总报告 | 所有表迁移完成后，显示时间统计、数据核对统计、存在差异的表详情 |
| 仅列源库表     | `-config xxx -list-tables` |
| 源库类型       | MySQL、Postgres、SQLite、MSSQL（sqlserver）、Oracle（oracle） |
| Dry-run 模式   | 所有带 `-config` 的用法都可加 `-dry-run` 做"只读试跑" |

所有带 `-config` 的用法都可加 `-dry-run` 做"只读试跑"，不写入目标库。

---

## 9. 迁移耗时汇总和数据核对

### 9.1 单表迁移耗时统计

每张表迁移完成后，会显示：

```
========================================
表 users 迁移完成
========================================
开始时间: 2026-01-27 15:30:00
结束时间: 2026-01-27 15:35:00
总耗时: 300.00 秒 (5.00 分钟)
源表记录数: 10000
目标表记录数: 10000
迁移记录数: 10000
数据核对: ✅ 无差异（源表 10000 条，目标表 10000 条）
========================================
```

**包含信息：**
- 开始时间（精确到秒）
- 结束时间（精确到秒）
- 总耗时（秒和分钟）
- 源表记录数
- 目标表记录数
- 迁移记录数
- 数据核对结果（无差异/目标表多/目标表少）

### 9.2 数据核对

程序会自动对比源表和目标表的记录数：

- ✅ **无差异**：源表和目标表记录数一致
- ⚠️ **目标表多**：目标表比源表多（可能存在重复数据或源表有删除）
- ❌ **目标表少**：目标表比源表少（可能存在数据丢失）

### 9.3 总体数据核对汇总报告

所有表迁移完成后，会显示总体数据核对汇总报告：

```
########################################
总体数据核对汇总报告
########################################
总表数: 10
存在差异的表数: 2

时间统计:
  开始时间: 2026-01-27 15:30:00
  结束时间: 2026-01-27 15:50:00
  总迁移耗时: 1250.50 秒 (20.84 分钟)

数据核对统计:
  源库总记录数: 100000
  目标库总记录数: 100050
  迁移总记录数: 100050
  总体差异: 50
  数据核对结果: ⚠️ 目标库比源库多 50 条

存在差异的表详情:
  ❌ users: 源库 10000 条, 目标库 10020 条, 多 20 条
  ❌ orders: 源库 5000 条, 目标库 5030 条, 多 30 条
########################################
```

**汇总信息包括：**
- 总表数
- 存在差异的表数
- 开始时间 / 结束时间
- 总迁移耗时（秒和分钟）
- 源库总记录数 / 目标库总记录数
- 迁移总记录数
- 总体差异
- 数据核对结果（无差异/目标库多/目标库少）
- 存在差异的表详情

### 9.4 使用场景

**监控迁移进度：**
- 实时查看每张表的迁移进度
- 了解每张表的耗时情况
- 掌握总体迁移时间

**数据质量保证：**
- 迁移后自动核对数据
- 发现数据差异及时处理
- 避免数据丢失或重复
- 总体数据核对确保整体一致性

**性能优化：**
- 根据耗时统计优化配置
- 识别慢表进行优化
- 合理设置批量大小

**问题排查：**
- 快速定位存在差异的表
- 了解差异的具体数量
- 分析数据不一致的原因

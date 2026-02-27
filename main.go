package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"regexp"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"
	_ "github.com/microsoft/go-mssqldb"
	_ "github.com/sijms/go-ora/v2"
)

type dbConfig struct {
	Driver string
	DSN    string
}

// simpleDB 是一个对不同数据库实现统一接口的封装
type simpleDB struct {
	cfg dbConfig
	db  *sql.DB
}

func newSimpleDB(cfg dbConfig) (*simpleDB, error) {
	cfg.Driver = normalizeDriver(cfg.Driver)
	db, err := sql.Open(cfg.Driver, cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("打开数据库失败 (%s): %w", cfg.Driver, err)
	}
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(30 * time.Minute)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("数据库连接失败 (%s): %w", cfg.Driver, err)
	}

	return &simpleDB{cfg: cfg, db: db}, nil
}

func (s *simpleDB) Close() error {
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

// columnMapping 定义字段映射（源字段 -> 目标字段，及类型覆盖）
type columnMapping struct {
	Source       string `json:"source"`                  // 源列名
	Target       string `json:"target"`                  // 目标列名（为空则等于 Source）
	TargetType   string `json:"target_type,omitempty"`   // 目标列类型（可选，自动建表用）
	Nullable     *bool  `json:"nullable,omitempty"`      // 是否可空（可选）
	DefaultValue string `json:"default_value,omitempty"` // 自动建表时的默认值表达式（可选）
}

// copyTableOptions 定义表复制选项
type copyTableOptions struct {
	Table          string
	TargetTable    string
	Where          string
	BatchSize      int
	DryRun         bool
	Columns        []columnMapping
	AutoCreate     bool
	IncrementalKey string // 增量同步的关键列名（如自增ID或时间戳）
	Since          string // 大于该值的记录才会被同步（> Since）
	Until          string // 小于等于该值的记录才会被同步（<= Until，可选）
	SelectSQL      string // 自定义 SELECT 查询（优先级最高）
}

// configTable 定义单张表的配置
type configTable struct {
	SourceTable    string          `json:"source_table"`
	TargetTable    string          `json:"target_table,omitempty"`
	Where          string          `json:"where,omitempty"`
	BatchSize      int             `json:"batch_size,omitempty"`
	AutoCreate     bool            `json:"auto_create,omitempty"`
	IncrementalKey string          `json:"incremental_key,omitempty"`
	Since          string          `json:"since,omitempty"`
	Until          string          `json:"until,omitempty"`
	Columns        []columnMapping `json:"columns,omitempty"`
	SelectSQL      string          `json:"select_sql,omitempty"` // 自定义 SELECT 查询（优先级最高）
}

// toolConfig 整体配置文件结构（支持新旧两种格式）
type toolConfig struct {
	// 旧版：直接 source/target + tables 数组
	Source *dbConfig     `json:"source,omitempty"`
	Target *dbConfig     `json:"target,omitempty"`
	Tables []configTable `json:"tables,omitempty"` // 旧版表清单

	// 新版：命名数据源 + 表清单（从源库全量或自定义）
	Sources map[string]dbConfig `json:"sources,omitempty"`
	Sync    *struct {
		Source string `json:"source"` // 数据源 key
		Target string `json:"target"`
	} `json:"sync,omitempty"`
	TableList *struct {
		FromSource bool          `json:"from_source,omitempty"` // true=从源库查询全量表
		Schema     string        `json:"schema,omitempty"`      // 可选 schema（mssql/oracle/postgres）
		Include    []string      `json:"include,omitempty"`     // 表名匹配（正则），为空表示全部
		Exclude    []string      `json:"exclude,omitempty"`     // 排除表名（正则）
		Defaults   *configTable  `json:"defaults,omitempty"`    // 从源拉表时的默认配置
		List       []configTable `json:"list,omitempty"`        // 自定义表清单
	} `json:"table_list,omitempty"`
}

func loadConfig(path string) (*toolConfig, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("打开配置文件失败: %w", err)
	}
	defer f.Close()

	data, err := io.ReadAll(f)
	if err != nil {
		return nil, fmt.Errorf("读取配置文件失败: %w", err)
	}

	var cfg toolConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("解析配置文件 JSON 失败: %w", err)
	}
	return &cfg, nil
}

// resolveConfig 解析出源、目标连接与表清单，兼容旧版与新版配置
func resolveConfig(cfg *toolConfig) (sourceCfg, targetCfg dbConfig, tables []configTable, err error) {
	// 新版：sources + sync + table_list
	if len(cfg.Sources) > 0 && cfg.Sync != nil {
		srcName := strings.TrimSpace(cfg.Sync.Source)
		dstName := strings.TrimSpace(cfg.Sync.Target)
		if srcName == "" || dstName == "" {
			err = fmt.Errorf("sync.source 与 sync.target 不能为空")
			return
		}
		var ok bool
		if sourceCfg, ok = cfg.Sources[srcName]; !ok {
			err = fmt.Errorf("sources 中未找到数据源: %s", srcName)
			return
		}
		if targetCfg, ok = cfg.Sources[dstName]; !ok {
			err = fmt.Errorf("sources 中未找到数据源: %s", dstName)
			return
		}
		if sourceCfg.Driver == "" || sourceCfg.DSN == "" || targetCfg.Driver == "" || targetCfg.DSN == "" {
			err = fmt.Errorf("数据源 %s / %s 的 driver 与 dsn 不能为空", srcName, dstName)
			return
		}
		sourceCfg.Driver = normalizeDriver(sourceCfg.Driver)
		targetCfg.Driver = normalizeDriver(targetCfg.Driver)

		if cfg.TableList == nil {
			err = fmt.Errorf("使用 sources/sync 时必须配置 table_list")
			return
		}
		tl := cfg.TableList
		if tl.FromSource {
			// 表清单由运行时从源库查询得到，这里先返回空列表，runWithConfig 里再查
			tables = nil
			if tl.Defaults != nil {
				tables = []configTable{*tl.Defaults}
			}
			return
		}
		if len(tl.List) == 0 {
			err = fmt.Errorf("table_list.from_source 为 false 时 table_list.list 不能为空")
			return
		}
		tables = tl.List
		return
	}

	// 旧版：source + target + tables
	if cfg.Source == nil || cfg.Target == nil {
		err = fmt.Errorf("请配置 source/target 或 sources + sync + table_list")
		return
	}
	sourceCfg = *cfg.Source
	targetCfg = *cfg.Target
	sourceCfg.Driver = normalizeDriver(sourceCfg.Driver)
	targetCfg.Driver = normalizeDriver(targetCfg.Driver)
	if sourceCfg.Driver == "" || sourceCfg.DSN == "" || targetCfg.Driver == "" || targetCfg.DSN == "" {
		err = fmt.Errorf("source/target 的 driver 与 dsn 不能为空")
		return
	}
	if len(cfg.Tables) == 0 {
		err = fmt.Errorf("tables 不能为空")
		return
	}
	tables = cfg.Tables
	return
}

func main() {
	configPath := flag.String("config", "", "JSON 配置文件路径（配置多表、多字段映射和增量同步）")

	srcDriver := flag.String("source-driver", "", "源数据库驱动，例如: mysql, postgres, sqlite3")
	srcDSN := flag.String("source-dsn", "", "源数据库 DSN 连接串")
	dstDriver := flag.String("target-driver", "", "目标数据库驱动，例如: mysql, postgres, sqlite3")
	dstDSN := flag.String("target-dsn", "", "目标数据库 DSN 连接串")
	table := flag.String("table", "", "需要复制的表名")
	where := flag.String("where", "", "可选的 WHERE 条件（不需要写 WHERE 关键词）")
	batchSize := flag.Int("batch", 1000, "批量提交大小")
	dryRun := flag.Bool("dry-run", false, "只打印将要执行的 SQL，而不真正写入目标库")
	incrementalKey := flag.String("inc-key", "", "增量同步关键列名（如自增ID或时间戳）")
	since := flag.String("since", "", "增量同步起始值（> since）")
	until := flag.String("until", "", "增量同步结束值（<= until，可选）")
	listTables := flag.Bool("list-tables", false, "仅列出源库表名（需配合 -config 使用），用于演示从源库拉取表清单")

	flag.Parse()

	// 优先走配置文件模式
	if strings.TrimSpace(*configPath) != "" {
		if *listTables {
			runListTables(*configPath)
			return
		}
		runWithConfig(*configPath, *dryRun)
		return
	}

	// 兼容原有命令行模式（单表复制）
	if *srcDriver == "" || *srcDSN == "" || *dstDriver == "" || *dstDSN == "" || *table == "" {
		fmt.Println("用法示例（配置文件模式）：")
		fmt.Println("  go run ./dbtool -config config.json")
		fmt.Println("  go run ./dbtool -config config.json -list-tables   # 仅列出源库表名")
		fmt.Println()
		fmt.Println("用法示例（命令行单表模式）：")
		fmt.Println("  go run ./dbtool -source-driver mysql -source-dsn \"user:pass@tcp(127.0.0.1:3306)/db1\" ^")
		fmt.Println("      -target-driver postgres -target-dsn \"postgres://user:pass@localhost:5432/db2?sslmode=disable\" ^")
		fmt.Println("      -table users -where \"status=1\" -batch 1000 -inc-key id -since 1000")
		flag.PrintDefaults()
		os.Exit(1)
	}

	srcCfg := dbConfig{Driver: strings.ToLower(*srcDriver), DSN: *srcDSN}
	dstCfg := dbConfig{Driver: strings.ToLower(*dstDriver), DSN: *dstDSN}

	log.Printf("连接源数据库: %s\n", srcCfg.Driver)
	src, err := newSimpleDB(srcCfg)
	if err != nil {
		log.Fatalf("源数据库连接失败: %v", err)
	}
	defer src.Close()

	log.Printf("连接目标数据库: %s\n", dstCfg.Driver)
	dst, err := newSimpleDB(dstCfg)
	if err != nil {
		log.Fatalf("目标数据库连接失败: %v", err)
	}
	defer dst.Close()

	opts := copyTableOptions{
		Table:          *table,
		TargetTable:    "",
		Where:          *where,
		BatchSize:      *batchSize,
		DryRun:         *dryRun,
		AutoCreate:     false,
		IncrementalKey: *incrementalKey,
		Since:          *since,
		Until:          *until,
	}

	_, _, _, _, err = copyTable(context.Background(), src, dst, opts)
	if err != nil {
		log.Fatalf("拷贝表数据失败: %v", err)
	}
}

// tableVerificationResult 记录单张表的数据核对结果
type tableVerificationResult struct {
	TableName     string
	SourceCount   int64
	TargetCount   int64
	MigratedCount int64
	Diff          int64
	HasDiff       bool
}

// runWithConfig 使用 JSON 配置文件执行多表同步
func runWithConfig(configPath string, cliDryRun bool) {
	cfg, err := loadConfig(configPath)
	if err != nil {
		log.Fatalf("加载配置文件失败: %v", err)
	}

	sourceCfg, targetCfg, tables, err := resolveConfig(cfg)
	if err != nil {
		log.Fatalf("解析配置失败: %v", err)
	}

	// 若为从源库拉取表清单，先连接源库查询表名列表
	if cfg.TableList != nil && cfg.TableList.FromSource && (len(tables) == 0 || (len(tables) == 1 && strings.TrimSpace(tables[0].SourceTable) == "")) {
		log.Printf("连接源数据库: %s\n", sourceCfg.Driver)
		src, errConn := newSimpleDB(sourceCfg)
		if errConn != nil {
			log.Fatalf("源数据库连接失败: %v", errConn)
		}
		schema := ""
		if cfg.TableList != nil {
			schema = strings.TrimSpace(cfg.TableList.Schema)
		}
		names, errList := listTablesFromSource(context.Background(), src, schema)
		_ = src.Close()
		if errList != nil {
			log.Fatalf("从源库获取表清单失败: %v", errList)
		}

		// 构建 list 中配置的表名集合（用于快速查找）
		// 注意：同一个 source_table 可能有多个配置（支持多次复制）
		listTableSet := make(map[string]bool)
		if cfg.TableList != nil {
			for _, t := range cfg.TableList.List {
				if strings.TrimSpace(t.SourceTable) != "" {
					listTableSet[t.SourceTable] = true
				}
			}
		}

		includeRe, excludeRe := compileTableFilters(cfg.TableList.Include, cfg.TableList.Exclude)

		// 先添加 list 中所有自定义配置的表（支持同一个表的多次复制）
		tables = make([]configTable, 0)
		if cfg.TableList != nil {
			for _, t := range cfg.TableList.List {
				if strings.TrimSpace(t.SourceTable) != "" {
					entry := t
					if entry.BatchSize <= 0 {
						entry.BatchSize = 1000
					}
					tables = append(tables, entry)
				}
			}
		}

		// 然后添加通过 include/exclude 过滤的表（使用 defaults 配置）
		defaults := cfg.TableList.Defaults
		for _, name := range names {
			// 如果在 list 中已经配置过了，跳过（避免重复）
			if _, exists := listTableSet[name]; exists {
				continue
			}
			// 应用 include/exclude 过滤规则
			if matchTableFilters(name, includeRe, excludeRe) {
				entry := configTable{SourceTable: name, BatchSize: 1000}
				if defaults != nil {
					entry.TargetTable = defaults.TargetTable
					entry.Where = defaults.Where
					entry.BatchSize = defaults.BatchSize
					entry.AutoCreate = defaults.AutoCreate
					entry.IncrementalKey = defaults.IncrementalKey
					entry.Since = defaults.Since
					entry.Until = defaults.Until
					entry.Columns = defaults.Columns
					if defaults.BatchSize > 0 {
						entry.BatchSize = defaults.BatchSize
					}
				}
				tables = append(tables, entry)
			}
		}
		log.Printf("从源库获取到 %d 张表\n", len(tables))
	}

	if len(tables) == 0 {
		log.Fatalf("表清单为空，请检查 table_list 或 tables 配置")
	}

	log.Printf("连接源数据库: %s\n", sourceCfg.Driver)
	src, err := newSimpleDB(sourceCfg)
	if err != nil {
		log.Fatalf("源数据库连接失败: %v", err)
	}
	defer src.Close()

	log.Printf("连接目标数据库: %s\n", targetCfg.Driver)
	dst, err := newSimpleDB(targetCfg)
	if err != nil {
		log.Fatalf("目标数据库连接失败: %v", err)
	}
	defer dst.Close()

	// 收集所有表的数据核对结果
	var verificationResults []tableVerificationResult
	var totalSourceCount int64
	var totalTargetCount int64
	var totalMigratedCount int64
	var totalDiff int64
	var diffTableCount int

	// 记录总开始时间
	totalStartTime := time.Now()

	for i, t := range tables {
		if strings.TrimSpace(t.SourceTable) == "" {
			log.Printf("第 %d 个表配置 source_table 为空，跳过", i)
			continue
		}
		opts := copyTableOptions{
			Table:          t.SourceTable,
			TargetTable:    t.TargetTable,
			Where:          t.Where,
			BatchSize:      t.BatchSize,
			DryRun:         cliDryRun,
			Columns:        t.Columns,
			AutoCreate:     t.AutoCreate,
			IncrementalKey: t.IncrementalKey,
			Since:          t.Since,
			Until:          t.Until,
			SelectSQL:      t.SelectSQL,
		}
		if opts.BatchSize <= 0 {
			opts.BatchSize = 1000
		}

		log.Printf("开始根据配置同步表: source=%s, target=%s\n",
			opts.Table, firstNonEmpty(opts.TargetTable, opts.Table))

		migratedCount, sourceCount, targetCount, _, err := copyTable(context.Background(), src, dst, opts)
		if err != nil {
			log.Fatalf("表 %s 同步失败: %v", opts.Table, err)
		}

		// 收集核对数据
		result := tableVerificationResult{
			TableName:     opts.Table,
			SourceCount:   sourceCount,
			TargetCount:   targetCount,
			MigratedCount: migratedCount,
		}
		if sourceCount >= 0 && targetCount >= 0 {
			result.Diff = targetCount - sourceCount
			result.HasDiff = result.Diff != 0
			if result.HasDiff {
				diffTableCount++
			}
		}
		verificationResults = append(verificationResults, result)

		// 累加总计
		if sourceCount >= 0 {
			totalSourceCount += sourceCount
		}
		if targetCount >= 0 {
			totalTargetCount += targetCount
		}
		totalMigratedCount += migratedCount
	}

	// 计算总体差异和总耗时
	totalDiff = totalTargetCount - totalSourceCount
	totalDuration := time.Since(totalStartTime)
	totalDurationSeconds := totalDuration.Seconds()
	totalEndTime := time.Now()

	// 打印总体数据核对汇总报告
	log.Printf("\n")
	log.Printf("########################################\n")
	log.Printf("总体数据核对汇总报告\n")
	log.Printf("########################################\n")
	log.Printf("总表数: %d\n", len(verificationResults))
	log.Printf("存在差异的表数: %d\n", diffTableCount)
	log.Printf("\n")
	log.Printf("时间统计:\n")
	log.Printf("  开始时间: %s\n", totalStartTime.Format("2006-01-02 15:04:05"))
	log.Printf("  结束时间: %s\n", totalEndTime.Format("2006-01-02 15:04:05"))
	log.Printf("  总迁移耗时: %.2f 秒 (%.2f 分钟)\n", totalDurationSeconds, totalDurationSeconds/60)
	log.Printf("  源库总记录数: %d\n", totalSourceCount)
	log.Printf("  目标库总记录数: %d\n", totalTargetCount)
	log.Printf("  迁移总记录数: %d\n", totalMigratedCount)
	log.Printf("  总体差异: %d\n", totalDiff)
	if totalDiff == 0 {
		log.Printf("  数据核对结果: ✅ 无差异\n")
	} else if totalDiff > 0 {
		log.Printf("  数据核对结果: ⚠️ 目标库比源库多 %d 条\n", totalDiff)
	} else {
		log.Printf("  数据核对结果: ❌ 目标库比源库少 %d 条\n", -totalDiff)
	}

	// 打印存在差异的表详情
	if diffTableCount > 0 {
		log.Printf("\n")
		log.Printf("存在差异的表详情:\n")
		for _, result := range verificationResults {
			if result.HasDiff {
				if result.Diff > 0 {
					log.Printf("  ❌ %s: 源库 %d 条, 目标库 %d 条, 多 %d 条\n",
						result.TableName, result.SourceCount, result.TargetCount, result.Diff)
				} else {
					log.Printf("  ❌ %s: 源库 %d 条, 目标库 %d 条, 少 %d 条\n",
						result.TableName, result.SourceCount, result.TargetCount, -result.Diff)
				}
			}
		}
	}
	log.Printf("########################################\n")
}

// compileTableFilters 编译 include/exclude 为正则
func compileTableFilters(include, exclude []string) (includeRe, excludeRe []*regexp.Regexp) {
	for _, s := range include {
		if s == "" {
			continue
		}
		re, err := regexp.Compile(s)
		if err != nil {
			log.Printf("table_list.include 正则无效 %q: %v", s, err)
			continue
		}
		includeRe = append(includeRe, re)
	}
	for _, s := range exclude {
		if s == "" {
			continue
		}
		re, err := regexp.Compile(s)
		if err != nil {
			log.Printf("table_list.exclude 正则无效 %q: %v", s, err)
			continue
		}
		excludeRe = append(excludeRe, re)
	}
	return includeRe, excludeRe
}

func matchTableFilters(name string, includeRe, excludeRe []*regexp.Regexp) bool {
	for _, re := range excludeRe {
		if re.MatchString(name) {
			return false
		}
	}
	if len(includeRe) == 0 {
		return true
	}
	for _, re := range includeRe {
		if re.MatchString(name) {
			return true
		}
	}
	return false
}

// listTablesFromSource 从源库查询表名列表（支持 mysql/postgres/sqlite3/sqlserver/oracle）
func listTablesFromSource(ctx context.Context, src *simpleDB, schema string) ([]string, error) {
	driver := normalizeDriver(src.cfg.Driver)
	switch driver {
	case "mysql":
		return listTablesMySQL(ctx, src.db, schema)
	case "postgres", "postgresql":
		return listTablesPostgres(ctx, src.db, schema)
	case "sqlite3":
		return listTablesSQLite(ctx, src.db)
	case "sqlserver", "mssql":
		return listTablesMSSQL(ctx, src.db, schema)
	case "oracle":
		return listTablesOracle(ctx, src.db, schema)
	default:
		return nil, fmt.Errorf("暂不支持从驱动 %s 拉取表清单", driver)
	}
}

func normalizeDriver(d string) string {
	d = strings.ToLower(strings.TrimSpace(d))
	if d == "mssql" {
		return "sqlserver"
	}
	return d
}

func listTablesMySQL(ctx context.Context, db *sql.DB, schema string) ([]string, error) {
	var query string
	var args []interface{}
	if schema == "" {
		query = `SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE() AND table_type = 'BASE TABLE' ORDER BY table_name`
	} else {
		query = `SELECT table_name FROM information_schema.tables WHERE table_schema = ? AND table_type = 'BASE TABLE' ORDER BY table_name`
		args = append(args, schema)
	}
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		out = append(out, name)
	}
	return out, rows.Err()
}

func listTablesPostgres(ctx context.Context, db *sql.DB, schema string) ([]string, error) {
	if schema == "" {
		schema = "public"
	}
	query := `SELECT table_name FROM information_schema.tables WHERE table_schema = $1 AND table_type = 'BASE TABLE' ORDER BY table_name`
	rows, err := db.QueryContext(ctx, query, schema)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		out = append(out, name)
	}
	return out, rows.Err()
}

func listTablesSQLite(ctx context.Context, db *sql.DB) ([]string, error) {
	rows, err := db.QueryContext(ctx, `SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		out = append(out, name)
	}
	return out, rows.Err()
}

func listTablesMSSQL(ctx context.Context, db *sql.DB, schema string) ([]string, error) {
	// 使用 sys.tables + sys.schemas，避免部分环境下 information_schema 视图不可用的问题
	if schema == "" {
		schema = "dbo"
	}
	query := `
SELECT t.name
FROM sys.tables AS t
INNER JOIN sys.schemas AS s ON t.schema_id = s.schema_id
WHERE s.name = @p1
ORDER BY t.name`
	rows, err := db.QueryContext(ctx, query, schema)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		out = append(out, name)
	}
	return out, rows.Err()
}

func listTablesOracle(ctx context.Context, db *sql.DB, schema string) ([]string, error) {
	var q string
	var args []interface{}
	if schema == "" {
		q = `SELECT table_name FROM user_tables ORDER BY table_name`
	} else {
		q = `SELECT table_name FROM all_tables WHERE owner = :1 ORDER BY table_name`
		args = append(args, strings.ToUpper(schema))
	}
	rows, err := db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		out = append(out, name)
	}
	return out, rows.Err()
}

func firstNonEmpty(a, b string) string {
	if strings.TrimSpace(a) != "" {
		return a
	}
	return b
}

// runListTables 仅连接源库并列出表名（用于演示“从源库拉取表清单”功能）
func runListTables(configPath string) {
	cfg, err := loadConfig(configPath)
	if err != nil {
		log.Fatalf("加载配置文件失败: %v", err)
	}
	sourceCfg, _, _, err := resolveConfig(cfg)
	if err != nil {
		log.Fatalf("解析配置失败: %v", err)
	}
	schema := ""
	if cfg.TableList != nil {
		schema = strings.TrimSpace(cfg.TableList.Schema)
	}
	log.Printf("连接源数据库: %s\n", sourceCfg.Driver)
	src, err := newSimpleDB(sourceCfg)
	if err != nil {
		log.Fatalf("源数据库连接失败: %v", err)
	}
	defer src.Close()
	names, err := listTablesFromSource(context.Background(), src, schema)
	if err != nil {
		log.Fatalf("获取表清单失败: %v", err)
	}
	if cfg.TableList != nil {
		includeRe, excludeRe := compileTableFilters(cfg.TableList.Include, cfg.TableList.Exclude)
		var filtered []string
		for _, name := range names {
			if matchTableFilters(name, includeRe, excludeRe) {
				filtered = append(filtered, name)
			}
		}
		names = filtered
	}
	fmt.Printf("共 %d 张表:\n", len(names))
	for _, name := range names {
		fmt.Println(name)
	}
}

// copyTable 将源数据库中的某个表的数据复制到目标数据库
// 假设源表和目标表结构一致（列名相同，顺序相同或可以自动检测）
// 返回：迁移记录数、源表记录数、目标表记录数、耗时（秒）、错误
func copyTable(ctx context.Context, src, dst *simpleDB, opts copyTableOptions) (int64, int64, int64, float64, error) {
	if opts.BatchSize <= 0 {
		opts.BatchSize = 1000
	}

	targetTable := firstNonEmpty(opts.TargetTable, opts.Table)

	// 记录开始时间
	startTime := time.Now()
	log.Printf("开始复制表 %s -> %s ...\n", opts.Table, targetTable)
	log.Printf("开始时间: %s\n", startTime.Format("2006-01-02 15:04:05"))

	// 获取源表记录数（用于数据核对）
	var sourceCount int64
	if strings.TrimSpace(opts.SelectSQL) != "" {
		// 使用自定义 SELECT 查询时，通过子查询获取记录数
		countQuery := "SELECT COUNT(*) FROM (" + opts.SelectSQL + ") AS tmp"
		err := src.db.QueryRowContext(ctx, countQuery).Scan(&sourceCount)
		if err != nil {
			log.Printf("警告：无法获取源表记录数: %v\n", err)
			sourceCount = -1
		} else {
			log.Printf("源表记录数: %d\n", sourceCount)
		}
	} else {
		countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s", opts.Table)
		var whereClause string
		if strings.TrimSpace(opts.Where) != "" {
			whereClause = " WHERE " + opts.Where
		}
		countQuery += whereClause
		err := src.db.QueryRowContext(ctx, countQuery).Scan(&sourceCount)
		if err != nil {
			log.Printf("警告：无法获取源表记录数: %v\n", err)
			sourceCount = -1
		} else {
			log.Printf("源表记录数: %d\n", sourceCount)
		}
	}

	var query string
	var rows *sql.Rows
	var err error

	// 优先使用自定义 SELECT 查询
	if strings.TrimSpace(opts.SelectSQL) != "" {
		query = opts.SelectSQL
		log.Printf("使用自定义 SELECT 查询\n")
		rows, err = src.db.QueryContext(ctx, query)
	} else {
		// 构建 SELECT 列清单（支持字段映射）
		selectCols := buildSelectColumns(opts)

		query = fmt.Sprintf("SELECT %s FROM %s", selectCols, opts.Table)

		// where 条件：用户自定义 + 增量条件
		var whereClauses []string
		if strings.TrimSpace(opts.Where) != "" {
			whereClauses = append(whereClauses, "("+opts.Where+")")
		}
		if strings.TrimSpace(opts.IncrementalKey) != "" && strings.TrimSpace(opts.Since) != "" {
			whereClauses = append(whereClauses,
				fmt.Sprintf("%s > '%s'", quoteIdent(opts.IncrementalKey, src.cfg.Driver), opts.Since))
		}
		if strings.TrimSpace(opts.IncrementalKey) != "" && strings.TrimSpace(opts.Until) != "" {
			whereClauses = append(whereClauses,
				fmt.Sprintf("%s <= '%s'", quoteIdent(opts.IncrementalKey, src.cfg.Driver), opts.Until))
		}
		if len(whereClauses) > 0 {
			query += " WHERE " + strings.Join(whereClauses, " AND ")
		}

		rows, err = src.db.QueryContext(ctx, query)
	}

	if err != nil {
		return 0, 0, 0, 0, fmt.Errorf("查询源表失败: %w", err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return 0, 0, 0, 0, fmt.Errorf("获取列信息失败: %w", err)
	}
	if len(cols) == 0 {
		return 0, 0, 0, 0, fmt.Errorf("表 %s 无任何列", opts.Table)
	}

	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return 0, 0, 0, 0, fmt.Errorf("获取列类型信息失败: %w", err)
	}

	// 自动建表
	if opts.AutoCreate {
		if err := ensureTargetTable(ctx, dst, targetTable, colTypes, opts); err != nil {
			return 0, 0, 0, 0, fmt.Errorf("自动建表失败: %w", err)
		}
	}

	// 根据字段映射决定插入列
	insertColumns := buildInsertColumns(cols, opts)

	insertSQL, err := buildInsertSQL(targetTable, insertColumns, dst.cfg.Driver)
	if err != nil {
		return 0, 0, 0, 0, err
	}

	if opts.DryRun {
		log.Println("Dry-Run 模式，仅打印将执行的 INSERT SQL：")
		log.Println(insertSQL)
	}

	tx, err := dst.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, 0, 0, 0, fmt.Errorf("开启目标库事务失败: %w", err)
	}
	defer func() {
		// 若外部没有提交，则回滚
		_ = tx.Rollback()
	}()

	valuePtrs := make([]interface{}, len(cols))
	valueHolders := make([]interface{}, len(cols))

	count := 0
	batchCount := 0

	for rows.Next() {
		for i := range valueHolders {
			valueHolders[i] = nil
			valuePtrs[i] = &valueHolders[i]
		}
		if err := rows.Scan(valuePtrs...); err != nil {
			return 0, 0, 0, 0, fmt.Errorf("扫描源表行失败: %w", err)
		}

		// 根据字段映射重排参数顺序
		args := reorderArgs(cols, insertColumns, valueHolders, opts)

		if opts.DryRun {
			// 仅打印一部分示例数据，避免日志过大
			if count < 5 {
				log.Printf("示例行 %d: %v\n", count+1, args)
			}
		} else {
			if _, err := tx.ExecContext(ctx, insertSQL, args...); err != nil {
				return 0, 0, 0, 0, fmt.Errorf("插入目标库失败: %w", err)
			}
		}

		count++
		batchCount++

		if !opts.DryRun && batchCount >= opts.BatchSize {
			if err := tx.Commit(); err != nil {
				return 0, 0, 0, 0, fmt.Errorf("提交事务失败: %w", err)
			}
			log.Printf("已提交 %d 条记录\n", count)
			// 开启新的事务
			tx, err = dst.db.BeginTx(ctx, nil)
			if err != nil {
				return 0, 0, 0, 0, fmt.Errorf("开启新事务失败: %w", err)
			}
			batchCount = 0
		}
	}

	if err := rows.Err(); err != nil {
		return 0, 0, 0, 0, fmt.Errorf("遍历源表行时出错: %w", err)
	}

	if !opts.DryRun {
		if err := tx.Commit(); err != nil {
			return 0, 0, 0, 0, fmt.Errorf("最终提交事务失败: %w", err)
		}
	}

	// 获取目标表记录数（用于数据核对）
	var targetCount int64
	targetCountQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s", quoteIdent(targetTable, dst.cfg.Driver))
	err = dst.db.QueryRowContext(ctx, targetCountQuery).Scan(&targetCount)
	if err != nil {
		log.Printf("警告：无法获取目标表记录数: %v\n", err)
		targetCount = -1
	} else {
		log.Printf("目标表记录数: %d\n", targetCount)
	}

	// 计算结束时间和总耗时
	endTime := time.Now()
	duration := endTime.Sub(startTime)
	durationSeconds := duration.Seconds()

	// 打印汇总信息
	log.Printf("========================================\n")
	log.Printf("表 %s 迁移完成\n", opts.Table)
	log.Printf("========================================\n")
	log.Printf("开始时间: %s\n", startTime.Format("2006-01-02 15:04:05"))
	log.Printf("结束时间: %s\n", endTime.Format("2006-01-02 15:04:05"))
	log.Printf("总耗时: %.2f 秒 (%.2f 分钟)\n", durationSeconds, durationSeconds/60)
	log.Printf("源表记录数: %d\n", sourceCount)
	log.Printf("目标表记录数: %d\n", targetCount)
	log.Printf("迁移记录数: %d\n", count)

	// 数据核对
	if sourceCount >= 0 && targetCount >= 0 {
		diff := targetCount - sourceCount
		if diff == 0 {
			log.Printf("数据核对: ✅ 无差异（源表 %d 条，目标表 %d 条）\n", sourceCount, targetCount)
		} else if diff > 0 {
			log.Printf("数据核对: ⚠️ 目标表比源表多 %d 条（可能存在重复数据或源表有删除）\n", diff)
		} else {
			log.Printf("数据核对: ❌ 目标表比源表少 %d 条（可能存在数据丢失）\n", -diff)
		}
	}
	log.Printf("========================================\n")

	return int64(count), sourceCount, targetCount, durationSeconds, nil
}

// buildSelectColumns 根据配置构建 SELECT 的列清单
func buildSelectColumns(opts copyTableOptions) string {
	if len(opts.Columns) == 0 {
		return "*"
	}
	var cols []string
	for _, c := range opts.Columns {
		if strings.TrimSpace(c.Source) == "" {
			continue
		}
		cols = append(cols, c.Source)
	}
	if len(cols) == 0 {
		return "*"
	}
	return strings.Join(cols, ", ")
}

// buildInsertColumns 根据配置和源列构建插入到目标库的列名列表
func buildInsertColumns(sourceCols []string, opts copyTableOptions) []string {
	if len(opts.Columns) == 0 {
		// 不做映射，源列名即目标列名
		return append([]string(nil), sourceCols...)
	}

	// 建立 source -> target 的映射
	mapping := make(map[string]string)
	for _, c := range opts.Columns {
		srcCol := strings.TrimSpace(c.Source)
		if srcCol == "" {
			continue
		}
		targetCol := strings.TrimSpace(c.Target)
		if targetCol == "" {
			targetCol = srcCol
		}
		mapping[srcCol] = targetCol
	}

	var result []string
	for _, srcCol := range sourceCols {
		if tgtCol, ok := mapping[srcCol]; ok {
			result = append(result, tgtCol)
		}
	}
	// 若映射为空，则退回全部列
	if len(result) == 0 {
		return append([]string(nil), sourceCols...)
	}
	return result
}

// reorderArgs 根据插入列顺序，重新排列参数
// - sourceCols: 源列名（查询结果的列顺序）
// - insertCols: 目标表要插入的列名（buildInsertColumns 的结果，通常是目标列名）
// - values:     源行数据，顺序与 sourceCols 对应
func reorderArgs(sourceCols, insertCols []string, values []interface{}, opts copyTableOptions) []interface{} {
	if len(insertCols) == 0 || len(sourceCols) == 0 {
		return values
	}

	// 源列名 -> 下标
	sourceIndex := make(map[string]int, len(sourceCols))
	for i, name := range sourceCols {
		sourceIndex[name] = i
	}

	// 目标列名 -> 源列名（来自字段映射配置）
	targetToSource := make(map[string]string)
	for _, c := range opts.Columns {
		src := strings.TrimSpace(c.Source)
		if src == "" {
			continue
		}
		tgt := strings.TrimSpace(c.Target)
		if tgt == "" {
			tgt = src
		}
		targetToSource[tgt] = src
	}

	args := make([]interface{}, 0, len(insertCols))
	for _, targetCol := range insertCols {
		// 优先通过字段映射找到源列名
		if srcCol, ok := targetToSource[targetCol]; ok {
			if idx, ok2 := sourceIndex[srcCol]; ok2 {
				args = append(args, values[idx])
				continue
			}
		}
		// 若无映射，则尝试目标列名与源列名相同
		if idx, ok := sourceIndex[targetCol]; ok {
			args = append(args, values[idx])
			continue
		}
		// 找不到对应列，填 nil（通常是用户在配置中定义了常量列或目标多余列）
		args = append(args, nil)
	}
	return args
}

// ensureTargetTable 在目标库中确保表存在，若不存在则根据源列类型创建
func ensureTargetTable(ctx context.Context, dst *simpleDB, table string, colTypes []*sql.ColumnType, opts copyTableOptions) error {
	if table == "" {
		return fmt.Errorf("自动建表失败：目标表名为空")
	}

	exists, err := checkTableExists(ctx, dst, table)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}

	ddl, err := buildCreateTableDDL(table, colTypes, dst.cfg.Driver, opts)
	if err != nil {
		return err
	}
	log.Printf("目标库中不存在表 %s，将自动创建：\n%s\n", table, ddl)

	if opts.DryRun {
		return nil
	}

	if _, err := dst.db.ExecContext(ctx, ddl); err != nil {
		return fmt.Errorf("执行建表语句失败: %w", err)
	}
	return nil
}

// checkTableExists 简单判断目标库是否存在某表（不同驱动做最基础兼容）
func checkTableExists(ctx context.Context, dst *simpleDB, table string) (bool, error) {
	driver := normalizeDriver(dst.cfg.Driver)

	var query string
	var args []interface{}
	switch driver {
	case "postgres", "postgresql":
		query = `SELECT 1 FROM information_schema.tables WHERE table_schema = current_schema() AND table_name = $1`
		args = append(args, table)
	case "mysql":
		query = `SELECT 1 FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = ?`
		args = append(args, table)
	case "sqlite3":
		query = `SELECT 1 FROM sqlite_master WHERE type='table' AND name = ?`
		args = append(args, table)
	case "sqlserver", "mssql":
		// 使用 sys.tables + sys.schemas 检查表是否存在
		query = `SELECT 1
FROM sys.tables AS t
INNER JOIN sys.schemas AS s ON t.schema_id = s.schema_id
WHERE s.name = SCHEMA_NAME() AND t.name = @p1`
		args = append(args, table)
	case "oracle":
		query = `SELECT 1 FROM user_tables WHERE table_name = :1`
		args = append(args, strings.ToUpper(table))
	default:
		// 尝试简单 SELECT 1 FROM table LIMIT 1
		query = fmt.Sprintf("SELECT 1 FROM %s LIMIT 1", quoteIdent(table, driver))
		row := dst.db.QueryRowContext(ctx, query)
		var tmp int
		if err := row.Scan(&tmp); err != nil {
			return false, nil
		}
		return true, nil
	}

	row := dst.db.QueryRowContext(ctx, query, args...)
	var tmp int
	if err := row.Scan(&tmp); err != nil {
		return false, nil
	}
	return true, nil
}

// buildCreateTableDDL 根据源列类型及可选字段配置生成目标库建表语句（基础映射）
// 对于 MySQL，会自动优化大字段类型以避免行大小超过 65535 字节限制
func buildCreateTableDDL(table string, colTypes []*sql.ColumnType, driver string, opts copyTableOptions) (string, error) {
	if table == "" {
		return "", fmt.Errorf("表名不能为空")
	}
	if len(colTypes) == 0 {
		return "", fmt.Errorf("列信息为空，无法建表")
	}

	driver = normalizeDriver(driver)

	// 映射：源列名 => 自定义配置
	colCfg := make(map[string]columnMapping)
	for _, c := range opts.Columns {
		if strings.TrimSpace(c.Source) != "" {
			colCfg[c.Source] = c
		}
	}

	var colsDDL []string

	// MySQL 特殊处理：如果字段数超过 30 个，将所有 VARCHAR/CHAR 转为 TEXT
	// 这样可以避免行大小超过 65535 字节限制
	useTextForLargeFields := driver == "mysql" && len(colTypes) > 30
	if useTextForLargeFields {
		log.Printf("表 %s 字段数较多(%d个)，将自动将 VARCHAR/CHAR 转为 TEXT 以避免行大小限制\n", table, len(colTypes))
	}

	for _, ct := range colTypes {
		srcName := ct.Name()
		cfg, hasCfg := colCfg[srcName]

		targetName := srcName
		if hasCfg && strings.TrimSpace(cfg.Target) != "" {
			targetName = strings.TrimSpace(cfg.Target)
		}

		// 目标类型
		var targetType string
		if hasCfg && strings.TrimSpace(cfg.TargetType) != "" {
			targetType = strings.TrimSpace(cfg.TargetType)
		} else {
			targetType = mapColumnType(ct, driver)
		}

		// MySQL 特殊处理：如果字段数较多，将 VARCHAR/CHAR 转为 TEXT
		// 注意：TEXT 类型只占用 9-12 字节行内存储，不计入行大小限制
		if useTextForLargeFields && (strings.HasPrefix(targetType, "VARCHAR") || strings.HasPrefix(targetType, "CHAR")) {
			targetType = "TEXT"
		}

		nullable := true
		if hasCfg && cfg.Nullable != nil {
			nullable = *cfg.Nullable
		} else if n, ok := ct.Nullable(); ok {
			nullable = n
		}

		definition := quoteIdent(targetName, driver) + " " + targetType
		if !nullable {
			definition += " NOT NULL"
		}
		if hasCfg && strings.TrimSpace(cfg.DefaultValue) != "" {
			definition += " DEFAULT " + cfg.DefaultValue
		}

		colsDDL = append(colsDDL, definition)
	}

	ddl := fmt.Sprintf("CREATE TABLE %s (\n  %s\n)", quoteIdent(table, driver), strings.Join(colsDDL, ",\n  "))
	return ddl, nil
}

// estimateColumnSize 预估字段类型占用的字节数（MySQL）
func estimateColumnSize(dbType string) int {
	dbType = strings.ToUpper(dbType)
	switch {
	case strings.Contains(dbType, "TINYINT"):
		return 1
	case strings.Contains(dbType, "SMALLINT"):
		return 2
	case strings.Contains(dbType, "MEDIUMINT"):
		return 3
	case strings.Contains(dbType, "INT"):
		return 4
	case strings.Contains(dbType, "BIGINT"):
		return 8
	case strings.Contains(dbType, "FLOAT"):
		return 4
	case strings.Contains(dbType, "DOUBLE"):
		return 8
	case strings.Contains(dbType, "DECIMAL"), strings.Contains(dbType, "NUMERIC"):
		// DECIMAL 预估 20 字节
		return 20
	case strings.Contains(dbType, "DATE"):
		return 3
	case strings.Contains(dbType, "TIME"):
		return 3
	case strings.Contains(dbType, "DATETIME"), strings.Contains(dbType, "TIMESTAMP"):
		return 5
	case strings.Contains(dbType, "YEAR"):
		return 1
	case strings.Contains(dbType, "CHAR"):
		// 提取 CHAR(n) 中的 n
		var n int
		fmt.Sscanf(dbType, "CHAR(%d)", &n)
		if n <= 0 {
			n = 1
		}
		return n * 3 // utf8mb4 每个字符最多 4 字节，保守估计 3 字节
	case strings.Contains(dbType, "VARCHAR"):
		// 提取 VARCHAR(n) 中的 n
		var n int
		fmt.Sscanf(dbType, "VARCHAR(%d)", &n)
		if n <= 0 {
			n = 255
		}
		return n * 3 // utf8mb4 每个字符最多 4 字节，保守估计 3 字节
	case strings.Contains(dbType, "TINYTEXT"):
		return 255 // TEXT 类型只占用 9-12 字节行内存储
	case strings.Contains(dbType, "TEXT"):
		return 9 // TEXT 类型只占用 9-12 字节行内存储
	case strings.Contains(dbType, "MEDIUMTEXT"), strings.Contains(dbType, "LONGTEXT"):
		return 9 // TEXT 类型只占用 9-12 字节行内存储
	case strings.Contains(dbType, "TINYBLOB"):
		return 255
	case strings.Contains(dbType, "BLOB"):
		return 9
	case strings.Contains(dbType, "MEDIUMBLOB"), strings.Contains(dbType, "LONGBLOB"):
		return 9
	case strings.Contains(dbType, "BOOL"):
		return 1
	default:
		return 255 // 默认预估
	}
}

// mapColumnType 做一个非常基础的类型映射（仅适合 demo，要在生产中使用需进一步完善）
func mapColumnType(ct *sql.ColumnType, driver string) string {
	dbType := strings.ToUpper(ct.DatabaseTypeName())
	driver = normalizeDriver(driver)
	switch driver {
	case "postgres", "postgresql":
		switch {
		case strings.Contains(dbType, "BIGINT"):
			return "BIGINT"
		case strings.Contains(dbType, "INT"):
			// 统一用 BIGINT，避免从 MSSQL/Oracle 等同步大整数值时超出 integer 范围
			return "BIGINT"
		case strings.Contains(dbType, "DOUBLE"), strings.Contains(dbType, "FLOAT"), strings.Contains(dbType, "REAL"):
			return "DOUBLE PRECISION"
		case strings.Contains(dbType, "DECIMAL"), strings.Contains(dbType, "NUMERIC"):
			return "NUMERIC"
		case strings.Contains(dbType, "BOOL"):
			return "BOOLEAN"
		case strings.Contains(dbType, "DATE"), strings.Contains(dbType, "TIME"):
			return "TIMESTAMP"
		case strings.Contains(dbType, "TEXT"), strings.Contains(dbType, "CHAR"), strings.Contains(dbType, "CLOB"):
			return "TEXT"
		default:
			return "TEXT"
		}
	case "mysql":
		switch {
		case strings.Contains(dbType, "INT"):
			return "INT"
		case strings.Contains(dbType, "BIGINT"):
			return "BIGINT"
		case strings.Contains(dbType, "DOUBLE"), strings.Contains(dbType, "FLOAT"):
			return "DOUBLE"
		case strings.Contains(dbType, "DECIMAL"), strings.Contains(dbType, "NUMERIC"):
			return "DECIMAL(18,6)"
		case strings.Contains(dbType, "BOOL"), strings.Contains(dbType, "TINYINT(1)"):
			return "TINYINT(1)"
		case strings.Contains(dbType, "DATE"), strings.Contains(dbType, "TIME"):
			return "DATETIME"
		case strings.Contains(dbType, "TEXT"):
			return "TEXT"
		case strings.Contains(dbType, "CHAR"):
			return "VARCHAR(255)"
		default:
			return "TEXT"
		}
	case "sqlserver", "mssql":
		switch {
		case strings.Contains(dbType, "INT"):
			return "INT"
		case strings.Contains(dbType, "BIGINT"):
			return "BIGINT"
		case strings.Contains(dbType, "FLOAT"), strings.Contains(dbType, "REAL"):
			return "FLOAT(53)"
		case strings.Contains(dbType, "DECIMAL"), strings.Contains(dbType, "NUMERIC"):
			return "DECIMAL(18,6)"
		case strings.Contains(dbType, "BIT"):
			return "BIT"
		case strings.Contains(dbType, "DATE"), strings.Contains(dbType, "TIME"):
			return "DATETIME2"
		case strings.Contains(dbType, "CHAR"), strings.Contains(dbType, "TEXT"), strings.Contains(dbType, "NCHAR"), strings.Contains(dbType, "NVARCHAR"):
			return "NVARCHAR(MAX)"
		default:
			return "NVARCHAR(MAX)"
		}
	case "oracle":
		switch {
		case strings.Contains(dbType, "INT"), strings.Contains(dbType, "NUMBER"):
			return "NUMBER"
		case strings.Contains(dbType, "FLOAT"), strings.Contains(dbType, "BINARY_FLOAT"):
			return "BINARY_DOUBLE"
		case strings.Contains(dbType, "DATE"), strings.Contains(dbType, "TIMESTAMP"):
			return "TIMESTAMP"
		case strings.Contains(dbType, "CHAR"), strings.Contains(dbType, "VARCHAR"), strings.Contains(dbType, "CLOB"):
			return "CLOB"
		default:
			return "VARCHAR2(4000)"
		}
	default: // sqlite3 等
		switch {
		case strings.Contains(dbType, "INT"):
			return "INTEGER"
		case strings.Contains(dbType, "DOUBLE"), strings.Contains(dbType, "FLOAT"), strings.Contains(dbType, "REAL"):
			return "REAL"
		case strings.Contains(dbType, "DECIMAL"), strings.Contains(dbType, "NUMERIC"):
			return "NUMERIC"
		default:
			return "TEXT"
		}
	}
}

// buildInsertSQL 根据不同驱动类型生成 INSERT 语句
func buildInsertSQL(table string, columns []string, driver string) (string, error) {
	if table == "" {
		return "", fmt.Errorf("表名不能为空")
	}
	if len(columns) == 0 {
		return "", fmt.Errorf("列名不能为空")
	}

	colList := make([]string, len(columns))
	for i, c := range columns {
		colList[i] = quoteIdent(c, driver)
	}

	placeholder := make([]string, len(columns))
	switch normalizeDriver(driver) {
	case "postgres", "postgresql":
		for i := range placeholder {
			placeholder[i] = fmt.Sprintf("$%d", i+1)
		}
	case "oracle":
		for i := range placeholder {
			placeholder[i] = fmt.Sprintf(":%d", i+1)
		}
	default:
		for i := range placeholder {
			placeholder[i] = "?"
		}
	}

	sqlStr := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		quoteIdent(table, driver),
		strings.Join(colList, ", "),
		strings.Join(placeholder, ", "),
	)
	return sqlStr, nil
}

// quoteIdent 对表名/列名做简单转义（演示用，未覆盖所有情况）
func quoteIdent(name, driver string) string {
	name = strings.TrimSpace(name)
	if name == "" {
		return name
	}
	driver = normalizeDriver(driver)
	switch driver {
	case "postgres", "postgresql":
		if strings.HasPrefix(name, `"`) && strings.HasSuffix(name, `"`) {
			return name
		}
		return `"` + name + `"`
	case "sqlserver", "mssql":
		if strings.HasPrefix(name, "[") && strings.HasSuffix(name, "]") {
			return name
		}
		return "[" + name + "]"
	case "oracle":
		if strings.HasPrefix(name, `"`) && strings.HasSuffix(name, `"`) {
			return name
		}
		return `"` + strings.ToUpper(name) + `"`
	default:
		if strings.HasPrefix(name, "`") && strings.HasSuffix(name, "`") {
			return name
		}
		return "`" + name + "`"
	}
}

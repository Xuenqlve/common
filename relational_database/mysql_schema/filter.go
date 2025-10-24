package mysql_schema

import (
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/xuenqlve/common/compare"
	"github.com/xuenqlve/common/errors"
	"github.com/xuenqlve/common/log"
	"github.com/xuenqlve/common/match"
)

type TableConfigs []TableFilterConfig

func (c TableConfigs) Check() error {
	if len(c) == 0 {
		return fmt.Errorf("mysql input configure error need table-config")
	} else {
		for _, tableCfg := range c {
			if tableCfg.Database == "" {
				return fmt.Errorf("mysql input configure error need table-config.database")
			}
			if len(tableCfg.AcceptTableRegex) == 0 {
				return fmt.Errorf("mysql input configure error need table-config.accept-table-regex")
			}
		}
	}
	return nil
}

func (c TableConfigs) FilterConfig() []map[string]any {
	result := make([]map[string]any, 0, len(c))
	for _, cfg := range c {
		result = append(result, cfg.FilterConfig())
	}
	return result
}

type TableFilterConfig struct {
	Database         string              `mapstructure:"database,omitempty" json:"database,omitempty"`
	AcceptTableRegex []string            `mapstructure:"accept-table-regex,omitempty" json:"accept-table-regex,omitempty"`
	IgnoreTableRegex []string            `mapstructure:"ignore-table-regex,omitempty" json:"ignore-table-regex,omitempty"`
	TableCondition   map[string]string   `mapstructure:"table-condition,omitempty" json:"table-condition,omitempty"`
	TableScanColumn  map[string][]string `mapstructure:"table-scan-column,omitempty" json:"table-scan-column,omitempty"`
}

func (c *TableFilterConfig) FilterConfig() map[string]any {
	return map[string]any{
		"database":           c.Database,
		"accept-table-regex": c.AcceptTableRegex,
		"ignore-table-regex": c.IgnoreTableRegex,
	}
}

func (c *TableFilterConfig) MatchRegex(db string, tables []string) []string {
	if !match.Glob(c.Database, db) {
		return []string{}
	}
	result := make([]string, 0, len(tables))
	for _, table := range tables {
		if match.MatchRegex(table, c.IgnoreTableRegex) {
			continue
		}
		if !match.MatchRegex(table, c.AcceptTableRegex) {
			continue
		}
		result = append(result, table)
	}
	return result
}

func (c *TableFilterConfig) InitScanColumn(table *Table) error {
	if scanColumn, ok := c.TableScanColumn[table.Table]; ok {
		if compare.CompareSlicesEquality(table.PrimaryIndex, scanColumn) {
			table.SetScanColumns(scanColumn)
			return nil
		}
		for _, ukGuideColumns := range table.UniqueIndex {
			if compare.CompareSlicesEquality(ukGuideColumns, scanColumn) {
				table.SetScanColumns(scanColumn)
				return nil
			}
		}
		return errors.Errorf("table-config.table-scan-column table:%s.%s scan column must be pk/uk", table.Database, table.Table)
	}
	return nil
}

type LoadConfigSchema struct {
	cfg map[string]TableFilterConfig
	*Schema
}

func NewLoadConfigSchema(conn *sql.DB, cfgs []TableFilterConfig) *LoadConfigSchema {
	cfgMap := map[string]TableFilterConfig{}
	for _, cfg := range cfgs {
		cfgMap[cfg.Database] = cfg
	}
	return &LoadConfigSchema{
		Schema: NewSchema(conn),
		cfg:    cfgMap,
	}
}

func (s *LoadConfigSchema) filterTables() (map[string][]string, error) {
	databases := make([]string, 0, len(s.cfg))
	for _, cfg := range s.cfg {
		databases = append(databases, cfg.Database)
	}
	schema, err := GetTablesByDatabases(s.conn, databases)
	if err != nil {
		return nil, errors.Trace(err)
	}
	schemaMap := map[string][]string{}
	for database, tables := range schema {
		cfg := s.cfg[database]
		newTables := cfg.MatchRegex(database, tables)
		if len(newTables) != 0 {
			schemaMap[database] = newTables
		}
	}
	return schemaMap, nil
}

func (s *LoadConfigSchema) Tables(createSql bool) ([]*Table, error) {
	var mux sync.Mutex
	var wg sync.WaitGroup

	var tableDefErr error
	tableDefs := make([]*Table, 0)
	schema, err := s.filterTables()
	if err != nil {
		return nil, errors.Trace(err)
	}
	for database, tables := range schema {
		for _, table := range tables {
			cfg := s.cfg[database]
			wg.Add(1)
			go func(table string, config TableFilterConfig) {
				defer wg.Done()
				var tableDef *Table
				if tableDef, err = s.Schema.GetTableDef(database, table); err != nil {
					log.Warnf("get tableDef err:%v", err)
					tableDefErr = err
					return
				}
				condition, ok := config.TableCondition[table]
				if ok {
					tableDef.SetScanCondition(condition)
				}
				if err = config.InitScanColumn(tableDef); err != nil {
					log.Warnf("InitScanColumn err:%v", err)
					tableDefErr = err
					return
				}
				if createSql {
					var createTableSql string
					createTableSql, err = s.Schema.GetCreateTableSQL(database, table)
					if err != nil {
						log.Warnf("GetCreateTableSQL err:%v", err)
						tableDefErr = err
						return
					}
					tableDef.SetCreateTableSql(createTableSql)
				}
				mux.Lock()
				tableDefs = append(tableDefs, tableDef)
				mux.Unlock()
			}(table, cfg)
		}
	}
	wg.Wait()
	if tableDefErr != nil {
		return nil, errors.Trace(tableDefErr)
	}
	s.sortTables(tableDefs)
	return tableDefs, nil
}

func (s *LoadConfigSchema) sortTables(tables []*Table) {
	sort.Slice(tables, func(i, j int) bool {
		return strings.Compare(tables[i].Table, tables[j].Table) < 0
	})
}

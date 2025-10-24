package mongodb_schema

import (
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/xuenqlve/common/compare"
	"github.com/xuenqlve/common/errors"
	"github.com/xuenqlve/common/log"
	"github.com/xuenqlve/common/match"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

type TableConfigs []TableFilterConfig

func (c TableConfigs) Check() error {
	if len(c) == 0 {
		return fmt.Errorf("mongodb input configure error need table-config")
	} else {
		for _, docCfg := range c {
			if docCfg.Database == "" {
				return fmt.Errorf("mongodb input configure error need table-config.database")
			}
			if len(docCfg.AcceptTableRegex) == 0 {
				return fmt.Errorf("mongodb input configure error need table-config.accept-table-regex")
			}
		}
	}
	return nil
}

func (c TableConfigs) FilterConfig() []map[string]any {
	result := make([]map[string]any, 0, len(c))
	for _, docCfg := range c {
		result = append(result, docCfg.FilterConfig())
	}
	return result
}

type TableFilterConfig struct {
	Database         string              `mapstructure:"database" json:"database"`
	AcceptTableRegex []string            `mapstructure:"accept-table-regex" json:"accept-table-regex"`
	IgnoreTableRegex []string            `mapstructure:"ignore-table-regex" json:"ignore-table-regex"`
	TableCondition   map[string]string   `mapstructure:"table-condition" json:"table-condition"`
	TableScanColumn  map[string][]string `mapstructure:"table-scan-column" json:"table-scan-column"`
}

func (c *TableFilterConfig) FilterConfig() map[string]any {
	return map[string]any{
		"database":           c.Database,
		"accept-table-regex": c.AcceptTableRegex,
		"ignore-table-regex": c.IgnoreTableRegex,
	}
}

func (c *TableFilterConfig) MatchRegex(db string, documents []string) []string {
	if !match.Glob(c.Database, db) {
		return []string{}
	}
	result := make([]string, 0, len(documents))
	for _, document := range documents {
		if strings.HasPrefix(document, "system.") {
			continue
		}
		if match.MatchRegex(document, c.IgnoreTableRegex) {
			continue
		}
		if !match.MatchRegex(document, c.AcceptTableRegex) {
			continue
		}
		result = append(result, document)
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

func NewLoadConfigSchema(conn *mongo.Client, cfg TableConfigs) *LoadConfigSchema {
	cfgMap := map[string]TableFilterConfig{}
	for _, c := range cfg {
		cfgMap[c.Database] = c
	}
	return &LoadConfigSchema{
		Schema: NewSchema(conn),
		cfg:    cfgMap,
	}
}

func (s *LoadConfigSchema) filterTables() (map[string][]string, error) {
	nsMap := make(map[string][]string, len(s.cfg))
	for _, cfg := range s.cfg {
		db := cfg.Database
		colNames, err := s.conn.Database(db).ListCollectionNames(nil, bson.M{})
		if err != nil {
			err = fmt.Errorf("get collection names of mongodb db[%v] error: %v", db, err)
			return nil, err
		}
		newCols := cfg.MatchRegex(db, colNames)
		if len(newCols) != 0 {
			nsMap[db] = newCols
		}
	}
	return nsMap, nil
}

func (s *LoadConfigSchema) Tables() ([]*Table, error) {
	var mux sync.Mutex
	var wg sync.WaitGroup
	tableDefs := make([]*Table, 0)
	var tableDefErr error

	schema, err := s.filterTables()
	if err != nil {
		return nil, errors.Trace(err)
	}

	for db, cols := range schema {
		for _, col := range cols {
			cfg := s.cfg[db]
			wg.Add(1)
			go func(col string, config TableFilterConfig) {
				defer wg.Done()
				var tableDef *Table
				if tableDef, err = s.GetTable(db, col); err != nil {
					log.Warnf("get tableDef err:%v", err)
					tableDefErr = err
					return
				}
				condition, ok := config.TableCondition[col]
				if ok {
					tableDef.SetScanCondition(condition)
				}
				mux.Lock()
				tableDefs = append(tableDefs, tableDef)
				mux.Unlock()
			}(col, cfg)
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

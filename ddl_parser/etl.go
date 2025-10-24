package ddl_parser

import "github.com/xuenqlve/common/transform"

// 满足任意 filter 即通过
func ETL(stmt Statement, opts ...ETLOption) (skip bool) {
	for _, opt := range opts {
		if opt.Extract(stmt) {
			if opt.Transform(stmt) {
				return true
			}
		}
	}
	return false
}

type ETLOption interface {
	Extract(stmt Statement) (hit bool)
	Transform(stmt Statement) (skip bool)
}

func WithRemoveColumn(database, table, column string) ETLOption {
	return &removeColumnETL{
		database: database,
		table:    table,
		column:   column,
	}
}

type removeColumnETL struct {
	database string
	table    string
	column   string
}

func (etl *removeColumnETL) Extract(stmt Statement) (hit bool) {
	if ds, ok := stmt.(createTableStatement); ok {
		if ds.Database() == etl.database && ds.Table() == etl.table {
			colsMap := transform.StringSliceToMap(ds.Columns())
			if _, ok = colsMap[etl.column]; ok {
				return true
			}
		}
	}
	if ds, ok := stmt.(tableConstraintsStatement); ok {
		if ds.Database() == etl.database && ds.Table() == etl.table {
			_, cols := ds.IndexColumns()
			colsMap := transform.StringSliceToMap(cols)
			if _, ok = colsMap[etl.column]; ok {
				return true
			}
		}
	}
	if ds, ok := stmt.(alterTableStatement); ok {
		if ds.Database() == etl.database && ds.Table() == etl.table {
			return ds.ExistColumn(etl.column)
		}
	}
	return false
}

func (etl *removeColumnETL) Transform(stmt Statement) (skip bool) {
	if ds, ok := stmt.(createTableStatement); ok {
		if len(ds.Columns()) == 1 {
			return true
		}
		ds.RemoveColumn(etl.column)
	}

	if ds, ok := stmt.(tableConstraintsStatement); ok {
		_, column := ds.IndexColumns()
		if len(column) == 1 {
			return true
		}
		ds.RemoveColumn(etl.column)
	}

	if ds, ok := stmt.(alterTableStatement); ok {
		if ds.FilterRemoveColumn(etl.column) {
			return true
		}
		ds.RemoveColumn(etl.column)
	}
	return false
}

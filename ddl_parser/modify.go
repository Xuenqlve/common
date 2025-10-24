package ddl_parser

import (
	"github.com/xuenqlve/common/transform"
)

type ModifyOption interface {
	Priority() int
	Apply(Statement)
}

const (
	HighestPriority = iota
	FirstPriority
	SecondPriority
	LowestPriority
)

func Modify(stmt Statement, opts ...ModifyOption) {
	highestPriorityList, firstPriorityList, secondPriorityList, lowestPriorityList := []ModifyOption{}, []ModifyOption{}, []ModifyOption{}, []ModifyOption{}
	for _, opt := range opts {
		switch opt.Priority() {
		case HighestPriority:
			highestPriorityList = append(highestPriorityList, opt)
		case FirstPriority:
			firstPriorityList = append(firstPriorityList, opt)
		case SecondPriority:
			secondPriorityList = append(secondPriorityList, opt)
		default:
			lowestPriorityList = append(lowestPriorityList, opt)
		}
	}
	for _, opt := range highestPriorityList {
		opt.Apply(stmt)
	}
	for _, opt := range firstPriorityList {
		opt.Apply(stmt)
	}
	for _, opt := range secondPriorityList {
		opt.Apply(stmt)
	}
	for _, opt := range lowestPriorityList {
		opt.Apply(stmt)
	}

	return
}

func WithDatabaseRename(old, new string) ModifyOption {
	return &databaseRenameOption{
		old: old,
		new: new,
	}
}

type databaseRenameOption struct {
	old string
	new string
}

func (o *databaseRenameOption) Priority() int {
	return LowestPriority
}

func (o *databaseRenameOption) Apply(s Statement) {
	if ds, ok := s.(databaseStatement); ok {
		ds.ReplaceDatabase(o.old, o.new)
	}
	if rts, ok := s.(renameTableStatement); ok {
		var database, table string
		database, table = rts.OldTable()
		if database == o.old {
			rts.ReplaceOldTable(o.new, table)
		}
		database, table = rts.NewTable()
		if database == o.old {
			rts.ReplaceNewTable(o.new, table)
		}
	}

	if ads, ok := s.(alterTableStatement); ok {
		if ads.AlterType() == string(RenameTable) {
			database, table := ads.RenameNewTable()
			if database == o.old {
				ads.ReplaceNewTable(o.new, table)
			}
		}
	}
	return
}

func WithTableRename(database string, old, new string) ModifyOption {
	return &tableRenameOption{
		database: database,
		old:      old,
		new:      new,
	}
}

type tableRenameOption struct {
	database string
	old      string
	new      string
}

func (o *tableRenameOption) Priority() int {
	return SecondPriority
}

func (o *tableRenameOption) Apply(s Statement) {
	if tds, ok := s.(tableStatement); ok {
		if tds.Database() == o.database && tds.Table() == o.old {
			tds.ReplaceTable(o.old, o.new)
		}
	}

	if ads, ok := s.(alterTableStatement); ok {
		if ads.AlterType() == string(RenameTable) {
			database, table := ads.RenameNewTable()
			if database == o.database && table == o.old {
				ads.ReplaceNewTable(database, o.new)
			}
		}
	}

	if rts, ok := s.(renameTableStatement); ok {
		var database, table string
		database, table = rts.OldTable()
		if database == o.database && table == o.old {
			rts.ReplaceOldTable(database, o.new)
		}
		database, table = rts.NewTable()
		if database == o.database && table == o.old {
			rts.ReplaceNewTable(database, o.new)
		}
	}
	return
}

func WithColumnRename(database, table, oldColumn, newColumn string) ModifyOption {
	return &columnRenameOption{
		database:  database,
		table:     table,
		oldColumn: oldColumn,
		newColumn: newColumn,
	}
}

type columnRenameOption struct {
	database  string
	table     string
	oldColumn string
	newColumn string
}

func (o *columnRenameOption) Priority() int {
	return FirstPriority
}

func (o *columnRenameOption) Apply(s Statement) {
	if ds, ok := s.(createTableStatement); ok {
		if ds.Database() == o.database && ds.Table() == o.table {
			colsMap := transform.StringSliceToMap(ds.Columns())
			if _, ok = colsMap[o.oldColumn]; ok {
				ds.ReplaceColumn(o.oldColumn, o.newColumn)
			}
		}
	}

	if ds, ok := s.(tableConstraintsStatement); ok {
		if ds.Database() == o.database && ds.Table() == o.table {
			ds.ReplaceColumn(o.oldColumn, o.newColumn)
		}
	}

	if ds, ok := s.(alterTableStatement); ok {
		if ds.Database() == o.database && ds.Table() == o.table {
			ds.ReplaceColumn(o.oldColumn, o.newColumn)
		}
	}

	return
}

func RemoveColumn(database, table, column string) ModifyOption {
	return &columnRemoveOption{
		database: database,
		table:    table,
		column:   column,
	}
}

type columnRemoveOption struct {
	database string
	table    string
	column   string
}

func (o *columnRemoveOption) Priority() int {
	return FirstPriority
}

func (o *columnRemoveOption) Apply(s Statement) {
	if ds, ok := s.(createTableStatement); ok {
		if ds.Database() == o.database && ds.Table() == o.table {
			colsMap := transform.StringSliceToMap(ds.Columns())
			if _, ok = colsMap[o.column]; ok {
				ds.RemoveColumn(o.column)
			}
		}
	}

	if ds, ok := s.(tableConstraintsStatement); ok {
		if ds.Database() == o.database && ds.Table() == o.table {
			_, cols := ds.IndexColumns()
			colsMap := transform.StringSliceToMap(cols)
			if _, ok = colsMap[o.column]; ok {
				ds.RemoveColumn(o.column)
			}
		}
	}

	if ds, ok := s.(alterTableStatement); ok {
		if ds.Database() == o.database && ds.Table() == o.table {
			ds.RemoveColumn(o.column)
		}
	}

	return
}

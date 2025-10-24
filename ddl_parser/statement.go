package ddl_parser

import (
	"github.com/xuenqlve/common/schema_store"
)

type DDL struct {
	Schema string
	SQL    any
}

type Loader interface {
	Parse(ddl DDL) ([]Statement, error)
}

type Statement interface {
	DDLType() schema_store.DDL
	SubmitModification(event int, parameter any)
	Metadata() Metadata
	GenerateSQL() (any, error)
}

type Metadata struct {
	Database string
	Table    string

	RenameDatabase string
	RenameTable    string
}

const (
	ReplaceDatabase = iota

	// relational database && nosql
	ReplaceTable
	ReplaceRenameTableSchemaMap
	ReplaceTableColumnMap
	ReplaceTableConstraintsMap
	RemoveTableColumn
	RemoveTableConstraintsColumn
	ReplaceAlterTable
)

func NewDatabaseStatement(stmt Statement, database string, resetDb bool) *DatabaseStatement {
	return &DatabaseStatement{
		Statement: stmt,
		database:  database,
		resetDb:   resetDb,
		dirty:     false,
	}
}

type databaseStatement interface {
	Statement
	Database() string
	ReplaceDatabase(old, new string)
}

type tableStatement interface {
	databaseStatement
	Table() string
	ReplaceTable(old, new string)
}

type renameTableStatement interface {
	Statement
	OldTable() (string, string)
	ReplaceOldTable(database string, table string)
	NewTable() (string, string)
	ReplaceNewTable(database string, table string)
}

type createTableStatement interface {
	tableStatement
	Columns() []string
	ReplaceColumn(old, new string)
	RemoveColumn(column string)
}

type tableConstraintsStatement interface {
	tableStatement
	IndexColumns() (string, []string)
	ReplaceColumn(old, new string)
	RemoveColumn(column string)
}

type alterTableStatement interface {
	tableStatement

	AlterType() string

	FilterRemoveColumn(column string) bool
	ExistColumn(column string) bool

	ReplaceColumn(old, new string)
	RemoveColumn(column string)

	RenameNewTable() (string, string)
	ReplaceNewTable(database, table string)
}

// create_database , drop database
type DatabaseStatement struct {
	database string
	dirty    bool
	resetDb  bool
	Statement
}

func (s *DatabaseStatement) Database() string {
	return s.database
}

func (s *DatabaseStatement) ReplaceDatabase(old, new string) {
	if s.database == old {
		s.database = new
		s.dirty = true
	}
	return
}

func (s *DatabaseStatement) GenerateSQL() (any, error) {
	if s.dirty || s.resetDb {
		s.SubmitModification(ReplaceDatabase, s.database)
	}
	return s.Statement.GenerateSQL()
}

func NewTableStatement(stmt Statement, database string, resetDb bool, table string) *TableStatement {
	return &TableStatement{
		databaseStatement: NewDatabaseStatement(stmt, database, resetDb),
		table:             table,
		dirty:             false,
	}
}

// truncate table drop_index
type TableStatement struct {
	databaseStatement
	table string
	dirty bool
}

func (s *TableStatement) Table() string {
	return s.table
}

func (s *TableStatement) ReplaceTable(old, new string) {
	if s.table == old {
		s.table = new
		s.dirty = true
	}
	return
}

func (s *TableStatement) GenerateSQL() (any, error) {
	if s.dirty {
		s.SubmitModification(ReplaceTable, s.table)
	}
	return s.databaseStatement.GenerateSQL()
}

type CreateTableColumnStatement struct {
	tableStatement
	columns     []string
	constraints map[string][]string
	indexesType map[string]IndexType

	replaceConstraints map[string]map[string]string
	replaceColumn      map[string]string

	removeColumn     []string
	removeConstraint map[string][]string

	dirty bool
}

type IndexType int

var (
	IndexTypePrimary    IndexType = 1
	IndexTypeUnique     IndexType = 2
	IndexTypeConstraint IndexType = 3
)

func NewCreateTableColumnStatement(stmt Statement, database string, resetDb bool, table string, columns []string, constraints map[string][]string, indexesType map[string]IndexType) *CreateTableColumnStatement {
	return &CreateTableColumnStatement{
		tableStatement:     NewTableStatement(stmt, database, resetDb, table),
		columns:            columns,
		constraints:        constraints,
		indexesType:        indexesType,
		replaceConstraints: map[string]map[string]string{},
		replaceColumn:      map[string]string{},
		removeColumn:       []string{},
		removeConstraint:   map[string][]string{},
		dirty:              false,
	}
}

func (s *CreateTableColumnStatement) Columns() []string {
	return s.columns
}

func (s *CreateTableColumnStatement) Constraints() map[string][]string {
	return s.constraints
}

func (s *CreateTableColumnStatement) Indexes() map[string]IndexType {
	return s.indexesType
}

func (s *CreateTableColumnStatement) ReplaceColumn(old, new string) {
	newColumns := make([]string, 0, len(s.columns))
	exist := false
	for _, col := range s.columns {
		if col == old {
			newColumns = append(newColumns, new)
			exist = true
		} else {
			newColumns = append(newColumns, col)
		}
	}
	if exist {
		s.dirty = true
		s.replaceColumn[old] = new
		s.columns = newColumns
	}
	for index, columns := range s.constraints {
		newCols := make([]string, 0, len(columns))
		hit := false
		for _, col := range columns {
			if col == old {
				newCols = append(newCols, new)
				hit = true
			} else {
				newCols = append(newCols, col)
			}
		}
		if hit {
			s.constraints[index] = newCols
			s.replaceConstraints[index] = map[string]string{
				old: new,
			}
			s.dirty = true
		}
	}
	return
}

func (s *CreateTableColumnStatement) RemoveColumn(column string) {
	newColumns := make([]string, 0, len(s.columns))
	hit := false
	for _, col := range s.columns {
		if col != column {
			newColumns = append(newColumns, col)
		} else {
			hit = true
		}
	}
	if hit {
		s.columns = newColumns
		s.removeColumn = append(s.removeColumn, column)
		s.dirty = true
	}

	for index, columns := range s.constraints {
		newCols := make([]string, 0, len(columns))
		hit = false
		for _, col := range columns {
			if col == column {
				hit = true
			} else {
				newCols = append(newCols, col)
			}
		}
		if hit {
			s.constraints[index] = newCols
			s.removeConstraint[index] = []string{
				column,
			}
			s.dirty = true
		}
	}

	return
}

func (s *CreateTableColumnStatement) GenerateSQL() (any, error) {
	if s.dirty {
		if len(s.replaceConstraints) > 0 {
			s.SubmitModification(ReplaceTableColumnMap, s.replaceColumn)
		}
		if len(s.replaceConstraints) > 0 {
			s.SubmitModification(ReplaceTableConstraintsMap, s.replaceConstraints)
		}
		if len(s.removeColumn) > 0 {
			s.SubmitModification(RemoveTableColumn, s.removeColumn)
		}
		if len(s.removeConstraint) > 0 {
			s.SubmitModification(RemoveTableConstraintsColumn, s.removeConstraint)
		}
	}
	return s.tableStatement.GenerateSQL()
}

type Table struct {
	Database string
	Table    string
}

func NewRenameTableStatement(stmt Statement, old, new Table, resetDb bool) *RenameTableStatement {
	return &RenameTableStatement{
		Statement: stmt,
		oldTable:  old,
		newTable:  new,
		resetDb:   resetDb,
	}
}

type RenameTableStatement struct {
	Statement
	oldTable Table
	newTable Table
	resetDb  bool
	dirty    bool
}

func (s *RenameTableStatement) OldTable() (string, string) {
	return s.oldTable.Database, s.oldTable.Table
}

func (s *RenameTableStatement) ReplaceOldTable(database string, table string) {
	s.oldTable.Database = database
	s.oldTable.Table = table
	s.dirty = true
	return
}

func (s *RenameTableStatement) NewTable() (string, string) {
	return s.newTable.Database, s.newTable.Table
}

func (s *RenameTableStatement) ReplaceNewTable(database string, table string) {
	s.newTable.Database = database
	s.newTable.Table = table
	s.dirty = true
	return
}

func (s *RenameTableStatement) GenerateSQL() (any, error) {
	if s.dirty || s.resetDb {
		s.SubmitModification(ReplaceRenameTableSchemaMap, map[Table]Table{s.oldTable: s.newTable})
	}
	return s.Statement.GenerateSQL()
}

type TableConstraintsStatement struct {
	tableStatement
	key           string
	indexType     IndexType
	columns       []string
	removeColumns []string
	replaceColumn map[string]string
	dirty         bool
}

func NewTableConstraintsStatement(stmt Statement, database string, resetDb bool, table, key string, columns []string, indexType IndexType) *TableConstraintsStatement {
	return &TableConstraintsStatement{
		tableStatement: NewTableStatement(stmt, database, resetDb, table),
		key:            key,
		columns:        columns,
		indexType:      indexType,
		removeColumns:  []string{},
		replaceColumn:  map[string]string{},
		dirty:          false,
	}
}

func (s *TableConstraintsStatement) IndexType() IndexType {
	return s.indexType
}

func (s *TableConstraintsStatement) IndexColumns() (string, []string) {
	return s.key, s.columns
}

func (s *TableConstraintsStatement) ReplaceColumn(old, new string) {
	newColumns := make([]string, 0, len(s.columns))
	exist := false
	for _, col := range s.columns {
		if col == old {
			newColumns = append(newColumns, new)
			exist = true
		} else {
			newColumns = append(newColumns, col)
		}
	}
	if exist {
		s.dirty = true
		s.columns = newColumns
		s.replaceColumn[old] = new
	}
	return
}

func (s *TableConstraintsStatement) RemoveColumn(column string) {
	newColumns := make([]string, 0, len(s.columns))
	hit := false
	for _, col := range s.columns {
		if col != column {
			newColumns = append(newColumns, col)
		} else {
			hit = true
		}
	}
	if hit {
		s.columns = newColumns
		s.removeColumns = append(s.removeColumns, column)
		s.dirty = true
	}
	return
}

func (s *TableConstraintsStatement) GenerateSQL() (any, error) {
	if s.dirty {
		if len(s.replaceColumn) > 0 {
			s.SubmitModification(ReplaceTableColumnMap, s.replaceColumn)
		}
		if len(s.removeColumns) > 0 {
			s.SubmitModification(RemoveTableColumn, s.removeColumns)
		}
	}
	return s.tableStatement.GenerateSQL()
}

type AlterTableStatement struct {
	tableStatement
	spec AlterSpec

	replaceColumn map[string]string
	removeColumn  []string
	dirty         bool
}

type AlterType string

func (t AlterType) String() string {
	return string(t)
}

const (
	RenameTable   AlterType = "RENAME TABLE"
	AddColumn     AlterType = "ADD COLUMN"
	DropColumn    AlterType = "DROP COLUMN"
	ModifyColumn  AlterType = "MODIFY COLUMN"
	ChangeColumn  AlterType = "CHANGE COLUMN"
	RenameColumn  AlterType = "RENAME COLUMN"
	AlterColumn   AlterType = "ALTER TABLE"
	AddConstraint AlterType = "ADD CONSTRAINT"
	DropIndex     AlterType = "DROP INDEX"
	RenameIndex   AlterType = "RENAME INDEX"
)

type AlterSpec struct {
	// add、change、 drop、 modify
	Type AlterType

	// change table
	NewTable Table

	// add modify change
	Columns []string

	// change、drop
	OldColumn string
	// rename column
	NewColumn string

	IndexName        string
	ConstraintColumn []string
	IndexType        IndexType

	// rename index
	OldConstraint string
	NewConstraint string
}

func NewAlterTableStatement(stmt Statement, database string, resetDb bool, table string, spec AlterSpec) *AlterTableStatement {
	return &AlterTableStatement{
		tableStatement: NewTableStatement(stmt, database, resetDb, table),
		spec:           spec,
		replaceColumn:  map[string]string{},
		removeColumn:   []string{},
		dirty:          false,
	}
}

func (s *AlterTableStatement) AlterSpec() AlterSpec {
	return s.spec
}

func (s *AlterTableStatement) AlterType() string {
	return string(s.spec.Type)
}

func (s *AlterTableStatement) ReplaceColumn(old, new string) {
	switch s.spec.Type {
	case AddColumn, ModifyColumn, AlterColumn:
		cols := make([]string, 0, len(s.spec.Columns))
		hit := false
		for _, col := range s.spec.Columns {
			if col == old {
				hit = true
				cols = append(cols, new)
			} else {
				cols = append(cols, col)
			}
		}
		if hit {
			s.dirty = true
			s.replaceColumn[old] = new
			s.spec.Columns = cols
		}
	case DropColumn:
		if s.spec.OldColumn == old {
			s.dirty = true
			s.spec.OldColumn = new
			s.replaceColumn[old] = new
		}
	case ChangeColumn:
		cols := make([]string, 0, len(s.spec.Columns))
		hit := false
		for _, col := range s.spec.Columns {
			if col == old {
				hit = true
				cols = append(cols, new)
			} else {
				cols = append(cols, col)
			}
		}
		if hit {
			s.dirty = true
			s.replaceColumn[old] = new
			s.spec.Columns = cols
		}
		if s.spec.OldColumn == old {
			s.dirty = true
			s.spec.OldColumn = new
			s.replaceColumn[old] = new
		}
	case RenameColumn:
		if s.spec.OldColumn == old {
			s.spec.OldColumn = new
			s.dirty = true
			s.replaceColumn[old] = new
		}
		if s.spec.NewColumn == old {
			s.spec.NewColumn = new
			s.dirty = true
			s.replaceColumn[old] = new
		}
	case AddConstraint:
		cols := make([]string, 0, len(s.spec.Columns))
		hit := false
		for _, col := range s.spec.ConstraintColumn {
			if col == old {
				hit = true
				cols = append(cols, new)
			} else {
				cols = append(cols, col)
			}
		}
		if hit {
			s.dirty = true
			s.replaceColumn[old] = new
			s.spec.ConstraintColumn = cols
		}
	}
}

func (s *AlterTableStatement) RemoveColumn(column string) {
	switch s.spec.Type {
	case AddColumn, ModifyColumn, AlterColumn:
		cols := make([]string, 0, len(s.spec.Columns))
		hit := false
		for _, col := range s.spec.Columns {
			if col == column {
				hit = true
			} else {
				cols = append(cols, col)
			}
		}
		if hit {
			s.dirty = true
			s.spec.Columns = cols
			s.removeColumn = append(s.removeColumn, column)
		}
	case AddConstraint:
		cols := make([]string, 0, len(s.spec.Columns))
		hit := false
		for _, col := range s.spec.ConstraintColumn {
			if col == column {
				hit = true
			} else {
				cols = append(cols, col)
			}
		}
		if hit {
			s.dirty = true
			s.spec.ConstraintColumn = cols
			s.removeColumn = append(s.removeColumn, column)
		}
	}
}

func (s *AlterTableStatement) ExistColumn(column string) bool {
	for _, col := range s.spec.Columns {
		if col == column {
			return true
		}
	}
	if s.spec.OldColumn == column {
		return true
	}
	if s.spec.NewColumn == column {
		return true
	}

	for _, col := range s.spec.ConstraintColumn {
		if col == column {
			return true
		}
	}
	return false
}

func (s *AlterTableStatement) FilterRemoveColumn(column string) bool {
	switch s.spec.Type {
	case AddColumn, AlterColumn, ModifyColumn:
		if len(s.spec.Columns) == 1 {
			return true
		}
	case AddConstraint:
		if len(s.spec.ConstraintColumn) == 1 {
			return true
		}
	case DropColumn:
		if s.spec.OldColumn == column {
			return true
		}
	case ChangeColumn:
		for _, col := range s.spec.Columns {
			if col == column {
				return true
			}
		}
		if s.spec.OldColumn == column {
			return true
		}
	case RenameColumn:
		if s.spec.OldColumn == column {
			return true
		}
		if s.spec.NewColumn == column {
			return true
		}
	}
	return false
}

func (s *AlterTableStatement) RenameNewTable() (string, string) {
	if s.spec.Type == RenameTable {
		return s.spec.NewTable.Database, s.spec.NewTable.Table
	}
	return "", ""
}

func (s *AlterTableStatement) ReplaceNewTable(database, table string) {
	if s.spec.Type == RenameTable {
		s.spec.NewTable.Database = database
		s.spec.NewTable.Table = table
		s.dirty = true
		return
	}
}

func (s *AlterTableStatement) GenerateSQL() (any, error) {
	if s.dirty {
		if len(s.replaceColumn) > 0 {
			s.SubmitModification(ReplaceTableColumnMap, s.replaceColumn)
		}
		if len(s.removeColumn) > 0 {
			s.SubmitModification(RemoveTableColumn, s.removeColumn)
		}
	}
	if s.spec.Type == RenameTable {
		s.SubmitModification(ReplaceAlterTable, s.spec.NewTable)
	}
	return s.tableStatement.GenerateSQL()
}

package ddl_parser

import (
	"fmt"
	"strings"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
	_ "github.com/pingcap/tidb/pkg/parser/test_driver"
	"github.com/xuenqlve/common/ddl_parser"
	"github.com/xuenqlve/common/log"
	"github.com/xuenqlve/common/schema_store"
	"github.com/xuenqlve/common/transform"
)

func NewPingCapLoader() PingCapLoader {
	return PingCapLoader{
		parser: parser.New(),
	}
}

type PingCapLoader struct {
	parser *parser.Parser
}

func (p *PingCapLoader) Parse(ddl ddl_parser.DDL) ([]ddl_parser.Statement, error) {
	sql, ok := ddl.SQL.(string)
	if !ok {
		return nil, fmt.Errorf("sql:%s failed to parse ddl_parser statement", ddl.SQL)
	}
	stmt, err := p.parser.ParseOneStmt(sql, "", "")
	if err != nil {
		return nil, fmt.Errorf("sql:%s failed to parse ddl_parser statement: %s", ddl.SQL, err.Error())
	}
	return p.makeStatement(stmt, ddl)
}

func (p *PingCapLoader) makeStatement(stmt ast.StmtNode, ddl ddl_parser.DDL) ([]ddl_parser.Statement, error) {
	switch v := stmt.(type) {
	case *ast.CreateDatabaseStmt:
		database := v.Name.String()
		resetDb := false
		if database == "" {
			database = ddl.Schema
			resetDb = true
		}
		md := ddl_parser.Metadata{
			Database: database,
		}
		return []ddl_parser.Statement{ddl_parser.NewDatabaseStatement(newPingCapStatement(schema_store.CREATE_DATABASE, v, md), database, resetDb)}, nil
	case *ast.DropDatabaseStmt:
		database := v.Name.String()
		resetDb := false
		if database == "" {
			database = ddl.Schema
			resetDb = true
		}
		md := ddl_parser.Metadata{
			Database: database,
		}
		return []ddl_parser.Statement{ddl_parser.NewDatabaseStatement(newPingCapStatement(schema_store.DROP_DATABASE, v, md), database, resetDb)}, nil
	case *ast.CreateTableStmt:
		database := v.Table.Schema.String()
		resetDb := false
		if database == "" {
			database = ddl.Schema
			resetDb = true
		}
		table := v.Table.Name.String()
		columns := make([]string, 0, len(v.Cols))
		for _, col := range v.Cols {
			columns = append(columns, col.Name.String())
		}
		constraints := make(map[string][]string, len(v.Constraints))
		indexesType := make(map[string]ddl_parser.IndexType, len(v.Constraints))
		for _, constraint := range v.Constraints {
			cols := make([]string, 0, len(constraint.Keys))
			for _, key := range constraint.Keys {
				cols = append(cols, key.Column.Name.String())
			}
			name := constraint.Name
			if constraint.Tp == ast.ConstraintPrimaryKey {
				name = "PRIMARY"
			}
			constraints[name] = cols
			indexesType[name] = getIndexType(constraint.Tp)
		}
		md := ddl_parser.Metadata{
			Database: database,
			Table:    table,
		}
		return []ddl_parser.Statement{ddl_parser.NewCreateTableColumnStatement(newPingCapStatement(schema_store.CREATE_TABLE, v, md), database, resetDb, table, columns, constraints, indexesType)}, nil
	case *ast.AlterTableStmt:
		stmts := make([]ddl_parser.Statement, 0, len(v.Specs))
		s := *v
		for _, specs := range v.Specs {
			sp := *specs
			stmts = append(stmts, alterTableStatement(s, sp, ddl))
		}
		return stmts, nil
	case *ast.DropTableStmt:
		stmts := make([]ddl_parser.Statement, 0, len(v.Tables))
		s := *v
		for _, table := range v.Tables {
			t := *table
			stmts = append(stmts, dropTableStatement(s, t, ddl))
		}
		return stmts, nil
	case *ast.RenameTableStmt:
		stmts := make([]ddl_parser.Statement, 0, len(v.TableToTables))
		s := *v
		for _, tableToTable := range v.TableToTables {
			t := *tableToTable
			stmts = append(stmts, renameTableStatement(s, t, ddl))
		}
		return stmts, nil
	case *ast.TruncateTableStmt:
		database, table := v.Table.Schema.String(), v.Table.Name.String()
		resetDb := false
		if database == "" {
			database = ddl.Schema
			resetDb = true
		}
		md := ddl_parser.Metadata{
			Database: database,
			Table:    table,
		}
		return []ddl_parser.Statement{ddl_parser.NewTableStatement(newPingCapStatement(schema_store.TRUNCATE_TABLE, v, md), database, resetDb, table)}, nil
	case *ast.CreateIndexStmt:
		columns := make([]string, 0, len(v.IndexPartSpecifications))
		for _, index := range v.IndexPartSpecifications {
			columns = append(columns, index.Column.Name.String())
		}
		database, table := v.Table.Schema.String(), v.Table.Name.String()
		resetDb := false
		if database == "" {
			database = ddl.Schema
			resetDb = true
		}
		indexType := ddl_parser.IndexTypeConstraint
		if v.KeyType == ast.IndexKeyTypeUnique {
			indexType = ddl_parser.IndexTypeUnique
		}

		md := ddl_parser.Metadata{
			Database: database,
			Table:    table,
		}
		return []ddl_parser.Statement{ddl_parser.NewTableConstraintsStatement(newPingCapStatement(schema_store.CREATE_INDEX, v, md), database, resetDb, table, v.IndexName, columns, indexType)}, nil
	case *ast.DropIndexStmt:
		database, table := v.Table.Schema.String(), v.Table.Name.String()
		resetDb := false
		if database == "" {
			database = ddl.Schema
			resetDb = true
		}
		md := ddl_parser.Metadata{
			Database: database,
			Table:    table,
		}
		return []ddl_parser.Statement{ddl_parser.NewTableConstraintsStatement(newPingCapStatement(schema_store.DROP_INDEX, v, md), database, resetDb, table, v.IndexName, []string{}, ddl_parser.IndexTypeConstraint)}, nil
	default:
		log.Warnf("not supported statement type: %T", stmt)
		return []ddl_parser.Statement{}, nil
	}
}

func alterTableStatement(stmt ast.AlterTableStmt, alterSpec ast.AlterTableSpec, ddl ddl_parser.DDL) ddl_parser.Statement {
	stmt.Specs = []*ast.AlterTableSpec{
		&alterSpec,
	}
	database, table := stmt.Table.Schema.String(), stmt.Table.Name.String()
	resetDb := false
	if database == "" {
		database = ddl.Schema
		resetDb = true
	}
	spec := ddl_parser.AlterSpec{}
	md := ddl_parser.Metadata{
		Database: database,
		Table:    table,
	}

	switch alterSpec.Tp {
	case ast.AlterTableRenameTable:
		spec.Type = ddl_parser.RenameTable
		renameDatabase := alterSpec.NewTable.Schema.String()
		renameTable := alterSpec.NewTable.Name.String()
		if renameDatabase == "" {
			renameDatabase = database
		}
		spec.NewTable.Database = renameDatabase
		spec.NewTable.Table = renameTable
		md.RenameDatabase = renameDatabase
		md.RenameTable = renameTable
	case ast.AlterTableAddColumns:
		addColumns := make([]string, 0, len(alterSpec.NewColumns))
		for _, column := range alterSpec.NewColumns {
			addColumns = append(addColumns, column.Name.String())
		}
		spec.Type = ddl_parser.AddColumn
		spec.Columns = addColumns
	case ast.AlterTableModifyColumn:
		spec.Type = ddl_parser.ModifyColumn
		cols := make([]string, 0, len(alterSpec.NewColumns))
		for _, col := range alterSpec.NewColumns {
			cols = append(cols, col.Name.String())
		}
		spec.Columns = cols
	case ast.AlterTableChangeColumn:
		spec.Type = ddl_parser.ChangeColumn
		cols := make([]string, 0, len(alterSpec.NewColumns))
		for _, col := range alterSpec.NewColumns {
			cols = append(cols, col.Name.String())
		}
		spec.Columns = cols
		spec.OldColumn = alterSpec.OldColumnName.String()
	case ast.AlterTableDropColumn:
		spec.Type = ddl_parser.DropColumn
		spec.OldColumn = alterSpec.OldColumnName.String()
	case ast.AlterTableRenameColumn:
		spec.Type = ddl_parser.RenameColumn
		spec.NewColumn = alterSpec.NewColumnName.String()
		spec.OldColumn = alterSpec.OldColumnName.String()
	case ast.AlterTableAlterColumn:
		spec.Type = ddl_parser.AlterColumn
		cols := make([]string, 0, len(alterSpec.NewColumns))
		for _, col := range alterSpec.NewColumns {
			cols = append(cols, col.Name.String())
		}
		spec.Columns = cols
	case ast.AlterTableAddConstraint:
		spec.Type = ddl_parser.AddConstraint
		cols := []string{}
		for _, col := range alterSpec.Constraint.Keys {
			cols = append(cols, col.Column.Name.String())
		}
		spec.Type = ddl_parser.AddConstraint
		spec.IndexName = alterSpec.Constraint.Name
		spec.ConstraintColumn = cols
		spec.IndexType = getIndexType(alterSpec.Constraint.Tp)
	case ast.AlterTableDropIndex:
		spec.Type = ddl_parser.DropIndex
		spec.IndexName = alterSpec.Name
	case ast.AlterTableRenameIndex:
		spec.Type = ddl_parser.RenameIndex
		spec.NewConstraint = alterSpec.FromKey.String()
		spec.OldConstraint = alterSpec.ToKey.String()
	default:
		log.Warnf("unknown alter table stmt.Text: %s", stmt.Text())
	}

	return ddl_parser.NewAlterTableStatement(newPingCapStatement(schema_store.ALTER_TABLE, &stmt, md), database, resetDb, table, spec)
}

func dropTableStatement(stmt ast.DropTableStmt, astTable ast.TableName, ddl ddl_parser.DDL) ddl_parser.Statement {
	stmt.Tables = []*ast.TableName{
		&astTable,
	}
	database, table := astTable.Schema.String(), astTable.Name.String()
	resetDb := false
	if database == "" {
		database = ddl.Schema
		resetDb = true
	}
	md := ddl_parser.Metadata{
		Database: database,
		Table:    table,
	}
	return ddl_parser.NewTableStatement(newPingCapStatement(schema_store.DROP_TABLE, &stmt, md), database, resetDb, table)
}

func renameTableStatement(stmt ast.RenameTableStmt, tableToTable ast.TableToTable, ddl ddl_parser.DDL) ddl_parser.Statement {
	stmt.TableToTables = []*ast.TableToTable{
		&tableToTable,
	}
	oldTable := ddl_parser.Table{
		Database: tableToTable.OldTable.Schema.String(),
		Table:    tableToTable.OldTable.Name.String(),
	}
	resetDb := false
	if oldTable.Database == "" {
		oldTable.Database = ddl.Schema
		resetDb = true
	}
	newTable := ddl_parser.Table{
		Database: tableToTable.NewTable.Schema.String(),
		Table:    tableToTable.NewTable.Name.String(),
	}
	if newTable.Database == "" {
		newTable.Database = ddl.Schema
		resetDb = true
	}

	md := ddl_parser.Metadata{
		Database:       oldTable.Database,
		Table:          oldTable.Table,
		RenameDatabase: newTable.Database,
		RenameTable:    newTable.Table,
	}
	return ddl_parser.NewRenameTableStatement(newPingCapStatement(schema_store.RENAME_TABLE, &stmt, md), oldTable, newTable, resetDb)
}

type PingCapStatement struct {
	ddlType     schema_store.DDL
	changeEvent map[int]any
	stmt        ast.StmtNode

	metadata ddl_parser.Metadata
}

func newPingCapStatement(ddlType schema_store.DDL, stmt ast.StmtNode, metadata ddl_parser.Metadata) *PingCapStatement {
	return &PingCapStatement{
		ddlType:     ddlType,
		stmt:        stmt,
		metadata:    metadata,
		changeEvent: map[int]any{},
	}
}

func (s *PingCapStatement) DDLType() schema_store.DDL {
	return s.ddlType
}

func (s *PingCapStatement) SubmitModification(event int, parameter any) {
	if database, ok := parameter.(string); ok && event == ddl_parser.ReplaceDatabase {
		s.metadata.Database = database
	}

	if table, ok := parameter.(string); ok && event == ddl_parser.ReplaceTable {
		s.metadata.Table = table
	}
	if s.ddlType == schema_store.RENAME_TABLE {
		if tableMap, ok := parameter.(map[ddl_parser.Table]ddl_parser.Table); ok && event == ddl_parser.ReplaceRenameTableSchemaMap {
			for oldSchema, newSchema := range tableMap {
				s.metadata.Database = oldSchema.Database
				s.metadata.Table = oldSchema.Table
				s.metadata.RenameDatabase = newSchema.Database
				s.metadata.RenameTable = newSchema.Table
			}
		}
	}
	if s.ddlType == schema_store.ALTER_TABLE && event == ddl_parser.ReplaceAlterTable {
		if table, ok := parameter.(ddl_parser.Table); ok {
			s.metadata.RenameDatabase = table.Database
			s.metadata.RenameTable = table.Table
		}
	}
	s.changeEvent[event] = parameter
}

func (s *PingCapStatement) Stmt() ast.StmtNode {
	return s.stmt
}

func (s *PingCapStatement) Metadata() ddl_parser.Metadata {
	return s.metadata
}

//func (s *PingCapStatement) SetSchema(database string) {
//	s.metadata.Database = database
//}

func (s *PingCapStatement) GenerateSQL() (any, error) {
	switch node := s.stmt.(type) {
	case *ast.CreateDatabaseStmt:
		return s.CreateDatabaseStmt(*node)
	case *ast.DropDatabaseStmt:
		return s.DropDatabaseStmt(*node)
	case *ast.CreateTableStmt:
		return s.CreateTableStmt(*node)
	case *ast.DropTableStmt:
		return s.DropTableStmt(*node)
	case *ast.RenameTableStmt:
		return s.RenameTableStmt(*node)
	case *ast.AlterTableStmt:
		return s.AlterTableStmt(*node)
	case *ast.CreateIndexStmt:
		return s.CreateIndexStmt(*node)
	case *ast.DropIndexStmt:
		return s.DropIndexStmt(*node)
	case *ast.TruncateTableStmt:
		return s.TruncateTableStmt(*node)
	default:
		return "", fmt.Errorf("mysql output worker unsupported ddl_parser:%v", s.ddlType)
	}
}

func (s *PingCapStatement) CreateDatabaseStmt(stmt ast.CreateDatabaseStmt) (string, error) {
	stmt.IfNotExists = true
	if value, ok := s.changeEvent[ddl_parser.ReplaceDatabase]; ok {
		database, ok := value.(string)
		if !ok {
			return "", fmt.Errorf("create_database statement change event ReplaceDatabase parameter:%v is not a string", value)
		}
		stmt.Name = ast.NewCIStr(database)
	}
	return restore(&stmt)
}

func (s *PingCapStatement) DropDatabaseStmt(stmt ast.DropDatabaseStmt) (string, error) {
	stmt.IfExists = true
	if value, ok := s.changeEvent[ddl_parser.ReplaceDatabase]; ok {
		database, ok := value.(string)
		if !ok {
			return "", fmt.Errorf("drop_database statement change event ReplaceDatabase parameter:%v is not a string", value)
		}
		stmt.Name = ast.NewCIStr(database)
	}
	return restore(&stmt)
}

func (s *PingCapStatement) CreateTableStmt(stmt ast.CreateTableStmt) (string, error) {
	stmt.IfNotExists = true
	if value, ok := s.changeEvent[ddl_parser.ReplaceDatabase]; ok {
		database, ok := value.(string)
		if !ok {
			return "", fmt.Errorf("create_table statement change event ReplaceDatabase parameter:%v is not a string", value)
		}
		stmt.Table.Schema = ast.NewCIStr(database)
	}

	if value, ok := s.changeEvent[ddl_parser.ReplaceTable]; ok {
		table, ok := value.(string)
		if !ok {
			return "", fmt.Errorf("create_table statement change event ReplaceTable parameter:%v is not a string", value)
		}
		stmt.Table.Name = ast.NewCIStr(table)
	}

	if value, ok := s.changeEvent[ddl_parser.ReplaceTableColumnMap]; ok {
		columnMap, ok := value.(map[string]string)
		if !ok {
			return "", fmt.Errorf("create_table statement change event ReplaceTableColumn parameter:%v is not a map[string]string", value)
		}
		newCols := make([]*ast.ColumnDef, 0, len(columnMap))
		for _, col := range stmt.Cols {
			if newCol, ok := columnMap[col.Name.Name.String()]; ok {
				col.Name.Name = ast.NewCIStr(newCol)
			}
			newCols = append(newCols, col)
		}
		stmt.Cols = newCols
	}

	if value, ok := s.changeEvent[ddl_parser.ReplaceTableConstraintsMap]; ok {
		constraintMap, ok := value.(map[string]map[string]string)
		if !ok {
			return "", fmt.Errorf("create_table statement change event ReplaceTableConstraints parameter:%v is not a map[string]map[string]string", value)
		}

		newConstraints := make([]*ast.Constraint, 0, len(stmt.Constraints))
		for _, constraint := range stmt.Constraints {
			if colMap, exist := constraintMap[constraint.Name]; exist {
				hit := false
				newKeys := make([]*ast.IndexPartSpecification, 0, len(constraint.Keys))
				for _, key := range constraint.Keys {
					if replaceColumn, exist := colMap[key.Column.Name.String()]; exist {
						key.Column.Name = ast.NewCIStr(replaceColumn)
						hit = true
					}
					newKeys = append(newKeys, key)
				}
				if hit {
					constraint.Keys = newKeys
				}
			}
			newConstraints = append(newConstraints, constraint)
		}
	}

	if value, ok := s.changeEvent[ddl_parser.RemoveTableColumn]; ok {
		dropCols, ok := value.([]string)
		if !ok {
			return "", fmt.Errorf("create_table statement change event DropTableColumn parameter:%v is not a []string", value)
		}
		dropColMap := transform.StringSliceToMap(dropCols)

		newCols := make([]*ast.ColumnDef, 0, len(stmt.Cols))
		hit := false
		for _, col := range stmt.Cols {
			if _, ok := dropColMap[col.Name.Name.String()]; ok {
				hit = true
				continue
			}
			newCols = append(newCols, col)
		}
		if hit {
			stmt.Cols = newCols
		}
	}
	if value, ok := s.changeEvent[ddl_parser.RemoveTableConstraintsColumn]; ok {
		dropConstraints, ok := value.(map[string][]string)
		if !ok {
			return "", fmt.Errorf("create_table statement change event DropTableConstraintsColumn parameter:%v is not a []string", value)
		}
		newConstraints := make([]*ast.Constraint, 0, len(stmt.Constraints))
		for _, constraint := range stmt.Constraints {
			if cols, exist := dropConstraints[constraint.Name]; exist {
				hit := false
				colMap := transform.StringSliceToMap(cols)
				newKeys := make([]*ast.IndexPartSpecification, 0, len(constraint.Keys))
				for _, key := range constraint.Keys {
					if _, exist = colMap[key.Column.Name.String()]; exist {
						hit = true
						continue
					}
					newKeys = append(newKeys, key)
				}
				if hit {
					constraint.Keys = newKeys
				}
			}
			newConstraints = append(newConstraints, constraint)
		}

	}
	if len(stmt.Cols) == 0 {
		return "", fmt.Errorf("make create table sql fail, table columns is empty")
	}
	return restore(&stmt)
}

func (s *PingCapStatement) DropTableStmt(stmt ast.DropTableStmt) (string, error) {
	if len(stmt.Tables) != 1 {
		return "", fmt.Errorf("drop_table ddl must be a single SQL statement")
	}
	tableStmt := stmt.Tables[0]
	if value, ok := s.changeEvent[ddl_parser.ReplaceDatabase]; ok {
		database, ok := value.(string)
		if !ok {
			return "", fmt.Errorf("drop_table statement change event ReplaceDatabase parameter:%v is not a string", value)
		}
		tableStmt.Schema = ast.NewCIStr(database)
		//stmt.Table.Schema = ast.NewCIStr(database)
	}
	if value, ok := s.changeEvent[ddl_parser.ReplaceTable]; ok {
		table, ok := value.(string)
		if !ok {
			return "", fmt.Errorf("drop_table statement change event ReplaceTable parameter:%v is not a string", value)
		}
		tableStmt.Name = ast.NewCIStr(table)
	}
	stmt.Tables = []*ast.TableName{
		tableStmt,
	}
	stmt.IfExists = true
	return restore(&stmt)
}

func (s *PingCapStatement) RenameTableStmt(stmt ast.RenameTableStmt) (string, error) {
	if len(stmt.TableToTables) != 1 {
		return "", fmt.Errorf("rename table ddl must be a single SQL statement")
	}
	tableToTable := stmt.TableToTables[0]
	if value, ok := s.changeEvent[ddl_parser.ReplaceRenameTableSchemaMap]; ok {
		renameMap, ok := value.(map[ddl_parser.Table]ddl_parser.Table)
		if !ok {
			return "", fmt.Errorf("rename_table statement change event RenameTableStmt parameter:%v is not a map[string]string", value)
		}
		for oldTable, newTable := range renameMap {
			tableToTable.OldTable.Schema = ast.NewCIStr(oldTable.Database)
			tableToTable.OldTable.Name = ast.NewCIStr(oldTable.Table)
			tableToTable.NewTable.Schema = ast.NewCIStr(newTable.Database)
			tableToTable.NewTable.Name = ast.NewCIStr(newTable.Table)
		}
	}
	stmt.TableToTables = []*ast.TableToTable{
		tableToTable,
	}
	return restore(&stmt)
}

func (s *PingCapStatement) TruncateTableStmt(stmt ast.TruncateTableStmt) (string, error) {
	if value, ok := s.changeEvent[ddl_parser.ReplaceDatabase]; ok {
		database, ok := value.(string)
		if !ok {
			return "", fmt.Errorf("truncate_table statement change event ReplaceDatabase parameter:%v is not a string", value)
		}
		stmt.Table.Schema = ast.NewCIStr(database)
	}
	if value, ok := s.changeEvent[ddl_parser.ReplaceTable]; ok {
		table, ok := value.(string)
		if !ok {
			return "", fmt.Errorf("truncate_table statement change event ReplaceTable parameter:%v is not a string", value)
		}
		stmt.Table.Name = ast.NewCIStr(table)
	}
	return restore(&stmt)
}

func (s *PingCapStatement) CreateIndexStmt(stmt ast.CreateIndexStmt) (string, error) {
	stmt.IfNotExists = false
	if value, ok := s.changeEvent[ddl_parser.ReplaceDatabase]; ok {
		database, ok := value.(string)
		if !ok {
			return "", fmt.Errorf("create_index statement change event ReplaceDatabase parameter:%v is not a string", value)
		}
		stmt.Table.Schema = ast.NewCIStr(database)
	}

	if value, ok := s.changeEvent[ddl_parser.ReplaceTable]; ok {
		table, ok := value.(string)
		if !ok {
			return "", fmt.Errorf("create_index statement change event ReplaceTable parameter:%v is not a string", value)
		}
		stmt.Table.Name = ast.NewCIStr(table)
	}

	if value, ok := s.changeEvent[ddl_parser.ReplaceTableColumnMap]; ok {
		colMap, ok := value.(map[string]string)
		if !ok {
			return "", fmt.Errorf("create_index statement change event ReplaceTableConstraints parameter:%v is not a map[string]string", value)
		}
		hit := false
		newKeys := make([]*ast.IndexPartSpecification, 0, len(stmt.IndexPartSpecifications))
		for _, key := range stmt.IndexPartSpecifications {
			if replaceColumn, exist := colMap[key.Column.Name.String()]; exist {
				key.Column.Name = ast.NewCIStr(replaceColumn)
				hit = true
			}
			newKeys = append(newKeys, key)
		}
		if hit {
			stmt.IndexPartSpecifications = newKeys
		}
	}

	if value, ok := s.changeEvent[ddl_parser.RemoveTableColumn]; ok {
		dropCols, ok := value.([]string)
		if !ok {
			return "", fmt.Errorf("create_index statement change event DropTableConstraintsColumn parameter:%v is not a []string", value)
		}
		hit := false
		newKeys := make([]*ast.IndexPartSpecification, 0, len(stmt.IndexPartSpecifications))
		colMap := transform.StringSliceToMap(dropCols)
		for _, key := range stmt.IndexPartSpecifications {
			if _, exist := colMap[key.Column.Name.String()]; exist {
				hit = true
				continue
			}
			newKeys = append(newKeys, key)
		}
		if hit {
			stmt.IndexPartSpecifications = newKeys
		}
	}
	if len(stmt.IndexPartSpecifications) == 0 {
		return "", fmt.Errorf("make create index sql fail, table columns is empty")
	}
	return restore(&stmt)
}

func (s *PingCapStatement) DropIndexStmt(stmt ast.DropIndexStmt) (string, error) {
	stmt.IfExists = false
	if value, ok := s.changeEvent[ddl_parser.ReplaceDatabase]; ok {
		database, ok := value.(string)
		if !ok {
			return "", fmt.Errorf("truncate_table statement change event ReplaceDatabase parameter:%v is not a string", value)
		}
		stmt.Table.Schema = ast.NewCIStr(database)
	}

	if value, ok := s.changeEvent[ddl_parser.ReplaceTable]; ok {
		table, ok := value.(string)
		if !ok {
			return "", fmt.Errorf("drop_index statement change event ReplaceTable parameter:%v is not a string", value)
		}
		stmt.Table.Name = ast.NewCIStr(table)
	}
	return restore(&stmt)
}

func (s *PingCapStatement) AlterTableStmt(stmt ast.AlterTableStmt) (string, error) {
	if len(stmt.Specs) != 1 {
		return "", fmt.Errorf("alter_table ddl must be a single SQL statement")
	}
	specs := stmt.Specs[0]

	if value, ok := s.changeEvent[ddl_parser.ReplaceAlterTable]; ok {
		table, ok := value.(ddl_parser.Table)
		if !ok {
			return "", fmt.Errorf("alter_table statement change event ReplaceAlterTable parameter:%v is not a ddl_parser.Table", value)
		}
		specs.NewTable.Name = ast.NewCIStr(table.Table)
		specs.NewTable.Schema = ast.NewCIStr(table.Database)
	}
	if value, ok := s.changeEvent[ddl_parser.ReplaceDatabase]; ok {
		database, ok := value.(string)
		if !ok {
			return "", fmt.Errorf("alter_table statement change event ReplaceDatabase parameter:%v is not a string", value)
		}
		stmt.Table.Schema = ast.NewCIStr(database)
	}
	if value, ok := s.changeEvent[ddl_parser.ReplaceTable]; ok {
		table, ok := value.(string)
		if !ok {
			return "", fmt.Errorf("alter_table statement change event ReplaceTable parameter:%v is not a string", value)
		}
		stmt.Table.Name = ast.NewCIStr(table)
	}
	if value, ok := s.changeEvent[ddl_parser.ReplaceTableColumnMap]; ok {
		colMap, ok := value.(map[string]string)
		if !ok {
			return "", fmt.Errorf("alter_table statement change event ReplaceTable parameter:%v is not a map[string]string", value)
		}
		for _, column := range specs.NewColumns {
			if newColumn, ok := colMap[column.Name.String()]; ok {
				column.Name.Name = ast.NewCIStr(newColumn)
			}
		}
		if specs.OldColumnName != nil {
			if newCol, ok := colMap[specs.OldColumnName.String()]; ok {
				specs.OldColumnName.Name = ast.NewCIStr(newCol)
			}
		}

		if specs.NewColumnName != nil {
			if newCol, ok := colMap[specs.NewColumnName.String()]; ok {
				specs.NewColumnName.Name = ast.NewCIStr(newCol)
			}
		}

		if specs.Constraint != nil {
			for _, key := range specs.Constraint.Keys {
				if newCol, ok := colMap[key.Column.Name.String()]; ok {
					key.Column.Name = ast.NewCIStr(newCol)
				}
			}

		}

	}
	if value, ok := s.changeEvent[ddl_parser.RemoveTableColumn]; ok {
		rmCols, ok := value.([]string)
		if !ok {
			return "", fmt.Errorf("alter_table statement change event ReplaceTable parameter:%v is not a []string", value)
		}
		colMap := transform.StringSliceToMap(rmCols)
		newColumns := make([]*ast.ColumnDef, 0, len(colMap))
		hit := false
		for _, column := range specs.NewColumns {
			if _, ok = colMap[column.Name.String()]; ok {
				hit = true
			} else {
				newColumns = append(newColumns, column)
			}
		}
		if hit {
			specs.NewColumns = newColumns
		}

		if len(newColumns) == 0 {
			return "", fmt.Errorf("make alter table sql fail, columns is empty")
		}

		if specs.Constraint != nil {
			newKeys := make([]*ast.IndexPartSpecification, 0, len(specs.Constraint.Keys))
			hit = false
			for _, key := range specs.Constraint.Keys {
				if _, ok = colMap[key.Column.Name.String()]; ok {
					hit = true
				} else {
					newKeys = append(newKeys, key)
				}
			}
			if hit {
				specs.Constraint.Keys = newKeys
			}
		}

	}
	stmt.Specs = []*ast.AlterTableSpec{
		specs,
	}
	return restore(&stmt)
}

func (s *PingCapStatement) generateSQL() (string, error) {
	writer := &strings.Builder{}
	ctx := format.NewRestoreCtx(format.RestoreStringSingleQuotes|format.RestoreKeyWordLowercase|format.RestoreNameBackQuotes, writer)
	err := s.stmt.Restore(ctx)
	if err != nil {
		return "", fmt.Errorf("error restore ddl_parser %s, err: %s", s.stmt.Text(), err)
	}
	return writer.String(), nil
}

func restore(node ast.Node) (string, error) {
	writer := &strings.Builder{}
	ctx := format.NewRestoreCtx(format.RestoreStringSingleQuotes|format.RestoreKeyWordLowercase|format.RestoreNameBackQuotes, writer)
	err := node.Restore(ctx)
	if err != nil {
		err = fmt.Errorf("error restore ddl_parser %s, err: %s", node.Text(), err)
		return "", err
	}
	return writer.String(), nil
}

func getIndexType(tp ast.ConstraintType) ddl_parser.IndexType {
	switch tp {
	case ast.ConstraintPrimaryKey:
		return ddl_parser.IndexTypePrimary
	case ast.ConstraintUniq, ast.ConstraintUniqKey, ast.ConstraintUniqIndex:
		return ddl_parser.IndexTypeUnique
	case ast.ConstraintKey, ast.ConstraintIndex:
		return ddl_parser.IndexTypeConstraint
	default:
		return ddl_parser.IndexTypeConstraint
	}
}

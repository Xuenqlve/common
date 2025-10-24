package mysql_schema

import (
	"fmt"

	"github.com/xuenqlve/common/ddl_parser"
	ddl "github.com/xuenqlve/common/relational_database/ddl_parser"
)

func NewDDLLoader() *DDLLoader {
	return &DDLLoader{
		PingCapLoader: ddl.NewPingCapLoader(),
	}
}

type DDLLoader struct {
	ddl.PingCapLoader
}

func (s *DDLLoader) Parse(schema, sql string) ([]DDLStatement, error) {
	ddls, err := s.PingCapLoader.Parse(ddl_parser.DDL{Schema: schema, SQL: sql})
	if err != nil {
		return nil, err
	}
	list := make([]DDLStatement, 0, len(ddls))
	for _, ddl := range ddls {
		list = append(list, DDLStatement{
			Statement: ddl,
		})
	}
	return list, nil
}

type DDLStatement struct {
	ddl_parser.Statement
}

func (s *DDLStatement) Metadata() ddl_parser.Metadata {
	return s.Statement.Metadata()
}

func (s *DDLStatement) GenerateSQL() (string, error) {
	sql, err := s.Statement.GenerateSQL()
	if err != nil {
		return "", err
	}
	sqlStr, ok := sql.(string)
	if !ok {
		return "", fmt.Errorf("generateSQL to string fail sql:%v", sql)
	}
	return sqlStr, nil
}

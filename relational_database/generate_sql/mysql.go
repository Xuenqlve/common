package generate_sql

import (
	"fmt"
	"strings"

	"github.com/xuenqlve/common/errors"
	"github.com/xuenqlve/common/relational_database/mysql"
	sql_tool "github.com/xuenqlve/common/sql"
)

type MySQLSQLExecution struct {
	Statement string
	Args      []any
}

func GenerateDeleteSQL() {

}

func analysisDeleteArgs(guideKeys map[string]any, inFlag bool) (statement []string, args []any, err error) {
	whereStatement := make([]string, 0, len(guideKeys))
	args = make([]any, 0, len(guideKeys))
	for key, value := range guideKeys {
		col := sql_tool.ColumnName(key)
		if inFlag {
			whereStatement = append(whereStatement, "?")
		} else {
			whereStatement = append(whereStatement, fmt.Sprintf("%s = ?", col))
		}
		args = append(args, value)
	}
	return whereStatement, args, nil
}

func generateDeleteSQLComplexKey(tableDef *mysql.Table, whereStatement []string) string {
	batchStatement := []string{}
	batchStatement = append(batchStatement, fmt.Sprintf("(%s)", strings.Join(whereStatement, " AND ")))
	return fmt.Sprintf("DELETE FROM `%s`.`%s` WHERE %s ", tableDef.Database, tableDef.Table, strings.Join(batchStatement, " OR "))
}

func generateDeleteSQLSingleKey(tableDef *mysql.Table, guideKey string, whereStatement []string) string {
	return fmt.Sprintf("DELETE FROM `%s`.`%s` WHERE %s in (%s) ", tableDef.Database, tableDef.Table, guideKey, strings.Join(whereStatement, ","))
}

func GenerateInsertSQL(tableDef *mysql.Table, batchPlaceHolders []string) string {
	return fmt.Sprintf("%s %s", insertSqlPrefix(tableDef), strings.Join(batchPlaceHolders, ","))
}

func GenerateInsertSectionSQL(tableDef *mysql.Table, data map[string]any, batchPlaceHolders []string) string {
	return fmt.Sprintf("%s %s", insertSqlPrefixByMessage(tableDef, data), strings.Join(batchPlaceHolders, ","))
}

func GenerateInsertIgnoreSQL(tableDef *mysql.Table, batchPlaceHolders []string) string {
	return fmt.Sprintf("%s %s", insertIgnoreSqlPrefix(tableDef), strings.Join(batchPlaceHolders, ","))
}

func GenerateInsertOnDuplicateKeyUpdateSQL(tableDef *mysql.Table, batchPlaceHolders []string) string {
	return fmt.Sprintf("%s %s %s", insertSqlPrefix(tableDef), strings.Join(batchPlaceHolders, ","), onDuplicateKeyUpdateSQLSuffix(tableDef))
}

func GenerateInsertUpdateSectionSQL(tableDef *mysql.Table, data map[string]any, batchPlaceHolders []string) string {
	return fmt.Sprintf("%s %s %s", insertSqlPrefixByMessage(tableDef, data), strings.Join(batchPlaceHolders, ","), onDuplicateKeyUpdateSQLSuffixByMessage(tableDef, data))
}

func GenerateReplaceSQL(tableDef *mysql.Table, batchPlaceHolders []string) string {
	return fmt.Sprintf("%s %s", replaceSqlPrefix(tableDef), strings.Join(batchPlaceHolders, ","))
}

func insertSqlPrefixByMessage(tableDef *mysql.Table, msgData map[string]any) string {
	columnNames := make([]string, 0, len(tableDef.Columns))
	for _, column := range tableDef.Columns {
		columnName := column.Name
		if _, ok := msgData[columnName]; !ok {
			continue
		}
		columnNames = append(columnNames, fmt.Sprintf("`%s`", columnName))
	}
	return fmt.Sprintf("INSERT INTO `%s`.`%s` (%s) VALUES", tableDef.Database, tableDef.Table, strings.Join(columnNames, ","))
}

func insertSqlPrefix(tableDef *mysql.Table) string {
	columnNames := make([]string, 0, len(tableDef.Columns))
	for _, column := range tableDef.Columns {
		columnName := column.Name
		columnNames = append(columnNames, fmt.Sprintf("`%s`", columnName))
	}
	return fmt.Sprintf("INSERT INTO `%s`.`%s` (%s) VALUES", tableDef.Database, tableDef.Table, strings.Join(columnNames, ","))
}

func insertIgnoreSqlPrefix(tableDef *mysql.Table) string {
	columnNames := make([]string, 0, len(tableDef.Columns))
	for _, column := range tableDef.Columns {
		columnName := column.Name
		columnNames = append(columnNames, fmt.Sprintf("`%s`", columnName))
	}
	return fmt.Sprintf("INSERT IGNORE INTO `%s`.`%s` (%s) VALUES", tableDef.Database, tableDef.Table, strings.Join(columnNames, ","))
}

func replaceSqlPrefix(tableDef *mysql.Table) string {
	columnNames := make([]string, 0, len(tableDef.Columns))
	for _, column := range tableDef.Columns {
		columnName := column.Name
		columnNames = append(columnNames, fmt.Sprintf("`%s`", columnName))
	}
	return fmt.Sprintf("REPLACE INTO `%s`.`%s` (%s) VALUES", tableDef.Database, tableDef.Table, strings.Join(columnNames, ","))
}

func onDuplicateKeyUpdateSQLSuffix(tableDef *mysql.Table) string {
	columnNamesAssign := make([]string, 0, len(tableDef.Columns))
	if len(tableDef.UniqueIndex) == 0 {
		return ""
	}
	for _, column := range tableDef.Columns {
		if column.IsGenerated {
			continue
		}
		columnName := column.Name
		columnNameInSQL := fmt.Sprintf("`%s`", columnName)
		columnNamesAssign = append(columnNamesAssign, fmt.Sprintf("%s = VALUES(%s)", columnNameInSQL, columnNameInSQL))
	}
	return fmt.Sprintf("ON DUPLICATE KEY UPDATE %s", strings.Join(columnNamesAssign, ","))
}

func onDuplicateKeyUpdateSQLSuffixByMessage(tableDef *mysql.Table, msgData map[string]any) string {
	columnNamesAssign := make([]string, 0, len(tableDef.Columns))
	if len(tableDef.UniqueIndex) == 0 {
		return ""
	}
	for _, column := range tableDef.Columns {
		columnName := column.Name
		if _, ok := msgData[columnName]; !ok {
			continue
		}
		if column.IsGenerated {
			continue
		}
		columnNameInSQL := fmt.Sprintf("`%s`", columnName)
		columnNamesAssign = append(columnNamesAssign, fmt.Sprintf("%s = VALUES(%s)", columnNameInSQL, columnNameInSQL))
	}
	return fmt.Sprintf("ON DUPLICATE KEY UPDATE %s", strings.Join(columnNamesAssign, ","))
}

func updateSqlPrefix(tableDef *mysql.Table, updateIgnore bool) (string, error) {
	if updateIgnore {
		return fmt.Sprintf("UPDATE IGNORE `%s`.`%s`", tableDef.Database, tableDef.Table), nil
	} else {
		return fmt.Sprintf("UPDATE `%s`.`%s` ", tableDef.Database, tableDef.Table), nil
	}
}

func GetSingleSqlPlaceHolderAndArgWithEncodedData(data map[string]any, tableDef *mysql.Table, bySource bool) (string, []any, error) {
	if err := validateSchema(data, tableDef); err != nil && !bySource {
		return "", nil, errors.Trace(err)
	}
	var placeHolders []string
	var args []any

	for _, column := range tableDef.Columns {
		columnName := column.Name
		columnData, ok := data[columnName]
		if !ok {
			if bySource {
				continue
			}
			return "", nil, errors.Errorf("db:%s, table:%s, column:%s missing data", tableDef.Database, tableDef.Table, columnName)
		}
		if column.IsGenerated && mysql.IsColumnSetDefault(columnData) {
			placeHolders = append(placeHolders, "DEFAULT")
			continue
		}
		args = append(args, adjustArgs(columnData, &column))
		placeHolders = append(placeHolders, "?")

	}
	singleSqlPlaceHolder := fmt.Sprintf("(%s)", strings.Join(placeHolders, ","))
	return singleSqlPlaceHolder, args, nil
}

func validateSchema(data map[string]any, tableDef *mysql.Table) error {
	columnLenInMsg := len(data)
	columnLenInTarget := len(tableDef.Columns)

	if columnLenInMsg != columnLenInTarget {
		return errors.Errorf("%s.%s: columnLenInMsg %d columnLenInTarget %d not equal", tableDef.Database, tableDef.Table, columnLenInMsg, columnLenInTarget)
	}

	return nil
}

func adjustArgs(arg any, column *mysql.Column) any {
	if arg == nil {
		return arg
	}
	if column.Type == mysql.TypeDatetime || column.Type == mysql.TypeTimestamp || column.Type == mysql.TypeDate { // datetime is in utc and should ignore location
		v, flag := mysql.ParseTime(arg, column.Type)
		if flag {
			return v.Format("2006-01-02 15:04:05.999999999")
		}
	}
	return arg
}

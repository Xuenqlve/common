package generate_sql

import (
	"fmt"
	"strings"

	"github.com/xuenqlve/common/errors"
	"github.com/xuenqlve/common/relational_database/mysql"
	sql_tool "github.com/xuenqlve/common/sql"
)

func GenerateDeleteSQL(rows []mysql.RowData, tableDef *mysql.Table) (statement string, args []any, err error) {
	var deleteInFlag bool
	var guideKey string

	if len(rows) == 0 {
		return "", nil, fmt.Errorf("no guide")
	}
	guideKeys := rows[0].GuideKeys
	if len(guideKeys) == 1 {
		deleteInFlag = true
		for key := range guideKeys {
			guideKey = sql_tool.ColumnName(key)
		}
	}
	batchStatement := make([]string, 0, len(rows))
	args = []any{}
	for _, msgContent := range rows {
		stmts, params := analysisDeleteArgs(msgContent.GuideKeys, deleteInFlag)
		args = append(args, params...)
		batchStatement = append(batchStatement, stmts...)
	}
	if deleteInFlag {
		statement = generateDeleteSQLSingleKey(tableDef, guideKey, batchStatement)
	} else {
		statement = generateDeleteSQLComplexKey(tableDef, batchStatement)
	}
	return statement, args, nil
}

func GenerateInsertSQL(rows []mysql.RowData, tableDef *mysql.Table) (statement string, args []any, err error) {
	batchPlaceHolders, args, err := placeHoldersAndArgsFromEncodedData(rows, tableDef, false)
	if err != nil {
		return "", nil, errors.Trace(err)
	}
	return generateInsertSQL(tableDef, batchPlaceHolders), args, nil
}

func GenerateInsertSectionSQL(rows []mysql.RowData, tableDef *mysql.Table) (statement string, args []any, err error) {
	msgData := rows[0].Data
	batchPlaceHolders, args, err := placeHoldersAndArgsFromEncodedData(rows, tableDef, true)
	if err != nil {
		return "", nil, errors.Trace(err)
	}
	return generateInsertSectionSQL(tableDef, msgData, batchPlaceHolders), args, nil
}

func GenerateInsertIgnoreSQL(rows []mysql.RowData, tableDef *mysql.Table) (statement string, args []any, err error) {
	batchPlaceHolders, args, err := placeHoldersAndArgsFromEncodedData(rows, tableDef, false)
	if err != nil {
		return "", nil, errors.Trace(err)
	}
	return generateInsertIgnoreSQL(tableDef, batchPlaceHolders), args, nil
}

func GenerateInsertOnDuplicateKeyUpdateSQL(rows []mysql.RowData, tableDef *mysql.Table) (statement string, args []any, err error) {
	batchPlaceHolders, args, err := placeHoldersAndArgsFromEncodedData(rows, tableDef, false)
	if err != nil {
		return "", nil, errors.Trace(err)
	}
	return generateInsertOnDuplicateKeyUpdateSQL(tableDef, batchPlaceHolders), args, nil
}

func GenerateInsertUpdateSectionSQL(rows []mysql.RowData, tableDef *mysql.Table) (statement string, args []any, err error) {
	msgData := rows[0].Data
	batchPlaceHolders, args, err := placeHoldersAndArgsFromEncodedData(rows, tableDef, true)
	if err != nil {
		return "", nil, errors.Trace(err)
	}
	return generateInsertUpdateSectionSQL(tableDef, msgData, batchPlaceHolders), args, nil
}

func GenerateReplaceSQL(rows []mysql.RowData, tableDef *mysql.Table) (statement string, args []any, err error) {
	batchPlaceHolders, args, err := placeHoldersAndArgsFromEncodedData(rows, tableDef, false)
	if err != nil {
		return "", nil, errors.Trace(err)
	}
	return generateReplaceSQL(tableDef, batchPlaceHolders), args, nil
}

func analysisDeleteArgs(guideKeys map[string]any, inFlag bool) (statement []string, args []any) {
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

	if inFlag {
		return whereStatement, args
	} else {
		return []string{fmt.Sprintf("(%s)", strings.Join(whereStatement, " AND "))}, args
	}
}

func generateDeleteSQLComplexKey(tableDef *mysql.Table, whereStatement []string) string {
	batchStatement := []string{}
	batchStatement = append(batchStatement, fmt.Sprintf("(%s)", strings.Join(whereStatement, " AND ")))
	return fmt.Sprintf("DELETE FROM `%s`.`%s` WHERE %s ", tableDef.Database, tableDef.Table, strings.Join(batchStatement, " OR "))
}

func generateDeleteSQLSingleKey(tableDef *mysql.Table, guideKey string, whereStatement []string) string {
	return fmt.Sprintf("DELETE FROM `%s`.`%s` WHERE %s in (%s) ", tableDef.Database, tableDef.Table, guideKey, strings.Join(whereStatement, ","))
}

func placeHoldersAndArgsFromEncodedData(msgBatch []mysql.RowData, tableDef *mysql.Table, bySource bool) ([]string, []any, error) {
	var batchPlaceHolders []string
	var batchArgs []any
	msgLen := len(msgBatch[0].Data)

	for _, msg := range msgBatch {
		if msg.Data == nil {
			return nil, nil, errors.Errorf("Data and MysqlRawBytes are null")
		}

		singleSqlPlaceHolders, singleSqlArgs, err := getSingleSqlPlaceHolderAndArgWithEncodedData(msg.Data, tableDef, bySource)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		batchPlaceHolders = append(batchPlaceHolders, singleSqlPlaceHolders)
		if len(singleSqlArgs) != msgLen {
			return nil, nil, fmt.Errorf("single sql args does not match message length")
		}
		batchArgs = append(batchArgs, singleSqlArgs...)
	}
	return batchPlaceHolders, batchArgs, nil
}

func generateInsertSQL(tableDef *mysql.Table, batchPlaceHolders []string) string {
	return fmt.Sprintf("%s %s", insertSqlPrefix(tableDef), strings.Join(batchPlaceHolders, ","))
}

func generateInsertSectionSQL(tableDef *mysql.Table, data map[string]any, batchPlaceHolders []string) string {
	return fmt.Sprintf("%s %s", insertSqlPrefixByMessage(tableDef, data), strings.Join(batchPlaceHolders, ","))
}

func generateInsertIgnoreSQL(tableDef *mysql.Table, batchPlaceHolders []string) string {
	return fmt.Sprintf("%s %s", insertIgnoreSqlPrefix(tableDef), strings.Join(batchPlaceHolders, ","))
}

func generateInsertOnDuplicateKeyUpdateSQL(tableDef *mysql.Table, batchPlaceHolders []string) string {
	return fmt.Sprintf("%s %s %s", insertSqlPrefix(tableDef), strings.Join(batchPlaceHolders, ","), onDuplicateKeyUpdateSQLSuffix(tableDef))
}

func generateInsertUpdateSectionSQL(tableDef *mysql.Table, data map[string]any, batchPlaceHolders []string) string {
	return fmt.Sprintf("%s %s %s", insertSqlPrefixByMessage(tableDef, data), strings.Join(batchPlaceHolders, ","), onDuplicateKeyUpdateSQLSuffixByMessage(tableDef, data))
}

func generateReplaceSQL(tableDef *mysql.Table, batchPlaceHolders []string) string {
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

func getSingleSqlPlaceHolderAndArgWithEncodedData(data map[string]any, tableDef *mysql.Table, bySource bool) (string, []any, error) {
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

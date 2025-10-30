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

func GenerateUpdateSQL(msgBatch []mysql.RowData, tableDef *mysql.Table, isSection bool) (string, []any, error) {
	statements := make([]string, 0, len(msgBatch))
	args := make([]any, 0, len(msgBatch)*len(tableDef.Columns))
	for _, msg := range msgBatch {
		setInfo, conditionInfo, tmpArgs, err := updateSetInfoAndArgsFromEncodedData(msg, tableDef, isSection)
		if err != nil {
			return "", []any{}, errors.Trace(err)
		}
		prefix, err := updateSqlPrefix(tableDef, false)
		if err != nil {
			return "", []any{}, errors.Trace(err)
		}
		statements = append(statements, fmt.Sprintf("%s SET %s WHERE %s", prefix, strings.Join(setInfo, ","), strings.Join(conditionInfo, " AND ")))
		args = append(args, tmpArgs...)
	}
	return strings.Join(statements, ";"), args, nil
}

// GenerateUpdateSQLByJoin update join
func GenerateUpdateSQLByJoin(msgBatch []mysql.RowData, tableDef *mysql.Table, isSection bool) (string, []any, error) {
	prefix, err := updateSqlPrefix(tableDef, false)
	if err != nil {
		return "", []any{}, errors.Trace(err)
	}

	var unionClauses []string
	var args []any

	for _, row := range msgBatch {
		if row.GuideKeys == nil || row.Old == nil || row.Data == nil {
			return "", []any{}, fmt.Errorf("GenerateUpdateSQLByJoin msgBatch contains nil row")
		}
		selectValues := make([]string, 0, len(row.GuideKeys)+len(row.Data))
		selectArgs := make([]any, 0, len(row.GuideKeys)+len(row.Data))

		for _, column := range tableDef.Columns {
			if _, ok := row.GuideKeys[column.Name]; ok {
				if _, ok := row.Old[column.Name]; ok {
					selectValues = append(selectValues, fmt.Sprintf("? AS `%s`", column.Name))
					selectArgs = append(selectArgs, row.Old[column.Name])
					continue
				}
			}
			if value, ok := row.Data[column.Name]; ok {
				selectValues = append(selectValues, fmt.Sprintf("? AS `%s`", column.Name))
				selectArgs = append(selectArgs, value)
			} else if isSection {
				continue
			} else {
				return "", []any{}, fmt.Errorf("column `%s` not found in row.Data", column.Name)
			}
		}

		unionClauses = append(unionClauses, fmt.Sprintf("SELECT %s", strings.Join(selectValues, ", ")))
		args = append(args, selectArgs...)
	}

	//USING
	usingColumns := make([]string, 0, len(msgBatch[0].GuideKeys))
	for key := range msgBatch[0].GuideKeys {
		usingColumns = append(usingColumns, fmt.Sprintf("`%s`", key))
	}

	//SET
	setColumns := make([]string, 0, len(tableDef.Columns))
	keyMap := make(map[string]struct{}, len(msgBatch[0].GuideKeys))
	for column := range msgBatch[0].GuideKeys {
		keyMap[column] = struct{}{}
	}

	for _, column := range tableDef.Columns {
		if _, ok := keyMap[column.Name]; !ok {
			continue
		}
		var caseStmt strings.Builder
		caseStmt.WriteString(fmt.Sprintf("a.`%s` = CASE ", column.Name))
		for _, row := range msgBatch {
			whenClause := make([]string, 0, len(row.GuideKeys))
			for keyInfo := range row.GuideKeys {
				whenClause = append(whenClause, fmt.Sprintf("`%s` = ?", keyInfo))
				args = append(args, row.Old[keyInfo])
			}
			caseStmt.WriteString(fmt.Sprintf(" WHEN %s THEN ? ",
				strings.Join(whenClause, " AND ")))
			args = append(args, row.Data[column.Name])
		}
		caseStmt.WriteString(" END")
		setColumns = append(setColumns, caseStmt.String())
	}

	for key := range msgBatch[0].Data {
		if _, isPK := msgBatch[0].GuideKeys[key]; !isPK {
			setColumns = append(setColumns, fmt.Sprintf("a.`%s` = b.`%s`", key, key))
		}
	}

	//WHERE
	whereStatement, whereArgs, err := buildUpdateWhereStatement(msgBatch)
	args = append(args, whereArgs...)

	sql := fmt.Sprintf("%s a JOIN ( %s ) b USING( %s ) SET %s WHERE %s",
		prefix,
		strings.Join(unionClauses, " UNION "),
		strings.Join(usingColumns, ", "),
		strings.Join(setColumns, ", "),
		whereStatement)

	return sql, args, nil
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

func updateSqlPrefix(tableDef *mysql.Table, updateIgnore bool) (string, error) {
	if updateIgnore {
		return fmt.Sprintf("UPDATE IGNORE `%s`.`%s`", tableDef.Database, tableDef.Table), nil
	} else {
		return fmt.Sprintf("UPDATE `%s`.`%s` ", tableDef.Database, tableDef.Table), nil
	}
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

func updateSetInfoAndArgsFromEncodedData(msgBatch mysql.RowData, tableDef *mysql.Table, isSection bool) ([]string, []string, []any, error) {
	if msgBatch.Data == nil || msgBatch.GuideKeys == nil {
		return nil, nil, nil, fmt.Errorf("data or guideKeys are nil")
	}
	setInfo := make([]string, 0, len(tableDef.Columns))
	conditionInfo := make([]string, 0, len(msgBatch.GuideKeys))
	args := make([]any, 0, len(tableDef.Columns)+len(msgBatch.GuideKeys))
	conditionArgs := make([]any, 0, len(msgBatch.GuideKeys))
	data := msgBatch.Data
	guideKeys := msgBatch.GuideKeys
	oldData := msgBatch.Old

	for _, column := range tableDef.Columns {
		columnName := column.Name
		_, ok := guideKeys[columnName]
		if ok {
			if keyData, exist := oldData[columnName]; exist {
				singleConditionInfo := fmt.Sprintf("`%s` = ?", columnName)
				conditionInfo = append(conditionInfo, singleConditionInfo)
				conditionArgs = append(conditionArgs, adjustArgs(keyData, &column))
			} else {
				return nil, nil, nil, fmt.Errorf("old data not found by key column: %s", columnName)
			}

		}
		columnData, ok := data[columnName]
		if !ok {
			if isSection {
				continue
			}
			return nil, nil, nil, errors.Errorf("db:%s, table:%s, column:%s missing data", tableDef.Database, tableDef.Table, columnName)
		}
		args = append(args, adjustArgs(columnData, &column))
		singleSetInfo := fmt.Sprintf("`%s` = ?", columnName)
		setInfo = append(setInfo, singleSetInfo)
	}
	for _, conditionArg := range conditionArgs {
		args = append(args, conditionArg)
	}
	return setInfo, conditionInfo, args, nil
}

func buildUpdateWhereStatement(msgBatch []mysql.RowData) (string, []any, error) {
	var inFlag bool
	var guideKey string
	batchStatement := []string{}
	batchArgs := []any{}

	if len(msgBatch) == 0 {
		return "", nil, fmt.Errorf("empty msgBatch")
	}

	guideKeys := msgBatch[0].GuideKeys
	if len(guideKeys) == 1 {
		inFlag = true
		for key := range guideKeys {
			guideKey = key
		}
	}
	for _, msgContent := range msgBatch {
		gKeys := msgContent.GuideKeys
		whereStatement := make([]string, 0, len(gKeys))
		args := make([]any, 0, len(gKeys))
		for key, _ := range gKeys {
			col := sql_tool.ColumnName(key)
			if inFlag {
				whereStatement = append(whereStatement, "?")
			} else {
				whereStatement = append(whereStatement, fmt.Sprintf("%s = ?", col))
			}
			if _, ok := msgContent.Old[key]; !ok {
				return "", nil, fmt.Errorf("key %s not found in msgBatch.Old", key)
			}
			args = append(args, msgContent.Old[key])
		}

		if inFlag {
			batchStatement = append(batchStatement, whereStatement...)
		} else {
			batchStatement = append(batchStatement, fmt.Sprintf("(%s)", strings.Join(whereStatement, " AND ")))
		}
		batchArgs = append(batchArgs, args...)
	}
	var whereStatement string
	if inFlag {
		whereStatement = fmt.Sprintf("%s in (%s) ", guideKey, strings.Join(batchStatement, ","))
	} else {
		whereStatement = fmt.Sprintf(" %s ", strings.Join(batchStatement, " OR "))
	}
	return whereStatement, batchArgs, nil
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

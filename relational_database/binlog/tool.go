package binlog

import (
	"database/sql"
	"fmt"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/xuenqlve/common/errors"
	"github.com/xuenqlve/common/log"
)

func NewStartBinlogTool(conn *sql.DB) *StartBinlogTool {
	return &StartBinlogTool{conn: conn}
}

type StartBinlogTool struct {
	conn *sql.DB
}

func (s *StartBinlogTool) CheckBinlogFormat() error {
	rows, err := s.conn.Query(`SHOW GLOBAL VARIABLES LIKE "binlog_format";`)
	if err != nil {
		return errors.Trace(err)
	}
	defer func() {
		if err = rows.Close(); err != nil {
			log.Warnf("failed to close rows: %s", err.Error())
		}
	}()
	// Show an example.
	/*
			   mysql> SHOW GLOBAL VARIABLES LIKE "binlog_format";
		       +---------------+-------+
		       | Variable_name | Value |
		       +---------------+-------+
		       | binlog_format | ROW   |
		       +---------------+-------+
	*/
	for rows.Next() {
		var (
			variable string
			value    string
		)
		err = rows.Scan(&variable, &value)

		if err != nil {
			return errors.Trace(err)
		}

		if variable == "binlog_format" && value != "ROW" {
			err = fmt.Errorf("binlog_format is not 'ROW': %v", value)
			return err
		}
	}

	if rows.Err() != nil {
		return errors.Trace(rows.Err())
	}
	if err = rows.Err(); err != nil {
		return errors.Trace(err)
	}
	// make sure binlog_row_image is FULL
	binlogRowImageRows, err := s.conn.Query(`SHOW GLOBAL VARIABLES LIKE 'binlog_row_image';`)
	if err != nil {
		return errors.Trace(err)
	}
	defer func() {
		if err = binlogRowImageRows.Close(); err != nil {
			log.Warnf("failed to close rows: %s", err.Error())
		}
	}()

	for binlogRowImageRows.Next() {
		var (
			variable string
			value    string
		)

		err = binlogRowImageRows.Scan(&variable, &value)
		if err != nil {
			return errors.Trace(err)
		}

		if variable == "binlog_row_image" && value != "FULL" {
			err = fmt.Errorf("binlog_row_image is not 'FULL' : %v", value)
			return err
		}
	}

	if binlogRowImageRows.Err() != nil {
		return errors.Trace(binlogRowImageRows.Err())
	}

	return nil
}

func (s *StartBinlogTool) CheckBinlogSQLLog() error {
	binlogRowsQueryLogEvent, err := s.conn.Query(`SHOW GLOBAL VARIABLES LIKE 'binlog_rows_query_log_events';`)
	if err != nil {
		return errors.Trace(err)
	}
	defer func() {
		if err = binlogRowsQueryLogEvent.Close(); err != nil {
			log.Warnf("failed to close rows:%s", err.Error())
		}
	}()
	for binlogRowsQueryLogEvent.Next() {
		var (
			variable string
			value    string
		)
		err = binlogRowsQueryLogEvent.Scan(&variable, &value)
		if err != nil {
			return errors.Trace(err)
		}
		if variable == "binlog_rows_query_log_events" && value != "ON" {
			err = fmt.Errorf("binlog_row_image is not 'ON' : %v", value)
			return err
		}
	}
	if binlogRowsQueryLogEvent.Err() != nil {
		return errors.Trace(binlogRowsQueryLogEvent.Err())
	}
	return nil
}

func (s *StartBinlogTool) GetMasterStatus() (mysql.Position, mysql.MysqlGTIDSet, error) {
	var (
		binlogPos mysql.Position
		gs        mysql.MysqlGTIDSet
	)
	rows, err := s.conn.Query(`SHOW MASTER STATUS`)
	defer func() {
		if err = rows.Close(); err != nil {
			log.Warnf("failed to close rows: %s", err.Error())
		}
	}()
	if err != nil {
		return binlogPos, gs, errors.Trace(err)
	}

	rowColumns, err := rows.Columns()
	if err != nil {
		return binlogPos, gs, errors.Trace(err)
	}

	var (
		gtid       string
		binlogName string
		pos        uint32
		nullPtr    interface{}
	)
	for rows.Next() {
		if len(rowColumns) == 5 {
			err = rows.Scan(&binlogName, &pos, &nullPtr, &nullPtr, &gtid)
		} else {
			err = rows.Scan(&binlogName, &pos, &nullPtr, &nullPtr)
		}
		if err != nil {
			return binlogPos, gs, errors.Trace(err)
		}

		binlogPos = mysql.Position{Name: binlogName, Pos: pos}
		generalGTIDSet, err := mysql.ParseMysqlGTIDSet(gtid)
		if err != nil {
			return binlogPos, gs, errors.Trace(err)
		}
		gs = *(generalGTIDSet.(*mysql.MysqlGTIDSet))
	}
	if rows.Err() != nil {
		return binlogPos, gs, errors.Trace(rows.Err())
	}
	return binlogPos, gs, nil
}

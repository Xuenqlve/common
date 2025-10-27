package binlog

import (
	"fmt"

	"github.com/go-mysql-org/go-mysql/mysql"
)

type Position struct {
	BinLogFileName string `mapstructure:"binlog-filename" toml:"binlog-filename" json:"binlog-filename"`
	BinLogFilePos  uint32 `mapstructure:"binlog-position" toml:"binlog-position" json:"binlog-position"`
	BinlogGTID     string `mapstructure:"gtid" toml:"gtid" json:"gtid"`
}

func (pos Position) mysqlPosition() mysql.Position {
	return mysql.Position{Name: pos.BinLogFileName, Pos: pos.BinLogFilePos}
}

func (pos Position) Check() (bool, error) {
	if pos.BinlogGTID != "" {
		return true, nil
	}

	if pos.BinLogFileName != "" && pos.BinLogFilePos != 0 {
		return true, nil
	} else if pos.BinLogFileName == "" && pos.BinLogFilePos == 0 {
		return false, nil
	} else {
		return false, fmt.Errorf("event_handler-filename & event_handler-position must be configured")
	}
}

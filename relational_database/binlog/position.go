package binlog

import (
	"database/sql"
	"fmt"
	"sync"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/xuenqlve/common/errors"
	"github.com/xuenqlve/common/log"
)

var binlogMu sync.Mutex

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

//func NewBinlogPositionValue() *PositionValue {
//	return &PositionValue{
//		CurrentPosition: &Position{},
//		StartPosition:   &Position{},
//	}
//}
//
//type PositionValue struct {
//	mu              sync.Mutex
//	CurrentPosition *Position `json:"current_position"`
//	StartPosition   *Position `json:"start_position"`
//}
//
//func (b *PositionValue) Copy(v *PositionValue) {
//	b.mu.Lock()
//	defer b.mu.Unlock()
//	b.StartPosition = v.StartPosition
//	b.CurrentPosition = v.CurrentPosition
//}
//
//func (b *PositionValue) PushCurrentPosition(pos Position) {
//	b.mu.Lock()
//	defer b.mu.Unlock()
//	b.CurrentPosition = &pos
//}
//
//func (b *PositionValue) GetCurrentPosition() Position {
//	b.mu.Lock()
//	defer b.mu.Unlock()
//	return *b.CurrentPosition
//}
//
//func (b *PositionValue) MergeUpload(pos position.Position) (position.Position, error) {
//	currentPos, ok := pos.Value.(Position)
//	if !ok {
//		return position.Position{}, fmt.Errorf("invalid position(BatchPosition) type")
//	}
//	b.PushCurrentPosition(currentPos)
//	return position.Position{
//		Meta:        pos.Meta,
//		ForceCommit: false,
//		Value:       b,
//	}, nil
//}
//
//func (b *PositionValue) ValueInterface() position.ValueInterface {
//	return position.ValueInterface{
//		ValueEncoder: EncodeBinlogPositionValue,
//		ValueDecoder: DecodeBinlogPositionValue,
//	}
//}
//
//func EncodeBinlogPositionValue(v interface{}) (string, error) {
//	value, ok := v.(*PositionValue)
//	if !ok {
//		return "", fmt.Errorf("invalid position value type: %v", reflect.TypeOf(v))
//	}
//	binlogMu.Lock()
//	defer binlogMu.Unlock()
//	data, err := json.Marshal(value)
//	if err != nil {
//		return "", err
//	}
//	return string(data), nil
//}
//
//func DecodeBinlogPositionValue(s string) (interface{}, error) {
//	value := PositionValue{}
//	if err := json.Unmarshal([]byte(s), &value); err != nil {
//		return nil, errors.Trace(err)
//	}
//	return &value, nil
//}
//func InitStartPosition(conn *sql.DB) (*PositionValue, error) {
//	sp := StartBinlogTool{conn: conn}
//	if err := sp.CheckBinlogFormat(); err != nil {
//		return nil, errors.Trace(err)
//	}
//	binlogPos, gtid, err := sp.GetMasterStatus()
//	if err != nil {
//		return nil, errors.Trace(err)
//	}
//	return &PositionValue{
//		CurrentPosition: &Position{
//			BinLogFileName: binlogPos.Name,
//			BinLogFilePos:  binlogPos.Pos,
//			BinlogGTID:     gtid.String(),
//		},
//		StartPosition: &Position{
//			BinLogFileName: binlogPos.Name,
//			BinLogFilePos:  binlogPos.Pos,
//			BinlogGTID:     gtid.String(),
//		},
//	}, nil
//}
//
//func NewStartBinlogTool(conn *sql.DB) *StartBinlogTool {
//	return &StartBinlogTool{conn: conn}
//}

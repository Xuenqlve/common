package binlog

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"strings"
	"sync/atomic"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-mysql-org/go-mysql/utils"
	uuid "github.com/satori/go.uuid"
	//"github.com/xuenqlve/common/data_source"
	"github.com/xuenqlve/common/ddl_parser"
	"github.com/xuenqlve/common/errors"
	"github.com/xuenqlve/common/event"
	"github.com/xuenqlve/common/log"
	"github.com/xuenqlve/common/schema_store"
)

type BinlogReaderConfig struct {
	Host          string   `mapstructure:"host" yaml:"host" toml:"host"`
	Port          uint16   `mapstructure:"port" yaml:"port" toml:"port"`
	User          string   `mapstructure:"user" yaml:"user" toml:"user"`
	Password      string   `mapstructure:"password" yaml:"password" toml:"password"`
	ServerID      uint32   `mapstructure:"app-id" yaml:"app-id" toml:"app-id"`
	StartPosition Position `mapstructure:"start-pos" yaml:"start-pos" toml:"start-pos"`
}

func (c *BinlogReaderConfig) SetStartPosition(pos Position) {
	c.StartPosition = pos
}

func (c *BinlogReaderConfig) Check() (bool, error) {
	pos := c.StartPosition
	if pos.BinlogGTID != "" {
		return true, nil
	}
	if pos.BinLogFileName != "" && pos.BinLogFilePos != 0 {
		return true, nil
	} else if pos.BinLogFileName == "" && pos.BinLogFilePos == 0 {
		return false, nil
	} else {
		return false, fmt.Errorf("binlog-filename & binlog-position must be configured")
	}
}

type BinlogReader struct {
	ctx             context.Context
	cancelFunc      context.CancelFunc
	conn            *sql.DB
	streamer        *replication.BinlogStreamer
	syncer          *replication.BinlogSyncer
	delay           *uint32
	cfg             BinlogReaderConfig
	eventHandler    EventHandler
	loader          ddl_parser.Loader
	timestamp       uint32
	currentPosition Position
	closed          atomic.Bool
}

func NewBinlogReader(ctx context.Context, cfg BinlogReaderConfig) (reader *BinlogReader, err error) {
	ctxWithCancel, cancelFunc := context.WithCancel(ctx)
	reader = &BinlogReader{
		ctx:             ctxWithCancel,
		cancelFunc:      cancelFunc,
		cfg:             cfg,
		delay:           new(uint32),
		currentPosition: cfg.StartPosition,
	}
	reader.closed.Store(false)
	reader.syncer = NewBinlogSyncer(cfg.ServerID, cfg.Host, cfg.Port, cfg.User, cfg.Password)
	reader.streamer, err = NewBinlogStreamer(reader.syncer, cfg.StartPosition)
	if err != nil {
		return nil, err
	}
	return
}

func (r *BinlogReader) Run() error {
	if r.eventHandler == nil {
		return fmt.Errorf("eventHandler is nil")
	}
	// 捕获 panic
	defer func() {
		if p := recover(); p != nil {
			// 检查是否是正常关闭导致的 panic
			if r.closed.Load() {
				// 正常关闭情况下的 panic，忽略
				return
			}
			// 其他类型的 panic，记录日志
			event.EventAdmin.Upload(event.ErrorEvent(event.SyncExceptionsPanicExit, fmt.Errorf("BinlogReader run panic: %v", p)))
		}
	}()

	// todo 初始化提交位点 force 为 false
	if err := r.eventHandler.OnPosSynced(r.currentPosition, false); err != nil {
		return errors.Trace(err)
	}
	for {
		// 检查是否已取消
		select {
		case <-r.ctx.Done():
			log.Infof("BinlogReader context canceled, exiting Run loop")
			return nil
		default:
		}

		event, err := r.streamer.GetEvent(r.ctx)
		if err != nil {
			// 检查是否是因为上下文取消导致的错误
			if r.ctx.Err() != nil {
				log.Infof("BinlogReader context canceled during GetEvent: %v", r.ctx.Err())
				return nil
			}
			return errors.Trace(err)
		}
		r.currentPosition.BinLogFilePos = event.Header.LogPos
		r.updateReplicationDelay(event)
		switch e := event.Event.(type) {
		// If the timestamp equals zero, the received rotate event is a fake rotate event
		// and contains only the name of the next event_handler file. Its log position should be
		// ignored.
		// See https://github.com/mysql/mysql-server/blob/8e797a5d6eb3a87f16498edcb7261a75897babae/sql/rpl_binlog_sender.h#L235
		// and https://github.com/mysql/mysql-server/blob/8cc757da3d87bf4a1f07dcfb2d3c96fed3806870/sql/rpl_binlog_sender.cc#L899
		case *replication.RotateEvent:
			if event.Header.Timestamp == 0 {
				fakeRotateLogName := string(e.NextLogName)
				log.Infof("received fake rotate event, next log name is %s", e.NextLogName)
				if fakeRotateLogName != r.currentPosition.BinLogFileName {
					log.Infof("log name changed, the fake rotate event will be handled as a real rotate event")
				} else {
					continue
				}
			}
			if compareBinlogPosition(mysql.Position{Name: string(e.NextLogName), Pos: uint32(e.Position)}, r.currentPosition.mysqlPosition(), 0) <= 0 {
				log.Infof(
					"[binlogTailer] skip rotate event: source event_handler Name %v, source event_handler StreamPos: %v; store Name: %v, store StreamPos: %v",
					e.NextLogName,
					e.Position,
					r.currentPosition.BinLogFileName,
					r.currentPosition.BinLogFilePos,
				)
				continue
			}
		}
		if err = r.handleEvent(event); err != nil {
			return errors.Trace(err)
		}
	}
}

func (r *BinlogReader) handleEvent(ev *replication.BinlogEvent) (err error) {
	savePos := false
	force := false
	currentPos := r.currentPosition
	currentPos.BinLogFilePos = ev.Header.LogPos

	r.eventHandler.SyncedTimestamp(ev.Header.Timestamp)
	switch e := ev.Event.(type) {
	case *replication.RotateEvent:
		savePos = true
		currentPos.BinLogFileName = string(e.NextLogName)
		currentPos.BinLogFilePos = uint32(e.Position)
	case *replication.XIDEvent:
		savePos = true
		force = true
		if err = r.eventHandler.OnXID(e.XID); err != nil {
			return errors.Trace(err)
		}
		if e.GSet != nil {
			currentPos.BinlogGTID = e.GSet.String()
		}
	case *replication.MariadbGTIDEvent:
		var gtidSet mysql.GTIDSet
		gtidSet, err = e.GTIDNext()
		if err != nil {
			return errors.Trace(err)
		}
		currentPos.BinlogGTID = gtidSet.String()
		if err = r.eventHandler.OnGTID(gtidSet.String()); err != nil {
			return errors.Trace(err)
		}
	case *replication.GTIDEvent:
		binlogGTID := ""
		binlogGTID, err = r.makeGTIDSet(e.SID, e.GNO)
		currentPos.BinlogGTID = binlogGTID
		if err = r.eventHandler.OnGTID(binlogGTID); err != nil {
			return errors.Trace(err)
		}
	case *replication.RowsQueryEvent:
		if err = r.eventHandler.OnRowsQueryEvent(e.Query); err != nil {
			return errors.Trace(err)
		}
	case *replication.TransactionPayloadEvent:
		for _, subEvent := range e.Events {
			if err = r.handleEvent(subEvent); err != nil {
				log.Errorf("handle transaction payload subevent at (%s, %d) error %v", currentPos.BinLogFileName, currentPos.BinLogFilePos, err)
				return errors.Trace(err)
			}
		}
	case *replication.QueryEvent:
		ddlSQL := strings.TrimSpace(string(e.Query))
		if ddlSQL == "BEGIN" || ddlSQL == "COMMIT" {
			return nil
		}
		savePos = true
		if e.GSet != nil {
			currentPos.BinlogGTID = e.GSet.String()
		}
		force = true
		if err = r.eventHandler.OnDDL(e.Schema, e.Query); err != nil {
			return errors.Trace(err)
		}
	case *replication.RowsEvent:
		var dmlType schema_store.DML
		switch ev.Header.EventType {
		case replication.WRITE_ROWS_EVENTv0, replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
			dmlType = schema_store.Insert
		case replication.UPDATE_ROWS_EVENTv0, replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
			dmlType = schema_store.Update
		case replication.DELETE_ROWS_EVENTv0, replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
			dmlType = schema_store.Delete
		default:
			return errors.Errorf("[RowsEvent] unknown rows event type: %v", ev.Header.EventType)
		}
		if err = r.eventHandler.OnRow(dmlType, e); err != nil {
			return errors.Trace(err)
		}
	}
	if savePos {
		r.currentPosition = currentPos
		r.timestamp = ev.Header.Timestamp
		if err = r.eventHandler.OnPosSynced(currentPos, force); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (r *BinlogReader) makeGTIDSet(sid []byte, gno int64) (string, error) {
	u, err := uuid.FromBytes(sid)
	if err != nil {
		return "", errors.Trace(err)
	}
	eventGTIDString := fmt.Sprintf("%s:1-%d", u.String(), gno)
	eventUUIDSet, err := mysql.ParseUUIDSet(eventGTIDString)
	if err != nil {
		return "", fmt.Errorf("[event_handler] failed at ParseUUIDSet %v eventGTIDString %s", err, eventGTIDString)
	}
	currentGTIDSet, err := mysql.ParseMysqlGTIDSet(r.currentPosition.BinlogGTID)
	if err != nil {
		return "", fmt.Errorf("[event_handler] failed at ParseMysqlGTIDSet %v", err)
	}
	s := currentGTIDSet.(*mysql.MysqlGTIDSet)
	s.AddSet(eventUUIDSet)
	return s.String(), nil
}

func (r *BinlogReader) updateReplicationDelay(ev *replication.BinlogEvent) {
	var newDelay uint32
	now := uint32(utils.Now().Unix())
	if now >= ev.Header.Timestamp {
		newDelay = now - ev.Header.Timestamp
	}
	atomic.StoreUint32(r.delay, newDelay)
}

func (r *BinlogReader) SetEventHandler(h EventHandler) {
	r.eventHandler = h
}

func (r *BinlogReader) GetDelay() uint32 {
	return atomic.LoadUint32(r.delay)
}

func (r *BinlogReader) SyncedTimestamp() uint32 {
	return r.timestamp
}

func (r *BinlogReader) Close() {
	// 如果已经关闭，直接返回
	if r.closed.Swap(true) {
		return
	}

	// 先取消上下文，停止 Run 方法中的循环
	r.cancelFunc()

	if r.eventHandler != nil {
		if err := r.eventHandler.OnPosSynced(r.currentPosition, true); err != nil {
			log.Errorf("close event_handler OnPosSynced err: %v", err)
		}
	}

	log.Infof("close binlog reader")

	// 关闭 syncer
	if r.syncer != nil {
		r.syncer.Close()
	}

	// 关闭数据库连接
	if r.conn != nil {
		if err := r.conn.Close(); err != nil {
			log.Errorf("close binlog connection err: %v", err)
		}
	}
}

func compareBinlogPosition(a, b mysql.Position, deviation float64) int {
	if a.Name < b.Name {
		return -1
	}

	if a.Name > b.Name {
		return 1
	}

	if math.Abs(float64(a.Pos-b.Pos)) <= deviation {
		return 0
	}

	if a.Pos < b.Pos {
		return -1
	}

	return 1
}

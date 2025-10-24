package binlog

import (
	"fmt"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/xuenqlve/common/errors"
)

func NewBinlogCanal(serverID uint32, host string, port uint16, user, password string) (*canal.Canal, error) {
	canalConfig := &canal.Config{
		ServerID:             serverID,
		Flavor:               "mysql",
		Addr:                 fmt.Sprintf("%s:%d", host, port),
		User:                 user,
		Password:             password,
		ParseTime:            true,
		MaxReconnectAttempts: 10,
	}
	return canal.NewCanal(canalConfig)
}

func CanalRunFrom(c *canal.Canal, position Position) error {
	if position.BinlogGTID == "" {
		pos := mysql.Position{
			Name: position.BinLogFileName,
			Pos:  position.BinLogFilePos,
		}
		return c.RunFrom(pos)

	} else {
		gtidSet, err := mysql.ParseMysqlGTIDSet(position.BinlogGTID)
		if err != nil {
			return err
		}
		return c.StartFromGTID(gtidSet)
	}
}

func NewBinlogSyncer(serverID uint32, host string, port uint16, user, password string) *replication.BinlogSyncer {
	syncerConfig := replication.BinlogSyncerConfig{
		ServerID:             serverID,
		Flavor:               "mysql",
		Host:                 host,
		Port:                 port,
		User:                 user,
		Password:             password,
		ParseTime:            true,
		MaxReconnectAttempts: 10,
	}

	return replication.NewBinlogSyncer(syncerConfig)
}

func NewBinlogStreamer(syncer *replication.BinlogSyncer, position Position) (streamer *replication.BinlogStreamer, err error) {
	if position.BinlogGTID == "" {
		pos := mysql.Position{
			Name: position.BinLogFileName,
			Pos:  position.BinLogFilePos,
		}
		streamer, err = syncer.StartSync(pos)
		if err != nil {
			return nil, errors.Trace(err)
		}
	} else {
		var gtidSet mysql.GTIDSet
		gtidSet, err = mysql.ParseMysqlGTIDSet(position.BinlogGTID)
		if err != nil {
			return nil, errors.Trace(err)
		}
		streamer, err = syncer.StartSyncGTID(gtidSet)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	return
}

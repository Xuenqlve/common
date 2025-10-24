package binlog

import (
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/xuenqlve/common/schema_store"
)

type EventHandler interface {
	OnXID(xid uint64) error

	OnGTID(gtid string) error

	OnRow(dmlType schema_store.DML, event *replication.RowsEvent) error

	// begin Commit 操作也会变为ddl
	OnDDL(schema, query []byte) error

	//OnRowsQueryEvent is called when binlog_rows_query_log_events=ON for each DML query.
	OnRowsQueryEvent(query []byte) error

	// OnPosSynced Use your own way to sync position. When force is true, sync position immediately.
	OnPosSynced(pos Position, force bool) error

	SyncedTimestamp(timestamp uint32)
}

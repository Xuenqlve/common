package oplog

import (
	"go.mongodb.org/mongo-driver/bson"
)

type EventHandler interface {
	OnInsertEvent(database, collection string, object bson.D) error

	OnDeleteEvent(database, collection string, key bson.D) error

	OnUpdateEvent(database, collection string, key bson.D, object bson.D) error

	// begin Commit 操作也会变为ddl
	OnDDLEvent(event Event) error

	// OnPosSynced Use your own way to sync position. When force is true, sync position immediately.
	OnPosSynced(pos Position, force bool) error

	SyncedTimestamp(timestamp uint32)
}

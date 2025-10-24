package schema_store

var DDLOperation = "DDL"

var (

	// mysql
	CREATE_DATABASE DDL = "CREATE DATABASE"
	DROP_DATABASE   DDL = "DROP DATABASE"
	CREATE_TABLE    DDL = "CREATE TABLE"
	ALTER_TABLE     DDL = "ALTER TABLE"
	DROP_TABLE      DDL = "DROP TABLE"
	RENAME_TABLE    DDL = "RENAME TABLE"
	TRUNCATE_TABLE  DDL = "TRUNCATE TABLE"
	CREATE_INDEX    DDL = "CREATE INDEX"
	DROP_INDEX      DDL = "DROP INDEX"
	UNKNOWN         DDL = "UNKNOWN"

	//mongo
	Create_Indexes DDL = "createIndexes"
	Create_Table   DDL = "createTable"
	Drop           DDL = "drop"
	Drop_Database  DDL = "dropDatabase"
	Drop_Indexes   DDL = "dropIndexes"
	Rename         DDL = "renameCollection"
	NOOP           DDL = "NOOP"
)

type DDL string

func (d DDL) String() string {
	return string(d)
}

var allDDLOperation = []DDL{CREATE_DATABASE, DROP_DATABASE, CREATE_TABLE, DROP_TABLE, RENAME_TABLE, ALTER_TABLE, TRUNCATE_TABLE, CREATE_INDEX, DROP_INDEX}

func GetAllDDLOperation() []DDL {
	return allDDLOperation
}

var DDLMap = map[string]DDL{
	CREATE_DATABASE.String(): CREATE_DATABASE,
	DROP_DATABASE.String():   DROP_DATABASE,
	CREATE_TABLE.String():    CREATE_TABLE,
	ALTER_TABLE.String():     ALTER_TABLE,
	DROP_TABLE.String():      DROP_TABLE,
	RENAME_TABLE.String():    RENAME_TABLE,
	TRUNCATE_TABLE.String():  TRUNCATE_TABLE,
	CREATE_INDEX.String():    CREATE_INDEX,
	DROP_INDEX.String():      DROP_INDEX,
	Create_Indexes.String():  Create_Indexes,
	Create_Table.String():    Create_Table,
	Drop.String():            Drop,
	Drop_Database.String():   Drop_Database,
	Drop_Indexes.String():    Drop_Indexes,
	Rename.String():          Rename,
}

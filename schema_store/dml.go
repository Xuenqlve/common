package schema_store

var DMLOperation = "DML"

type DML string

var (
	Insert DML = "insert"
	Update DML = "update"
	Delete DML = "delete"
)

var DMLMap = map[string]DML{
	Insert.String(): Insert,
	Update.String(): Update,
	Delete.String(): Delete,
}

func (d DML) String() string {
	return string(d)
}

var allDMLOperation = []DML{Insert, Update, Delete}

func GetAllDMLOperation() []DML {
	return allDMLOperation
}

func GetAllDMLOperationMap() map[string]struct{} {
	m := make(map[string]struct{})
	for _, op := range allDMLOperation {
		m[op.String()] = struct{}{}
	}
	return m
}

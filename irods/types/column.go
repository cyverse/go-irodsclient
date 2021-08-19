package types

// ColumnType is a type of iRODS Column
type ColumnType string

const (
	// ColumnTypeInteger is for integer column
	ColumnTypeInteger ColumnType = "Integer"
	// ColumnTypeString is for string column
	ColumnTypeString ColumnType = "String"
	// ColumnTypeDateTime is for datetime column
	ColumnTypeDateTime ColumnType = "DateTime"
)

// IRODSColumn is a struct holding a column
type IRODSColumn struct {
	Type    ColumnType
	ICatKey string
	ICatID  int
}

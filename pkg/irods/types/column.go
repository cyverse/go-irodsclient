package types

type ColumnType string

const (
	ColumnTypeInteger  ColumnType = "Integer"
	ColumnTypeString   ColumnType = "String"
	ColumnTypeDateTime ColumnType = "DateTime"
)

// IRODSColumn ..
type IRODSColumn struct {
	Type    ColumnType
	ICatKey string
	ICatID  int
}

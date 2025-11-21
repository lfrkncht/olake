package driver

import "github.com/datazip-inc/olake/types"

// Define a mapping of MS SQL Server data types to internal data types
var mssqlTypeToDataTypes = map[string]types.DataType{
	// Exact numerics
	"bigint":   types.Int64,
	"int":      types.Int32,
	"smallint": types.Int32,
	"tinyint":  types.Int32,
	"bit":      types.Bool,

	// Approximate numerics
	"float":   types.Float64,
	"real":    types.Float32,
	"decimal": types.Float64,
	"numeric": types.Float64,
	"money":   types.Float64,
	"smallmoney": types.Float64,

	// Character strings
	"char":    types.String,
	"varchar": types.String,
	"text":    types.String,

	// Unicode character strings
	"nchar":    types.String,
	"nvarchar": types.String,
	"ntext":    types.String,

	// Binary strings
	"binary":    types.String,
	"varbinary": types.String,
	"image":     types.String,

	// Date and time types
	"date":           types.Timestamp,
	"datetime":       types.Timestamp,
	"datetime2":      types.Timestamp,
	"smalldatetime":  types.Timestamp,
	"datetimeoffset": types.Timestamp,
	"time":           types.String,

	// Other data types
	"uniqueidentifier": types.String, // UUID
	"xml":              types.String,
	"json":             types.String,

	// SQL Server specific types
	"sql_variant":      types.String,
	"hierarchyid":      types.String,
	"geometry":         types.String,
	"geography":        types.String,
	"rowversion":       types.String,
	"timestamp":        types.String,

	// Large value types (deprecated but still used)
	"varchar(max)":   types.String,
	"nvarchar(max)":  types.String,
	"varbinary(max)": types.String,
}

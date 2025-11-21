package driver

import (
	"context"
	"fmt"

	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/types"
)

// PreCDC is a stub - CDC is not currently supported for MS SQL Server
func (m *MSSQL) PreCDC(ctx context.Context, streams []types.StreamInterface) error {
	return fmt.Errorf("CDC is not currently supported for MS SQL Server")
}

// StreamChanges is a stub - CDC is not currently supported for MS SQL Server
func (m *MSSQL) StreamChanges(ctx context.Context, stream types.StreamInterface, processFn abstract.CDCMsgFn) error {
	return fmt.Errorf("CDC is not currently supported for MS SQL Server")
}

// PostCDC is a stub - CDC is not currently supported for MS SQL Server
func (m *MSSQL) PostCDC(ctx context.Context, stream types.StreamInterface, success bool, readerID string) error {
	// No-op for now since CDC is not supported
	return nil
}

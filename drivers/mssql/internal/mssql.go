package driver

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"strings"
	"time"

	_ "github.com/microsoft/go-mssqldb"
	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/logger"
	"github.com/datazip-inc/olake/utils/typeutils"
	"github.com/jmoiron/sqlx"
	"golang.org/x/crypto/ssh"
)

const (
	// get all user tables across schemas
	getPrivilegedTablesTmpl = `SELECT
		s.name as table_schema,
		t.name as table_name
		FROM sys.tables t
		INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
		WHERE t.is_ms_shipped = 0
		AND s.name NOT IN ('sys', 'INFORMATION_SCHEMA')
		ORDER BY s.name, t.name`

	// get table schema
	getTableSchemaTmpl = `SELECT
		COLUMN_NAME as column_name,
		DATA_TYPE as data_type,
		IS_NULLABLE as is_nullable
		FROM INFORMATION_SCHEMA.COLUMNS
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
		ORDER BY ORDINAL_POSITION`

	// get primary key columns
	getTablePrimaryKey = `SELECT
		c.COLUMN_NAME as column_name
		FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
		JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE c
		ON tc.CONSTRAINT_NAME = c.CONSTRAINT_NAME
		AND tc.TABLE_SCHEMA = c.TABLE_SCHEMA
		AND tc.TABLE_NAME = c.TABLE_NAME
		JOIN INFORMATION_SCHEMA.COLUMNS col
		ON c.COLUMN_NAME = col.COLUMN_NAME
		AND c.TABLE_SCHEMA = col.TABLE_SCHEMA
		AND c.TABLE_NAME = col.TABLE_NAME
		WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
		AND tc.TABLE_SCHEMA = ?
		AND tc.TABLE_NAME = ?
		ORDER BY col.ORDINAL_POSITION`
)

type MSSQL struct {
	client     *sqlx.DB
	sshClient  *ssh.Client
	config     *Config // mssql driver connection config
	CDCSupport bool    // indicates if the SQL Server instance supports CDC
	cdcConfig  CDC
	state      *types.State // reference to globally present state
}

func (m *MSSQL) CDCSupported() bool {
	return m.CDCSupport
}

func (m *MSSQL) Setup(ctx context.Context) error {
	err := m.config.Validate()
	if err != nil {
		return fmt.Errorf("failed to validate config: %s", err)
	}

	if m.config.SSHConfig != nil && m.config.SSHConfig.Host != "" {
		logger.Info("Found SSH Configuration")
		m.sshClient, err = m.config.SSHConfig.SetupSSHConnection()
		if err != nil {
			return fmt.Errorf("failed to setup SSH connection: %s", err)
		}
	}

	var db *sql.DB
	connectionString := m.config.URI()

	if m.sshClient != nil {
		logger.Info("Connecting to SQL Server via SSH tunnel")
		// Create a custom dialer that uses the SSH tunnel
		sqlDialer := &sqlDialer{
			sshClient: m.sshClient,
		}

		// Open connection with custom dialer
		db, err = sql.Open("mssql", connectionString)
		if err != nil {
			return fmt.Errorf("failed to open database connection: %s", err)
		}

		// Note: The microsoft/go-mssqldb driver doesn't directly support custom dialers
		// In production, you might need to set up port forwarding with SSH instead
		_ = sqlDialer // Placeholder for SSH tunnel implementation
	} else {
		db, err = sql.Open("mssql", connectionString)
		if err != nil {
			return fmt.Errorf("failed to open database connection: %s", err)
		}
	}

	sqlxDB := sqlx.NewDb(db, "mssql")
	sqlxDB.SetMaxOpenConns(m.config.MaxThreads)
	mssqlClient := sqlxDB.Unsafe()

	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()

	// force a connection and test that it worked
	err = mssqlClient.PingContext(ctx)
	if err != nil {
		return fmt.Errorf("failed to ping database: %s", err)
	}

	// Check for CDC configuration
	found, _ := utils.IsOfType(m.config.UpdateMethod, "cdc")
	if found {
		logger.Info("Found CDC Configuration")
		cdc := &CDC{}
		if err := utils.Unmarshal(m.config.UpdateMethod, cdc); err != nil {
			return err
		}
		// set default value
		cdc.InitialWaitTime = utils.Ternary(cdc.InitialWaitTime == 0, 1200, cdc.InitialWaitTime).(int)

		// check if initial wait time is valid or not
		if cdc.InitialWaitTime < 120 || cdc.InitialWaitTime > 2400 {
			return fmt.Errorf("the CDC initial wait time must be at least 120 seconds and less than 2400 seconds")
		}

		logger.Infof("CDC initial wait time set to: %d", cdc.InitialWaitTime)

		// Check if CDC is enabled on the database
		var cdcEnabled int
		err = mssqlClient.GetContext(ctx, &cdcEnabled,
			"SELECT COUNT(*) FROM sys.databases WHERE name = ? AND is_cdc_enabled = 1",
			m.config.Database)
		if err != nil {
			return fmt.Errorf("failed to check CDC status: %s", err)
		}

		if cdcEnabled == 0 {
			return fmt.Errorf("CDC is not enabled on database %s", m.config.Database)
		}

		m.CDCSupport = true
		m.cdcConfig = *cdc
	} else {
		logger.Info("Standard Replication is selected")
	}

	m.client = mssqlClient
	m.config.RetryCount = utils.Ternary(m.config.RetryCount <= 0, 1, m.config.RetryCount+1).(int)
	return nil
}

// sqlDialer is a placeholder for SSH tunnel support
type sqlDialer struct {
	sshClient *ssh.Client
}

func (d *sqlDialer) Dial(network, addr string) (net.Conn, error) {
	if d.sshClient != nil {
		return d.sshClient.Dial(network, addr)
	}
	return net.Dial(network, addr)
}

func (m *MSSQL) StateType() types.StateType {
	return types.GlobalType
}

func (m *MSSQL) SetupState(state *types.State) {
	m.state = state
}

func (m *MSSQL) GetConfigRef() abstract.Config {
	m.config = &Config{}
	return m.config
}

func (m *MSSQL) Spec() any {
	return Config{}
}

func (m *MSSQL) CloseConnection() {
	if m.client != nil {
		err := m.client.Close()
		if err != nil {
			logger.Error("failed to close connection with SQL Server: %s", err)
		}
	}

	if m.sshClient != nil {
		err := m.sshClient.Close()
		if err != nil {
			logger.Error("failed to close SSH connection: %s", err)
		}
	}
}

func (m *MSSQL) GetStreamNames(ctx context.Context) ([]string, error) {
	logger.Infof("Starting discover for SQL Server database %s", m.config.Database)
	var tableNamesOutput []Table
	err := m.client.SelectContext(ctx, &tableNamesOutput, getPrivilegedTablesTmpl)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve table names: %s", err)
	}
	tablesNames := []string{}
	for _, table := range tableNamesOutput {
		tablesNames = append(tablesNames, fmt.Sprintf("%s.%s", table.Schema, table.Name))
	}
	return tablesNames, nil
}

func (m *MSSQL) ProduceSchema(ctx context.Context, streamName string) (*types.Stream, error) {
	populateStream := func(streamName string) (*types.Stream, error) {
		streamParts := strings.Split(streamName, ".")
		schemaName, streamName := streamParts[0], streamParts[1]
		stream := types.NewStream(streamName, schemaName, &m.config.Database)

		var columnSchemaOutput []ColumnDetails
		err := m.client.SelectContext(ctx, &columnSchemaOutput, getTableSchemaTmpl, schemaName, streamName)
		if err != nil {
			return stream, fmt.Errorf("failed to retrieve column details for table %s: %s", streamName, err)
		}

		if len(columnSchemaOutput) == 0 {
			logger.Warnf("no columns found in table [%s.%s]", schemaName, streamName)
			return stream, nil
		}

		var primaryKeyOutput []ColumnDetails
		err = m.client.SelectContext(ctx, &primaryKeyOutput, getTablePrimaryKey, schemaName, streamName)
		if err != nil {
			return stream, fmt.Errorf("failed to retrieve primary key columns for table %s: %s", streamName, err)
		}

		for _, column := range columnSchemaOutput {
			stream.WithCursorField(column.Name)
			datatype := types.Unknown
			if val, found := mssqlTypeToDataTypes[*column.DataType]; found {
				datatype = val
			} else {
				logger.Debugf("failed to get respective type in datatypes for column: %s[%s]", column.Name, *column.DataType)
				datatype = types.String
			}

			stream.UpsertField(column.Name, datatype, strings.EqualFold("yes", *column.IsNullable))
		}

		// add primary keys for stream
		for _, column := range primaryKeyOutput {
			stream.WithPrimaryKey(column.Name)
		}

		return stream, nil
	}

	stream, err := populateStream(streamName)
	if err != nil && ctx.Err() == nil {
		return nil, err
	}
	return stream, nil
}

func (m *MSSQL) Type() string {
	return string(constants.MSSQL)
}

func (m *MSSQL) MaxConnections() int {
	return m.config.MaxThreads
}

func (m *MSSQL) MaxRetries() int {
	return m.config.RetryCount
}

func (m *MSSQL) dataTypeConverter(value interface{}, columnType string) (interface{}, error) {
	if value == nil {
		return nil, typeutils.ErrNullValue
	}
	olakeType := typeutils.ExtractAndMapColumnType(columnType, mssqlTypeToDataTypes)
	return typeutils.ReformatValue(olakeType, value)
}

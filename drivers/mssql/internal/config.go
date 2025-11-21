package driver

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/utils"
)

// Config represents the configuration for connecting to a Microsoft SQL Server database
type Config struct {
	Host             string            `json:"host"`
	Port             int               `json:"port"`
	Database         string            `json:"database"`
	Username         string            `json:"username"`
	Password         string            `json:"password"`
	Schema           string            `json:"schema"` // Default schema (typically 'dbo')
	JDBCURLParams    map[string]string `json:"jdbc_url_params"`
	SSLConfiguration *utils.SSLConfig  `json:"ssl"`
	UpdateMethod     interface{}       `json:"update_method"`
	MaxThreads       int               `json:"max_threads"`
	RetryCount       int               `json:"retry_count"`
	SSHConfig        *utils.SSHConfig  `json:"ssh_config"`
	ConnectionURL    *url.URL          `json:"-"` // Internal use only
}

// CDC configuration for SQL Server Change Data Capture
type CDC struct {
	// Initial wait time must be in range [120,2400), default value 1200
	InitialWaitTime int `json:"initial_wait_time"`
}

// URI generates the connection string for SQL Server
func (c *Config) URI() string {
	// SQL Server connection string format:
	// sqlserver://username:password@host:port?database=dbname&param=value

	// Set default port if not specified
	if c.Port == 0 {
		c.Port = 1433
	}

	// Set default schema if not specified
	if c.Schema == "" {
		c.Schema = "dbo"
	}

	// Build the connection URL
	connectionURL := &url.URL{
		Scheme: "sqlserver",
		User:   url.UserPassword(c.Username, c.Password),
		Host:   fmt.Sprintf("%s:%d", c.Host, c.Port),
	}

	query := connectionURL.Query()

	// Add database name
	if c.Database != "" {
		query.Add("database", c.Database)
	}

	// Add SSL/TLS configuration
	if c.SSLConfiguration != nil {
		if c.SSLConfiguration.Mode == "disable" {
			query.Add("encrypt", "disable")
		} else {
			query.Add("encrypt", "true")
			if c.SSLConfiguration.Mode == "require" || c.SSLConfiguration.Mode == "verify-ca" {
				query.Add("TrustServerCertificate", "false")
			} else if c.SSLConfiguration.Mode == "allow" || c.SSLConfiguration.Mode == "prefer" {
				query.Add("TrustServerCertificate", "true")
			}
		}
	} else {
		// Default to encrypted connection with trusted certificate
		query.Add("encrypt", "true")
		query.Add("TrustServerCertificate", "true")
	}

	// Add additional connection parameters if available
	if len(c.JDBCURLParams) > 0 {
		for k, v := range c.JDBCURLParams {
			query.Add(k, v)
		}
	}

	connectionURL.RawQuery = query.Encode()
	c.ConnectionURL = connectionURL

	return connectionURL.String()
}

// Validate checks the configuration for any missing or invalid fields
func (c *Config) Validate() error {
	if c.Host == "" {
		return fmt.Errorf("empty host name")
	} else if strings.Contains(c.Host, "https") || strings.Contains(c.Host, "http") {
		return fmt.Errorf("host should not contain http or https")
	}

	// Validate port
	if c.Port <= 0 || c.Port > 65535 {
		return fmt.Errorf("invalid port number: must be between 1 and 65535")
	}

	// Validate required fields
	if c.Username == "" {
		return fmt.Errorf("username is required")
	}

	if c.Password == "" {
		return fmt.Errorf("password is required")
	}

	if c.Database == "" {
		return fmt.Errorf("database is required")
	}

	// Set default schema if not specified
	if c.Schema == "" {
		c.Schema = "dbo"
	}

	// Set default number of threads if not provided
	if c.MaxThreads <= 0 {
		c.MaxThreads = constants.DefaultThreadCount
	}

	// Set default retry count if not provided
	if c.RetryCount <= 0 {
		c.RetryCount = constants.DefaultRetryCount
	}

	// Validate SSL configuration if provided
	if c.SSLConfiguration != nil {
		err := c.SSLConfiguration.Validate()
		if err != nil {
			return fmt.Errorf("failed to validate ssl config: %s", err)
		}
	}

	return utils.Validate(c)
}

// Table represents a SQL Server table
type Table struct {
	Schema string `db:"table_schema"`
	Name   string `db:"table_name"`
}

// ColumnDetails represents column metadata
type ColumnDetails struct {
	Name       string  `db:"column_name"`
	DataType   *string `db:"data_type"`
	IsNullable *string `db:"is_nullable"`
}

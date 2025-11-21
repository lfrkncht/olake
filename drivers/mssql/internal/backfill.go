package driver

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/pkg/jdbc"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/logger"
	"github.com/datazip-inc/olake/utils/typeutils"
	"github.com/jmoiron/sqlx"
)

// withIsolation starts a transaction with REPEATABLE READ isolation for MSSQL
// Note: SQL Server driver doesn't support ReadOnly flag in TxOptions
func withIsolation(ctx context.Context, client *sqlx.DB, fn func(tx *sql.Tx) error) error {
	tx, err := client.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelRepeatableRead,
	})
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %s", err)
	}
	defer func() {
		if rerr := tx.Rollback(); rerr != nil && rerr != sql.ErrTxDone {
			logger.Warnf("transaction rollback failed: %s", rerr)
		}
	}()
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}

func (m *MSSQL) ChunkIterator(ctx context.Context, stream types.StreamInterface, chunk types.Chunk, OnMessage abstract.BackfillMsgFn) error {
	opts := jdbc.DriverOptions{
		Driver: constants.MSSQL,
		Stream: stream,
		State:  m.state,
	}
	thresholdFilter, args, err := jdbc.ThresholdFilter(ctx, opts)
	if err != nil {
		return fmt.Errorf("failed to set threshold filter: %s", err)
	}

	filter, err := jdbc.SQLFilter(stream, m.Type(), thresholdFilter)
	if err != nil {
		return fmt.Errorf("failed to parse filter during chunk iteration: %s", err)
	}

	// Begin transaction with read committed snapshot isolation
	return withIsolation(ctx, m.client, func(tx *sql.Tx) error {
		// Build query for the chunk
		pkColumns := stream.GetStream().SourceDefinedPrimaryKey.Array()
		chunkColumn := stream.Self().StreamMetadata.ChunkColumn
		sort.Strings(pkColumns)

		logger.Debugf("Starting backfill from %v to %v with filter: %s, args: %v", chunk.Min, chunk.Max, filter, args)

		// Get chunks from state or calculate new ones
		stmt := ""
		if chunkColumn != "" {
			stmt = mssqlChunkScanQuery(stream, []string{chunkColumn}, chunk, filter)
		} else if len(pkColumns) > 0 {
			stmt = mssqlChunkScanQuery(stream, pkColumns, chunk, filter)
		} else {
			stmt = mssqlLimitOffsetScanQuery(stream, chunk, filter)
		}

		logger.Debugf("Executing chunk query: %s", stmt)
		setter := jdbc.NewReader(ctx, stmt, func(ctx context.Context, query string, queryArgs ...any) (*sql.Rows, error) {
			return tx.QueryContext(ctx, query, args...)
		})

		// Capture and process rows
		return setter.Capture(func(rows *sql.Rows) error {
			record := make(types.Record)
			err := jdbc.MapScan(rows, record, m.dataTypeConverter)
			if err != nil {
				return fmt.Errorf("failed to scan record data as map: %s", err)
			}
			return OnMessage(ctx, record)
		})
	})
}

func (m *MSSQL) GetOrSplitChunks(ctx context.Context, pool *destination.WriterPool, stream types.StreamInterface) (*types.Set[types.Chunk], error) {
	var approxRowCount int64
	var avgRowSize any

	// SQL Server specific query for table statistics
	approxRowCountQuery := fmt.Sprintf(`
		SELECT
			SUM(p.rows) as approx_row_count,
			CASE WHEN SUM(p.rows) > 0
				THEN (SUM(a.total_pages) * 8192.0) / SUM(p.rows)
				ELSE 0
			END as avg_row_size
		FROM sys.partitions p
		INNER JOIN sys.allocation_units a ON p.partition_id = a.container_id
		INNER JOIN sys.tables t ON p.object_id = t.object_id
		INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
		WHERE s.name = ? AND t.name = ?
		AND p.index_id IN (0,1)
		GROUP BY t.object_id`)

	err := m.client.QueryRowContext(ctx, approxRowCountQuery, stream.Namespace(), stream.Name()).Scan(&approxRowCount, &avgRowSize)
	if err != nil {
		return nil, fmt.Errorf("failed to get approx row count and avg row size: %s", err)
	}

	if approxRowCount == 0 {
		var hasRows bool
		existsQuery := fmt.Sprintf(`SELECT CASE WHEN EXISTS (SELECT TOP 1 1 FROM %s.%s) THEN 1 ELSE 0 END`,
			jdbc.QuoteIdentifier(stream.Namespace(), constants.MSSQL), jdbc.QuoteIdentifier(stream.Name(), constants.MSSQL))
		err := m.client.QueryRowContext(ctx, existsQuery).Scan(&hasRows)

		if err != nil {
			return nil, fmt.Errorf("failed to check if table has rows: %s", err)
		}

		if hasRows {
			return nil, fmt.Errorf("stats not populated for table[%s]. Please run UPDATE STATISTICS to update table statistics", stream.ID())
		}

		logger.Warnf("Table %s is empty, skipping chunking", stream.ID())
		return types.NewSet[types.Chunk](), nil
	}

	pool.AddRecordsToSyncStats(approxRowCount)

	// avgRowSize is returned as []uint8 which is converted to float64
	avgRowSizeFloat, err := typeutils.ReformatFloat64(avgRowSize)
	if err != nil {
		return nil, fmt.Errorf("failed to get avg row size: %s", err)
	}

	chunkSize := int64(math.Ceil(float64(constants.EffectiveParquetSize) / avgRowSizeFloat))
	chunks := types.NewSet[types.Chunk]()
	chunkColumn := stream.Self().StreamMetadata.ChunkColumn

	// Split via primary key
	splitViaPrimaryKey := func(stream types.StreamInterface, chunks *types.Set[types.Chunk]) error {
		return withIsolation(ctx, m.client, func(tx *sql.Tx) error {
			// Get primary key column using the provided function
			pkColumns := stream.GetStream().SourceDefinedPrimaryKey.Array()
			if chunkColumn != "" {
				pkColumns = []string{chunkColumn}
			}
			sort.Strings(pkColumns)

			// Get table extremes
			minVal, maxVal, err := m.getTableExtremes(ctx, stream, pkColumns, tx)
			if err != nil {
				return fmt.Errorf("failed to get table extremes: %s", err)
			}
			if minVal == nil {
				return nil
			}

			chunks.Insert(types.Chunk{
				Min: nil,
				Max: utils.ConvertToString(minVal),
			})

			logger.Infof("Stream %s extremes - min: %v, max: %v", stream.ID(), utils.ConvertToString(minVal), utils.ConvertToString(maxVal))

			// Generate chunks based on range
			query := nextChunkEndQueryMSSQL(stream, pkColumns, chunkSize)
			currentVal := minVal
			for {
				// Split the current value into parts
				columns := strings.Split(utils.ConvertToString(currentVal), ",")

				// Create args array with the correct number of arguments for the query
				args := make([]interface{}, 0)
				for columnIndex := 0; columnIndex < len(pkColumns); columnIndex++ {
					// For each column combination in the WHERE clause, we need to add the necessary parts
					for partIndex := 0; partIndex <= columnIndex && partIndex < len(columns); partIndex++ {
						args = append(args, columns[partIndex])
					}
				}

				var nextValRaw interface{}
				err := tx.QueryRowContext(ctx, query, args...).Scan(&nextValRaw)
				if err == sql.ErrNoRows || nextValRaw == nil {
					break
				} else if err != nil {
					return fmt.Errorf("failed to get next chunk end: %s", err)
				}

				if currentVal != nil && nextValRaw != nil {
					chunks.Insert(types.Chunk{
						Min: utils.ConvertToString(currentVal),
						Max: utils.ConvertToString(nextValRaw),
					})
				}
				currentVal = nextValRaw
			}

			if currentVal != nil {
				chunks.Insert(types.Chunk{
					Min: utils.ConvertToString(currentVal),
					Max: nil,
				})
			}

			return nil
		})
	}

	limitOffsetChunking := func(chunks *types.Set[types.Chunk]) error {
		return withIsolation(ctx, m.client, func(tx *sql.Tx) error {
			chunks.Insert(types.Chunk{
				Min: nil,
				Max: utils.ConvertToString(chunkSize),
			})
			lastChunk := chunkSize
			for lastChunk < approxRowCount {
				chunks.Insert(types.Chunk{
					Min: utils.ConvertToString(lastChunk),
					Max: utils.ConvertToString(lastChunk + chunkSize),
				})
				lastChunk += chunkSize
			}
			chunks.Insert(types.Chunk{
				Min: utils.ConvertToString(lastChunk),
				Max: nil,
			})
			return nil
		})
	}

	if stream.GetStream().SourceDefinedPrimaryKey.Len() > 0 || chunkColumn != "" {
		err = splitViaPrimaryKey(stream, chunks)
	} else {
		err = limitOffsetChunking(chunks)
	}

	return chunks, err
}

func (m *MSSQL) getTableExtremes(ctx context.Context, stream types.StreamInterface, pkColumns []string, tx *sql.Tx) (min, max any, err error) {
	query := minMaxQueryMSSQL(stream, pkColumns)
	err = tx.QueryRowContext(ctx, query).Scan(&min, &max)
	return min, max, err
}

// SQL Server specific query builders
func mssqlChunkScanQuery(stream types.StreamInterface, pkColumns []string, chunk types.Chunk, filter string) string {
	quotedTable := jdbc.QuoteTable(stream.Namespace(), stream.Name(), constants.MSSQL)

	whereClause := ""
	if chunk.Min != nil && chunk.Max != nil {
		whereClause = fmt.Sprintf(" WHERE %s > '%s' AND %s <= '%s'",
			pkColumns[0], chunk.Min, pkColumns[0], chunk.Max)
	} else if chunk.Min != nil {
		whereClause = fmt.Sprintf(" WHERE %s > '%s'", pkColumns[0], chunk.Min)
	} else if chunk.Max != nil {
		whereClause = fmt.Sprintf(" WHERE %s <= '%s'", pkColumns[0], chunk.Max)
	}

	if filter != "" {
		if whereClause != "" {
			whereClause += " AND " + filter
		} else {
			whereClause = " WHERE " + filter
		}
	}

	return fmt.Sprintf("SELECT * FROM %s%s ORDER BY %s",
		quotedTable, whereClause, strings.Join(pkColumns, ", "))
}

func mssqlLimitOffsetScanQuery(stream types.StreamInterface, chunk types.Chunk, filter string) string {
	quotedTable := jdbc.QuoteTable(stream.Namespace(), stream.Name(), constants.MSSQL)

	query := fmt.Sprintf("SELECT * FROM %s", quotedTable)
	query = utils.Ternary(filter == "", query, fmt.Sprintf("%s WHERE %s", query, filter)).(string)

	offset := int64(0)
	if chunk.Min != nil {
		offset, _ = strconv.ParseInt(chunk.Min.(string), 10, 64)
	}

	limit := int64(1000) // Default limit
	if chunk.Max != nil {
		maxVal, _ := strconv.ParseInt(chunk.Max.(string), 10, 64)
		limit = maxVal - offset
	}

	return fmt.Sprintf("%s ORDER BY (SELECT NULL) OFFSET %d ROWS FETCH NEXT %d ROWS ONLY",
		query, offset, limit)
}

func minMaxQueryMSSQL(stream types.StreamInterface, pkColumns []string) string {
	tableName := jdbc.QuoteTable(stream.Namespace(), stream.Name(), constants.MSSQL)

	if len(pkColumns) == 1 {
		return fmt.Sprintf("SELECT MIN(%s), MAX(%s) FROM %s",
			jdbc.QuoteIdentifier(pkColumns[0], constants.MSSQL), jdbc.QuoteIdentifier(pkColumns[0], constants.MSSQL), tableName)
	}

	// For composite keys, concatenate them
	concatFields := make([]string, len(pkColumns))
	for i, col := range pkColumns {
		concatFields[i] = fmt.Sprintf("CAST(%s AS VARCHAR(MAX))", jdbc.QuoteIdentifier(col, constants.MSSQL))
	}
	concatExpr := strings.Join(concatFields, " + ',' + ")

	return fmt.Sprintf("SELECT MIN(%s), MAX(%s) FROM %s", concatExpr, concatExpr, tableName)
}

func nextChunkEndQueryMSSQL(stream types.StreamInterface, pkColumns []string, chunkSize int64) string {
	tableName := jdbc.QuoteTable(stream.Namespace(), stream.Name(), constants.MSSQL)

	if len(pkColumns) == 1 {
		col := jdbc.QuoteIdentifier(pkColumns[0], constants.MSSQL)
		return fmt.Sprintf(`
			SELECT %s
			FROM %s
			WHERE %s > ?
			ORDER BY %s
			OFFSET %d ROWS FETCH NEXT 1 ROW ONLY`,
			col, tableName, col, col, chunkSize)
	}

	// For composite keys
	concatFields := make([]string, len(pkColumns))
	quotedColumns := make([]string, len(pkColumns))
	for i, col := range pkColumns {
		quotedCol := jdbc.QuoteIdentifier(col, constants.MSSQL)
		concatFields[i] = fmt.Sprintf("CAST(%s AS VARCHAR(MAX))", quotedCol)
		quotedColumns[i] = quotedCol
	}
	concatExpr := strings.Join(concatFields, " + ',' + ")

	whereConditions := make([]string, len(pkColumns))
	for i := range pkColumns {
		conditions := make([]string, i+1)
		for j := 0; j <= i; j++ {
			if j < i {
				conditions[j] = fmt.Sprintf("%s = ?", quotedColumns[j])
			} else {
				conditions[j] = fmt.Sprintf("%s > ?", quotedColumns[j])
			}
		}
		whereConditions[i] = "(" + strings.Join(conditions, " AND ") + ")"
	}

	return fmt.Sprintf(`
		SELECT %s
		FROM %s
		WHERE %s
		ORDER BY %s
		OFFSET %d ROWS FETCH NEXT 1 ROW ONLY`,
		concatExpr, tableName,
		strings.Join(whereConditions, " OR "),
		strings.Join(quotedColumns, ", "),
		chunkSize)
}

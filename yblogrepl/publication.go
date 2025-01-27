package yblogrepl

import (
	"context"
	"fmt"
	"strings"

	"github.com/lib/pq"
	"github.com/yugabyte/pgx/v5/pgconn"
)

type PublicationParams struct {
	Name            string
	Tables          []string
	AllTables       bool
	PublishInsert   *bool
	PublishUpdate   *bool
	PublishDelete   *bool
	PublishTruncate *bool
}

func CreatePublication(ctx context.Context, conn *pgconn.PgConn, params PublicationParams) error {
	if params.Name == "" {
		return fmt.Errorf("publication name cannot be empty")
	}

	query := fmt.Sprintf("CREATE PUBLICATION %s ",
		pq.QuoteIdentifier(params.Name))

	if params.AllTables {
		query += "FOR ALL TABLES "
	} else if len(params.Tables) > 0 {
		tableNames := make([]string, len(params.Tables))
		for i, table := range params.Tables {
			if strings.Contains(table, ".") {
				parts := strings.SplitN(table, ".", 2)
				schema := pq.QuoteIdentifier(parts[0])
				tableName := pq.QuoteIdentifier(parts[1])
				tableNames[i] = schema + "." + tableName
			} else {
				tableNames[i] = pq.QuoteIdentifier(table)
			}
		}
		query += "FOR TABLE " + strings.Join(tableNames, ", ") + " "
	}

	var options []string

	var publishOperations []string
	if params.PublishInsert != nil && *params.PublishInsert {
		publishOperations = append(publishOperations, "insert")
	}
	if params.PublishUpdate != nil && *params.PublishUpdate {
		publishOperations = append(publishOperations, "update")
	}
	if params.PublishDelete != nil && *params.PublishDelete {
		publishOperations = append(publishOperations, "delete")
	}
	if params.PublishTruncate != nil && *params.PublishTruncate {
		publishOperations = append(publishOperations, "truncate")
	}
	if len(publishOperations) > 0 {
		options = append(options, fmt.Sprintf("publish = '%s'", strings.Join(publishOperations, ", ")))
	}

	if len(options) > 0 {
		query += " WITH (" + strings.Join(options, ", ") + ")"
	}

	result := conn.Exec(ctx, query)
	_, err := result.ReadAll()
	if err != nil {
		return fmt.Errorf("failed to create publication: %w", err)
	}
	return nil
}

func DropPublication(ctx context.Context, conn *pgconn.PgConn, name string) error {
	query := fmt.Sprintf("DROP PUBLICATION %s", pq.QuoteIdentifier(name))
	result := conn.Exec(ctx, query)
	_, err := result.ReadAll()
	if err != nil {
		return fmt.Errorf("failed to drop publication: %w", err)
	}
	return nil
}

func CheckPublicationExists(ctx context.Context, conn *pgconn.PgConn, name string) (bool, error) {

	sql := "SELECT 1 FROM pg_publication WHERE pubname = $1"
	result := conn.ExecParams(ctx, sql, [][]byte{[]byte(name)}, nil, nil, nil)

	cmdTag, err := result.Close()
	if err != nil {
		return false, err
	}

	return cmdTag.RowsAffected() > 0, nil
}

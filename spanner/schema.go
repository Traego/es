package spannerstore

import (
	"context"
	_ "embed"
	"fmt"
	"strings"

	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
)

//go:embed schema.sql
var schema string

// SchemaDDL returns the statements for a fresh database. It is not an upgrade
// migration for the original traego-svcs schema.
func SchemaDDL() []string {
	return strings.Split(strings.TrimSuffix(strings.TrimSpace(schema), ";"), ";\n")
}

// CreateSchema creates the adapter's fixed Events, Topics, and Cursors tables.
// The caller owns the admin client and must supply an existing empty database.
// Calling this on an existing schema returns an error rather than modifying it.
func CreateSchema(ctx context.Context, admin *database.DatabaseAdminClient, databaseName string) error {
	if admin == nil || strings.TrimSpace(databaseName) == "" {
		return fmt.Errorf("admin client and database name are required")
	}
	op, err := admin.UpdateDatabaseDdl(ctx, &databasepb.UpdateDatabaseDdlRequest{Database: databaseName, Statements: SchemaDDL()})
	if err != nil {
		return fmt.Errorf("create schema: %w", err)
	}
	return op.Wait(ctx)
}

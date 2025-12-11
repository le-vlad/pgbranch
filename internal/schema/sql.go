package schema

import (
	"fmt"
	"strings"
	"time"
)

// SQLGenerator generates SQL statements from a ChangeSet.
type SQLGenerator struct {
	IncludeComments bool
}

func NewSQLGenerator() *SQLGenerator {
	return &SQLGenerator{
		IncludeComments: true,
	}
}

func (g *SQLGenerator) Generate(cs *ChangeSet) []string {
	var statements []string

	for _, change := range cs.Changes {
		sql := g.GenerateChange(change)
		if sql != "" {
			if g.IncludeComments {
				comment := g.generateComment(change)
				if comment != "" {
					statements = append(statements, comment)
				}
			}
			statements = append(statements, sql)
		}
	}

	return statements
}

func (g *SQLGenerator) GenerateChange(c Change) string {
	switch change := c.(type) {
	case *CreateTableChange:
		return g.generateCreateTable(change)
	case *DropTableChange:
		return g.generateDropTable(change)
	case *AddColumnChange:
		return g.generateAddColumn(change)
	case *DropColumnChange:
		return g.generateDropColumn(change)
	case *AlterColumnChange:
		return g.generateAlterColumn(change)
	case *CreateIndexChange:
		return g.generateCreateIndex(change)
	case *DropIndexChange:
		return g.generateDropIndex(change)
	case *AddConstraintChange:
		return g.generateAddConstraint(change)
	case *DropConstraintChange:
		return g.generateDropConstraint(change)
	case *CreateEnumChange:
		return g.generateCreateEnum(change)
	case *DropEnumChange:
		return g.generateDropEnum(change)
	case *AddEnumValueChange:
		return g.generateAddEnumValue(change)
	case *CreateFunctionChange:
		return g.generateCreateFunction(change)
	case *DropFunctionChange:
		return g.generateDropFunction(change)
	case *ReplaceFunctionChange:
		return g.generateReplaceFunction(change)
	default:
		return ""
	}
}

func (g *SQLGenerator) generateComment(c Change) string {
	destructive := ""
	if c.IsDestructive() {
		destructive = " (DESTRUCTIVE)"
	}
	return fmt.Sprintf("-- %s%s", c.Description(), destructive)
}

func (g *SQLGenerator) generateCreateTable(c *CreateTableChange) string {
	table := c.Table
	var sb strings.Builder

	sb.WriteString(fmt.Sprintf("CREATE TABLE %s (\n", quoteIdent(table.FullName())))

	columns := table.SortedColumns()
	for i, col := range columns {
		sb.WriteString(fmt.Sprintf("    %s", g.columnDefinition(col)))
		if i < len(columns)-1 {
			sb.WriteString(",")
		}
		sb.WriteString("\n")
	}

	sb.WriteString(");")

	var extra []string
	for _, con := range table.SortedConstraints() {
		if con.Type != ConstraintPrimaryKey {
			extra = append(extra, g.generateAddConstraint(&AddConstraintChange{
				TableName:  table.FullName(),
				Constraint: con,
			}))
		}
	}

	result := sb.String()
	if len(extra) > 0 {
		result += "\n" + strings.Join(extra, "\n")
	}

	return result
}

func (g *SQLGenerator) columnDefinition(col *Column) string {
	var sb strings.Builder

	sb.WriteString(quoteIdent(col.Name))
	sb.WriteString(" ")
	sb.WriteString(col.FullType())

	if !col.IsNullable {
		sb.WriteString(" NOT NULL")
	}

	if col.DefaultValue != nil {
		sb.WriteString(" DEFAULT ")
		sb.WriteString(*col.DefaultValue)
	}

	return sb.String()
}

func (g *SQLGenerator) generateDropTable(c *DropTableChange) string {
	return fmt.Sprintf("DROP TABLE %s;", quoteIdent(c.Table.FullName()))
}

func (g *SQLGenerator) generateAddColumn(c *AddColumnChange) string {
	return fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s;",
		quoteIdent(c.TableName),
		g.columnDefinition(c.Column),
	)
}

func (g *SQLGenerator) generateDropColumn(c *DropColumnChange) string {
	return fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s;",
		quoteIdent(c.TableName),
		quoteIdent(c.Column.Name),
	)
}

func (g *SQLGenerator) generateAlterColumn(c *AlterColumnChange) string {
	var statements []string
	tableName := quoteIdent(c.TableName)
	colName := quoteIdent(c.ColumnName)

	if c.Alteration.TypeChanged {
		statements = append(statements,
			fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s TYPE %s;",
				tableName, colName, c.Alteration.NewType),
		)
	}

	if c.Alteration.NullableChanged {
		if c.Alteration.NewNullable {
			statements = append(statements,
				fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s DROP NOT NULL;",
					tableName, colName),
			)
		} else {
			statements = append(statements,
				fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s SET NOT NULL;",
					tableName, colName),
			)
		}
	}

	if c.Alteration.DefaultChanged {
		if c.Alteration.NewDefault == nil {
			statements = append(statements,
				fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s DROP DEFAULT;",
					tableName, colName),
			)
		} else {
			statements = append(statements,
				fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s SET DEFAULT %s;",
					tableName, colName, *c.Alteration.NewDefault),
			)
		}
	}

	return strings.Join(statements, "\n")
}

func (g *SQLGenerator) generateCreateIndex(c *CreateIndexChange) string {
	if c.Index.Definition != "" {
		return c.Index.Definition + ";"
	}

	unique := ""
	if c.Index.IsUnique {
		unique = "UNIQUE "
	}

	return fmt.Sprintf("CREATE %sINDEX %s ON %s (%s);",
		unique,
		quoteIdent(c.Index.Name),
		quoteIdent(c.Index.TableName),
		strings.Join(quoteIdents(c.Index.Columns), ", "),
	)
}

func (g *SQLGenerator) generateDropIndex(c *DropIndexChange) string {
	return fmt.Sprintf("DROP INDEX %s;", quoteIdent(c.Index.Name))
}

func (g *SQLGenerator) generateAddConstraint(c *AddConstraintChange) string {
	return fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s %s;",
		quoteIdent(c.TableName),
		quoteIdent(c.Constraint.Name),
		c.Constraint.Definition,
	)
}

func (g *SQLGenerator) generateDropConstraint(c *DropConstraintChange) string {
	return fmt.Sprintf("ALTER TABLE %s DROP CONSTRAINT %s;",
		quoteIdent(c.TableName),
		quoteIdent(c.Constraint.Name),
	)
}

func (g *SQLGenerator) generateCreateEnum(c *CreateEnumChange) string {
	values := make([]string, len(c.Enum.Values))
	for i, v := range c.Enum.Values {
		values[i] = quoteLiteral(v)
	}
	return fmt.Sprintf("CREATE TYPE %s AS ENUM (%s);",
		quoteIdent(c.Enum.FullName()),
		strings.Join(values, ", "),
	)
}

func (g *SQLGenerator) generateDropEnum(c *DropEnumChange) string {
	return fmt.Sprintf("DROP TYPE %s;", quoteIdent(c.Enum.FullName()))
}

func (g *SQLGenerator) generateAddEnumValue(c *AddEnumValueChange) string {
	if c.After != "" {
		return fmt.Sprintf("ALTER TYPE %s ADD VALUE %s AFTER %s;",
			quoteIdent(c.EnumName),
			quoteLiteral(c.Value),
			quoteLiteral(c.After),
		)
	}
	return fmt.Sprintf("ALTER TYPE %s ADD VALUE %s;",
		quoteIdent(c.EnumName),
		quoteLiteral(c.Value),
	)
}

func (g *SQLGenerator) generateCreateFunction(c *CreateFunctionChange) string {
	return c.Function.Definition + ";"
}

func (g *SQLGenerator) generateDropFunction(c *DropFunctionChange) string {
	return fmt.Sprintf("DROP FUNCTION %s;", c.Function.FullName())
}

func (g *SQLGenerator) generateReplaceFunction(c *ReplaceFunctionChange) string {
	def := c.NewFunction.Definition
	if strings.HasPrefix(def, "CREATE FUNCTION") {
		def = "CREATE OR REPLACE" + def[6:]
	}
	return def + ";"
}

func (g *SQLGenerator) GenerateMigrationFile(cs *ChangeSet, description string) string {
	var sb strings.Builder

	sb.WriteString(fmt.Sprintf("-- Migration generated by pgbranch\n"))
	sb.WriteString(fmt.Sprintf("-- Generated at: %s\n", time.Now().Format(time.RFC3339)))
	if description != "" {
		sb.WriteString(fmt.Sprintf("-- Description: %s\n", description))
	}
	sb.WriteString("\n")

	summary := cs.Summary()
	if len(summary) > 0 {
		sb.WriteString("-- Changes:\n")
		for changeType, count := range summary {
			sb.WriteString(fmt.Sprintf("--   %s: %d\n", changeType, count))
		}
		sb.WriteString("\n")
	}

	if cs.HasDestructive() {
		sb.WriteString(fmt.Sprintf("-- WARNING: This migration contains %d destructive change(s)\n\n",
			cs.DestructiveCount()))
	}

	sb.WriteString("BEGIN;\n\n")

	statements := g.Generate(cs)
	for _, stmt := range statements {
		sb.WriteString(stmt)
		sb.WriteString("\n")
		if !strings.HasPrefix(stmt, "--") {
			sb.WriteString("\n")
		}
	}

	sb.WriteString("COMMIT;\n")

	return sb.String()
}

func quoteIdent(name string) string {
	if isSimpleIdent(name) {
		return name
	}
	escaped := strings.ReplaceAll(name, `"`, `""`)
	return `"` + escaped + `"`
}

func quoteIdents(names []string) []string {
	result := make([]string, len(names))
	for i, name := range names {
		result[i] = quoteIdent(name)
	}
	return result
}

func quoteLiteral(s string) string {
	escaped := strings.ReplaceAll(s, "'", "''")
	return "'" + escaped + "'"
}

func isSimpleIdent(name string) bool {
	if len(name) == 0 {
		return false
	}

	first := name[0]
	if !((first >= 'a' && first <= 'z') || first == '_') {
		return false
	}

	for i := 1; i < len(name); i++ {
		c := name[i]
		if !((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_') {
			return false
		}
	}

	reserved := map[string]bool{
		"select": true, "from": true, "where": true, "table": true,
		"index": true, "user": true, "order": true, "group": true,
		"by": true, "as": true, "on": true, "join": true,
	}
	return !reserved[strings.ToLower(name)]
}

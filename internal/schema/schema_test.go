package schema

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestColumnFullType(t *testing.T) {
	tests := []struct {
		name     string
		column   Column
		expected string
	}{
		{
			name:     "simple integer",
			column:   Column{DataType: "integer"},
			expected: "integer",
		},
		{
			name:     "varchar with length",
			column:   Column{DataType: "character varying", CharMaxLength: intPtr(255)},
			expected: "varchar(255)",
		},
		{
			name:     "char with length",
			column:   Column{DataType: "character", CharMaxLength: intPtr(10)},
			expected: "char(10)",
		},
		{
			name:     "numeric with precision",
			column:   Column{DataType: "numeric", NumericPrecision: intPtr(10)},
			expected: "numeric(10)",
		},
		{
			name:     "numeric with precision and scale",
			column:   Column{DataType: "numeric", NumericPrecision: intPtr(10), NumericScale: intPtr(2)},
			expected: "numeric(10,2)",
		},
		{
			name:     "array type",
			column:   Column{DataType: "text", IsArray: true},
			expected: "text[]",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.column.FullType())
		})
	}
}

func TestColumnEquals(t *testing.T) {
	tests := []struct {
		name     string
		col1     Column
		col2     Column
		expected bool
	}{
		{
			name:     "identical columns",
			col1:     Column{Name: "id", DataType: "integer", IsNullable: false},
			col2:     Column{Name: "id", DataType: "integer", IsNullable: false},
			expected: true,
		},
		{
			name:     "different names",
			col1:     Column{Name: "id", DataType: "integer"},
			col2:     Column{Name: "user_id", DataType: "integer"},
			expected: false,
		},
		{
			name:     "different types",
			col1:     Column{Name: "id", DataType: "integer"},
			col2:     Column{Name: "id", DataType: "bigint"},
			expected: false,
		},
		{
			name:     "different nullable",
			col1:     Column{Name: "id", DataType: "integer", IsNullable: true},
			col2:     Column{Name: "id", DataType: "integer", IsNullable: false},
			expected: false,
		},
		{
			name:     "different defaults",
			col1:     Column{Name: "id", DataType: "integer", DefaultValue: strPtr("0")},
			col2:     Column{Name: "id", DataType: "integer", DefaultValue: strPtr("1")},
			expected: false,
		},
		{
			name:     "one has default, other doesn't",
			col1:     Column{Name: "id", DataType: "integer", DefaultValue: strPtr("0")},
			col2:     Column{Name: "id", DataType: "integer"},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.col1.Equals(&tt.col2))
		})
	}
}

func TestDiffTables(t *testing.T) {
	t.Run("detect new table", func(t *testing.T) {
		from := NewSchema("test")
		to := NewSchema("test")

		to.Tables["users"] = &Table{
			Name:   "users",
			Schema: "public",
			Columns: map[string]*Column{
				"id": {Name: "id", DataType: "integer", Position: 1},
			},
		}

		cs := Diff(from, to)

		require.Len(t, cs.Changes, 1)
		assert.Equal(t, ChangeCreateTable, cs.Changes[0].Type())
	})

	t.Run("detect dropped table", func(t *testing.T) {
		from := NewSchema("test")
		to := NewSchema("test")

		from.Tables["users"] = &Table{
			Name:   "users",
			Schema: "public",
		}

		cs := Diff(from, to)

		require.Len(t, cs.Changes, 1)
		assert.Equal(t, ChangeDropTable, cs.Changes[0].Type())
		assert.True(t, cs.Changes[0].IsDestructive())
	})
}

func TestDiffColumns(t *testing.T) {
	t.Run("detect new column", func(t *testing.T) {
		from := NewSchema("test")
		to := NewSchema("test")

		from.Tables["users"] = NewTable("users", "public")
		from.Tables["users"].Columns["id"] = &Column{Name: "id", DataType: "integer", Position: 1}

		to.Tables["users"] = NewTable("users", "public")
		to.Tables["users"].Columns["id"] = &Column{Name: "id", DataType: "integer", Position: 1}
		to.Tables["users"].Columns["email"] = &Column{Name: "email", DataType: "text", Position: 2}

		cs := Diff(from, to)

		require.Len(t, cs.Changes, 1)
		assert.Equal(t, ChangeAddColumn, cs.Changes[0].Type())

		addCol := cs.Changes[0].(*AddColumnChange)
		assert.Equal(t, "email", addCol.Column.Name)
	})

	t.Run("detect dropped column", func(t *testing.T) {
		from := NewSchema("test")
		to := NewSchema("test")

		from.Tables["users"] = NewTable("users", "public")
		from.Tables["users"].Columns["id"] = &Column{Name: "id", DataType: "integer", Position: 1}
		from.Tables["users"].Columns["email"] = &Column{Name: "email", DataType: "text", Position: 2}

		to.Tables["users"] = NewTable("users", "public")
		to.Tables["users"].Columns["id"] = &Column{Name: "id", DataType: "integer", Position: 1}

		cs := Diff(from, to)

		require.Len(t, cs.Changes, 1)
		assert.Equal(t, ChangeDropColumn, cs.Changes[0].Type())
		assert.True(t, cs.Changes[0].IsDestructive())
	})

	t.Run("detect altered column type", func(t *testing.T) {
		from := NewSchema("test")
		to := NewSchema("test")

		from.Tables["users"] = NewTable("users", "public")
		from.Tables["users"].Columns["count"] = &Column{Name: "count", DataType: "integer", Position: 1}

		to.Tables["users"] = NewTable("users", "public")
		to.Tables["users"].Columns["count"] = &Column{Name: "count", DataType: "bigint", Position: 1}

		cs := Diff(from, to)

		require.Len(t, cs.Changes, 1)
		assert.Equal(t, ChangeAlterColumn, cs.Changes[0].Type())

		alterCol := cs.Changes[0].(*AlterColumnChange)
		assert.True(t, alterCol.Alteration.TypeChanged)
		assert.Equal(t, "integer", alterCol.Alteration.OldType)
		assert.Equal(t, "bigint", alterCol.Alteration.NewType)
	})

	t.Run("detect nullable change", func(t *testing.T) {
		from := NewSchema("test")
		to := NewSchema("test")

		from.Tables["users"] = NewTable("users", "public")
		from.Tables["users"].Columns["email"] = &Column{Name: "email", DataType: "text", IsNullable: true, Position: 1}

		to.Tables["users"] = NewTable("users", "public")
		to.Tables["users"].Columns["email"] = &Column{Name: "email", DataType: "text", IsNullable: false, Position: 1}

		cs := Diff(from, to)

		require.Len(t, cs.Changes, 1)
		alterCol := cs.Changes[0].(*AlterColumnChange)
		assert.True(t, alterCol.Alteration.NullableChanged)
		assert.False(t, alterCol.Alteration.NewNullable)
	})
}

func TestDiffEnums(t *testing.T) {
	t.Run("detect new enum", func(t *testing.T) {
		from := NewSchema("test")
		to := NewSchema("test")

		to.Enums["status"] = &Enum{
			Name:   "status",
			Schema: "public",
			Values: []string{"pending", "active", "deleted"},
		}

		cs := Diff(from, to)

		require.Len(t, cs.Changes, 1)
		assert.Equal(t, ChangeCreateEnum, cs.Changes[0].Type())
	})

	t.Run("detect added enum value", func(t *testing.T) {
		from := NewSchema("test")
		to := NewSchema("test")

		from.Enums["status"] = &Enum{
			Name:   "status",
			Values: []string{"pending", "active"},
		}

		to.Enums["status"] = &Enum{
			Name:   "status",
			Values: []string{"pending", "active", "deleted"},
		}

		cs := Diff(from, to)

		require.Len(t, cs.Changes, 1)
		assert.Equal(t, ChangeAddEnumValue, cs.Changes[0].Type())

		addVal := cs.Changes[0].(*AddEnumValueChange)
		assert.Equal(t, "deleted", addVal.Value)
		assert.Equal(t, "active", addVal.After)
	})
}

func TestDiffFunctions(t *testing.T) {
	t.Run("detect new function", func(t *testing.T) {
		from := NewSchema("test")
		to := NewSchema("test")

		to.Functions["greet(text)"] = &Function{
			Name:       "greet",
			Arguments:  "text",
			ReturnType: "text",
			Definition: "CREATE FUNCTION greet(name text) RETURNS text AS $$ SELECT 'Hello, ' || name $$ LANGUAGE sql",
			BodyHash:   "abc123",
		}

		cs := Diff(from, to)

		require.Len(t, cs.Changes, 1)
		assert.Equal(t, ChangeCreateFunction, cs.Changes[0].Type())
	})

	t.Run("detect modified function", func(t *testing.T) {
		from := NewSchema("test")
		to := NewSchema("test")

		from.Functions["greet(text)"] = &Function{
			Name:       "greet",
			Arguments:  "text",
			ReturnType: "text",
			BodyHash:   "old_hash",
		}

		to.Functions["greet(text)"] = &Function{
			Name:       "greet",
			Arguments:  "text",
			ReturnType: "text",
			BodyHash:   "new_hash",
		}

		cs := Diff(from, to)

		require.Len(t, cs.Changes, 1)
		assert.Equal(t, ChangeReplaceFunction, cs.Changes[0].Type())
	})
}

func TestChangeSetDestructive(t *testing.T) {
	cs := NewChangeSet()

	// Add non-destructive change
	cs.Add(&AddColumnChange{
		TableName: "users",
		Column:    &Column{Name: "email", DataType: "text"},
	})

	assert.False(t, cs.HasDestructive())
	assert.Equal(t, 0, cs.DestructiveCount())

	// Add destructive change
	cs.Add(&DropColumnChange{
		TableName: "users",
		Column:    &Column{Name: "old_field"},
	})

	assert.True(t, cs.HasDestructive())
	assert.Equal(t, 1, cs.DestructiveCount())
}

func TestSQLGeneration(t *testing.T) {
	gen := NewSQLGenerator()
	gen.IncludeComments = false

	t.Run("add column", func(t *testing.T) {
		change := &AddColumnChange{
			TableName: "users",
			Column: &Column{
				Name:       "email",
				DataType:   "text",
				IsNullable: false,
			},
		}

		sql := gen.GenerateChange(change)
		assert.Equal(t, "ALTER TABLE users ADD COLUMN email text NOT NULL;", sql)
	})

	t.Run("drop column", func(t *testing.T) {
		change := &DropColumnChange{
			TableName: "users",
			Column:    &Column{Name: "old_field"},
		}

		sql := gen.GenerateChange(change)
		assert.Equal(t, "ALTER TABLE users DROP COLUMN old_field;", sql)
	})

	t.Run("create enum", func(t *testing.T) {
		change := &CreateEnumChange{
			Enum: &Enum{
				Name:   "status",
				Values: []string{"pending", "active"},
			},
		}

		sql := gen.GenerateChange(change)
		assert.Equal(t, "CREATE TYPE status AS ENUM ('pending', 'active');", sql)
	})

	t.Run("add enum value", func(t *testing.T) {
		change := &AddEnumValueChange{
			EnumName: "status",
			Value:    "deleted",
			After:    "active",
		}

		sql := gen.GenerateChange(change)
		assert.Equal(t, "ALTER TYPE status ADD VALUE 'deleted' AFTER 'active';", sql)
	})
}

func TestOrderChanges(t *testing.T) {
	cs := NewChangeSet()

	// Add changes in wrong order
	cs.Add(&DropColumnChange{TableName: "users", Column: &Column{Name: "old"}})
	cs.Add(&CreateEnumChange{Enum: &Enum{Name: "status", Values: []string{"a"}}})
	cs.Add(&AddColumnChange{TableName: "users", Column: &Column{Name: "new"}})
	cs.Add(&CreateTableChange{Table: &Table{Name: "logs"}})

	ordered := OrderChanges(cs)

	// Verify order: enums first, then tables, then add columns, then drop columns
	require.Len(t, ordered.Changes, 4)
	assert.Equal(t, ChangeCreateEnum, ordered.Changes[0].Type())
	assert.Equal(t, ChangeCreateTable, ordered.Changes[1].Type())
	assert.Equal(t, ChangeAddColumn, ordered.Changes[2].Type())
	assert.Equal(t, ChangeDropColumn, ordered.Changes[3].Type())
}

func intPtr(i int) *int {
	return &i
}

func strPtr(s string) *string {
	return &s
}

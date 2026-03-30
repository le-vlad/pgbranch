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

func TestTableFullName(t *testing.T) {
	tests := []struct {
		name     string
		table    Table
		expected string
	}{
		{
			name:     "public schema",
			table:    Table{Name: "users", Schema: "public"},
			expected: "users",
		},
		{
			name:     "empty schema",
			table:    Table{Name: "users", Schema: ""},
			expected: "users",
		},
		{
			name:     "custom schema",
			table:    Table{Name: "users", Schema: "billing"},
			expected: "billing.users",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.table.FullName())
		})
	}
}

func TestTableSortedColumns(t *testing.T) {
	tbl := NewTable("users", "public")
	tbl.Columns["email"] = &Column{Name: "email", DataType: "text", Position: 3}
	tbl.Columns["id"] = &Column{Name: "id", DataType: "integer", Position: 1}
	tbl.Columns["name"] = &Column{Name: "name", DataType: "text", Position: 2}

	cols := tbl.SortedColumns()
	require.Len(t, cols, 3)
	assert.Equal(t, "id", cols[0].Name)
	assert.Equal(t, "name", cols[1].Name)
	assert.Equal(t, "email", cols[2].Name)
}

func TestTableSortedIndexes(t *testing.T) {
	tbl := NewTable("users", "public")
	tbl.Indexes["idx_email"] = &Index{Name: "idx_email"}
	tbl.Indexes["idx_active"] = &Index{Name: "idx_active"}
	tbl.Indexes["idx_name"] = &Index{Name: "idx_name"}

	idxs := tbl.SortedIndexes()
	require.Len(t, idxs, 3)
	assert.Equal(t, "idx_active", idxs[0].Name)
	assert.Equal(t, "idx_email", idxs[1].Name)
	assert.Equal(t, "idx_name", idxs[2].Name)
}

func TestTableSortedConstraints(t *testing.T) {
	tbl := NewTable("users", "public")
	tbl.Constraints["users_pkey"] = &Constraint{Name: "users_pkey"}
	tbl.Constraints["fk_org"] = &Constraint{Name: "fk_org"}
	tbl.Constraints["chk_email"] = &Constraint{Name: "chk_email"}

	cons := tbl.SortedConstraints()
	require.Len(t, cons, 3)
	assert.Equal(t, "chk_email", cons[0].Name)
	assert.Equal(t, "fk_org", cons[1].Name)
	assert.Equal(t, "users_pkey", cons[2].Name)
}

func TestIndexEquals(t *testing.T) {
	base := Index{Name: "idx_users_email", IsUnique: true, Type: "btree", Columns: []string{"email"}}

	tests := []struct {
		name     string
		other    Index
		expected bool
	}{
		{
			name:     "identical",
			other:    Index{Name: "idx_users_email", IsUnique: true, Type: "btree", Columns: []string{"email"}},
			expected: true,
		},
		{
			name:     "different uniqueness",
			other:    Index{Name: "idx_users_email", IsUnique: false, Type: "btree", Columns: []string{"email"}},
			expected: false,
		},
		{
			name:     "different type",
			other:    Index{Name: "idx_users_email", IsUnique: true, Type: "hash", Columns: []string{"email"}},
			expected: false,
		},
		{
			name:     "different columns",
			other:    Index{Name: "idx_users_email", IsUnique: true, Type: "btree", Columns: []string{"email", "name"}},
			expected: false,
		},
		{
			name:     "different name",
			other:    Index{Name: "idx_other", IsUnique: true, Type: "btree", Columns: []string{"email"}},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, base.Equals(&tt.other))
		})
	}
}

func TestConstraintEquals(t *testing.T) {
	base := Constraint{Name: "users_pkey", Type: ConstraintPrimaryKey, Definition: "PRIMARY KEY (id)"}

	tests := []struct {
		name     string
		other    Constraint
		expected bool
	}{
		{
			name:     "identical",
			other:    Constraint{Name: "users_pkey", Type: ConstraintPrimaryKey, Definition: "PRIMARY KEY (id)"},
			expected: true,
		},
		{
			name:     "different type",
			other:    Constraint{Name: "users_pkey", Type: ConstraintUnique, Definition: "PRIMARY KEY (id)"},
			expected: false,
		},
		{
			name:     "different definition",
			other:    Constraint{Name: "users_pkey", Type: ConstraintPrimaryKey, Definition: "PRIMARY KEY (id, name)"},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, base.Equals(&tt.other))
		})
	}
}

func TestEnumFullName(t *testing.T) {
	tests := []struct {
		name     string
		enum     Enum
		expected string
	}{
		{
			name:     "public schema",
			enum:     Enum{Name: "status", Schema: "public"},
			expected: "status",
		},
		{
			name:     "empty schema",
			enum:     Enum{Name: "status", Schema: ""},
			expected: "status",
		},
		{
			name:     "custom schema",
			enum:     Enum{Name: "status", Schema: "billing"},
			expected: "billing.status",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.enum.FullName())
		})
	}
}

func TestEnumEquals(t *testing.T) {
	base := Enum{Name: "status", Values: []string{"pending", "active"}}

	tests := []struct {
		name     string
		other    Enum
		expected bool
	}{
		{
			name:     "identical",
			other:    Enum{Name: "status", Values: []string{"pending", "active"}},
			expected: true,
		},
		{
			name:     "different values",
			other:    Enum{Name: "status", Values: []string{"pending", "deleted"}},
			expected: false,
		},
		{
			name:     "different length",
			other:    Enum{Name: "status", Values: []string{"pending"}},
			expected: false,
		},
		{
			name:     "different name",
			other:    Enum{Name: "role", Values: []string{"pending", "active"}},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, base.Equals(&tt.other))
		})
	}
}

func TestFunctionSignature(t *testing.T) {
	f := Function{Name: "greet", Arguments: "text, integer"}
	assert.Equal(t, "greet(text, integer)", f.Signature())
}

func TestFunctionFullName(t *testing.T) {
	tests := []struct {
		name     string
		fn       Function
		expected string
	}{
		{
			name:     "public schema",
			fn:       Function{Name: "greet", Schema: "public", Arguments: "text"},
			expected: "greet(text)",
		},
		{
			name:     "empty schema",
			fn:       Function{Name: "greet", Schema: "", Arguments: "text"},
			expected: "greet(text)",
		},
		{
			name:     "custom schema",
			fn:       Function{Name: "greet", Schema: "billing", Arguments: "text"},
			expected: "billing.greet(text)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.fn.FullName())
		})
	}
}

func TestFunctionEquals(t *testing.T) {
	base := Function{Name: "greet", Arguments: "text", ReturnType: "text", BodyHash: "abc123"}

	tests := []struct {
		name     string
		other    Function
		expected bool
	}{
		{
			name:     "identical",
			other:    Function{Name: "greet", Arguments: "text", ReturnType: "text", BodyHash: "abc123"},
			expected: true,
		},
		{
			name:     "different return type",
			other:    Function{Name: "greet", Arguments: "text", ReturnType: "integer", BodyHash: "abc123"},
			expected: false,
		},
		{
			name:     "different body hash",
			other:    Function{Name: "greet", Arguments: "text", ReturnType: "text", BodyHash: "xyz789"},
			expected: false,
		},
		{
			name:     "different signature",
			other:    Function{Name: "greet", Arguments: "integer", ReturnType: "text", BodyHash: "abc123"},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, base.Equals(&tt.other))
		})
	}
}

func TestSchemaSortedTables(t *testing.T) {
	s := NewSchema("test")
	s.Tables["users"] = NewTable("users", "public")
	s.Tables["accounts"] = NewTable("accounts", "public")
	s.Tables["logs"] = NewTable("logs", "public")

	tables := s.SortedTables()
	require.Len(t, tables, 3)
	assert.Equal(t, "accounts", tables[0].Name)
	assert.Equal(t, "logs", tables[1].Name)
	assert.Equal(t, "users", tables[2].Name)
}

func TestSchemaSortedEnums(t *testing.T) {
	s := NewSchema("test")
	s.Enums["status"] = &Enum{Name: "status"}
	s.Enums["color"] = &Enum{Name: "color"}
	s.Enums["role"] = &Enum{Name: "role"}

	enums := s.SortedEnums()
	require.Len(t, enums, 3)
	assert.Equal(t, "color", enums[0].Name)
	assert.Equal(t, "role", enums[1].Name)
	assert.Equal(t, "status", enums[2].Name)
}

func TestSchemaSortedFunctions(t *testing.T) {
	s := NewSchema("test")
	s.Functions["greet(text)"] = &Function{Name: "greet", Arguments: "text"}
	s.Functions["add(int, int)"] = &Function{Name: "add", Arguments: "int, int"}
	s.Functions["process()"] = &Function{Name: "process", Arguments: ""}

	fns := s.SortedFunctions()
	require.Len(t, fns, 3)
	assert.Equal(t, "add(int, int)", fns[0].Signature())
	assert.Equal(t, "greet(text)", fns[1].Signature())
	assert.Equal(t, "process()", fns[2].Signature())
}

func TestChangeSetIsEmpty(t *testing.T) {
	cs := NewChangeSet()
	assert.True(t, cs.IsEmpty())

	cs.Add(&AddColumnChange{TableName: "users", Column: &Column{Name: "email", DataType: "text"}})
	assert.False(t, cs.IsEmpty())
}

func TestChangeSetSummary(t *testing.T) {
	cs := NewChangeSet()
	cs.Add(&AddColumnChange{TableName: "users", Column: &Column{Name: "email", DataType: "text"}})
	cs.Add(&AddColumnChange{TableName: "users", Column: &Column{Name: "name", DataType: "text"}})
	cs.Add(&DropColumnChange{TableName: "users", Column: &Column{Name: "old"}})
	cs.Add(&CreateTableChange{Table: &Table{Name: "logs"}})

	summary := cs.Summary()
	assert.Equal(t, 2, summary[ChangeAddColumn])
	assert.Equal(t, 1, summary[ChangeDropColumn])
	assert.Equal(t, 1, summary[ChangeCreateTable])
}

func TestCreateTableChange(t *testing.T) {
	c := &CreateTableChange{Table: &Table{Name: "users", Schema: "public"}}

	assert.Equal(t, ChangeCreateTable, c.Type())
	assert.False(t, c.IsDestructive())
	assert.Equal(t, "users", c.ObjectName())
	assert.Equal(t, "Create table users", c.Description())
}

func TestDropTableChange(t *testing.T) {
	c := &DropTableChange{Table: &Table{Name: "users", Schema: "public"}}

	assert.Equal(t, ChangeDropTable, c.Type())
	assert.True(t, c.IsDestructive())
	assert.Equal(t, "users", c.ObjectName())
	assert.Equal(t, "Drop table users", c.Description())
}

func TestAddColumnChange(t *testing.T) {
	c := &AddColumnChange{TableName: "users", Column: &Column{Name: "email", DataType: "text"}}

	assert.Equal(t, ChangeAddColumn, c.Type())
	assert.False(t, c.IsDestructive())
	assert.Equal(t, "users.email", c.ObjectName())
	assert.Equal(t, "Add column users.email (text)", c.Description())
}

func TestDropColumnChange(t *testing.T) {
	c := &DropColumnChange{TableName: "users", Column: &Column{Name: "old_field"}}

	assert.Equal(t, ChangeDropColumn, c.Type())
	assert.True(t, c.IsDestructive())
	assert.Equal(t, "users.old_field", c.ObjectName())
	assert.Equal(t, "Drop column users.old_field", c.Description())
}

func TestAlterColumnChange(t *testing.T) {
	t.Run("type change is destructive", func(t *testing.T) {
		c := &AlterColumnChange{
			TableName:  "users",
			ColumnName: "count",
			Alteration: ColumnAlteration{TypeChanged: true, OldType: "integer", NewType: "bigint"},
		}
		assert.True(t, c.IsDestructive())
		assert.Equal(t, ChangeAlterColumn, c.Type())
		assert.Equal(t, "users.count", c.ObjectName())
		assert.Contains(t, c.Description(), "type integer → bigint")
	})

	t.Run("nullable to not null is destructive", func(t *testing.T) {
		c := &AlterColumnChange{
			TableName:  "users",
			ColumnName: "email",
			Alteration: ColumnAlteration{NullableChanged: true, OldNullable: true, NewNullable: false},
		}
		assert.True(t, c.IsDestructive())
		assert.Contains(t, c.Description(), "set not null")
	})

	t.Run("not null to nullable is not destructive", func(t *testing.T) {
		c := &AlterColumnChange{
			TableName:  "users",
			ColumnName: "email",
			Alteration: ColumnAlteration{NullableChanged: true, OldNullable: false, NewNullable: true},
		}
		assert.False(t, c.IsDestructive())
		assert.Contains(t, c.Description(), "set nullable")
	})

	t.Run("default change only is not destructive", func(t *testing.T) {
		c := &AlterColumnChange{
			TableName:  "users",
			ColumnName: "status",
			Alteration: ColumnAlteration{DefaultChanged: true, NewDefault: strPtr("'active'")},
		}
		assert.False(t, c.IsDestructive())
		assert.Contains(t, c.Description(), "set default 'active'")
	})

	t.Run("drop default", func(t *testing.T) {
		c := &AlterColumnChange{
			TableName:  "users",
			ColumnName: "status",
			Alteration: ColumnAlteration{DefaultChanged: true, OldDefault: strPtr("'active'"), NewDefault: nil},
		}
		assert.Contains(t, c.Description(), "drop default")
	})

	t.Run("multiple alterations in description", func(t *testing.T) {
		c := &AlterColumnChange{
			TableName:  "users",
			ColumnName: "count",
			Alteration: ColumnAlteration{
				TypeChanged:     true,
				OldType:         "integer",
				NewType:         "bigint",
				NullableChanged: true,
				NewNullable:     false,
				DefaultChanged:  true,
				NewDefault:      strPtr("0"),
			},
		}
		desc := c.Description()
		assert.Contains(t, desc, "type integer → bigint")
		assert.Contains(t, desc, "set not null")
		assert.Contains(t, desc, "set default 0")
	})
}

func TestCreateIndexChange(t *testing.T) {
	t.Run("regular index", func(t *testing.T) {
		c := &CreateIndexChange{Index: &Index{Name: "idx_email", TableName: "users"}}
		assert.Equal(t, ChangeCreateIndex, c.Type())
		assert.False(t, c.IsDestructive())
		assert.Equal(t, "idx_email", c.ObjectName())
		assert.Equal(t, "Create index idx_email on users", c.Description())
	})

	t.Run("unique index", func(t *testing.T) {
		c := &CreateIndexChange{Index: &Index{Name: "idx_email", TableName: "users", IsUnique: true}}
		assert.Contains(t, c.Description(), "unique index")
	})
}

func TestDropIndexChange(t *testing.T) {
	c := &DropIndexChange{Index: &Index{Name: "idx_email"}}
	assert.Equal(t, ChangeDropIndex, c.Type())
	assert.False(t, c.IsDestructive())
	assert.Equal(t, "idx_email", c.ObjectName())
	assert.Equal(t, "Drop index idx_email", c.Description())
}

func TestAddConstraintChange(t *testing.T) {
	c := &AddConstraintChange{
		TableName:  "users",
		Constraint: &Constraint{Name: "fk_org", Type: ConstraintForeignKey},
	}
	assert.Equal(t, ChangeAddConstraint, c.Type())
	assert.False(t, c.IsDestructive())
	assert.Equal(t, "fk_org", c.ObjectName())
	assert.Equal(t, "Add FOREIGN KEY constraint fk_org on users", c.Description())
}

func TestDropConstraintChange(t *testing.T) {
	t.Run("foreign key is destructive", func(t *testing.T) {
		c := &DropConstraintChange{
			TableName:  "users",
			Constraint: &Constraint{Name: "fk_org", Type: ConstraintForeignKey},
		}
		assert.Equal(t, ChangeDropConstraint, c.Type())
		assert.True(t, c.IsDestructive())
		assert.Equal(t, "fk_org", c.ObjectName())
		assert.Contains(t, c.Description(), "Drop FOREIGN KEY constraint fk_org from users")
	})

	t.Run("check constraint is not destructive", func(t *testing.T) {
		c := &DropConstraintChange{
			TableName:  "users",
			Constraint: &Constraint{Name: "chk_email", Type: ConstraintCheck},
		}
		assert.False(t, c.IsDestructive())
	})

	t.Run("unique constraint is not destructive", func(t *testing.T) {
		c := &DropConstraintChange{
			TableName:  "users",
			Constraint: &Constraint{Name: "uniq_email", Type: ConstraintUnique},
		}
		assert.False(t, c.IsDestructive())
	})
}

func TestCreateEnumChange(t *testing.T) {
	c := &CreateEnumChange{Enum: &Enum{Name: "status", Schema: "public"}}
	assert.Equal(t, ChangeCreateEnum, c.Type())
	assert.False(t, c.IsDestructive())
	assert.Equal(t, "status", c.ObjectName())
	assert.Equal(t, "Create enum status", c.Description())
}

func TestDropEnumChange(t *testing.T) {
	c := &DropEnumChange{Enum: &Enum{Name: "status", Schema: "public"}}
	assert.Equal(t, ChangeDropEnum, c.Type())
	assert.True(t, c.IsDestructive())
	assert.Equal(t, "status", c.ObjectName())
	assert.Equal(t, "Drop enum status", c.Description())
}

func TestAddEnumValueChange(t *testing.T) {
	t.Run("with after", func(t *testing.T) {
		c := &AddEnumValueChange{EnumName: "status", Value: "deleted", After: "active"}
		assert.Equal(t, ChangeAddEnumValue, c.Type())
		assert.False(t, c.IsDestructive())
		assert.Equal(t, "status", c.ObjectName())
		assert.Equal(t, "Add value 'deleted' to enum status after 'active'", c.Description())
	})

	t.Run("without after", func(t *testing.T) {
		c := &AddEnumValueChange{EnumName: "status", Value: "deleted"}
		assert.Equal(t, "Add value 'deleted' to enum status", c.Description())
	})
}

func TestCreateFunctionChange(t *testing.T) {
	c := &CreateFunctionChange{Function: &Function{Name: "greet", Schema: "public", Arguments: "text"}}
	assert.Equal(t, ChangeCreateFunction, c.Type())
	assert.False(t, c.IsDestructive())
	assert.Equal(t, "greet(text)", c.ObjectName())
	assert.Equal(t, "Create function greet(text)", c.Description())
}

func TestDropFunctionChange(t *testing.T) {
	c := &DropFunctionChange{Function: &Function{Name: "greet", Schema: "public", Arguments: "text"}}
	assert.Equal(t, ChangeDropFunction, c.Type())
	assert.False(t, c.IsDestructive())
	assert.Equal(t, "greet(text)", c.ObjectName())
	assert.Equal(t, "Drop function greet(text)", c.Description())
}

func TestReplaceFunctionChange(t *testing.T) {
	c := &ReplaceFunctionChange{
		OldFunction: &Function{Name: "greet", Schema: "public", Arguments: "text"},
		NewFunction: &Function{Name: "greet", Schema: "public", Arguments: "text"},
	}
	assert.Equal(t, ChangeReplaceFunction, c.Type())
	assert.False(t, c.IsDestructive())
	assert.Equal(t, "greet(text)", c.ObjectName())
	assert.Equal(t, "Replace function greet(text)", c.Description())
}

func TestGenerateDropTable(t *testing.T) {
	gen := NewSQLGenerator()
	gen.IncludeComments = false

	sql := gen.GenerateChange(&DropTableChange{Table: &Table{Name: "users", Schema: "public"}})
	assert.Equal(t, "DROP TABLE users;", sql)
}

func TestGenerateAlterColumn(t *testing.T) {
	gen := NewSQLGenerator()
	gen.IncludeComments = false

	t.Run("type change", func(t *testing.T) {
		sql := gen.GenerateChange(&AlterColumnChange{
			TableName:  "users",
			ColumnName: "count",
			Alteration: ColumnAlteration{TypeChanged: true, NewType: "bigint"},
		})
		assert.Equal(t, "ALTER TABLE users ALTER COLUMN count TYPE bigint;", sql)
	})

	t.Run("set not null", func(t *testing.T) {
		sql := gen.GenerateChange(&AlterColumnChange{
			TableName:  "users",
			ColumnName: "email",
			Alteration: ColumnAlteration{NullableChanged: true, NewNullable: false},
		})
		assert.Equal(t, "ALTER TABLE users ALTER COLUMN email SET NOT NULL;", sql)
	})

	t.Run("drop not null", func(t *testing.T) {
		sql := gen.GenerateChange(&AlterColumnChange{
			TableName:  "users",
			ColumnName: "email",
			Alteration: ColumnAlteration{NullableChanged: true, NewNullable: true},
		})
		assert.Equal(t, "ALTER TABLE users ALTER COLUMN email DROP NOT NULL;", sql)
	})

	t.Run("set default", func(t *testing.T) {
		sql := gen.GenerateChange(&AlterColumnChange{
			TableName:  "users",
			ColumnName: "status",
			Alteration: ColumnAlteration{DefaultChanged: true, NewDefault: strPtr("'active'")},
		})
		assert.Equal(t, "ALTER TABLE users ALTER COLUMN status SET DEFAULT 'active';", sql)
	})

	t.Run("drop default", func(t *testing.T) {
		sql := gen.GenerateChange(&AlterColumnChange{
			TableName:  "users",
			ColumnName: "status",
			Alteration: ColumnAlteration{DefaultChanged: true, NewDefault: nil},
		})
		assert.Equal(t, "ALTER TABLE users ALTER COLUMN status DROP DEFAULT;", sql)
	})
}

func TestGenerateCreateIndex(t *testing.T) {
	gen := NewSQLGenerator()
	gen.IncludeComments = false

	t.Run("with definition", func(t *testing.T) {
		sql := gen.GenerateChange(&CreateIndexChange{
			Index: &Index{Name: "idx_email", Definition: "CREATE INDEX idx_email ON users USING btree (email)"},
		})
		assert.Equal(t, "CREATE INDEX idx_email ON users USING btree (email);", sql)
	})

	t.Run("with columns", func(t *testing.T) {
		sql := gen.GenerateChange(&CreateIndexChange{
			Index: &Index{Name: "idx_email", TableName: "users", Columns: []string{"email"}},
		})
		assert.Equal(t, "CREATE INDEX idx_email ON users (email);", sql)
	})

	t.Run("unique", func(t *testing.T) {
		sql := gen.GenerateChange(&CreateIndexChange{
			Index: &Index{Name: "idx_email", TableName: "users", Columns: []string{"email"}, IsUnique: true},
		})
		assert.Equal(t, "CREATE UNIQUE INDEX idx_email ON users (email);", sql)
	})
}

func TestGenerateDropIndex(t *testing.T) {
	gen := NewSQLGenerator()
	gen.IncludeComments = false

	sql := gen.GenerateChange(&DropIndexChange{Index: &Index{Name: "idx_email"}})
	assert.Equal(t, "DROP INDEX idx_email;", sql)
}

func TestGenerateAddConstraint(t *testing.T) {
	gen := NewSQLGenerator()
	gen.IncludeComments = false

	sql := gen.GenerateChange(&AddConstraintChange{
		TableName:  "users",
		Constraint: &Constraint{Name: "fk_org", Definition: "FOREIGN KEY (org_id) REFERENCES orgs(id)"},
	})
	assert.Equal(t, "ALTER TABLE users ADD CONSTRAINT fk_org FOREIGN KEY (org_id) REFERENCES orgs(id);", sql)
}

func TestGenerateDropConstraint(t *testing.T) {
	gen := NewSQLGenerator()
	gen.IncludeComments = false

	sql := gen.GenerateChange(&DropConstraintChange{
		TableName:  "users",
		Constraint: &Constraint{Name: "fk_org"},
	})
	assert.Equal(t, "ALTER TABLE users DROP CONSTRAINT fk_org;", sql)
}

func TestGenerateDropEnum(t *testing.T) {
	gen := NewSQLGenerator()
	gen.IncludeComments = false

	sql := gen.GenerateChange(&DropEnumChange{Enum: &Enum{Name: "status", Schema: "public"}})
	assert.Equal(t, "DROP TYPE status;", sql)
}

func TestGenerateAddEnumValueWithoutAfter(t *testing.T) {
	gen := NewSQLGenerator()
	gen.IncludeComments = false

	sql := gen.GenerateChange(&AddEnumValueChange{EnumName: "status", Value: "deleted"})
	assert.Equal(t, "ALTER TYPE status ADD VALUE 'deleted';", sql)
}

func TestGenerateCreateFunction(t *testing.T) {
	gen := NewSQLGenerator()
	gen.IncludeComments = false

	sql := gen.GenerateChange(&CreateFunctionChange{
		Function: &Function{
			Name:       "greet",
			Definition: "CREATE FUNCTION greet(name text) RETURNS text AS $$ SELECT 'Hello' $$ LANGUAGE sql",
		},
	})
	assert.Equal(t, "CREATE FUNCTION greet(name text) RETURNS text AS $$ SELECT 'Hello' $$ LANGUAGE sql;", sql)
}

func TestGenerateDropFunction(t *testing.T) {
	gen := NewSQLGenerator()
	gen.IncludeComments = false

	sql := gen.GenerateChange(&DropFunctionChange{
		Function: &Function{Name: "greet", Schema: "public", Arguments: "text"},
	})
	assert.Equal(t, "DROP FUNCTION greet(text);", sql)
}

func TestGenerateReplaceFunction(t *testing.T) {
	gen := NewSQLGenerator()
	gen.IncludeComments = false

	sql := gen.GenerateChange(&ReplaceFunctionChange{
		NewFunction: &Function{
			Name:       "greet",
			Definition: "CREATE FUNCTION greet(name text) RETURNS text AS $$ SELECT 'Hi' $$ LANGUAGE sql",
		},
	})
	assert.Equal(t, "CREATE OR REPLACE FUNCTION greet(name text) RETURNS text AS $$ SELECT 'Hi' $$ LANGUAGE sql;", sql)
}

func TestGenerateCreateTable(t *testing.T) {
	gen := NewSQLGenerator()
	gen.IncludeComments = false

	tbl := NewTable("users", "public")
	tbl.Columns["id"] = &Column{Name: "id", DataType: "integer", IsNullable: false, Position: 1, DefaultValue: strPtr("nextval('users_id_seq')")}
	tbl.Columns["email"] = &Column{Name: "email", DataType: "text", IsNullable: false, Position: 2}

	sql := gen.GenerateChange(&CreateTableChange{Table: tbl})
	assert.Contains(t, sql, "CREATE TABLE users (")
	assert.Contains(t, sql, "id integer NOT NULL DEFAULT nextval('users_id_seq')")
	assert.Contains(t, sql, "email text NOT NULL")
	assert.Contains(t, sql, ");")
}

func TestGenerateMigrationFile(t *testing.T) {
	gen := NewSQLGenerator()
	gen.IncludeComments = false

	cs := NewChangeSet()
	cs.Add(&AddColumnChange{TableName: "users", Column: &Column{Name: "email", DataType: "text", IsNullable: false}})
	cs.Add(&DropColumnChange{TableName: "users", Column: &Column{Name: "old_field"}})

	result := gen.GenerateMigrationFile(cs, "add email, drop old_field")

	assert.Contains(t, result, "-- Migration generated by pgbranch")
	assert.Contains(t, result, "-- Description: add email, drop old_field")
	assert.Contains(t, result, "-- Changes:")
	assert.Contains(t, result, "WARNING")
	assert.Contains(t, result, "destructive")
	assert.Contains(t, result, "BEGIN;")
	assert.Contains(t, result, "COMMIT;")
}

func TestQuoteIdent(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "simple identifier",
			input:    "users",
			expected: "users",
		},
		{
			name:     "reserved word",
			input:    "user",
			expected: `"user"`,
		},
		{
			name:     "reserved word select",
			input:    "select",
			expected: `"select"`,
		},
		{
			name:     "identifier with dot",
			input:    "billing.users",
			expected: `"billing.users"`,
		},
		{
			name:     "identifier with uppercase",
			input:    "Users",
			expected: `"Users"`,
		},
		{
			name:     "identifier with space",
			input:    "my table",
			expected: `"my table"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, quoteIdent(tt.input))
		})
	}
}

func TestGenerateWithComments(t *testing.T) {
	gen := NewSQLGenerator()
	gen.IncludeComments = true

	cs := NewChangeSet()
	cs.Add(&DropTableChange{Table: &Table{Name: "users", Schema: "public"}})

	stmts := gen.Generate(cs)
	require.Len(t, stmts, 2)
	assert.Contains(t, stmts[0], "-- Drop table users (DESTRUCTIVE)")
	assert.Equal(t, "DROP TABLE users;", stmts[1])
}

func TestValidateChanges(t *testing.T) {
	t.Run("drop column warning", func(t *testing.T) {
		cs := NewChangeSet()
		cs.Add(&DropColumnChange{TableName: "users", Column: &Column{Name: "email"}})

		warnings, errors := ValidateChanges(cs)
		assert.Len(t, errors, 0)
		require.Len(t, warnings, 1)
		assert.Contains(t, warnings[0], "Dropping column")
		assert.Contains(t, warnings[0], "users.email")
	})

	t.Run("drop table warning", func(t *testing.T) {
		cs := NewChangeSet()
		cs.Add(&DropTableChange{Table: &Table{Name: "users", Schema: "public"}})

		warnings, errors := ValidateChanges(cs)
		assert.Len(t, errors, 0)
		require.Len(t, warnings, 1)
		assert.Contains(t, warnings[0], "Dropping table")
		assert.Contains(t, warnings[0], "users")
	})

	t.Run("set not null warning", func(t *testing.T) {
		cs := NewChangeSet()
		cs.Add(&AlterColumnChange{
			TableName:  "users",
			ColumnName: "email",
			Alteration: ColumnAlteration{NullableChanged: true, NewNullable: false},
		})

		warnings, errors := ValidateChanges(cs)
		assert.Len(t, errors, 0)
		require.Len(t, warnings, 1)
		assert.Contains(t, warnings[0], "NOT NULL")
		assert.Contains(t, warnings[0], "users.email")
	})

	t.Run("string to numeric type change is error", func(t *testing.T) {
		cs := NewChangeSet()
		cs.Add(&AlterColumnChange{
			TableName:  "users",
			ColumnName: "count",
			Alteration: ColumnAlteration{TypeChanged: true, OldType: "text", NewType: "integer"},
		})

		warnings, errors := ValidateChanges(cs)
		assert.Len(t, warnings, 0)
		require.Len(t, errors, 1)
		assert.Contains(t, errors[0], "text")
		assert.Contains(t, errors[0], "integer")
	})

	t.Run("numeric to string type change is warning", func(t *testing.T) {
		cs := NewChangeSet()
		cs.Add(&AlterColumnChange{
			TableName:  "users",
			ColumnName: "count",
			Alteration: ColumnAlteration{TypeChanged: true, OldType: "integer", NewType: "text"},
		})

		warnings, errors := ValidateChanges(cs)
		assert.Len(t, errors, 0)
		require.Len(t, warnings, 1)
		assert.Contains(t, warnings[0], "integer")
		assert.Contains(t, warnings[0], "text")
		assert.Contains(t, warnings[0], "precision")
	})

	t.Run("no warnings or errors for safe changes", func(t *testing.T) {
		cs := NewChangeSet()
		cs.Add(&AddColumnChange{TableName: "users", Column: &Column{Name: "email", DataType: "text"}})
		cs.Add(&CreateTableChange{Table: &Table{Name: "logs", Schema: "public"}})

		warnings, errors := ValidateChanges(cs)
		assert.Empty(t, warnings)
		assert.Empty(t, errors)
	})
}

func intPtr(i int) *int {
	return &i
}

func strPtr(s string) *string {
	return &s
}

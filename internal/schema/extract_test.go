package schema

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockRows struct {
	data    [][]any
	index   int
	scanErr error
	err     error
}

func (m *mockRows) Close()                                        {}
func (m *mockRows) Err() error                                    { return m.err }
func (m *mockRows) CommandTag() pgconn.CommandTag                 { return pgconn.CommandTag{} }
func (m *mockRows) FieldDescriptions() []pgconn.FieldDescription  { return nil }
func (m *mockRows) RawValues() [][]byte                           { return nil }
func (m *mockRows) Conn() *pgx.Conn                               { return nil }

func (m *mockRows) Next() bool {
	if m.index >= len(m.data) {
		return false
	}
	m.index++
	return true
}

func (m *mockRows) Values() ([]any, error) {
	if m.index == 0 || m.index > len(m.data) {
		return nil, fmt.Errorf("no current row")
	}
	return m.data[m.index-1], nil
}

func (m *mockRows) Scan(dest ...any) error {
	if m.scanErr != nil {
		return m.scanErr
	}
	row := m.data[m.index-1]
	for i, src := range row {
		if i >= len(dest) {
			break
		}
		if src == nil {
			continue
		}
		dv := reflect.ValueOf(dest[i])
		if dv.Kind() != reflect.Ptr {
			return fmt.Errorf("dest[%d] is not a pointer", i)
		}
		sv := reflect.ValueOf(src)
		if sv.Type().AssignableTo(dv.Elem().Type()) {
			dv.Elem().Set(sv)
		} else if sv.Type().ConvertibleTo(dv.Elem().Type()) {
			dv.Elem().Set(sv.Convert(dv.Elem().Type()))
		} else {
			return fmt.Errorf("cannot assign %v to %v", sv.Type(), dv.Elem().Type())
		}
	}
	return nil
}

type mockConn struct {
	results map[string]*mockRows
}

func (m *mockConn) Query(_ context.Context, sql string, _ ...any) (pgx.Rows, error) {
	for key, rows := range m.results {
		if strings.Contains(sql, key) {
			return rows, nil
		}
	}
	return &mockRows{}, nil
}

func TestExtract_Tables(t *testing.T) {
	conn := &mockConn{results: map[string]*mockRows{
		"information_schema.tables": {data: [][]any{
			{"users", "public"},
			{"orders", "public"},
		}},
	}}

	ext := NewExtractor(conn)
	schema, err := ext.Extract(context.Background(), "testdb")
	require.NoError(t, err)

	assert.Len(t, schema.Tables, 2)
	assert.Contains(t, schema.Tables, "users")
	assert.Contains(t, schema.Tables, "orders")
	assert.Equal(t, "public", schema.Tables["users"].Schema)
	assert.Equal(t, "public", schema.Tables["orders"].Schema)
}

func TestExtract_Columns(t *testing.T) {
	conn := &mockConn{results: map[string]*mockRows{
		"information_schema.tables": {data: [][]any{
			{"users", "public"},
		}},
		"information_schema.columns": {data: [][]any{
			{"id", "integer", "NO", (*string)(nil), 1, (*int)(nil), (*int)(nil), (*int)(nil), "int4"},
			{"name", "character varying", "YES", strPtr("'unnamed'"), 2, intPtr(255), (*int)(nil), (*int)(nil), "varchar"},
			{"tags", "ARRAY", "YES", (*string)(nil), 3, (*int)(nil), (*int)(nil), (*int)(nil), "_text"},
		}},
	}}

	ext := NewExtractor(conn)
	schema, err := ext.Extract(context.Background(), "testdb")
	require.NoError(t, err)

	tbl := schema.Tables["users"]
	require.NotNil(t, tbl)
	assert.Len(t, tbl.Columns, 3)

	id := tbl.Columns["id"]
	require.NotNil(t, id)
	assert.Equal(t, "integer", id.DataType)
	assert.False(t, id.IsNullable)
	assert.Nil(t, id.DefaultValue)

	name := tbl.Columns["name"]
	require.NotNil(t, name)
	assert.Equal(t, "character varying", name.DataType)
	assert.True(t, name.IsNullable)
	require.NotNil(t, name.DefaultValue)
	assert.Equal(t, "'unnamed'", *name.DefaultValue)
	require.NotNil(t, name.CharMaxLength)
	assert.Equal(t, 255, *name.CharMaxLength)

	tags := tbl.Columns["tags"]
	require.NotNil(t, tags)
	assert.True(t, tags.IsArray)
	assert.Equal(t, "text", tags.ElementType)
	assert.Equal(t, "text", tags.DataType)
}

func TestExtract_Indexes(t *testing.T) {
	conn := &mockConn{results: map[string]*mockRows{
		"information_schema.tables": {data: [][]any{
			{"users", "public"},
		}},
		"pg_index": {data: [][]any{
			{"users_pkey", "btree", true, true, "CREATE UNIQUE INDEX users_pkey ON public.users USING btree (id)", []string{"id"}},
			{"users_email_idx", "btree", true, false, "CREATE UNIQUE INDEX users_email_idx ON public.users USING btree (email)", []string{"email"}},
		}},
	}}

	ext := NewExtractor(conn)
	schema, err := ext.Extract(context.Background(), "testdb")
	require.NoError(t, err)

	tbl := schema.Tables["users"]
	require.NotNil(t, tbl)
	assert.Len(t, tbl.Indexes, 2)

	pkey := tbl.Indexes["users_pkey"]
	require.NotNil(t, pkey)
	assert.Equal(t, "btree", pkey.Type)
	assert.True(t, pkey.IsUnique)
	assert.True(t, pkey.IsPrimary)
	assert.Equal(t, []string{"id"}, pkey.Columns)

	emailIdx := tbl.Indexes["users_email_idx"]
	require.NotNil(t, emailIdx)
	assert.True(t, emailIdx.IsUnique)
	assert.False(t, emailIdx.IsPrimary)
	assert.Equal(t, []string{"email"}, emailIdx.Columns)
}

func TestExtract_Constraints(t *testing.T) {
	conn := &mockConn{results: map[string]*mockRows{
		"information_schema.tables": {data: [][]any{
			{"users", "public"},
		}},
		"pg_constraint": {data: [][]any{
			{"users_pkey", "PRIMARY KEY", "PRIMARY KEY (id)", []string{"id"}, (*string)(nil), []string(nil), (*string)(nil), (*string)(nil)},
			{"orders_user_fk", "FOREIGN KEY", "FOREIGN KEY (user_id) REFERENCES users(id)", []string{"user_id"}, strPtr("users"), []string{"id"}, strPtr("CASCADE"), strPtr("NO ACTION")},
		}},
	}}

	ext := NewExtractor(conn)
	schema, err := ext.Extract(context.Background(), "testdb")
	require.NoError(t, err)

	tbl := schema.Tables["users"]
	require.NotNil(t, tbl)
	assert.Len(t, tbl.Constraints, 2)

	pk := tbl.Constraints["users_pkey"]
	require.NotNil(t, pk)
	assert.Equal(t, ConstraintPrimaryKey, pk.Type)
	assert.Equal(t, []string{"id"}, pk.Columns)

	fk := tbl.Constraints["orders_user_fk"]
	require.NotNil(t, fk)
	assert.Equal(t, ConstraintForeignKey, fk.Type)
	assert.Equal(t, "users", fk.RefTable)
	assert.Equal(t, []string{"id"}, fk.RefColumns)
	assert.Equal(t, "CASCADE", fk.OnDelete)
	assert.Equal(t, "NO ACTION", fk.OnUpdate)
}

func TestExtract_Enums(t *testing.T) {
	conn := &mockConn{results: map[string]*mockRows{
		"typtype = 'e'": {data: [][]any{
			{"status", "public", []string{"active", "inactive", "deleted"}},
		}},
	}}

	ext := NewExtractor(conn)
	schema, err := ext.Extract(context.Background(), "testdb")
	require.NoError(t, err)

	assert.Len(t, schema.Enums, 1)
	assert.Contains(t, schema.Enums, "status")

	e := schema.Enums["status"]
	assert.Equal(t, "public", e.Schema)
	assert.Equal(t, []string{"active", "inactive", "deleted"}, e.Values)
}

func TestExtract_Functions(t *testing.T) {
	conn := &mockConn{results: map[string]*mockRows{
		"pg_proc": {data: [][]any{
			{"greet", "public", "name text", "text", "sql", "CREATE FUNCTION greet(name text) RETURNS text AS $$ SELECT 'Hello' $$ LANGUAGE sql"},
		}},
	}}

	ext := NewExtractor(conn)
	schema, err := ext.Extract(context.Background(), "testdb")
	require.NoError(t, err)

	assert.Len(t, schema.Functions, 1)

	fn := schema.Functions["greet(name text)"]
	require.NotNil(t, fn)
	assert.Equal(t, "greet", fn.Name)
	assert.Equal(t, "public", fn.Schema)
	assert.Equal(t, "name text", fn.Arguments)
	assert.Equal(t, "text", fn.ReturnType)
	assert.Equal(t, "sql", fn.Language)
	assert.NotEmpty(t, fn.BodyHash)
}

func TestExtract_EmptyDatabase(t *testing.T) {
	conn := &mockConn{results: map[string]*mockRows{}}

	ext := NewExtractor(conn)
	schema, err := ext.Extract(context.Background(), "testdb")
	require.NoError(t, err)

	assert.Empty(t, schema.Tables)
	assert.Empty(t, schema.Enums)
	assert.Empty(t, schema.Functions)
}

type errorConn struct{}

func (m *errorConn) Query(_ context.Context, sql string, _ ...any) (pgx.Rows, error) {
	if strings.Contains(sql, "information_schema.tables") {
		return nil, fmt.Errorf("connection refused")
	}
	return &mockRows{}, nil
}

func TestExtract_QueryError(t *testing.T) {
	ext := NewExtractor(&errorConn{})
	_, err := ext.Extract(context.Background(), "testdb")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "connection refused")
}

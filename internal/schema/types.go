package schema

import (
	"fmt"
	"sort"
	"strings"
)

type Schema struct {
	Name      string
	Tables    map[string]*Table
	Enums     map[string]*Enum
	Functions map[string]*Function
}

func NewSchema(name string) *Schema {
	return &Schema{
		Name:      name,
		Tables:    make(map[string]*Table),
		Enums:     make(map[string]*Enum),
		Functions: make(map[string]*Function),
	}
}

type Table struct {
	Name        string
	Schema      string
	Columns     map[string]*Column
	Indexes     map[string]*Index
	Constraints map[string]*Constraint
}

func NewTable(name, schema string) *Table {
	return &Table{
		Name:        name,
		Schema:      schema,
		Columns:     make(map[string]*Column),
		Indexes:     make(map[string]*Index),
		Constraints: make(map[string]*Constraint),
	}
}

func (t *Table) FullName() string {
	if t.Schema == "" || t.Schema == "public" {
		return t.Name
	}
	return fmt.Sprintf("%s.%s", t.Schema, t.Name)
}

func (t *Table) SortedColumns() []*Column {
	cols := make([]*Column, 0, len(t.Columns))
	for _, col := range t.Columns {
		cols = append(cols, col)
	}
	sort.Slice(cols, func(i, j int) bool {
		return cols[i].Position < cols[j].Position
	})
	return cols
}

func (t *Table) SortedIndexes() []*Index {
	idxs := make([]*Index, 0, len(t.Indexes))
	for _, idx := range t.Indexes {
		idxs = append(idxs, idx)
	}
	sort.Slice(idxs, func(i, j int) bool {
		return idxs[i].Name < idxs[j].Name
	})
	return idxs
}

func (t *Table) SortedConstraints() []*Constraint {
	cons := make([]*Constraint, 0, len(t.Constraints))
	for _, c := range t.Constraints {
		cons = append(cons, c)
	}
	sort.Slice(cons, func(i, j int) bool {
		return cons[i].Name < cons[j].Name
	})
	return cons
}

type Column struct {
	Name         string
	DataType     string
	IsNullable   bool
	DefaultValue *string
	Position     int

	CharMaxLength *int

	NumericPrecision *int
	NumericScale     *int

	IsArray     bool
	ElementType string
}

func (c *Column) FullType() string {
	typ := c.DataType

	if c.CharMaxLength != nil {
		if strings.HasPrefix(typ, "character varying") {
			typ = fmt.Sprintf("varchar(%d)", *c.CharMaxLength)
		} else if typ == "character" {
			typ = fmt.Sprintf("char(%d)", *c.CharMaxLength)
		}
	}

	if c.NumericPrecision != nil {
		if c.NumericScale != nil && *c.NumericScale > 0 {
			typ = fmt.Sprintf("numeric(%d,%d)", *c.NumericPrecision, *c.NumericScale)
		} else if typ == "numeric" {
			typ = fmt.Sprintf("numeric(%d)", *c.NumericPrecision)
		}
	}

	if c.IsArray {
		typ = typ + "[]"
	}

	return typ
}

func (c *Column) Equals(other *Column) bool {
	if c.Name != other.Name {
		return false
	}
	if c.FullType() != other.FullType() {
		return false
	}
	if c.IsNullable != other.IsNullable {
		return false
	}
	if c.DefaultValue == nil && other.DefaultValue == nil {
		return true
	}
	if c.DefaultValue == nil || other.DefaultValue == nil {
		return false
	}
	return *c.DefaultValue == *other.DefaultValue
}

type Index struct {
	Name       string
	TableName  string
	Columns    []string
	IsUnique   bool
	IsPrimary  bool
	Type       string // btree, hash, gin, gist, etc.
	Definition string // full index definition from pg_get_indexdef
}

func (i *Index) Equals(other *Index) bool {
	if i.Name != other.Name {
		return false
	}
	if i.IsUnique != other.IsUnique {
		return false
	}
	if i.IsPrimary != other.IsPrimary {
		return false
	}
	if i.Type != other.Type {
		return false
	}
	if len(i.Columns) != len(other.Columns) {
		return false
	}
	for idx, col := range i.Columns {
		if col != other.Columns[idx] {
			return false
		}
	}
	return true
}

type ConstraintType string

const (
	ConstraintPrimaryKey ConstraintType = "PRIMARY KEY"
	ConstraintForeignKey ConstraintType = "FOREIGN KEY"
	ConstraintUnique     ConstraintType = "UNIQUE"
	ConstraintCheck      ConstraintType = "CHECK"
	ConstraintExclusion  ConstraintType = "EXCLUDE"
)

type Constraint struct {
	Name       string
	Type       ConstraintType
	TableName  string
	Columns    []string
	Definition string

	RefTable   string
	RefColumns []string
	OnDelete   string
	OnUpdate   string
}

func (c *Constraint) Equals(other *Constraint) bool {
	if c.Name != other.Name {
		return false
	}
	if c.Type != other.Type {
		return false
	}
	return c.Definition == other.Definition
}

type Enum struct {
	Name   string
	Schema string
	Values []string
}

func (e *Enum) FullName() string {
	if e.Schema == "" || e.Schema == "public" {
		return e.Name
	}
	return fmt.Sprintf("%s.%s", e.Schema, e.Name)
}

func (e *Enum) Equals(other *Enum) bool {
	if e.Name != other.Name {
		return false
	}
	if len(e.Values) != len(other.Values) {
		return false
	}
	for i, v := range e.Values {
		if v != other.Values[i] {
			return false
		}
	}
	return true
}

type Function struct {
	Name       string
	Schema     string
	Arguments  string
	ReturnType string
	Language   string
	Definition string
	BodyHash   string
}

func (f *Function) Signature() string {
	return fmt.Sprintf("%s(%s)", f.Name, f.Arguments)
}

func (f *Function) FullName() string {
	if f.Schema == "" || f.Schema == "public" {
		return f.Signature()
	}
	return fmt.Sprintf("%s.%s", f.Schema, f.Signature())
}

func (f *Function) Equals(other *Function) bool {
	if f.Signature() != other.Signature() {
		return false
	}
	if f.ReturnType != other.ReturnType {
		return false
	}
	return f.BodyHash == other.BodyHash
}

func (s *Schema) SortedTables() []*Table {
	tables := make([]*Table, 0, len(s.Tables))
	for _, t := range s.Tables {
		tables = append(tables, t)
	}
	sort.Slice(tables, func(i, j int) bool {
		return tables[i].Name < tables[j].Name
	})
	return tables
}

func (s *Schema) SortedEnums() []*Enum {
	enums := make([]*Enum, 0, len(s.Enums))
	for _, e := range s.Enums {
		enums = append(enums, e)
	}
	sort.Slice(enums, func(i, j int) bool {
		return enums[i].Name < enums[j].Name
	})
	return enums
}

func (s *Schema) SortedFunctions() []*Function {
	funcs := make([]*Function, 0, len(s.Functions))
	for _, f := range s.Functions {
		funcs = append(funcs, f)
	}
	sort.Slice(funcs, func(i, j int) bool {
		return funcs[i].Signature() < funcs[j].Signature()
	})
	return funcs
}

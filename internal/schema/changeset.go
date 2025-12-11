package schema

import "fmt"

// ChangeType represents the type of schema change.
type ChangeType string

const (
	// Table changes
	ChangeCreateTable ChangeType = "CREATE_TABLE"
	ChangeDropTable   ChangeType = "DROP_TABLE"

	// Column changes
	ChangeAddColumn   ChangeType = "ADD_COLUMN"
	ChangeDropColumn  ChangeType = "DROP_COLUMN"
	ChangeAlterColumn ChangeType = "ALTER_COLUMN"

	// Index changes
	ChangeCreateIndex ChangeType = "CREATE_INDEX"
	ChangeDropIndex   ChangeType = "DROP_INDEX"

	// Constraint changes
	ChangeAddConstraint  ChangeType = "ADD_CONSTRAINT"
	ChangeDropConstraint ChangeType = "DROP_CONSTRAINT"

	// Enum changes
	ChangeCreateEnum   ChangeType = "CREATE_ENUM"
	ChangeDropEnum     ChangeType = "DROP_ENUM"
	ChangeAddEnumValue ChangeType = "ADD_ENUM_VALUE"

	// Function changes
	ChangeCreateFunction  ChangeType = "CREATE_FUNCTION"
	ChangeDropFunction    ChangeType = "DROP_FUNCTION"
	ChangeReplaceFunction ChangeType = "REPLACE_FUNCTION"
)

// Change represents a single schema change.
type Change interface {
	// Type returns the type of change.
	Type() ChangeType

	// IsDestructive returns true if this change could result in data loss.
	IsDestructive() bool

	// Description returns a human-readable description of the change.
	Description() string

	// ObjectName returns the name of the object being changed.
	ObjectName() string
}

type ChangeSet struct {
	Changes []Change
}

func NewChangeSet() *ChangeSet {
	return &ChangeSet{
		Changes: make([]Change, 0),
	}
}

func (cs *ChangeSet) Add(c Change) {
	cs.Changes = append(cs.Changes, c)
}

func (cs *ChangeSet) IsEmpty() bool {
	return len(cs.Changes) == 0
}

func (cs *ChangeSet) HasDestructive() bool {
	for _, c := range cs.Changes {
		if c.IsDestructive() {
			return true
		}
	}
	return false
}

func (cs *ChangeSet) DestructiveCount() int {
	count := 0
	for _, c := range cs.Changes {
		if c.IsDestructive() {
			count++
		}
	}
	return count
}

func (cs *ChangeSet) ByType(t ChangeType) []Change {
	var result []Change
	for _, c := range cs.Changes {
		if c.Type() == t {
			result = append(result, c)
		}
	}
	return result
}

func (cs *ChangeSet) Summary() map[ChangeType]int {
	summary := make(map[ChangeType]int)
	for _, c := range cs.Changes {
		summary[c.Type()]++
	}
	return summary
}

type CreateTableChange struct {
	Table *Table
}

func (c *CreateTableChange) Type() ChangeType    { return ChangeCreateTable }
func (c *CreateTableChange) IsDestructive() bool { return false }
func (c *CreateTableChange) ObjectName() string  { return c.Table.FullName() }
func (c *CreateTableChange) Description() string {
	return fmt.Sprintf("Create table %s", c.Table.FullName())
}

type DropTableChange struct {
	Table *Table
}

func (c *DropTableChange) Type() ChangeType    { return ChangeDropTable }
func (c *DropTableChange) IsDestructive() bool { return true }
func (c *DropTableChange) ObjectName() string  { return c.Table.FullName() }
func (c *DropTableChange) Description() string {
	return fmt.Sprintf("Drop table %s", c.Table.FullName())
}

type AddColumnChange struct {
	TableName string
	Column    *Column
}

func (c *AddColumnChange) Type() ChangeType    { return ChangeAddColumn }
func (c *AddColumnChange) IsDestructive() bool { return false }
func (c *AddColumnChange) ObjectName() string {
	return fmt.Sprintf("%s.%s", c.TableName, c.Column.Name)
}
func (c *AddColumnChange) Description() string {
	return fmt.Sprintf("Add column %s.%s (%s)", c.TableName, c.Column.Name, c.Column.FullType())
}

type DropColumnChange struct {
	TableName string
	Column    *Column
}

func (c *DropColumnChange) Type() ChangeType    { return ChangeDropColumn }
func (c *DropColumnChange) IsDestructive() bool { return true }
func (c *DropColumnChange) ObjectName() string {
	return fmt.Sprintf("%s.%s", c.TableName, c.Column.Name)
}
func (c *DropColumnChange) Description() string {
	return fmt.Sprintf("Drop column %s.%s", c.TableName, c.Column.Name)
}

type ColumnAlteration struct {
	TypeChanged     bool
	OldType         string
	NewType         string
	NullableChanged bool
	OldNullable     bool
	NewNullable     bool
	DefaultChanged  bool
	OldDefault      *string
	NewDefault      *string
}

type AlterColumnChange struct {
	TableName  string
	ColumnName string
	OldColumn  *Column
	NewColumn  *Column
	Alteration ColumnAlteration
}

func (c *AlterColumnChange) Type() ChangeType { return ChangeAlterColumn }
func (c *AlterColumnChange) IsDestructive() bool {
	if c.Alteration.TypeChanged {
		return true
	}
	if c.Alteration.NullableChanged && !c.Alteration.NewNullable {
		return true
	}
	return false
}
func (c *AlterColumnChange) ObjectName() string {
	return fmt.Sprintf("%s.%s", c.TableName, c.ColumnName)
}
func (c *AlterColumnChange) Description() string {
	var parts []string
	if c.Alteration.TypeChanged {
		parts = append(parts, fmt.Sprintf("type %s → %s", c.Alteration.OldType, c.Alteration.NewType))
	}
	if c.Alteration.NullableChanged {
		if c.Alteration.NewNullable {
			parts = append(parts, "set nullable")
		} else {
			parts = append(parts, "set not null")
		}
	}
	if c.Alteration.DefaultChanged {
		if c.Alteration.NewDefault == nil {
			parts = append(parts, "drop default")
		} else {
			parts = append(parts, fmt.Sprintf("set default %s", *c.Alteration.NewDefault))
		}
	}
	return fmt.Sprintf("Alter column %s.%s: %s", c.TableName, c.ColumnName, joinParts(parts))
}

type CreateIndexChange struct {
	Index *Index
}

func (c *CreateIndexChange) Type() ChangeType    { return ChangeCreateIndex }
func (c *CreateIndexChange) IsDestructive() bool { return false }
func (c *CreateIndexChange) ObjectName() string  { return c.Index.Name }
func (c *CreateIndexChange) Description() string {
	unique := ""
	if c.Index.IsUnique {
		unique = "unique "
	}
	return fmt.Sprintf("Create %sindex %s on %s", unique, c.Index.Name, c.Index.TableName)
}

type DropIndexChange struct {
	Index *Index
}

func (c *DropIndexChange) Type() ChangeType    { return ChangeDropIndex }
func (c *DropIndexChange) IsDestructive() bool { return false } // Indexes can be recreated
func (c *DropIndexChange) ObjectName() string  { return c.Index.Name }
func (c *DropIndexChange) Description() string {
	return fmt.Sprintf("Drop index %s", c.Index.Name)
}

type AddConstraintChange struct {
	TableName  string
	Constraint *Constraint
}

func (c *AddConstraintChange) Type() ChangeType    { return ChangeAddConstraint }
func (c *AddConstraintChange) IsDestructive() bool { return false }
func (c *AddConstraintChange) ObjectName() string  { return c.Constraint.Name }
func (c *AddConstraintChange) Description() string {
	return fmt.Sprintf("Add %s constraint %s on %s", c.Constraint.Type, c.Constraint.Name, c.TableName)
}

type DropConstraintChange struct {
	TableName  string
	Constraint *Constraint
}

func (c *DropConstraintChange) Type() ChangeType { return ChangeDropConstraint }
func (c *DropConstraintChange) IsDestructive() bool {
	return c.Constraint.Type == ConstraintForeignKey
}
func (c *DropConstraintChange) ObjectName() string { return c.Constraint.Name }
func (c *DropConstraintChange) Description() string {
	return fmt.Sprintf("Drop %s constraint %s from %s", c.Constraint.Type, c.Constraint.Name, c.TableName)
}

type CreateEnumChange struct {
	Enum *Enum
}

func (c *CreateEnumChange) Type() ChangeType    { return ChangeCreateEnum }
func (c *CreateEnumChange) IsDestructive() bool { return false }
func (c *CreateEnumChange) ObjectName() string  { return c.Enum.FullName() }
func (c *CreateEnumChange) Description() string {
	return fmt.Sprintf("Create enum %s", c.Enum.FullName())
}

type DropEnumChange struct {
	Enum *Enum
}

func (c *DropEnumChange) Type() ChangeType    { return ChangeDropEnum }
func (c *DropEnumChange) IsDestructive() bool { return true }
func (c *DropEnumChange) ObjectName() string  { return c.Enum.FullName() }
func (c *DropEnumChange) Description() string {
	return fmt.Sprintf("Drop enum %s", c.Enum.FullName())
}

type AddEnumValueChange struct {
	EnumName string
	Value    string
	After    string
}

func (c *AddEnumValueChange) Type() ChangeType    { return ChangeAddEnumValue }
func (c *AddEnumValueChange) IsDestructive() bool { return false }
func (c *AddEnumValueChange) ObjectName() string  { return c.EnumName }
func (c *AddEnumValueChange) Description() string {
	if c.After != "" {
		return fmt.Sprintf("Add value '%s' to enum %s after '%s'", c.Value, c.EnumName, c.After)
	}
	return fmt.Sprintf("Add value '%s' to enum %s", c.Value, c.EnumName)
}

type CreateFunctionChange struct {
	Function *Function
}

func (c *CreateFunctionChange) Type() ChangeType    { return ChangeCreateFunction }
func (c *CreateFunctionChange) IsDestructive() bool { return false }
func (c *CreateFunctionChange) ObjectName() string  { return c.Function.FullName() }
func (c *CreateFunctionChange) Description() string {
	return fmt.Sprintf("Create function %s", c.Function.Signature())
}

type DropFunctionChange struct {
	Function *Function
}

func (c *DropFunctionChange) Type() ChangeType    { return ChangeDropFunction }
func (c *DropFunctionChange) IsDestructive() bool { return false } // Functions can be recreated
func (c *DropFunctionChange) ObjectName() string  { return c.Function.FullName() }
func (c *DropFunctionChange) Description() string {
	return fmt.Sprintf("Drop function %s", c.Function.Signature())
}

type ReplaceFunctionChange struct {
	OldFunction *Function
	NewFunction *Function
}

func (c *ReplaceFunctionChange) Type() ChangeType    { return ChangeReplaceFunction }
func (c *ReplaceFunctionChange) IsDestructive() bool { return false }
func (c *ReplaceFunctionChange) ObjectName() string  { return c.NewFunction.FullName() }
func (c *ReplaceFunctionChange) Description() string {
	return fmt.Sprintf("Replace function %s", c.NewFunction.Signature())
}

func joinParts(parts []string) string {
	if len(parts) == 0 {
		return ""
	}
	result := parts[0]
	for i := 1; i < len(parts); i++ {
		result += ", " + parts[i]
	}
	return result
}

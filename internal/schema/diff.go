package schema

// Diff compares two schemas and returns a ChangeSet representing
// the changes needed to transform 'from' into 'to'.
func Diff(from, to *Schema) *ChangeSet {
	cs := NewChangeSet()

	diffEnums(from, to, cs)

	diffTables(from, to, cs)

	diffFunctions(from, to, cs)

	return cs
}

func diffEnums(from, to *Schema, cs *ChangeSet) {
	for name, fromEnum := range from.Enums {
		if _, exists := to.Enums[name]; !exists {
			cs.Add(&DropEnumChange{Enum: fromEnum})
		}
	}

	for name, toEnum := range to.Enums {
		fromEnum, exists := from.Enums[name]
		if !exists {
			cs.Add(&CreateEnumChange{Enum: toEnum})
			continue
		}

		diffEnumValues(fromEnum, toEnum, cs)
	}
}

func diffEnumValues(from, to *Enum, cs *ChangeSet) {
	fromValues := make(map[string]int) // value -> position
	for i, v := range from.Values {
		fromValues[v] = i
	}

	for i, v := range to.Values {
		if _, exists := fromValues[v]; !exists {
			after := ""
			if i > 0 {
				after = to.Values[i-1]
			}
			cs.Add(&AddEnumValueChange{
				EnumName: to.Name,
				Value:    v,
				After:    after,
			})
		}
	}
}

func diffTables(from, to *Schema, cs *ChangeSet) {
	for name, fromTable := range from.Tables {
		if _, exists := to.Tables[name]; !exists {
			cs.Add(&DropTableChange{Table: fromTable})
		}
	}

	for name, toTable := range to.Tables {
		fromTable, exists := from.Tables[name]
		if !exists {
			cs.Add(&CreateTableChange{Table: toTable})
			continue
		}

		diffTableContents(fromTable, toTable, cs)
	}
}

func diffTableContents(from, to *Table, cs *ChangeSet) {
	diffColumns(from, to, cs)
	diffIndexes(from, to, cs)
	diffConstraints(from, to, cs)
}

func diffColumns(from, to *Table, cs *ChangeSet) {
	tableName := to.FullName()

	for name, fromCol := range from.Columns {
		if _, exists := to.Columns[name]; !exists {
			cs.Add(&DropColumnChange{
				TableName: tableName,
				Column:    fromCol,
			})
		}
	}

	for name, toCol := range to.Columns {
		fromCol, exists := from.Columns[name]
		if !exists {
			cs.Add(&AddColumnChange{
				TableName: tableName,
				Column:    toCol,
			})
			continue
		}

		if !fromCol.Equals(toCol) {
			alteration := computeColumnAlteration(fromCol, toCol)
			cs.Add(&AlterColumnChange{
				TableName:  tableName,
				ColumnName: name,
				OldColumn:  fromCol,
				NewColumn:  toCol,
				Alteration: alteration,
			})
		}
	}
}

func computeColumnAlteration(from, to *Column) ColumnAlteration {
	alt := ColumnAlteration{}

	fromType := from.FullType()
	toType := to.FullType()
	if fromType != toType {
		alt.TypeChanged = true
		alt.OldType = fromType
		alt.NewType = toType
	}

	if from.IsNullable != to.IsNullable {
		alt.NullableChanged = true
		alt.OldNullable = from.IsNullable
		alt.NewNullable = to.IsNullable
	}

	fromDefault := from.DefaultValue
	toDefault := to.DefaultValue
	if (fromDefault == nil) != (toDefault == nil) {
		alt.DefaultChanged = true
		alt.OldDefault = fromDefault
		alt.NewDefault = toDefault
	} else if fromDefault != nil && toDefault != nil && *fromDefault != *toDefault {
		alt.DefaultChanged = true
		alt.OldDefault = fromDefault
		alt.NewDefault = toDefault
	}

	return alt
}

func diffIndexes(from, to *Table, cs *ChangeSet) {
	for name, fromIdx := range from.Indexes {
		if fromIdx.IsPrimary {
			continue
		}
		if _, exists := to.Indexes[name]; !exists {
			cs.Add(&DropIndexChange{Index: fromIdx})
		}
	}

	for name, toIdx := range to.Indexes {
		if toIdx.IsPrimary {
			continue
		}
		fromIdx, exists := from.Indexes[name]
		if !exists {
			cs.Add(&CreateIndexChange{Index: toIdx})
			continue
		}

		if !fromIdx.Equals(toIdx) {
			cs.Add(&DropIndexChange{Index: fromIdx})
			cs.Add(&CreateIndexChange{Index: toIdx})
		}
	}
}

func diffConstraints(from, to *Table, cs *ChangeSet) {
	tableName := to.FullName()

	for name, fromCon := range from.Constraints {
		if _, exists := to.Constraints[name]; !exists {
			cs.Add(&DropConstraintChange{
				TableName:  tableName,
				Constraint: fromCon,
			})
		}
	}

	for name, toCon := range to.Constraints {
		fromCon, exists := from.Constraints[name]
		if !exists {
			cs.Add(&AddConstraintChange{
				TableName:  tableName,
				Constraint: toCon,
			})
			continue
		}

		if !fromCon.Equals(toCon) {
			cs.Add(&DropConstraintChange{
				TableName:  tableName,
				Constraint: fromCon,
			})
			cs.Add(&AddConstraintChange{
				TableName:  tableName,
				Constraint: toCon,
			})
		}
	}
}

func diffFunctions(from, to *Schema, cs *ChangeSet) {
	for sig, fromFn := range from.Functions {
		if _, exists := to.Functions[sig]; !exists {
			cs.Add(&DropFunctionChange{Function: fromFn})
		}
	}

	for sig, toFn := range to.Functions {
		fromFn, exists := from.Functions[sig]
		if !exists {
			cs.Add(&CreateFunctionChange{Function: toFn})
			continue
		}

		if !fromFn.Equals(toFn) {
			cs.Add(&ReplaceFunctionChange{
				OldFunction: fromFn,
				NewFunction: toFn,
			})
		}
	}
}

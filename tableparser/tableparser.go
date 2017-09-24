package tableparser

import (
	"database/sql"
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"time"
)

type Table struct {
	Schema      string
	Name        string
	Fields      []Field
	Indexes     []Index
	Constraints []Constraint
	Triggers    []Trigger
	//
	conn *sql.DB
}

type Index struct {
	NonUnique    bool
	KeyName      string
	SeqInIndex   int
	ColumnName   string
	Collation    string
	Cardinality  int
	SubPart      sql.NullInt64
	Packed       sql.NullString
	Null         string
	IndexType    string
	Comment      string
	IndexComment string
}

type Constraint struct {
	ConstraintName        string
	ColumnName            string
	ReferencedTableSchema string
	ReferencedTableName   string
	ReferencedColumnName  string
}

type Field struct {
	TableCatalog           string
	TableSchema            string
	TableName              string
	ColumnName             string
	OrdinalPosition        int
	ColumnDefault          sql.NullString
	IsNullable             bool
	DataType               string
	CharacterMaximumLength sql.NullInt64
	CharacterOctetLength   sql.NullInt64
	NumericPrecision       sql.NullInt64
	NumericScale           sql.NullInt64
	DatetimePrecision      sql.NullInt64
	CharacterSetName       sql.NullString
	CollationName          sql.NullString
	ColumnType             string
	ColumnKey              string
	Extra                  string
	Privileges             string
	ColumnComment          string
	GenerationExpression   string
	SetEnumVals            []string
	Constraint             *Constraint
}

type Trigger struct {
	Trigger             string
	Event               string
	Table               string
	Statement           string
	Timing              string
	Created             time.Time
	SQLMode             string
	Definer             string
	CharacterSetClient  string
	CollationConnection string
	DatabaseCollation   string
}

func NewTable(db *sql.DB, schema, tableName string) (*Table, error) {
	table := &Table{
		Schema: url.QueryEscape(schema),
		Name:   url.QueryEscape(tableName),
		conn:   db,
	}

	var err error
	table.Indexes, err = getIndexes(db, table.Schema, table.Name)
	if err != nil {
		return nil, err
	}
	table.Constraints, err = getConstraints(db, table.Schema, table.Name)
	if err != nil {
		return nil, err
	}
	table.Triggers, err = getTriggers(db, table.Schema, table.Name)
	if err != nil {
		return nil, err
	}

	err = table.parse()
	if err != nil {
		return nil, err
	}
	table.conn = nil // to save memory since it is not going to be used again
	return table, nil
}

func (t *Table) parse() error {
	//                           +--------------------------- field type
	//                           |          +---------------- field size / enum values:
	//                           |          |                    decimal(10,2) or enum('a','b')
	//                           |          |       +-------- extra info (unsigned, etc)
	//                           |          |       |
	re := regexp.MustCompile("^(.*?)(?:\\((.*?)\\)(.*))?$")
	query := "SELECT `TABLE_CATALOG`," +
		"`TABLE_SCHEMA`," +
		"`TABLE_NAME`," +
		"`COLUMN_NAME`," +
		"`ORDINAL_POSITION`," +
		"`COLUMN_DEFAULT`," +
		"`IS_NULLABLE`," +
		"`DATA_TYPE`," +
		"`CHARACTER_MAXIMUM_LENGTH`," +
		"`CHARACTER_OCTET_LENGTH`," +
		"`NUMERIC_PRECISION`," +
		"`NUMERIC_SCALE`," +
		"`DATETIME_PRECISION`," +
		"`CHARACTER_SET_NAME`," +
		"`COLLATION_NAME`," +
		"`COLUMN_TYPE`," +
		"`COLUMN_KEY`," +
		"`EXTRA`," +
		"`PRIVILEGES`," +
		"`COLUMN_COMMENT`," +
		"`GENERATION_EXPRESSION`" +
		" FROM `information_schema`.`COLUMNS`" +
		fmt.Sprintf(" WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s'", t.Schema, t.Name)

	constraints := constraintsAsMap(t.Constraints)

	rows, err := t.conn.Query(query)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var f Field
		var allowNull string
		err := rows.Scan(&f.TableCatalog,
			&f.TableSchema,
			&f.TableName,
			&f.ColumnName,
			&f.OrdinalPosition,
			&f.ColumnDefault,
			&allowNull,
			&f.DataType,
			&f.CharacterMaximumLength,
			&f.CharacterOctetLength,
			&f.NumericPrecision,
			&f.NumericScale,
			&f.DatetimePrecision,
			&f.CharacterSetName,
			&f.CollationName,
			&f.ColumnType,
			&f.ColumnKey,
			&f.Extra,
			&f.Privileges,
			&f.ColumnComment,
			&f.GenerationExpression,
		)
		if err != nil {
			fmt.Println(err)
			continue
		}

		allowedValues := []string{}
		if f.DataType == "enum" || f.DataType == "set" {
			m := re.FindStringSubmatch(f.ColumnType)
			if len(m) < 2 {
				continue
			}
			vals := strings.Split(m[2], ",")
			for _, val := range vals {
				val = strings.TrimPrefix(val, "'")
				val = strings.TrimSuffix(val, "'")
				allowedValues = append(allowedValues, val)
			}
		}

		f.SetEnumVals = allowedValues
		f.IsNullable = allowNull == "YES"
		f.Constraint = constraints[f.ColumnName]

		t.Fields = append(t.Fields, f)
	}
	if rows.Err() != nil {
		return rows.Err()
	}
	return nil
}

func getIndexes(db *sql.DB, schema, tableName string) ([]Index, error) {
	query := fmt.Sprintf("SHOW INDEXES FROM `%s`.`%s`", schema, tableName)
	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	indexes := []Index{}

	for rows.Next() {
		var i Index
		var table string
		err := rows.Scan(&table, &i.NonUnique, &i.KeyName, &i.SeqInIndex,
			&i.ColumnName, &i.Collation, &i.Cardinality, &i.SubPart,
			&i.Packed, &i.Null, &i.IndexType, &i.Comment, &i.IndexComment)
		if err != nil {
			return nil, fmt.Errorf("cannot read constraints: %s", err)
		}
		indexes = append(indexes, i)
	}

	return indexes, nil
}

func getConstraints(db *sql.DB, schema, tableName string) ([]Constraint, error) {
	query := "SELECT tc.CONSTRAINT_NAME, " +
		"kcu.COLUMN_NAME, " +
		"kcu.REFERENCED_TABLE_SCHEMA, " +
		"kcu.REFERENCED_TABLE_NAME, " +
		"kcu.REFERENCED_COLUMN_NAME " +
		"FROM information_schema.TABLE_CONSTRAINTS tc " +
		"LEFT JOIN information_schema.KEY_COLUMN_USAGE kcu " +
		"ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME " +
		"WHERE tc.CONSTRAINT_TYPE = 'FOREIGN KEY' " +
		fmt.Sprintf("AND tc.TABLE_SCHEMA = '%s' ", schema) +
		fmt.Sprintf("AND tc.TABLE_NAME = '%s'", tableName)
	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	constraints := []Constraint{}

	for rows.Next() {
		var c Constraint
		err := rows.Scan(&c.ConstraintName, &c.ColumnName, &c.ReferencedTableSchema,
			&c.ReferencedTableName, &c.ReferencedColumnName)
		if err != nil {
			return nil, fmt.Errorf("cannot read constraints: %s", err)
		}
		constraints = append(constraints, c)
	}

	return constraints, nil
}

func getTriggers(db *sql.DB, schema, tableName string) ([]Trigger, error) {
	query := fmt.Sprintf("SHOW TRIGGERS FROM `%s` LIKE '%s'", schema, tableName)
	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	triggers := []Trigger{}

	for rows.Next() {
		var t Trigger
		err := rows.Scan(&t.Trigger, &t.Event, &t.Table, &t.Statement, &t.Timing,
			&t.Created, &t.SQLMode, &t.Definer, &t.CharacterSetClient, &t.CollationConnection,
			&t.DatabaseCollation)
		if err != nil {
			return nil, fmt.Errorf("cannot read constraints: %s", err)
		}
		triggers = append(triggers, t)
	}

	return triggers, nil
}

func constraintsAsMap(constraints []Constraint) map[string]*Constraint {
	m := make(map[string]*Constraint)
	for _, c := range constraints {
		m[c.ColumnName] = &Constraint{
			ConstraintName:        c.ConstraintName,
			ColumnName:            c.ColumnName,
			ReferencedTableSchema: c.ReferencedTableSchema,
			ReferencedTableName:   c.ReferencedTableName,
			ReferencedColumnName:  c.ReferencedColumnName,
		}
	}
	return m
}

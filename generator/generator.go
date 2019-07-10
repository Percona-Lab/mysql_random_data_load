package generator

import (
	"database/sql"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/Percona-Lab/mysql_random_data_load/internal/getters"
	"github.com/Percona-Lab/mysql_random_data_load/tableparser"
	"github.com/gosuri/uiprogress"
	log "github.com/sirupsen/logrus"
)

type Getter interface {
	Value() interface{}
	Quote() string
	String() string
}
type InsertValues []Getter
type insertFunction func(*sql.DB, string, chan int, chan bool, *sync.WaitGroup)

var (
	maxValues = map[string]int64{
		"tinyint":   0XF,
		"smallint":  0xFF,
		"mediumint": 0x7FFFF,
		"int":       0x7FFFFFFF,
		"integer":   0x7FFFFFFF,
		"float":     0x7FFFFFFF,
		"decimal":   0x7FFFFFFF,
		"double":    0x7FFFFFFF,
		"bigint":    0x7FFFFFFFFFFFFFFF,
	}
)

func Run(db *sql.DB, table *tableparser.Table, bar *uiprogress.Bar, sem chan bool,
	rowValues InsertValues, count, bulkSize int, insertFunc insertFunction, newLineOnEachRow bool) (int, error) {
	if count == 0 {
		return 0, nil
	}
	var wg sync.WaitGroup
	insertQuery := GenerateInsertStmt(table)
	rowsChan := make(chan []Getter, 1000)
	okRowsChan := countRowsOK(count, bar)

	go GenerateInsertData(count*bulkSize, rowValues, rowsChan)
	defaultSeparator1 := ""
	if newLineOnEachRow {
		defaultSeparator1 = "\n"
	}

	i := 0
	rowsCount := 0
	sep1, sep2 := defaultSeparator1, ""

	for i < count {
		rowData := <-rowsChan
		rowsCount++
		insertQuery += sep1 + " ("
		for _, field := range rowData {
			insertQuery += sep2 + field.Quote()
			sep2 = ", "
		}
		insertQuery += ")"
		sep1 = ", "
		if newLineOnEachRow {
			sep1 += "\n"
		}
		sep2 = ""
		if rowsCount < bulkSize {
			continue
		}

		insertQuery += ";\n"
		<-sem
		wg.Add(1)
		go insertFunc(db, insertQuery, okRowsChan, sem, &wg)

		insertQuery = GenerateInsertStmt(table)
		sep1, sep2 = defaultSeparator1, ""
		rowsCount = 0
		i++
	}

	wg.Wait()
	okCount := <-okRowsChan
	return okCount, nil
}

func RunInsert(db *sql.DB, insertQuery string, resultsChan chan int, sem chan bool, wg *sync.WaitGroup) {
	result, err := db.Exec(insertQuery)
	if err != nil {
		log.Debugf("Cannot run insert: %s", err)
		resultsChan <- 0
		sem <- true
		wg.Done()
		return
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		log.Errorf("Cannot get rows affected after insert: %s", err)
	}
	resultsChan <- int(rowsAffected)
	sem <- true
	wg.Done()
}

// GenerateInsertData will generate 'rows' items, where each item in the channel has 'bulkSize' rows.
// For example:
// We need to load 6 rows using a bulk insert having 2 rows per insert, like this:
// INSERT INTO table (f1, f2, f3) VALUES (?, ?, ?), (?, ?, ?)
//
// This function will put into rowsChan 3 elements, each one having the values for 2 rows:
// rowsChan <- [ v1-1, v1-2, v1-3, v2-1, v2-2, v2-3 ]
// rowsChan <- [ v3-1, v3-2, v3-3, v4-1, v4-2, v4-3 ]
// rowsChan <- [ v1-5, v5-2, v5-3, v6-1, v6-2, v6-3 ]
//
func GenerateInsertData(count int, values InsertValues, rowsChan chan []Getter) {
	for i := 0; i < count; i++ {
		insertRow := make([]Getter, 0, len(values))
		for _, val := range values {
			insertRow = append(insertRow, val)
		}
		rowsChan <- insertRow
	}
}

func GenerateInsertStmt(table *tableparser.Table) string {
	fields := getFieldNames(table.Fields)
	query := fmt.Sprintf("INSERT IGNORE INTO %s.%s (%s) VALUES ",
		backticks(table.Schema),
		backticks(table.Name),
		strings.Join(fields, ","),
	)
	return query
}

func getFieldNames(fields []tableparser.Field) []string {
	var fieldNames []string
	for _, field := range fields {
		if !isSupportedType(field.DataType) {
			continue
		}
		if !field.IsNullable && field.ColumnKey == "PRI" &&
			strings.Contains(field.Extra, "auto_increment") {
			continue
		}
		fieldNames = append(fieldNames, backticks(field.ColumnName))
	}
	return fieldNames
}

func backticks(val string) string {
	if strings.HasPrefix(val, "`") && strings.HasSuffix(val, "`") {
		return url.QueryEscape(val)
	}
	return "`" + url.QueryEscape(val) + "`"
}

func isSupportedType(fieldType string) bool {
	supportedTypes := map[string]bool{
		"tinyint":    true,
		"smallint":   true,
		"mediumint":  true,
		"int":        true,
		"integer":    true,
		"bigint":     true,
		"float":      true,
		"decimal":    true,
		"double":     true,
		"char":       true,
		"varchar":    true,
		"date":       true,
		"datetime":   true,
		"timestamp":  true,
		"time":       true,
		"year":       true,
		"tinyblob":   true,
		"tinytext":   true,
		"blob":       true,
		"text":       true,
		"mediumblob": true,
		"mediumtext": true,
		"longblob":   true,
		"longtext":   true,
		"binary":     true,
		"varbinary":  true,
		"enum":       true,
		"set":        true,
	}
	_, ok := supportedTypes[fieldType]
	return ok
}

// This go-routine keeps track of how many rows were actually inserted
// by the bulk inserts since one or more rows could generate duplicated
// keys so, not allways the number of inserted rows = number of rows in
// the bulk insert
func countRowsOK(count int, bar *uiprogress.Bar) chan int {
	var totalOk int
	resultsChan := make(chan int, 10000)
	go func() {
		for i := 0; i < count; i++ {
			okCount := <-resultsChan
			for j := 0; j < okCount; j++ {
				bar.Incr()
			}
			totalOk += okCount
		}
		resultsChan <- totalOk
	}()
	return resultsChan
}

// MakeValueFuncs returns an array of functions to generate all the values needed for a single row
func MakeValueFuncs(conn *sql.DB, fields []tableparser.Field) InsertValues {
	var values []Getter
	for _, field := range fields {
		if !field.IsNullable && field.ColumnKey == "PRI" && strings.Contains(field.Extra, "auto_increment") {
			continue
		}
		if field.Constraint != nil {
			samples, err := getSamples(conn, field.Constraint.ReferencedTableSchema,
				field.Constraint.ReferencedTableName,
				field.Constraint.ReferencedColumnName,
				100, field.DataType)
			if err != nil {
				log.Printf("cannot get samples for field %q: %s\n", field.ColumnName, err)
				continue
			}
			values = append(values, getters.NewRandomSample(field.ColumnName, samples, field.IsNullable))
			continue
		}
		maxValue := maxValues["bigint"]
		if m, ok := maxValues[field.DataType]; ok {
			maxValue = m
		}
		switch field.DataType {
		case "tinyint", "smallint", "mediumint", "int", "integer", "bigint":
			values = append(values, getters.NewRandomInt(field.ColumnName, maxValue, field.IsNullable))
		case "float", "decimal", "double":
			values = append(values, getters.NewRandomDecimal(field.ColumnName,
				field.NumericPrecision.Int64-field.NumericScale.Int64, field.IsNullable))
		case "char", "varchar":
			values = append(values, getters.NewRandomString(field.ColumnName,
				field.CharacterMaximumLength.Int64, field.IsNullable))
		case "date":
			values = append(values, getters.NewRandomDate(field.ColumnName, field.IsNullable))
		case "datetime", "timestamp":
			values = append(values, getters.NewRandomDateTime(field.ColumnName, field.IsNullable))
		case "tinyblob", "tinytext", "blob", "text", "mediumtext", "mediumblob", "longblob", "longtext":
			values = append(values, getters.NewRandomString(field.ColumnName,
				field.CharacterMaximumLength.Int64, field.IsNullable))
		case "time":
			values = append(values, getters.NewRandomTime(field.IsNullable))
		case "year":
			values = append(values, getters.NewRandomIntRange(field.ColumnName, int64(time.Now().Year()-1),
				int64(time.Now().Year()), field.IsNullable))
		case "enum", "set":
			values = append(values, getters.NewRandomEnum(field.SetEnumVals, field.IsNullable))
		case "binary", "varbinary":
			values = append(values, getters.NewRandomString(field.ColumnName, field.CharacterMaximumLength.Int64, field.IsNullable))
		default:
			log.Printf("cannot get field type: %s: %s\n", field.ColumnName, field.DataType)
		}
	}

	return values
}

func getSamples(conn *sql.DB, schema, table, field string, samples int64, dataType string) ([]interface{}, error) {
	var count int64
	var query string

	queryCount := fmt.Sprintf("SELECT COUNT(*) FROM `%s`.`%s`", schema, table)
	if err := conn.QueryRow(queryCount).Scan(&count); err != nil {
		return nil, fmt.Errorf("cannot get count for table %q: %s", table, err)
	}

	if count < samples {
		query = fmt.Sprintf("SELECT `%s` FROM `%s`.`%s`", field, schema, table)
	} else {
		query = fmt.Sprintf("SELECT `%s` FROM `%s`.`%s` WHERE RAND() <= .3 LIMIT %d",
			field, schema, table, samples)
	}

	rows, err := conn.Query(query)
	if err != nil {
		return nil, fmt.Errorf("cannot get samples: %s, %s", query, err)
	}
	defer rows.Close()

	values := []interface{}{}

	for rows.Next() {
		var err error
		var val interface{}

		switch dataType {
		case "tinyint", "smallint", "mediumint", "int", "integer", "bigint", "year":
			var v int64
			err = rows.Scan(&v)
			val = v
		case "char", "varchar", "blob", "text", "mediumtext",
			"mediumblob", "longblob", "longtext":
			var v string
			err = rows.Scan(&v)
			val = v
		case "binary", "varbinary":
			var v []rune
			err = rows.Scan(&v)
			val = v
		case "float", "decimal", "double":
			var v float64
			err = rows.Scan(&v)
			val = v
		case "date", "time", "datetime", "timestamp":
			var v time.Time
			err = rows.Scan(&v)
			val = v
		}
		if err != nil {
			return nil, fmt.Errorf("cannot scan sample: %s", err)
		}
		values = append(values, val)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("cannot get samples: %s", err)
	}
	return values, nil
}

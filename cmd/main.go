package main

import (
	"database/sql"
	"fmt"
	"log"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Percona-Lab/random_data_load/internal/getters"
	"github.com/Percona-Lab/random_data_load/tableparser"
	"github.com/gosuri/uiprogress"
	"github.com/kr/pretty"

	. "github.com/go-sql-driver/mysql"
	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

var (
	app        = kingpin.New("chat", "A command-line chat application.")
	host       = app.Flag("host", "Host name/IP").Short('h').Default("127.0.0.1").String()
	port       = app.Flag("port", "Port").Short('P').Default("3306").Int()
	user       = app.Flag("user", "User").Short('u').String()
	pass       = app.Flag("password", "Password").Short('p').String()
	maxThreads = app.Flag("max-threads", "Maximum number of threads to run inserts").Default("1").Int()
	debug      = app.Flag("debug", "Log debugging information").Bool()
	bulkSize   = app.Flag("bulk-size", "Number of rows per insert statement").Default("1000").Int()
	schema     = app.Arg("database", "Database").Required().String()
	tableName  = app.Arg("table", "Table").Required().String()
	rows       = app.Arg("rows", "Number of rows to insert").Required().Int()
	maxRetries = app.Arg("max-retries", "Number of rows to insert").Default("10000").Int64()
	progress   = app.Arg("show-progressbar", "Show progress bar").Default("true").Bool()

	validFunctions = []string{"int", "string", "date", "date_in_range"}
	maxValues      = map[string]int64{
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

type insertValues []getters.Getter

func (g insertValues) String() string {
	vals := []string{}
	for _, val := range g {
		vals = append(vals, val.String())
	}
	return strings.Join(vals, ", ")
}

func main() {
	kingpin.MustParse(app.Parse(os.Args[1:]))

	address := *host
	net := "unix"
	if address != "localhost" {
		net = "tcp"
	}
	if *port != 0 {
		address = fmt.Sprintf("%s:%d", address, *port)
	}

	dsn := Config{
		User:      *user,
		Passwd:    *pass,
		Addr:      address,
		Net:       net,
		DBName:    "",
		ParseTime: true,
	}

	db, err := sql.Open("mysql", dsn.FormatDSN())
	if err != nil {
		panic(err)
	}
	defer db.Close()

	// SET TimeZone to UTC to avoid errors due to random dates & daylight saving valid values
	if _, err = db.Exec(`SET @@session.time_zone = "+00:00"`); err != nil {
		log.Printf("Cannot set time zone to UTC: %s\n", err)
		os.Exit(1)
	}

	table, err := tableparser.NewTable(db, *schema, *tableName)
	if err != nil {
		log.Printf("cannot get table %s struct: %s", *tableName, err)
		os.Exit(1)
	}
	if *debug {
		pretty.Println(table)
	}
	if len(table.Triggers) > 0 {
		log.Printf("There are triggers on the %s table that might affect this process:", *tableName)
		for _, t := range table.Triggers {
			log.Printf("Trigger %q, %s %s", t.Trigger, t.Timing, t.Event)
			log.Printf("Statement: %s", t.Statement)
		}
	}

	var wg sync.WaitGroup
	var okRowsCount int64
	values := makeValueFuncs(db, table.Fields)

	resultsChan := make(chan int)
	rowsChan := makeRowsChan(*rows, values)

	if *maxThreads < 1 {
		*maxThreads = 1
	}

	log.Println("Starting")

	bar := uiprogress.AddBar(*rows).AppendCompleted().PrependElapsed()
	if *progress {
		uiprogress.Start()
	}

	// This go-routine keeps track of how many rows were actually inserted
	// by the bulk inserts since one or more rows could generate duplicated
	// keys so, not allways the number of inserted rows = number of rows in
	// the bulk insert

	go func() {
		for okCount := range resultsChan {
			for i := 0; i < okCount; i++ {
				bar.Incr()
			}
			atomic.AddInt64(&okRowsCount, int64(okCount))
		}
		wg.Done()
	}()

	for i := 0; i < *maxThreads; i++ {
		wg.Add(1)
		go runInsert(db, table, *bulkSize, rowsChan, resultsChan, &wg)
	}
	wg.Wait()

	wg.Add(1)
	close(resultsChan)
	wg.Wait()

	if okRowsCount != int64(*rows) {
		log.Printf("Adding extra %d rows.", int64(*rows)-okRowsCount)
		okCount, errors := loadExtraRows(db, table, int64(*rows)-okRowsCount, values, *maxRetries, bar)
		okRowsCount += okCount
		// If there are still errors
		if okRowsCount != int64(*rows) && len(errors) > 0 {
			for _, err := range errors {
				fmt.Println(err)
			}
		}
	}

	time.Sleep(500 * time.Millisecond) // Let the progress bar to update
	fmt.Printf("Total rows inserted: %d\n", okRowsCount)
}

func loadExtraRows(db *sql.DB, table *tableparser.Table, rows int64, values insertValues,
	maxRetryCount int64, bar *uiprogress.Bar) (int64, []error) {
	var okCount int64
	var retryCount int
	var errors []error
	for okCount < rows && retryCount < 1000 {
		retryCount++
		if err := runOneInsert(db, table, values); err != nil {
			errors = append(errors, err)
			continue
		}
		okCount++
		bar.Incr()
	}
	return okCount, errors
}

func makeRowsChan(rows int, values insertValues) chan insertValues {
	preloadCount := 10000
	if rows < preloadCount {
		preloadCount = rows
	}

	rowsChan := make(chan insertValues, preloadCount)
	go func() {
		for i := 0; i < rows; i++ {
			rowsChan <- values
		}
		close(rowsChan)
	}()
	return rowsChan
}

func runInsert(db *sql.DB, table *tableparser.Table, bulkSize int, valsChan chan insertValues,
	resultsChan chan int, wg *sync.WaitGroup) {
	defer wg.Done()

	fields := getFieldNames(table.Fields)
	baseSQL := fmt.Sprintf("INSERT IGNORE INTO %s.%s (%s) VALUES ",
		backticks(table.Schema),
		backticks(table.Name),
		strings.Join(fields, ","),
	)
	separator := ""
	sql := baseSQL
	var count int

	for vals := range valsChan {
		sql += separator + "(" + vals.String() + ")\n"
		separator = ", "

		count++
		if count < bulkSize {
			continue
		}
		result, err := db.Exec(sql)

		separator = ""
		count = 0
		sql = baseSQL

		if err != nil {
			resultsChan <- int(0)
			continue
		}
		rowsAffected, _ := result.RowsAffected()
		resultsChan <- int(rowsAffected)
	}

	if count > 0 {
		result, err := db.Exec(sql)
		if err != nil {
			log.Printf("cannot run insert: %s", err)
			resultsChan <- int(0)
			return
		}
		rowsAffected, _ := result.RowsAffected()
		resultsChan <- int(rowsAffected)
	}
}

func runOneInsert(db *sql.DB, table *tableparser.Table, vals insertValues) error {
	fields := getFieldNames(table.Fields)
	query := fmt.Sprintf("INSERT INTO %s.%s (%s) VALUES (%s)",
		backticks(table.Schema),
		backticks(table.Name),
		strings.Join(fields, ","),
		vals.String(),
	)
	if _, err := db.Exec(query); err != nil {
		return err
	}
	return nil
}

func makeValueFuncs(conn *sql.DB, fields []tableparser.Field) insertValues {
	var values []getters.Getter
	for _, field := range fields {
		if !field.IsNullable && field.ColumnKey == "PRI" &&
			strings.Contains(field.Extra, "auto_increment") {
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
		case "char", "varchar", "varbinary":
			values = append(values, getters.NewRandomString(field.ColumnName,
				field.CharacterMaximumLength.Int64, field.IsNullable))
		case "date":
			values = append(values, getters.NewRandomDate(field.ColumnName, field.IsNullable))
		case "datetime", "timestamp":
			values = append(values, getters.NewRandomDateTime(field.ColumnName, field.IsNullable))
		case "blob", "text", "mediumtext", "mediumblob", "longblob", "longtext":
			values = append(values, getters.NewRandomString(field.ColumnName,
				field.CharacterMaximumLength.Int64, field.IsNullable))
		case "time":
			values = append(values, getters.NewRandomTime(field.IsNullable))
		case "year":
			values = append(values, getters.NewRandomIntRange(field.ColumnName, int64(time.Now().Year()-1),
				int64(time.Now().Year()), field.IsNullable))
		case "enum", "set":
			values = append(values, getters.NewRandomEnum(field.SetEnumVals, field.IsNullable))
		default:
			log.Printf("cannot get field type: %s: %s\n", field.ColumnName, field.DataType)
		}
	}

	return values
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
		case "char", "varchar", "varbinary", "blob", "text", "mediumtext",
			"mediumblob", "longblob", "longtext":
			var v string
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
		"blob":       true,
		"text":       true,
		"mediumblob": true,
		"mediumtext": true,
		"longblob":   true,
		"longtext":   true,
		"varbinary":  true,
		"enum":       true,
		"set":        true,
	}
	_, ok := supportedTypes[fieldType]
	return ok
}

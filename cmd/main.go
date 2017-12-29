package main

import (
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/Percona-Lab/mysql_random_data_load/internal/getters"
	"github.com/Percona-Lab/mysql_random_data_load/tableparser"
	"github.com/go-sql-driver/mysql"
	"github.com/gosuri/uiprogress"
	"github.com/kr/pretty"
	"github.com/pkg/errors"

	log "github.com/sirupsen/logrus"
	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

var (
	app        = kingpin.New("mysql_random_data_loader", "MySQL Random Data Loader")
	host       = app.Flag("host", "Host name/IP").Short('h').Default("127.0.0.1").String()
	port       = app.Flag("port", "Port").Short('P').Default("3306").Int()
	user       = app.Flag("user", "User").Short('u').String()
	pass       = app.Flag("password", "Password").Short('p').String()
	maxThreads = app.Flag("max-threads", "Maximum number of threads to run inserts").Int()
	debug      = app.Flag("debug", "Log debugging information").Bool()
	bulkSize   = app.Flag("bulk-size", "Number of rows per insert statement").Default("1000").Int()
	noProgress = app.Flag("no-progressbar", "Show progress bar").Default("false").Bool()
	qps        = app.Flag("qps", "Queries per second. 0 = unlimited").Default("0").Int()
	maxRetries = app.Flag("max-retries", "Number of rows to insert").Default("100").Int()

	schema    = app.Arg("database", "Database").Required().String()
	tableName = app.Arg("table", "Table").Required().String()
	rows      = app.Arg("rows", "Number of rows to insert").Required().Int()

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

	dsn := mysql.Config{
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
	db.SetMaxOpenConns(100)

	// SET TimeZone to UTC to avoid errors due to random dates & daylight saving valid values
	if _, err = db.Exec(`SET @@session.time_zone = "+00:00"`); err != nil {
		log.Printf("Cannot set time zone to UTC: %s\n", err)
		db.Close()
		os.Exit(1)
	}

	table, err := tableparser.NewTable(db, *schema, *tableName)
	if err != nil {
		log.Printf("cannot get table %s struct: %s", *tableName, err)
		db.Close()
		os.Exit(1)
	}

	log.SetFormatter(&log.TextFormatter{FullTimestamp: true})
	if *debug {
		log.SetLevel(log.DebugLevel)
	}
	log.Debug(pretty.Sprint(table))

	if len(table.Triggers) > 0 {
		log.Warnf("There are triggers on the %s table that might affect this process:", *tableName)
		for _, t := range table.Triggers {
			log.Warnf("Trigger %q, %s %s", t.Trigger, t.Timing, t.Event)
			log.Warnf("Statement: %s", t.Statement)
		}
	}

	if *bulkSize > *rows {
		*bulkSize = *rows
	}

	if maxThreads == nil {
		*maxThreads = runtime.NumCPU() * 10
	}

	if *maxThreads < 1 {
		*maxThreads = 1
	}

	log.Info("Starting")

	bar := uiprogress.AddBar(*rows).AppendCompleted().PrependElapsed()
	if !*noProgress {
		uiprogress.Start()
	}

	// Example: want 11 rows with bulksize 4:
	// count = int(11 / 4) = 2 -> 2 bulk inserts having 4 rows each = 8 rows
	// We need to run this insert twice:
	// INSERT INTO table (f1, f2) VALUES (?, ?), (?, ?), (?, ?), (?, ?)
	// remainder = rows - count = 11 - 8 = 3
	// And then, we need to run this insert once to complete 11 rows
	// INSERT INTO table (f1, f2) VALUES (?, ?), (?, ?), (?, ?)
	count := *rows / *bulkSize
	remainder := *rows - count**bulkSize
	semaphores := makeSemaphores(*maxThreads)
	rowValues := makeValueFuncs(db, table.Fields)
	log.Debugf("Must run %d bulk inserts having %d rows each", count, *bulkSize)
	okCount, err := run(db, table, bar, semaphores, rowValues, count, *bulkSize, *qps)
	var okrCount, okiCount int // remainder & individual inserts OK count
	if remainder > 0 {
		log.Debugf("Must run 1 extra bulk insert having %d rows, to complete %d rows", remainder, *rows)
		okrCount, err = run(db, table, bar, semaphores, rowValues, 1, remainder, *qps)
	}

	// If there were errors and at this point we have less rows than *rows,
	// retry adding individual rows (no bulk inserts)
	totalOkCount := okCount + okrCount
	retries := 0
	if totalOkCount < *rows {
		log.Debugf("Running extra %d individual inserts (duplicated keys?)", *rows-totalOkCount)
	}
	for totalOkCount < *rows && retries < *maxRetries {
		okiCount, _ = run(db, table, bar, semaphores, rowValues, *rows-totalOkCount, 1, *qps)
		retries++
		totalOkCount += okiCount
	}

	time.Sleep(500 * time.Millisecond) // Let the progress bar to update
	log.Printf("%d rows inserted", totalOkCount)
	db.Close()
}

func run(db *sql.DB, table *tableparser.Table, bar *uiprogress.Bar, sem chan bool, rowValues insertValues, count, size int, qps int) (int, error) {
	if count == 0 {
		return 0, nil
	}
	var wg sync.WaitGroup

	bulkStmt, err := db.Prepare(generateInsertStmt(table, size))
	if err != nil {
		return 0, errors.Wrap(err, "Cannot prepare bulk insert")
	}

	rowsChan := make(chan []interface{}, 1000)

	okRowsChan := countRowsOK(count, bar)

	go generateInsertData(count, size, rowValues, rowsChan)

	var ticker <-chan time.Time
	if qps > 0 {
		delay := time.Second / time.Duration(qps)
		ticker = time.NewTicker(delay).C
	}
	for i := 0; i < count; i++ {
		rowData := <-rowsChan
		<-sem
		if ticker != nil {
			<-ticker
			log.Debugf("QPS in effect. Inserting ...")
		}
		wg.Add(1)
		go runInsert(bulkStmt, rowData, okRowsChan, sem, &wg)
	}

	wg.Wait()
	okCount := <-okRowsChan
	return okCount, nil
}

func makeSemaphores(count int) chan bool {
	sem := make(chan bool, count)
	for i := 0; i < count; i++ {
		sem <- true
	}
	return sem
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

// generateInsertData will generate 'rows' items, where each item in the channel has 'bulkSize' rows.
// For example:
// We need to load 6 rows using a bulk insert having 2 rows per insert, like this:
// INSERT INTO table (f1, f2, f3) VALUES (?, ?, ?), (?, ?, ?)
//
// This function will put into rowsChan 3 elements, each one having the values for 2 rows:
// rowsChan <- [ v1-1, v1-2, v1-3, v2-1, v2-2, v2-3 ]
// rowsChan <- [ v3-1, v3-2, v3-3, v4-1, v4-2, v4-3 ]
// rowsChan <- [ v1-5, v5-2, v5-3, v6-1, v6-2, v6-3 ]
//
func generateInsertData(count, size int, values insertValues, rowsChan chan []interface{}) {
	//runtime.LockOSThread()
	for i := 0; i < count; i++ {
		insertRow := make([]interface{}, 0, len(values))
		for j := 0; j < size; j++ {
			for _, val := range values {
				insertRow = append(insertRow, val.Value())
			}
		}
		rowsChan <- insertRow
	}
}

func generateInsertStmt(table *tableparser.Table, size int) string {
	fields := getFieldNames(table.Fields)
	query := fmt.Sprintf("INSERT IGNORE INTO %s.%s (%s) VALUES ",
		backticks(table.Schema),
		backticks(table.Name),
		strings.Join(fields, ","),
	)

	// Build the placeholders group for each row, including parenthesis: (?, ?, ...)
	placeholders := "("
	sep := ""
	for i := 0; i < len(fields); i++ {
		placeholders += sep + "?"
		sep = ", "
	}
	placeholders += ")"

	// Join 'bulkSize' placeholders groups to the query
	// INSERT INTO db.table (f1, f2, ...) VALUES (?, ?, ...), (?, ?, ...), ....
	sep = ""
	for i := 0; i < size; i++ {
		query += sep + placeholders
		sep = ", "
	}

	return query
}

func runInsert(stmt *sql.Stmt, data []interface{}, resultsChan chan int, sem chan bool, wg *sync.WaitGroup) {
	result, err := stmt.Exec(data...)
	if err != nil {
		log.Debugf("Cannot run insert: %s", err)
		resultsChan <- 0
		wg.Done()
		return
	}

	rowsAffected, _ := result.RowsAffected()
	resultsChan <- int(rowsAffected)
	sem <- true
	wg.Done()
}

// makeValueFuncs returns an array of functions to generate all the values needed for a single row
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

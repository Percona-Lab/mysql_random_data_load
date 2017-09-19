package main

import (
	"database/sql"
	"fmt"
	"log"
	"net/url"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Percona-Lab/random_data_load/internal/getters"
	"github.com/Percona-Lab/random_data_load/tableparser"
	"github.com/kr/pretty"

	"github.com/gosuri/uiprogress"

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
	dbName     = app.Arg("database", "Database").Required().String()
	tableName  = app.Arg("table", "Table").Required().String()
	rows       = app.Arg("rows", "Number of rows to insert").Required().Int()

	validFunctions = []string{"int", "string", "date", "date_in_range"}
	masks          = map[string]int64{
		"tinyint":   0XF,
		"smallint":  0xFF,
		"mediumint": 0x7FFFF,
		"int":       0x7FFFFFFF,
		"integer":   0x7FFFFFFF,
		"bigint":    0x7FFFFFFFFFFFFFFF,
	}
)

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
		User:            *user,
		Passwd:          *pass,
		Addr:            address,
		Net:             net,
		DBName:          *dbName,
		ParseTime:       true,
		ClientFoundRows: true,
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

	table, err := tableparser.Parse(db, *tableName)
	if err != nil {
		log.Printf("cannot get table %s struct: %s", *tableName, err)
		os.Exit(1)
	}
	if *debug {
		pretty.Println(table)
	}

	sql := makeInsert(*dbName, *tableName, 1, table.Fields)
	if *debug {
		fmt.Println(sql)
	}

	var wg sync.WaitGroup
	var okRowsCount int64
	values := makeValueFuncs(table.Fields)
	resultsChan := make(chan int)

	rowsChan := makeRowsChan(*rows, values)

	if *maxThreads < 1 {
		*maxThreads = 1
	}

	log.Println("Starting")

	bar := uiprogress.AddBar(*rows).AppendCompleted()
	uiprogress.Start()

	// This go-routine keeps track of how many rows were actually inserted
	// by the bulk inserts since one or more rows could generate duplicated
	// keys so, not allways the number of inserted rows = number of rows in
	// the bulk insert
	go func() {
		for okCount := range resultsChan {
			bar.Set(bar.Current() + okCount)
			atomic.AddInt64(&okRowsCount, int64(okCount))
		}
	}()

	for i := 0; i < *maxThreads; i++ {
		wg.Add(1)
		go runInsert(db, table, *bulkSize, rowsChan, resultsChan, &wg)
	}
	wg.Wait()

	// Let the counter go-rutine to run for the last time
	runtime.Gosched()
	close(resultsChan)

	if okRowsCount != int64(*rows) {
		loadExtraRows(db, table, int64(*rows)-okRowsCount, values)
		bar.Set(*rows)
	}
}

func loadExtraRows(db *sql.DB, table *tableparser.Table, rows int64, values []getters.Getter) {
	var okCount int64
	for okCount < rows {
		vals := make([]interface{}, len(values))
		for j, val := range values {
			vals[j] = val.Value()
		}

		if err := runOneInsert(db, table, vals); err != nil {
			continue
		}
		okCount++
	}
}

func makeRowsChan(rows int, values []getters.Getter) chan []interface{} {
	preloadCount := 10000
	if rows < preloadCount {
		preloadCount = rows
	}

	rowsChan := make(chan []interface{}, preloadCount)
	go func() {
		for i := 0; i < rows; i++ {
			vals := make([]interface{}, len(values))
			for j, val := range values {
				vals[j] = val.Value()
			}
			rowsChan <- vals
		}
		close(rowsChan)
	}()
	return rowsChan
}

func runInsert(db *sql.DB, table *tableparser.Table, bulkSize int, valsChan chan []interface{},
	resultsChan chan int, wg *sync.WaitGroup) {
	//
	fields, placeholders := getFieldsAndPlaceholders(table.Fields)
	baseSQL := fmt.Sprintf("INSERT IGNORE INTO %s (%s) VALUES ",
		backticks(table.Name),
		strings.Join(fields, ","),
	)
	separator := ""
	sql := baseSQL
	bulkVals := []interface{}{}
	var count int

	for vals := range valsChan {
		sql += separator + "(" + strings.Join(placeholders, ",") + ")"
		separator = ", "
		bulkVals = append(bulkVals, vals...)
		count++
		if count < bulkSize {
			continue
		}
		result, _ := db.Exec(sql, bulkVals...)
		rowsAffected, _ := result.RowsAffected()
		resultsChan <- int(rowsAffected)
		separator = ""
		sql = baseSQL
		bulkVals = []interface{}{}
		count = 0
	}
	if count > 0 && len(bulkVals) > 0 {
		result, _ := db.Exec(sql, bulkVals...)
		rowsAffected, _ := result.RowsAffected()
		resultsChan <- int(rowsAffected)
	}
	wg.Done()
}

func runOneInsert(db *sql.DB, table *tableparser.Table, vals []interface{}) error {
	fields, placeholders := getFieldsAndPlaceholders(table.Fields)
	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		backticks(table.Name),
		strings.Join(fields, ","),
		strings.Join(placeholders, ","),
	)
	if _, err := db.Exec(query, vals...); err != nil {
		return err
	}
	return nil
}

func makeValueFuncs(fields []tableparser.Field) []getters.Getter {
	var values []getters.Getter
	for _, field := range fields {
		if !field.AllowsNull && field.Key == "PRI" &&
			strings.Contains(field.Extra, "auto_increment") {
			continue
		}
		mask := masks["bigint"]
		if m, ok := masks[field.Type]; ok {
			mask = m
		}
		switch field.Type {
		case "tinyint", "smallint", "mediumint", "int", "integer", "bigint":
			values = append(values, getters.NewRandomInt(mask, field.AllowsNull))
		case "float", "decimal", "double":
			values = append(values, getters.NewRandomDecimal(field.Size, field.AllowsNull))
		case "char", "varchar", "varbinary":
			values = append(values, getters.NewRandomString(field.Size, field.AllowsNull))
		case "date":
			values = append(values, getters.NewRandomDate(field.AllowsNull))
		case "datetime", "timestamp":
			values = append(values, getters.NewRandomDateTime(field.AllowsNull))
		case "blob", "text", "mediumtext", "mediumblob", "longblob", "longtext":
			values = append(values, getters.NewRandomString(field.Size, field.AllowsNull))
		case "time":
			values = append(values, getters.NewRandomTime(field.AllowsNull))
		case "year":
			values = append(values, getters.NewRandomIntRange(int64(time.Now().Year()-1),
				int64(time.Now().Year()), field.AllowsNull))
		case "enum", "set":
			values = append(values, getters.NewRandomEnum(field.AllowedValues, field.AllowsNull))
		default:
			log.Printf("cannot get field type: %s: %s\n", field.Name, field.Type)
		}
	}

	return values
}

func getFieldsAndPlaceholders(fields []tableparser.Field) ([]string, []string) {
	var fieldNames, placeHolders []string
	for _, field := range fields {
		if !isSupportedType(field.Type) {
			continue
		}
		fieldNames = append(fieldNames, backticks(field.Name))
		if !field.AllowsNull && field.Key == "PRI" &&
			strings.Contains(field.Extra, "auto_increment") {
			placeHolders = append(placeHolders, "NULL")
		} else {
			placeHolders = append(placeHolders, "?")
		}
	}
	return fieldNames, placeHolders
}

func makeInsert(dbName, tableName string, bulkCount int, fields []tableparser.Field) string {
	var fieldNames, placeHolders []string
	for _, field := range fields {
		if !isSupportedType(field.Type) {
			continue
		}
		fieldNames = append(fieldNames, backticks(field.Name))
		if !field.AllowsNull && !field.Default.Valid && field.Key == "PRI" &&
			strings.Contains(field.Extra, "auto_increment") {
			placeHolders = append(placeHolders, "NULL")
		} else {
			placeHolders = append(placeHolders, "?")
		}
	}

	sql := fmt.Sprintf("INSERT INTO %s.%s (%s) VALUES (%s)",
		backticks(dbName),
		backticks(tableName),
		strings.Join(fieldNames, ","),
		strings.Join(placeHolders, ","),
	)

	for i := 1; i < bulkCount; i++ {
		sql += ", (" + strings.Join(placeHolders, ",") + ")"
	}

	return sql

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

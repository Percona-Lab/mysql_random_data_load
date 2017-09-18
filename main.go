package main

import (
	"database/sql"
	"fmt"
	"log"
	"net/url"
	"os"
	"strings"
	"sync"
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
	masks          = map[string]uint64{
		"tinyint":   0XF,
		"smallint":  0xFF,
		"mediumint": 0xFFF,
		"int":       0xFFFF,
		"integer":   0xFFFF,
		"bigint":    0xFFFFFFFF,
	}
)

func main() {
	kingpin.MustParse(app.Parse(os.Args[1:]))

	address := *host
	if *port != 0 {
		address += ":" + string(*port)
	}

	dsn := Config{
		User:      *user,
		Passwd:    *pass,
		Addr:      address,
		DBName:    *dbName,
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

	stmt, err := db.Prepare(sql)
	if err != nil {
		log.Printf("cannot prepare %q: %s", sql, err)
		os.Exit(1)
	}
	defer stmt.Close()

	values := makeValueFuncs(table.Fields)
	rowsChan := makeRowsChan(*rows, values)

	if *maxThreads < 1 {
		*maxThreads = 1
	}
	var wg sync.WaitGroup

	log.Println("Starting")

	uiprogress.Start()
	bar := uiprogress.AddBar(*rows).PrependElapsed().AppendCompleted()

	fields, placeholders := getFieldsAndPlaceholders(table.Fields)
	for i := 0; i < *maxThreads; i++ {
		wg.Add(1)
		go runInsert(*dbName, *tableName, *bulkSize, fields, placeholders, db, rowsChan, bar, &wg)
	}
	wg.Wait()
}

func makeRowsChan(rows int, values []getters.Getter) chan []interface{} {
	preloadCount := 10000
	if rows < preloadCount {
		preloadCount = rows
	}
	rowsChan := make(chan []interface{}, preloadCount)
	go func() {
		vals := make([]interface{}, len(values))
		for i := 0; i < rows; i++ {
			for i, val := range values {
				vals[i] = val.Value()
			}
			rowsChan <- vals
		}
		close(rowsChan)
	}()

	return rowsChan
}

func runInsert(dbName string, tableName string, bulkSize int, fieldNames []string,
	placeholders []string, db *sql.DB, valsChan chan []interface{},
	bar *uiprogress.Bar, wg *sync.WaitGroup) {
	baseSQL := fmt.Sprintf("INSERT INTO %s.%s (%s) VALUES ",
		backticks(dbName),
		backticks(tableName),
		strings.Join(fieldNames, ","),
	)
	separator := ""
	sql := baseSQL
	bulkVals := make([]interface{}, 0, len(fieldNames))
	count := 0

	for vals := range valsChan {
		sql += separator + "(" + strings.Join(placeholders, ",") + ")"
		separator = ", "
		bar.Incr()
		bulkVals = append(bulkVals, vals...)
		count++
		if count < bulkSize {
			continue
		}
		_, err := db.Exec(sql, bulkVals...)
		if err != nil {
			log.Printf("Error inserting values: %s\n", err)
		}
		separator = ""
		sql = baseSQL
		bulkVals = nil
		count = 0
	}
	if count > 0 {
		_, err := db.Exec(sql, bulkVals...)
		if err != nil {
			log.Printf("Error inserting values: %s\n", err)
		}
	}
	wg.Done()
}

func makeValueFuncs(fields []tableparser.Field) []getters.Getter {
	var values []getters.Getter
	for _, field := range fields {
		if !field.AllowsNull && !field.Default.Valid && field.Key == "PRI" &&
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
		if !field.AllowsNull && !field.Default.Valid && field.Key == "PRI" &&
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

package main

import (
	"database/sql"
	"fmt"
	"os"
	"os/user"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/Percona-Lab/mysql_random_data_load/generator"
	"github.com/Percona-Lab/mysql_random_data_load/tableparser"
	"github.com/go-ini/ini"
	"github.com/go-sql-driver/mysql"
	"github.com/gosuri/uiprogress"
	"github.com/kr/pretty"

	log "github.com/sirupsen/logrus"
	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

type cliOptions struct {
	app *kingpin.Application

	// Arguments
	Schema    *string
	TableName *string
	Rows      *int
	// Flags
	BulkSize   *int
	ConfigFile *string
	Debug      *bool
	Factor     *float64
	Host       *string
	MaxRetries *int
	MaxThreads *int
	NoProgress *bool
	Pass       *string
	Port       *int
	Print      *bool
	Samples    *int64
	User       *string
	Version    *bool
}

type mysqlOptions struct {
	Host     string
	Password string
	Port     int
	Sock     string
	User     string
}

var (
	opts *cliOptions

	validFunctions = []string{"int", "string", "date", "date_in_range"}

	Version   = "0.0.0."
	Commit    = "<sha1>"
	Branch    = "branch-name"
	Build     = "2017-01-01"
	GoVersion = "1.9.2"
)

const (
	defaultMySQLConfigSection = "client"
	defaultConfigFile         = "~/.my.cnf"
	defaultBulkSize           = 1000
)

func main() {

	opts, err := processCliParams()
	if err != nil {
		log.Fatal(err.Error())
	}

	if *opts.Version {
		fmt.Printf("Version   : %s\n", Version)
		fmt.Printf("Commit    : %s\n", Commit)
		fmt.Printf("Branch    : %s\n", Branch)
		fmt.Printf("Build     : %s\n", Build)
		fmt.Printf("Go version: %s\n", GoVersion)
		return
	}

	address := *opts.Host
	net := "unix"
	if address != "localhost" {
		net = "tcp"
	}
	if *opts.Port != 0 {
		address = fmt.Sprintf("%s:%d", address, *opts.Port)
	}

	dsn := mysql.Config{
		User:                 *opts.User,
		Passwd:               *opts.Pass,
		Addr:                 address,
		Net:                  net,
		DBName:               "",
		ParseTime:            true,
		AllowNativePasswords: true,
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

	table, err := tableparser.NewTable(db, *opts.Schema, *opts.TableName)
	if err != nil {
		log.Printf("cannot get table %s struct: %s", *opts.TableName, err)
		db.Close()
		os.Exit(1)
	}

	log.SetFormatter(&log.TextFormatter{FullTimestamp: true})
	if *opts.Debug {
		log.SetLevel(log.DebugLevel)
		*opts.NoProgress = true
	}
	log.Debug(pretty.Sprint(table))

	if len(table.Triggers) > 0 {
		log.Warnf("There are triggers on the %s table that might affect this process:", *opts.TableName)
		for _, t := range table.Triggers {
			log.Warnf("Trigger %q, %s %s", t.Trigger, t.Timing, t.Event)
			log.Warnf("Statement: %s", t.Statement)
		}
	}

	if *opts.Rows < 1 {
		db.Close() // golint:noerror
		log.Warnf("Number of rows < 1. There is nothing to do. Exiting")
		os.Exit(1)
	}

	if *opts.BulkSize > *opts.Rows {
		*opts.BulkSize = *opts.Rows
	}
	if *opts.BulkSize < 1 {
		*opts.BulkSize = defaultBulkSize
	}

	if opts.MaxThreads == nil {
		*opts.MaxThreads = runtime.NumCPU() * 10
	}

	if *opts.MaxThreads < 1 {
		*opts.MaxThreads = 1
	}

	if !*opts.Print {
		log.Info("Starting")
	}

	// Example: want 11 rows with bulksize 4:
	// count = int(11 / 4) = 2 -> 2 bulk inserts having 4 rows each = 8 rows
	// We need to run this insert twice:
	// INSERT INTO table (f1, f2) VALUES (?, ?), (?, ?), (?, ?), (?, ?)
	// remainder = rows - count = 11 - 8 = 3
	// And then, we need to run this insert once to complete 11 rows
	// INSERT INTO table (f1, f2) VALUES (?, ?), (?, ?), (?, ?)
	newLineOnEachRow := false
	count := *opts.Rows / *opts.BulkSize
	remainder := *opts.Rows - count**opts.BulkSize
	semaphores := makeSemaphores(*opts.MaxThreads)
	rowValues := generator.MakeValueFuncs(db, table.Fields)
	log.Debugf("Must run %d bulk inserts having %d rows each", count, *opts.BulkSize)

	runInsertFunc := generator.RunInsert
	if *opts.Print {
		*opts.MaxThreads = 1
		*opts.NoProgress = true
		newLineOnEachRow = true
		runInsertFunc = func(db *sql.DB, insertQuery string, resultsChan chan int, sem chan bool, wg *sync.WaitGroup) {
			fmt.Println(insertQuery)
			resultsChan <- *opts.BulkSize
			sem <- true
			wg.Done()
		}
	}

	bar := uiprogress.AddBar(*opts.Rows).AppendCompleted().PrependElapsed()
	if !*opts.NoProgress {
		uiprogress.Start()
	}

	okCount, err := generator.Run(db, table, bar, semaphores, rowValues, count, *opts.BulkSize, runInsertFunc, newLineOnEachRow)
	if err != nil {
		log.Errorln(err)
	}
	var okrCount, okiCount int // remainder & individual inserts OK count
	if remainder > 0 {
		log.Debugf("Must run 1 extra bulk insert having %d rows, to complete %d rows", remainder, *opts.Rows)
		okrCount, err = generator.Run(db, table, bar, semaphores, rowValues, 1, remainder, runInsertFunc, newLineOnEachRow)
		if err != nil {
			log.Errorln(err)
		}
	}

	// If there were errors and at this point we have less rows than *rows,
	// retry adding individual rows (no bulk inserts)
	totalOkCount := okCount + okrCount
	retries := 0
	if totalOkCount < *opts.Rows {
		log.Debugf("Running extra %d individual inserts (duplicated keys?)", *opts.Rows-totalOkCount)
	}
	for totalOkCount < *opts.Rows && retries < *opts.MaxRetries {
		okiCount, err = generator.Run(db, table, bar, semaphores, rowValues, *opts.Rows-totalOkCount, 1, runInsertFunc, newLineOnEachRow)
		if err != nil {
			log.Errorf("Cannot run extra insert: %s", err)
		}

		retries++
		totalOkCount += okiCount
	}

	time.Sleep(500 * time.Millisecond) // Let the progress bar to update
	if !*opts.Print {
		log.Printf("%d rows inserted", totalOkCount)
	}
	db.Close()
}

func makeSemaphores(count int) chan bool {
	sem := make(chan bool, count)
	for i := 0; i < count; i++ {
		sem <- true
	}
	return sem
}

func processCliParams() (*cliOptions, error) {
	app := kingpin.New("mysql_random_data_loader", "MySQL Random Data Loader")

	opts := &cliOptions{
		app:        app,
		BulkSize:   app.Flag("bulk-size", "Number of rows per insert statement").Default(fmt.Sprintf("%d", defaultBulkSize)).Int(),
		ConfigFile: app.Flag("config-file", "MySQL config file").Default(expandHomeDir(defaultConfigFile)).String(),
		Debug:      app.Flag("debug", "Log debugging information").Bool(),
		Factor:     app.Flag("fk-samples-factor", "Percentage used to get random samples for foreign keys fields").Default("0.3").Float64(),
		Host:       app.Flag("host", "Host name/IP").Short('h').String(),
		MaxRetries: app.Flag("max-retries", "Number of rows to insert").Default("100").Int(),
		MaxThreads: app.Flag("max-threads", "Maximum number of threads to run inserts").Default("1").Int(),
		NoProgress: app.Flag("no-progress", "Show progress bar").Default("false").Bool(),
		Pass:       app.Flag("password", "Password").Short('p').String(),
		Port:       app.Flag("port", "Port").Short('P').Int(),
		Print:      app.Flag("print", "Print queries to the standard output instead of inserting them into the db").Bool(),
		Samples:    app.Flag("max-fk-samples", "Maximum number of samples for foreign keys fields").Default("100").Int64(),
		User:       app.Flag("user", "User").Short('u').String(),
		Version:    app.Flag("version", "Show version and exit").Bool(),

		Schema:    app.Arg("database", "Database").Required().String(),
		TableName: app.Arg("table", "Table").Required().String(),
		Rows:      app.Arg("rows", "Number of rows to insert").Required().Int(),
	}
	_, err := app.Parse(os.Args[1:])

	if err != nil {
		return nil, err
	}

	if mysqlOpts, err := readMySQLConfigFile(*opts.ConfigFile); err == nil {
		checkMySQLParams(opts, mysqlOpts)
	}

	return opts, nil
}

func checkMySQLParams(opts *cliOptions, mysqlOpts *mysqlOptions) {
	if *opts.Host == "" && mysqlOpts.Host != "" {
		*opts.Host = mysqlOpts.Host
	}

	if *opts.Port == 0 && mysqlOpts.Port != 0 {
		*opts.Port = mysqlOpts.Port
	}

	if *opts.User == "" && mysqlOpts.User != "" {
		*opts.User = mysqlOpts.User
	}

	if *opts.Pass == "" && mysqlOpts.Password != "" {
		*opts.Pass = mysqlOpts.Password
	}
}

func readMySQLConfigFile(filename string) (*mysqlOptions, error) {
	cfg, err := ini.Load(expandHomeDir(filename))
	if err != nil {
		return nil, err
	}

	section := cfg.Section(defaultMySQLConfigSection)
	port, _ := section.Key("port").Int()

	mysqlOpts := &mysqlOptions{
		Host:     section.Key("host").String(),
		Port:     port,
		User:     section.Key("user").String(),
		Password: section.Key("password").String(),
	}

	return mysqlOpts, nil
}

func expandHomeDir(dir string) string {
	if !strings.HasPrefix(dir, "~") {
		return dir
	}
	u, err := user.Current()
	if err != nil {
		return dir
	}
	return u.HomeDir + strings.TrimPrefix(dir, "~")
}

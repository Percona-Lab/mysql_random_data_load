package cmd

import (
	"database/sql"
	"fmt"
	"net"
	"os"
	"sync"

	"github.com/Percona-Lab/mysql_random_data_load/internal/insert"
	"github.com/Percona-Lab/mysql_random_data_load/internal/ptdsn"
	"github.com/Percona-Lab/mysql_random_data_load/tableparser"
	"github.com/apoorvam/goterminal"
	"github.com/go-sql-driver/mysql"
	"github.com/pkg/errors"
)

type RunCmd struct {
	DSN        string `name:"dsn" help:"Connection string in Pecona toolkit"`
	Database   string `name:"database" short:"d" help:"Database schema"`
	Table      string `name:"table" short:"t" help:"Table name"`
	Host       string `name:"host" short:"H" help:"Host name/IP"`
	Port       int    `name:"port" short:"P" help:"MySQL port to connect to"`
	User       string `name:"user" short:"u" help:"MySQL username"`
	Password   string `name:"password" short:"p" help:"MySQL password"`
	ConfigFile string `name:"config-file" help:"MySQL config file"`

	Rows     int64 `name:"rows" required:"true" help:"Number of rows to insert"`
	BulkSize int64 `name:"bulk-size" help:"Number of rows per insert statement" default:"1000"`
	DryRun   bool  `name:"dry-run" help:"Print queries to the standard output instead of inserting them into the db"`
	Quiet    bool  `name:"quiet" help:"Do not print progress bar"`
}

// Run starts inserting data.
func (cmd *RunCmd) Run() error {
	dsn, err := cmd.mysqlParams()
	if err != nil {
		return err
	}

	db, err := cmd.connect(dsn)
	if err != nil {
		return err
	}

	table, err := tableparser.New(db, dsn.Database, dsn.Table)
	if err != nil {
		return errors.Wrap(err, "cannot parse table")
	}

	_, err = cmd.run(db, table)
	return err
}

func (cmd *RunCmd) run(db *sql.DB, table *tableparser.Table) (int64, error) {
	ins := insert.New(db, table)
	wg := &sync.WaitGroup{}

	if !cmd.Quiet && !cmd.DryRun {
		wg.Add(1)
		startProgressBar(cmd.Rows, ins.NotifyChan(), wg)
	}

	if cmd.DryRun {
		return ins.DryRun(cmd.Rows, cmd.BulkSize)
	}

	n, err := ins.Run(cmd.Rows, cmd.BulkSize)
	wg.Wait()
	return n, err
}

func startProgressBar(total int64, c chan int64, wg *sync.WaitGroup) {
	go func() {
		writer := goterminal.New(os.Stdout)
		var count int64
		for n := range c {
			count += n
			writer.Clear()
			fmt.Fprintf(writer, "Writing (%d/%d) rows...\n", count, total)
			writer.Print() //nolint
		}
		writer.Reset()
		wg.Done()
	}()
}

func (cmd *RunCmd) connect(dsn *ptdsn.PTDSN) (*sql.DB, error) {
	netType := "tcp"
	address := net.JoinHostPort(dsn.Host, fmt.Sprintf("%d", dsn.Port))

	if dsn.Host == "localhost" {
		netType = "unix"
		address = dsn.Host
	}

	cfg := &mysql.Config{
		User:                    dsn.User,
		Passwd:                  dsn.Password,
		Net:                     netType,
		Addr:                    address,
		DBName:                  dsn.Database,
		AllowCleartextPasswords: true,
		AllowNativePasswords:    true,
		AllowOldPasswords:       true,
		CheckConnLiveness:       true,
		ParseTime:               true,
	}

	return sql.Open("mysql", cfg.FormatDSN())
}

func (cmd *RunCmd) mysqlParams() (*ptdsn.PTDSN, error) {
	dsn, err := ptdsn.Parse(cmd.DSN)
	if err != nil {
		return nil, errors.Wrap(err, "cannot get connection parameters")
	}

	if cmd.Database != "" {
		dsn.Database = cmd.Database
	}
	if cmd.Table != "" {
		dsn.Table = cmd.Table
	}

	if dsn.Database == "" {
		return nil, fmt.Errorf("you need to specify a database")
	}
	if dsn.Table == "" {
		return nil, fmt.Errorf("you need to specify a table name")
	}

	if cmd.Host != "" {
		dsn.Host = cmd.Host
	}
	if cmd.Port != 0 {
		dsn.Port = cmd.Port
	}
	if cmd.User != "" {
		dsn.User = cmd.User
	}
	if cmd.Password != "" {
		dsn.Password = cmd.Password
	}

	return dsn, nil
}

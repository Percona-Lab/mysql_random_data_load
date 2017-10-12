package testutils

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"

	"github.com/go-sql-driver/mysql"
)

var (
	basedir string
	db      *sql.DB
)

func BaseDir() string {
	if basedir != "" {
		return basedir
	}
	out, err := exec.Command("git", "rev-parse", "--show-toplevel").Output()
	if err != nil {
		return ""
	}

	basedir = strings.TrimSpace(string(out))
	return basedir
}

func GetMySQLConnection(tb testing.TB) *sql.DB {
	if db != nil {
		return db
	}

	dsn := os.Getenv("TEST_DSN")
	if dsn == "" {
		fmt.Printf("%s TEST_DSN environment variable is empty", caller())
		tb.FailNow()
	}

	// Parse the DSN in the env var and ensure it has parseTime & multiStatements enabled
	// MultiStatements is required for LoadQueriesFromFile
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		fmt.Printf("%s cannot parse DSN %q: %s", caller(), dsn, err)
		tb.FailNow()
	}
	if cfg.Params == nil {
		cfg.Params = make(map[string]string)
	}
	cfg.ParseTime = true
	cfg.MultiStatements = true

	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		fmt.Printf("%s cannot connect to the db: DSN: %s\n%s", caller(), dsn, err)
		tb.FailNow()
	}
	return db
}

func LoadQueriesFromFile(tb testing.TB, filename string) {
	conn := GetMySQLConnection(tb)
	file := filepath.Join("testdata", filename)
	data, err := ioutil.ReadFile(file)
	if err != nil {
		fmt.Printf("%s cannot load json file %q: %s\n\n", caller(), file, err)
		tb.FailNow()
	}
	_, err = conn.Exec(string(data))
	if err != nil {
		fmt.Printf("%s cannot load queries from %q: %s\n\n", caller(), file, err)
		tb.FailNow()
	}
}

func LoadJson(tb testing.TB, filename string, dest interface{}) {
	file := filepath.Join("testdata", filename)
	data, err := ioutil.ReadFile(file)
	if err != nil {
		fmt.Printf("%s cannot load json file %q: %s\n\n", caller(), file, err)
		tb.FailNow()
	}

	err = json.Unmarshal(data, dest)
	if err != nil {
		fmt.Printf("%s cannot unmarshal the contents of %q into %T: %s\n\n", caller(), file, dest, err)
		tb.FailNow()
	}
}

func WriteJson(tb testing.TB, filename string, data interface{}) {
	file := filepath.Join("testdata", filename)
	buf, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		fmt.Printf("%s cannot marshal %T into %q: %s\n\n", caller(), data, file, err)
		tb.FailNow()
	}
	err = ioutil.WriteFile(file, buf, os.ModePerm)
	if err != nil {
		fmt.Printf("%s cannot write file %q: %s\n\n", caller(), file, err)
		tb.FailNow()
	}
}

// assert fails the test if the condition is false.
func Assert(tb testing.TB, condition bool, msg string, v ...interface{}) {
	if !condition {
		fmt.Printf("%s "+msg+"\n\n", append([]interface{}{caller()}, v...)...)
		tb.FailNow()
	}
}

// ok fails the test if an err is not nil.
func Ok(tb testing.TB, err error, args ...interface{}) {
	if err != nil {
		msg := fmt.Sprintf("%s: unexpected error: %s\n\n", caller(), err.Error())
		if len(args) > 0 {
			msg = fmt.Sprintf("%s: %s "+args[0].(string), append([]interface{}{caller(), err}, args[1:]...)) + "\n\n"
		}
		fmt.Println(msg)
		tb.FailNow()
	}
}

func NotOk(tb testing.TB, err error) {
	if err == nil {
		fmt.Printf("%s: expected error is nil\n\n", caller())
		tb.FailNow()
	}
}

// equals fails the test if exp is not equal to act.
func Equals(tb testing.TB, exp, act interface{}) {
	if !reflect.DeepEqual(exp, act) {
		fmt.Printf("%s\n\n\texp: %#v\n\n\tgot: %#v\n\n", caller(), exp, act)
		tb.FailNow()
	}
}

// Get the caller's function name and line to show a better error message
func caller() string {
	_, file, line, _ := runtime.Caller(2)
	return fmt.Sprintf("%s:%d", filepath.Base(file), line)
}

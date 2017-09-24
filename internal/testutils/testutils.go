package testutils

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

var basedir string
var db *sql.DB

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

func GetMySQLConnection() *sql.DB {
	if db != nil {
		return db
	}
	dsn := os.Getenv("TEST_DSN")
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		panic(fmt.Sprintf("cannot connect to the db: DSN: %s\n%s", dsn, err))
	}
	return db
}

func LoadJson(filename string, dest interface{}) error {
	file := filepath.Join(BaseDir(), "/", filename)
	data, err := ioutil.ReadFile(file)
	if err != nil {
		return err
	}

	err = json.Unmarshal(data, dest)
	if err != nil {
		return err
	}

	return nil
}

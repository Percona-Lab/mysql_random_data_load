package tableparser

import (
	"os"
	"reflect"
	"testing"

	"github.com/Percona-Lab/random_data_load/internal/testutils"
	_ "github.com/go-sql-driver/mysql"
)

func TestParse(t *testing.T) {
	os.Setenv("TEST_DSN", "root:root@tcp(127.1:3306)/")

	db := testutils.GetMySQLConnection()

	table, err := NewTable(db, "sakila", "film")
	if err != nil {
		t.Error(err)
	}
	var want *Table
	testutils.LoadJson("/tests/table001.json", &want)

	if !reflect.DeepEqual(table, want) {
		t.Error("Table film was not correctly parsed")
	}
}

func TestGetTriggers(t *testing.T) {
	os.Setenv("TEST_DSN", "root:root@tcp(127.1:3306)/")

}

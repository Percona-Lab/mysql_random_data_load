package tableparser

import (
	"reflect"
	"testing"

	tu "github.com/Percona-Lab/random_data_load/internal/testutils"
	_ "github.com/go-sql-driver/mysql"
)

func TestParse(t *testing.T) {
	db := tu.GetMySQLConnection(t)

	table, err := NewTable(db, "sakila", "film")
	if err != nil {
		t.Error(err)
	}
	var want *Table
	tu.LoadJson(t, "table001.json", &want)

	if !reflect.DeepEqual(table, want) {
		t.Error("Table film was not correctly parsed")
	}
}

func TestGetIndexes(t *testing.T) {
	db := tu.GetMySQLConnection(t)
	want := []Index{}
	tu.LoadJson(t, "indexes.json", &want)

	idx, err := getIndexes(db, "sakila", "film_actor")
	tu.Ok(t, err)
	tu.Equals(t, idx, want)
}

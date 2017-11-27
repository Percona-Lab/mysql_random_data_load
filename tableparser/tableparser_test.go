package tableparser

import (
	"reflect"
	"testing"

	tu "github.com/Percona-Lab/random_data_load/testutils"
	_ "github.com/go-sql-driver/mysql"
	version "github.com/hashicorp/go-version"
)

func TestParse56(t *testing.T) {
	db := tu.GetMySQLConnection(t)
	v := tu.GetVersion(t, db)
	v56, _ := version.NewVersion("5.6")

	if v.GreaterThan(v56) {
		t.Skipf("This test runs under MySQL < 5.7 and version is %s", v.String())
	}

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

func TestParse57(t *testing.T) {
	db := tu.GetMySQLConnection(t)
	v := tu.GetVersion(t, db)
	v57, _ := version.NewVersion("5.7")

	if v.LessThan(v57) {
		t.Skipf("This test runs under MySQL 5.7+ and version is %s", v.String())
	}

	table, err := NewTable(db, "sakila", "film")
	if err != nil {
		t.Error(err)
	}
	if tu.UpdateSamples() {
		tu.WriteJson(t, "table002.json", table)
	}
	var want *Table
	tu.LoadJson(t, "table002.json", &want)

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

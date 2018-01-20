package tableparser

import (
	"reflect"
	"testing"
	"time"

	tu "github.com/Percona-Lab/mysql_random_data_load/testutils"
	"github.com/apex/log"
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
	want := make(map[string]Index)
	tu.LoadJson(t, "indexes.json", &want)

	idx, err := getIndexes(db, "sakila", "film_actor")
	if tu.UpdateSamples() {
		tu.WriteJson(t, "indexes.json", idx)
	}
	tu.Ok(t, err)
	tu.Equals(t, idx, want)
}

func TestGetTriggers(t *testing.T) {
	db := tu.GetMySQLConnection(t)
	want := []Trigger{}
	v572, _ := version.NewVersion("5.7.2")
	v800, _ := version.NewVersion("8.0.0")

	sampleFile := "trigers-8.0.0.json"
	if tu.GetVersion(t, db).LessThan(v800) {
		sampleFile = "trigers-5.7.2.json"
	}
	if tu.GetVersion(t, db).LessThan(v572) {
		sampleFile = "trigers-5.7.1.json"
	}

	tu.LoadJson(t, sampleFile, &want)

	triggers, err := getTriggers(db, "sakila", "rental")
	// fake timestamp to make it constant/testeable
	triggers[0].Created.Time = time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC)

	if tu.UpdateSamples() {
		log.Info("Updating sample file: " + sampleFile)
		tu.WriteJson(t, sampleFile, triggers)
	}
	tu.Ok(t, err)
	tu.Equals(t, triggers, want)
}

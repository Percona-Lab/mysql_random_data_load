package main

import (
	"fmt"
	"reflect"
	"testing"

	tu "github.com/Percona-Lab/random_data_load/internal/testutils"
	"github.com/Percona-Lab/random_data_load/tableparser"
)

func TestParse(t *testing.T) {
	db := tu.GetMySQLConnection(t)

	table, err := tableparser.NewTable(db, "sakila", "rental")
	tu.Ok(t, err)
	values := makeValueFuncs(db, table.Fields)

	wantRows := 1
	rowsChan := makeRowsChan(wantRows, values)
	count := 0
	for c := range rowsChan {
		count++
		f := []string{}
		for _, v := range c {
			f = append(f, fmt.Sprintf("%v %T", v, v))
		}
	}
	tu.Assert(t, count == wantRows, "count is not correct: want: %d, have %d", wantRows, count)
}

func TestGetSamples(t *testing.T) {
	conn := tu.GetMySQLConnection(t)
	var wantRows int64 = 100
	samples, err := getSamples(conn, "sakila", "inventory", "inventory_id", wantRows, "int")
	tu.Ok(t, err, "error getting samples")
	_, ok := samples[0].(int64)
	tu.Assert(t, ok, "Wrong data type.")
	tu.Assert(t, int64(len(samples)) == wantRows,
		"Wrong number of samples. Have %d, want 100.", len(samples))
}

func TestValueFuncs(t *testing.T) {
	conn := tu.GetMySQLConnection(t)

	table, err := tableparser.NewTable(conn, "sakila", "rental")
	tu.Ok(t, err)

	values := makeValueFuncs(conn, table.Fields)
	tu.Assert(t, len(values) == 6, "Wrong number of value functions. Have %d, want 6", len(values))

	wantTypes := []string{
		"*getters.RandomDateInRange",
		"*getters.RandomSample",
		"*getters.RandomSample",
		"*getters.RandomDateInRange",
		"*getters.RandomSample",
		"*getters.RandomDateInRange",
	}

	for i, vf := range values {
		gotType := reflect.ValueOf(vf).Type().String()
		tu.Assert(t, gotType == wantTypes[i],
			"Wrong value function type for field %d, %s. Have %s, want %s\n", i,
			table.Fields[i].ColumnName, gotType, wantTypes[i])
	}
}

func TestMakeRowsChan(t *testing.T) {
	conn := tu.GetMySQLConnection(t)

	table, err := tableparser.NewTable(conn, "sakila", "rental")
	tu.Ok(t, err)

	wantRows := 10000

	values := makeValueFuncs(conn, table.Fields)
	rowsChan := makeRowsChan(wantRows, values)

	count := 0
	for r := range rowsChan {
		count++
		s := []string{}
		for _, v := range r {
			s = append(s, fmt.Sprintf("%v", v))
		}
	}
	tu.Assert(t, count == wantRows, "Invalid number of generated rows. Have %d, want %d", count, wantRows)
}

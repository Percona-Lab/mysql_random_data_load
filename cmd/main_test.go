package main

import (
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/Percona-Lab/random_data_load/internal/testutils"
	"github.com/Percona-Lab/random_data_load/tableparser"
	"github.com/kr/pretty"
)

func TestParse(t *testing.T) {
	os.Setenv("TEST_DSN", "root:root@tcp(127.1:3306)/")

	db := testutils.GetMySQLConnection()

	table, err := tableparser.NewTable(db, "sakila", "rental")
	if err != nil {
		t.Error(err)
	}
	values := makeValueFuncs(db, table.Fields)
	pretty.Println(table)

	wantRows := 1
	rowsChan := makeRowsChan(wantRows, values)
	count := 0
	for c := range rowsChan {
		count++
		f := []string{}
		for _, v := range c {
			f = append(f, fmt.Sprintf("%v %T", v, v))
		}
		fmt.Println(strings.Join(f, ", "))
	}
	if count != wantRows {
		t.Errorf("count is not correct: want: %d, have %d", wantRows, count)
	}
}

func TestGetSamples(t *testing.T) {
	os.Setenv("TEST_DSN", "root:root@tcp(127.1:3306)/")

	conn := testutils.GetMySQLConnection()
	var wantRows int64 = 100
	samples, err := getSamples(conn, "sakila", "inventory", "inventory_id", wantRows, "int")
	if err != nil {
		t.Errorf("error getting samples: %s", err)
	}
	_, ok := samples[0].(int64)
	if !ok {
		t.Errorf("Wrong data type.")
	}
	if int64(len(samples)) > wantRows {
		t.Errorf("Wrong number of samples. Have %d, want 100.", len(samples))
	}
}

func TestValueFuncs(t *testing.T) {
	os.Setenv("TEST_DSN", "root:root@tcp(127.1:3306)/")

	conn := testutils.GetMySQLConnection()

	table, err := tableparser.NewTable(conn, "sakila", "rental")
	if err != nil {
		t.Error(err)
	}

	values := makeValueFuncs(conn, table.Fields)
	if len(values) != 6 {
		t.Errorf("Wrong number of value functions. Have %d, want 6", len(values))
	}

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
		if gotType != wantTypes[i] {
			fmt.Printf("Wrong value function type for field %d, %s. Have %s, want %s\n", i,
				table.Fields[i].ColumnName, gotType, wantTypes[i])
		}
	}
}
func TestMakeRowsChan(t *testing.T) {
	os.Setenv("TEST_DSN", "root:root@tcp(127.1:3306)/")

	conn := testutils.GetMySQLConnection()

	table, err := tableparser.NewTable(conn, "sakila", "rental")
	if err != nil {
		t.Error(err)
	}

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
		fmt.Println(strings.Join(s, ", "))
	}
	if count != wantRows {
		t.Errorf("Invalid number of generated rows. Have %d, want %d", count, wantRows)
	}
}

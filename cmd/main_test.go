package main

import (
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/Percona-Lab/mysql_random_data_load/internal/getters"
	"github.com/Percona-Lab/mysql_random_data_load/tableparser"
	tu "github.com/Percona-Lab/mysql_random_data_load/testutils"
)

//func TestParse(t *testing.T) {
//	db := tu.GetMySQLConnection(t)
//
//	table, err := tableparser.NewTable(db, "sakila", "rental")
//	tu.Ok(t, err)
//	values := makeValueFuncs(db, table.Fields)
//
//	wantRows := 1
//	rowsChan := makeRowsChan(wantRows, values)
//	count := 0
//	for c := range rowsChan {
//		count++
//		f := []string{}
//		for _, v := range c {
//			f = append(f, fmt.Sprintf("%v %T", v, v))
//		}
//	}
//	tu.Assert(t, count == wantRows, "count is not correct: want: %d, have %d", wantRows, count)
//}

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

func TestGenerateInsertData(t *testing.T) {
	wantRows := 3
	bulkSize := 2

	values := []getters.Getter{
		getters.NewRandomInt("f1", 100, false),
		getters.NewRandomString("f2", 10, false),
		getters.NewRandomDate("f3", false),
	}

	rowsChan := make(chan []interface{}, 100)
	count := 0
	wg := &sync.WaitGroup{}
	wg.Add(1)

	go func() {
		for {
			select {
			case <-time.After(10 * time.Millisecond):
				wg.Done()
				return
			case row := <-rowsChan:
				if reflect.TypeOf(row[0]).String() != "int64" {
					fmt.Printf("Expected '*getters.RandomInt' for field [0], got %s\n", reflect.TypeOf(row[0]).String())
					t.Fail()
				}
				if reflect.TypeOf(row[1]).String() != "string" {
					fmt.Printf("Expected '*getters.RandomString' for field [0], got %s\n", reflect.TypeOf(row[1]).String())
					t.Fail()
				}
				if reflect.TypeOf(row[2]).String() != "time.Time" {
					fmt.Printf("Expected '*getters.RandomDate' for field [0], got %s\n", reflect.TypeOf(row[2]).String())
					t.Fail()
				}
				count++
			}
		}
	}()

	generateInsertData(wantRows, bulkSize, values, rowsChan)

	wg.Wait()
	tu.Assert(t, count == 3, "Invalid number of rows")
}

func TestGenerateInsertStmt(t *testing.T) {
	var table *tableparser.Table
	tu.LoadJson(t, "sakila.film.json", &table)
	want := "INSERT IGNORE INTO `sakila`.`film` " +
		"(`title`,`description`,`release_year`,`language_id`," +
		"`original_language_id`,`rental_duration`,`rental_rate`," +
		"`length`,`replacement_cost`,`rating`,`special_features`," +
		"`last_update`) " +
		"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)," +
		" (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"

	query := generateInsertStmt(table, 2)
	tu.Equals(t, query, want)

	// Check with only one row just to ensure there are no extra commas in the generated query
	want = "INSERT IGNORE INTO `sakila`.`film` " +
		"(`title`,`description`,`release_year`,`language_id`," +
		"`original_language_id`,`rental_duration`,`rental_rate`," +
		"`length`,`replacement_cost`,`rating`,`special_features`," +
		"`last_update`) " +
		"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"

	query = generateInsertStmt(table, 1)
	tu.Equals(t, query, want)
}

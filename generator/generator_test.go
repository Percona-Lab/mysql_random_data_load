package generator

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

func TestGenerateInsertData(t *testing.T) {
	wantRows := 3

	values := []Getter{
		getters.NewRandomInt("f1", 100, false),
		getters.NewRandomString("f2", 10, false),
		getters.NewRandomDate("f3", false),
	}

	rowsChan := make(chan []Getter, 100)
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
				if reflect.TypeOf(row[0]).String() != "*getters.RandomInt" {
					fmt.Printf("Expected '*getters.RandomInt' for field [0], got %q\n", reflect.TypeOf(row[0]).String())
					t.Fail()
				}
				if reflect.TypeOf(row[1]).String() != "*getters.RandomString" {
					fmt.Printf("Expected '*getters.RandomString' for field [1], got %q\n", reflect.TypeOf(row[1]).String())
					t.Fail()
				}
				if reflect.TypeOf(row[2]).String() != "*getters.RandomDate" {
					fmt.Printf("Expected '*getters.RandomDate' for field [2], got %q\n", reflect.TypeOf(row[2]).String())
					t.Fail()
				}
				count++
			}
		}
	}()

	GenerateInsertData(wantRows, values, rowsChan)

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
		"`last_update`) VALUES "

	query := GenerateInsertStmt(table)
	tu.Equals(t, want, query)
}

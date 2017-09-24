package dbutils

import (
	"database/sql"
	"fmt"
	"net/url"

	_ "github.com/go-sql-driver/mysql"
)

/*
EXPLAIN SELECT COUNT(*) FROM sakila.actor:
+----+-------------+-------+------------+-------+---------------+---------------------+---------+------+------+----------+-------------+
| id | select_type | table | partitions | type  | possible_keys | key                 | key_len | ref  | rows | filtered | Extra       |
+----+-------------+-------+------------+-------+---------------+---------------------+---------+------+------+----------+-------------+
|  1 | SIMPLE      | actor | NULL       | index | NULL          | idx_actor_last_name | 137     | NULL |  200 |   100.00 | Using index |
+----+-------------+-------+------------+-------+---------------+---------------------+---------+------+------+----------+-------------+
*/
type ExplainRow struct {
	ID           int
	SelectType   string
	Table        string
	Partitions   sql.NullString
	Type         string
	PossibleKeys sql.NullString
	Key          sql.NullString
	KeyLen       sql.NullInt64
	Ref          sql.NullString
	Rows         int64
	Filtered     float64
	Extra        sql.NullString
}

func GetApproxRowsCount(conn *sql.DB, schema, table string) (int64, error) {
	query := fmt.Sprintf("EXPLAIN SELECT COUNT(*) FROM `%s`.`%s`",
		url.QueryEscape(schema), url.QueryEscape(table))
	var exp ExplainRow
	err := conn.QueryRow(query).Scan(&exp.ID, &exp.SelectType, &exp.Table, &exp.Partitions, &exp.Type,
		&exp.PossibleKeys, &exp.Key, &exp.KeyLen, &exp.Ref, &exp.Rows, &exp.Filtered, &exp.Extra)
	if err != nil {
		return 0, err
	}
	return exp.Rows, nil
}

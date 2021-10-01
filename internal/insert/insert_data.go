package insert

import "github.com/Percona-Lab/mysql_random_data_load/internal/getters"

type insertValues []getters.Getter

func (iv insertValues) String() string {
	sep := ""
	query := "("

	for _, v := range iv {
		query += sep + v.Quote()
		sep = ", "
	}
	query += ")"

	return query
}

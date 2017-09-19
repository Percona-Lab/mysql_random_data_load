package tableparser

import (
	"database/sql"
	"log"
	"math"
	"net/url"
	"regexp"
	"strconv"
	"strings"
)

type Table struct {
	Name    string
	Fields  []Field
	Indexes []Index
}

type Index struct {
}

type Field struct {
	Name          string
	Type          string
	Default       sql.NullString
	Definition    string
	Size          float64
	Key           string
	AllowsNull    bool
	Extra         string
	AllowedValues []string
	Mask          uint64
}

func Parse(db *sql.DB, tableName string) (*Table, error) {
	table := &Table{
		Name: tableName,
	}
	var err error
	table.Fields, err = parseTable(db, tableName)
	if err != nil {
		return nil, err
	}
	return table, nil
}

func parseTable(db *sql.DB, tableName string) ([]Field, error) {
	var fields []Field

	query := "DESCRIBE " + backticks(tableName)
	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	//                           +--------------------------- field type
	//                           |          +---------------- field size
	//                           |          |       +-------- extra info (unsigned, etc)
	//                           |          |       |
	re := regexp.MustCompile("^(.*?)(?:\\((.*?)\\)(.*))?$")
	for rows.Next() {
		var fieldName, definition, null, key, extra string
		var defaultValue sql.NullString
		err := rows.Scan(&fieldName, &definition, &null, &key, &defaultValue, &extra)
		if err != nil {
			continue
		}
		m := re.FindStringSubmatch(definition)
		if len(m) < 2 {
			log.Print("cannot detect field type for %s: %s", fieldName, definition)
			continue
		}
		fType := m[1]
		fSize := m[2]
		//fextra := m[3]

		var size float64
		if canHaveSize(fType) {
			if strings.Contains(fSize, ",") {
				// Example: DECIMAL(10,2)
				n := strings.Split(fSize, ",")
				intSize, _ := strconv.ParseInt(n[0], 10, 64)
				decSize, _ := strconv.ParseInt(n[1], 10, 64)
				size = float64(intSize-decSize) + float64(decSize)/math.Pow10(len(n[1]))
			} else {
				// Example: VARCHAR(255). Using float because of DECIMAL
				size, _ = strconv.ParseFloat(fSize, 64)
			}
			if size == 0 {
				size = defaultSize(fType)
			}
		}

		allowedValues := []string{}
		if fType == "enum" || fType == "set" {
			vals := strings.Split(fSize, ",")
			for _, val := range vals {
				val = strings.TrimPrefix(val, "'")
				val = strings.TrimSuffix(val, "'")
				allowedValues = append(allowedValues, val)
			}
		}

		fields = append(fields, Field{
			Name:          fieldName,
			Type:          fType,
			Definition:    definition,
			Size:          size,
			Key:           key,
			AllowsNull:    null == "YES",
			Extra:         extra,
			Default:       defaultValue,
			AllowedValues: allowedValues,
		})
	}
	if rows.Err() != nil {
		return nil, rows.Err()
	}
	return fields, nil
}

func defaultSize(fieldType string) float64 {
	sizes := map[string]float64{
		"float":  5.2,
		"double": 5.2,
		"year":   4,
	}
	if size, ok := sizes[fieldType]; ok {
		return size
	}
	return 0
}

func canHaveSize(fieldType string) bool {
	t := map[string]bool{
		"decimal":      true,
		"varchar":      true,
		"char":         true,
		"varcharacter": true,
		"varbinary":    true,
		"float":        true,
		"double":       true,
	}
	_, ok := t[fieldType]
	return ok
}

func backticks(val string) string {
	if strings.HasPrefix(val, "`") && strings.HasSuffix(val, "`") {
		return url.QueryEscape(val)
	}
	return "`" + url.QueryEscape(val) + "`"
}

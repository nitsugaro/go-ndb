package ndb

import (
	"fmt"
	"reflect"
)

func (b *DBBridge) ExecuteQuery(query string, args ...any) ([]M, error) {
	fmt.Println(query)
	rows, err := b.db.Query(query, args...)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	cols, _ := rows.Columns()
	result := []M{}
	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		row := M{}
		for i, col := range cols {
			v := reflect.ValueOf(vals[i])
			if v.Kind() == reflect.Slice {
				row[col] = fmt.Sprintf("%s", vals[i])
			} else {
				row[col] = vals[i]
			}
		}
		result = append(result, row)
	}

	return result, nil
}

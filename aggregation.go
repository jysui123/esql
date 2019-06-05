package esql

import (
	"fmt"
	"strings"

	"github.com/xwb1989/sqlparser"
)

func (e *ESql) convertGroupByExpr(expr sqlparser.GroupBy) (dsl string, err error) {
	var groupByStrSlice []string
	for _, groupByExpr := range expr {
		switch groupByItem := groupByExpr.(type) {
		case *sqlparser.ColName:
			// TODO: raise error if colName is duplicated
			colNameStr := groupByItem.Name.String()
			groupByStr := fmt.Sprintf(`{"group_%v": {"terms": {"field": "%v", "missing_bucket": true}}}`, colNameStr, colNameStr)
			groupByStrSlice = append(groupByStrSlice, groupByStr)
		default:
			err = fmt.Errorf(`esql: GROUP BY %T not supported`, groupByExpr)
			return "", err
		}
	}
	dsl = strings.Join(groupByStrSlice, ",")
	// TODO: magic size number
	dsl = fmt.Sprintf(`{"groupby": {"composite": {"size": 1000, "sources": [%v]}}}`, dsl)
	return dsl, nil
}

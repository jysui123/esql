package esql

import (
	"fmt"
	"strings"

	"github.com/xwb1989/sqlparser"
)

func (e *ESql) convertAgg(sel sqlparser.Select) (dsl string, err error) {
	colNameSetGroupBy := make(map[string]int)
	var dslGroupBy string
	if len(sel.GroupBy) != 0 {
		dslGroupBy, colNameSetGroupBy, err = e.convertGroupByExpr(sel.GroupBy)
		if err != nil {
			return "", err
		}
	}
	aggFuncExprSlice, colNameSlice, aggNameSlice, err := e.extractSelectedExpr(sel.SelectExprs)
	if err != nil {
		return "", err
	}
	// verify don't select col name out side agg group name
	if err = e.checkAggCompatibility(colNameSlice, colNameSetGroupBy, aggNameSlice); err != nil {
		return "", err
	}

	// handle selected aggregation functions
	aggNameSlice, aggTargetSlice, aggTagSlice, aggTagSet, err := e.getAggSelect(aggFuncExprSlice)
	if err != nil {
		return "", err
	}

	// handle order by aggregation functions
	aggNameOrderBySlice, aggTargetOrderBySlice, aggTagOrderBySlice, aggDirOrderBySlice, aggTagOrderBySet, err := e.getAggOrderBy(sel.OrderBy)
	if err != nil {
		return "", err
	}

	// handle having aggregation functions
	// aggNameHavingSlice, aggTargetHavingSlice, aggTagHavingSlice, aggTagHavingSet, err := e.getAggHaving(sel.Having)
	// if err != nil {
	// 	return "", err
	// }

	// add necessary aggregations originated from order by and having
	for tag, i := range aggTagOrderBySet {
		if _, exist := aggTagSet[tag]; !exist && tag != "_count" {
			aggTagSet[tag] = len(aggTagSet)
			aggNameSlice = append(aggNameSlice, aggNameOrderBySlice[i])
			aggTargetSlice = append(aggTargetSlice, aggTargetOrderBySlice[i])
			aggTagSlice = append(aggTagSlice, aggTagOrderBySlice[i])
		}
	}

	// generate inside aggs field
	var dslAgg string
	if len(aggTagSlice) > 0 {
		var dslAggSlice []string
		for i, tag := range aggTagSlice {
			dslAgg := fmt.Sprintf(`"%v": {"%v": {"field": "%v"}}`, tag, aggNameSlice[i], aggTargetSlice[i])
			dslAggSlice = append(dslAggSlice, dslAgg)
		}
		if len(aggTagOrderBySlice) > 0 {
			var dslOrderSlice []string
			for i, tag := range aggTagOrderBySlice {
				dslOrder := fmt.Sprintf(`{"%v": {"order": "%v"}}`, tag, aggDirOrderBySlice[i])
				dslOrderSlice = append(dslOrderSlice, dslOrder)
			}
			dslAggOrder := strings.Join(dslOrderSlice, ",")
			// TODO: magic size number
			dslAggOrder = fmt.Sprintf(`"bucket_sort": {"bucket_sort": {"sort": [%v], "size": %v}}`, dslAggOrder, 1000)
			dslAggSlice = append(dslAggSlice, dslAggOrder)
		}
		dslAgg = "{" + strings.Join(dslAggSlice, ",") + "}"
	}

	// generate final dsl for aggs field
	if len(dslGroupBy) == 0 && len(aggTagSlice) == 0 {
		dsl = ""
	} else if len(aggTagSlice) == 0 {
		dsl = fmt.Sprintf(`{"groupby": {%v}}`, dslGroupBy)
	} else if len(dslGroupBy) == 0 {
		dsl = dslAgg
	} else {
		dsl = fmt.Sprintf(`{"groupby": {%v, "aggs": %v}}`, dslGroupBy, dslAgg)
	}
	return dsl, nil
}

func (e *ESql) checkAggCompatibility(colNameSlice []string, colNameGroupBy map[string]int, aggNameSlice []string) (err error) {
	for _, aggName := range aggNameSlice {
		colNameGroupBy[aggName] = 1
	}
	if len(colNameGroupBy) == 0 {
		return nil
	}
	for _, colNameStr := range colNameSlice {
		if _, exist := colNameGroupBy[colNameStr]; !exist {
			err = fmt.Errorf(`esql: select column %v that not in group by`, colNameStr)
			return err
		}
	}
	return nil
}

func (e *ESql) getAggHaving(having *sqlparser.Where) ([]string, []string, []string, map[string]int, error) {
	var aggNameSlice, aggTargetSlice, aggTagSlice []string
	aggTagSet := make(map[string]int)
	if having != nil {
		err := fmt.Errorf(`esql: HAVING not supported`)
		return nil, nil, nil, nil, err
	}
	return aggNameSlice, aggTargetSlice, aggTagSlice, aggTagSet, nil
}

func (e *ESql) getAggOrderBy(orderBy sqlparser.OrderBy) ([]string, []string, []string, []string, map[string]int, error) {
	var aggNameSlice, aggTargetSlice, aggDirSlice, aggTagSlice []string
	aggTagDirSet := make(map[string]string) // tag -> asc / desc
	aggTagSet := make(map[string]int)       // tag -> offset, for compatiblity checking

	for _, orderExpr := range orderBy {
		aggStr := strings.Trim(sqlparser.String(orderExpr.Expr), "`")
		if strings.ContainsAny(aggStr, "()") {
			// TODO: do more sanity checks, like prevent order the same target with different directions
			// eg: COUNT(colA) -> count_colA
			aggStr = strings.Trim(aggStr, ")")
			strParts := strings.Split(aggStr, "(")
			aggNameStr := strings.ToLower(strParts[0])
			aggTargetStr := strParts[1]
			var aggTagStr string
			aggStr = strings.ToLower(aggStr)
			switch aggNameStr {
			case "count":
				if !strings.Contains(aggStr, "distinct") {
					aggTagStr = "_count"
				} else {
					aggTargetParts := strings.Split(aggTargetStr, " ")
					aggTagStr = aggNameStr + "_distinct_" + aggTargetParts[1]
				}
			case "sum", "min", "max", "avg":
				if strings.Contains(aggStr, "distinct") {
					err := fmt.Errorf(`esql: order by aggregation function %v w/ DISTINCT not supported`, aggNameStr)
					return nil, nil, nil, nil, nil, err
				}
				aggTagStr = aggNameStr + "_" + aggTargetStr
			default:
				err := fmt.Errorf(`esql: order by aggregation function %v not supported`, aggNameStr)
				return nil, nil, nil, nil, nil, err
			}
			if dir, exist := aggTagDirSet[aggTagStr]; exist {
				if dir != orderExpr.Direction {
					err := fmt.Errorf(`esql: order by aggregation direction conflict`)
					return nil, nil, nil, nil, nil, err
				}
				continue
			}
			aggTagDirSet[aggTagStr] = orderExpr.Direction
			aggTagSet[aggTagStr] = len(aggTagSet)
			aggNameSlice = append(aggNameSlice, aggNameStr)
			aggTargetSlice = append(aggTargetSlice, aggTargetStr)
			aggTagSlice = append(aggTagSlice, aggTagStr)
			aggDirSlice = append(aggDirSlice, orderExpr.Direction)
		}
	}
	return aggNameSlice, aggTargetSlice, aggTagSlice, aggDirSlice, aggTagSet, nil
}

func (e *ESql) getAggSelect(exprs []*sqlparser.FuncExpr) ([]string, []string, []string, map[string]int, error) {
	var aggNameSlice, aggTargetSlice, aggTagSlice []string
	aggTagSet := make(map[string]int) // tag -> offset, for compatibility checking

	for _, funcExpr := range exprs {
		aggNameStr := strings.ToLower(funcExpr.Name.String())
		aggTargetStr := sqlparser.String(funcExpr.Exprs)
		aggTargetStr = strings.Trim(aggTargetStr, "`")
		var aggTagStr string
		switch aggNameStr {
		case "count":
			// no need to handle count(*) since the size of bucket is always returned
			if aggTargetStr == "*" {
				continue
			}
			if funcExpr.Distinct {
				aggTagStr = aggNameStr + "_distinct_" + aggTargetStr
				aggNameStr = "cardinality"
			} else {
				aggTagStr = aggNameStr + "_" + aggTargetStr
				// * ES SQL translate API just ignore non DISTINCT COUNT since the count of a bucket is always
				// * returned. However, we don't want count null value of a certain field, as a result we count
				// * documents w/ non-null value of the target field by "value_count" keyword
				aggNameStr = "value_count"
			}
		case "avg", "sum", "min", "max":
			if funcExpr.Distinct {
				err := fmt.Errorf(`esql: aggregation function %v w/ DISTINCT not supported`, aggNameStr)
				return nil, nil, nil, nil, err
			}
			aggTagStr = aggNameStr + "_" + aggTargetStr
		default:
			err := fmt.Errorf(`esql: aggregation function %v not supported`, aggNameStr)
			return nil, nil, nil, nil, err
		}
		if _, exist := aggTagSet[aggTagStr]; exist {
			continue
		}
		aggTagSet[aggTagStr] = len(aggTagSet)
		aggNameSlice = append(aggNameSlice, aggNameStr)
		aggTargetSlice = append(aggTargetSlice, aggTargetStr)
		aggTagSlice = append(aggTagSlice, aggTagStr)
	}

	return aggNameSlice, aggTargetSlice, aggTagSlice, aggTagSet, nil
}

func (e *ESql) extractSelectedExpr(expr sqlparser.SelectExprs) ([]*sqlparser.FuncExpr, []string, []string, error) {
	var aggFuncExprSlice []*sqlparser.FuncExpr
	var colNameSlice, aggNameSlice []string
	for _, selectExpr := range expr {
		// from sqlparser's definition, we need to first convert the selectExpr to AliasedExpr
		// and then check whether AliasedExpr is a FuncExpr or just ColName
		switch selectExpr.(type) {
		case *sqlparser.AliasedExpr:
			aliasedExpr := selectExpr.(*sqlparser.AliasedExpr)
			switch aliasedExpr.Expr.(type) {
			case *sqlparser.FuncExpr:
				funcExpr := aliasedExpr.Expr.(*sqlparser.FuncExpr)
				aggFuncExprSlice = append(aggFuncExprSlice, funcExpr)
				aggNameSlice = append(aggNameSlice, sqlparser.String(funcExpr.Exprs))
			case *sqlparser.ColName:
				colName := aliasedExpr.Expr.(*sqlparser.ColName)
				colNameSlice = append(colNameSlice, strings.Trim(sqlparser.String(colName), "`"))
			default:
				err := fmt.Errorf(`esql: %T not supported in select body`, aliasedExpr)
				return nil, nil, nil, err
			}
		default:
		}
	}
	return aggFuncExprSlice, colNameSlice, aggNameSlice, nil
}

func (e *ESql) convertGroupByExpr(expr sqlparser.GroupBy) (dsl string, colNameSet map[string]int, err error) {
	var groupByStrSlice []string
	colNameSet = make(map[string]int)
	for _, groupByExpr := range expr {
		switch groupByItem := groupByExpr.(type) {
		case *sqlparser.ColName:
			colNameStr := groupByItem.Name.String()
			if _, exist := colNameSet[colNameStr]; !exist {
				colNameSet[colNameStr] = 1
				groupByStr := fmt.Sprintf(`{"group_%v": {"terms": {"field": "%v", "missing_bucket": true}}}`, colNameStr, colNameStr)
				groupByStrSlice = append(groupByStrSlice, groupByStr)
			}
		default:
			err = fmt.Errorf(`esql: GROUP BY %T not supported`, groupByExpr)
			return "", nil, err
		}
	}
	dsl = strings.Join(groupByStrSlice, ",")
	// TODO: magic size number, use "after" for pagination
	dsl = fmt.Sprintf(`"composite": {"size": %v, "sources": [%v]}`, 1000, dsl)
	return dsl, colNameSet, nil
}

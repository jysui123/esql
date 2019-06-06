package esql

import (
	"fmt"
	"strings"

	"github.com/xwb1989/sqlparser"
)

func (e *ESql) convertAgg(sel sqlparser.Select) (dsl string, err error) {
	var dslGroupBy, dslAggFunc string
	colNameSetGroupBy := make(map[string]int)
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
	if len(aggFuncExprSlice) != 0 {
		dslAggFunc, err = e.convertAggFuncExpr(aggFuncExprSlice, sel.OrderBy)
		if err != nil {
			return "", err
		}
	}
	// * here "groupby" is just an assigned name to the aggregation, it can be any non-reserved word
	// * we just follow the ES sql translate API to name it "groupby"
	if len(dslGroupBy) == 0 && len(dslAggFunc) == 0 {
		dsl = ""
	} else if len(dslAggFunc) == 0 {
		dsl = fmt.Sprintf(`{"groupby": {%v}}`, dslGroupBy)
	} else if len(dslGroupBy) == 0 {
		dsl = dslAggFunc
	} else {
		dsl = fmt.Sprintf(`{"groupby": {%v, "aggs": %v}}`, dslGroupBy, dslAggFunc)
	}
	// fmt.Printf("group: " + dslGroupBy + "\n")
	// fmt.Printf("aggFunc: " + dslAggFunc + "\n")
	// fmt.Printf("aggAll: " + dsl + "\n")
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

func (e *ESql) convertAggFuncExpr(exprs []*sqlparser.FuncExpr, orderBy sqlparser.OrderBy) (dsl string, err error) {
	var aggSlice, orderAggsSlice, orderAggsDirSlice []string
	orderTagSet := make(map[string]int)
	for _, orderExpr := range orderBy {
		orderTargetStr := strings.Trim(sqlparser.String(orderExpr.Expr), "`")
		if strings.ContainsAny(orderTargetStr, "()") {
			// TODO: do more sanity checks, like prevent order the same target with different directions
			// eg: COUNT(colA) -> count_colA
			orderTargetStr = strings.Trim(orderTargetStr, ")")
			strParts := strings.Split(orderTargetStr, "(")
			strParts[0] = strings.ToLower(strParts[0])
			orderAggStr := strings.ToLower(strParts[0]) + "_" + strParts[1]
			// convert count_distinct colName to count_distinct_colName, to match the aggregation tag
			orderAggStr = strings.Replace(orderAggStr, " ", "_", -1)
			if strParts[0] == "count" && !strings.Contains(orderAggStr, "distinct") {
				orderAggStr = "_count"
			}
			// avoid duplicate
			if _, exist := orderTagSet[orderAggStr]; !exist {
				orderTagSet[orderAggStr] = 1
				orderAggsSlice = append(orderAggsSlice, orderAggStr)
				orderAggsDirSlice = append(orderAggsDirSlice, orderExpr.Direction)
			}
		}
	}
	if len(orderTagSet) > 0 {
		var bucketSortSlice []string
		for i := 0; i < len(orderAggsSlice); i++ {
			bucketSortStr := fmt.Sprintf(`{"%v": {"order": "%v"}}`, orderAggsSlice[i], orderAggsDirSlice[i])
			bucketSortSlice = append(bucketSortSlice, bucketSortStr)
		}
		aggSortStr := strings.Join(bucketSortSlice, ",")
		// TODO: magic size number
		aggSortStr = fmt.Sprintf(`"bucket_sort": {"bucket_sort": {"sort": [%v], "size": %v}}`, aggSortStr, 1000)
		aggSlice = append(aggSlice, aggSortStr)
	}

	aggTagSet := make(map[string]int) // used for detect conflict between agg and order by
	for _, funcExpr := range exprs {
		funcNameStr := strings.ToLower(funcExpr.Name.String())
		funcArguStr := sqlparser.String(funcExpr.Exprs)
		funcArguStr = strings.Trim(funcArguStr, "`")
		var funcAggTag string
		if funcExpr.Distinct {
			funcAggTag = funcNameStr + "_distinct_" + funcArguStr
		} else {
			funcAggTag = funcNameStr + "_" + funcArguStr
		}

		switch funcNameStr {
		case "count":
			// no need to handle since the size of bucket is always returned
			if funcArguStr == "*" {
				continue
			}
			if _, exist := aggTagSet[funcAggTag]; exist {
				continue
			}
			aggTagSet[funcAggTag] = 1
			var aggStr string
			if funcExpr.Distinct {
				aggStr = fmt.Sprintf(`"%v": {"cardinality": {"field": "%v"}}`, funcAggTag, funcArguStr)
			} else {
				// * ES SQL translate API just ignore non DISTINCT COUNT since the count of a bucket is always
				// * returned. However, we don't want count null value of a certain field, as a result we count
				// * documents w/ non-null value of the target field by "value_count" keyword
				aggStr = fmt.Sprintf(`"%v": {"value_count": {"field": "%v"}}`, funcAggTag, funcArguStr)
			}
			aggSlice = append(aggSlice, aggStr)
		case "avg", "max", "min", "sum", "stats":
			if funcExpr.Distinct {
				err = fmt.Errorf(`esql: aggregation function %v w/ DISTINCT not supported`, funcNameStr)
				return "", err
			}
			if _, exist := aggTagSet[funcAggTag]; exist {
				continue
			}
			aggTagSet[funcAggTag] = 1
			aggStr := fmt.Sprintf(`"%v": {"%v": {"field": "%v"}}`, funcAggTag, funcNameStr, funcArguStr)
			aggSlice = append(aggSlice, aggStr)
		default:
			err = fmt.Errorf(`esql: aggregation function %v not supported`, funcNameStr)
			return "", err
		}
	}
	// check the order function is valid
	for orderTag := range orderTagSet {
		if _, exist := aggTagSet[orderTag]; !exist && orderTag != "_count" {
			err = fmt.Errorf(`esql: order by not specified aggregation function %v`, orderTag)
			return "", err
		}
	}

	if len(aggSlice) > 0 {
		dsl = "{" + strings.Join(aggSlice, ",") + "}"
	}
	return dsl, nil
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
	// TODO: magic size number, use "after" to page
	dsl = fmt.Sprintf(`"composite": {"size": %v, "sources": [%v]}`, 1000, dsl)
	return dsl, colNameSet, nil
}

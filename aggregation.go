package esql

import (
	"fmt"
	"strings"

	"github.com/xwb1989/sqlparser"
)

func (e *ESql) convertAgg(sel sqlparser.Select) (dsl string, err error) {
	var dslGroupBy, dslAggFunc string
	var colNameSetGroupBy map[string]int
	if len(sel.GroupBy) != 0 {
		dslGroupBy, colNameSetGroupBy, err = e.convertGroupByExpr(sel.GroupBy)
		if err != nil {
			return "", err
		}
	}
	aggFuncExprSlice, colNameSlice, err := e.extractSelectedExpr(sel.SelectExprs)
	if err != nil {
		return "", err
	}
	if err = e.checkAggCompatibility(colNameSlice, colNameSetGroupBy); err != nil {
		return "", err
	}
	if len(aggFuncExprSlice) != 0 {
		dslAggFunc, err = e.convertAggFuncExpr(aggFuncExprSlice)
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

func (e *ESql) checkAggCompatibility(colNameSlice []string, colNameGroupBy map[string]int) (err error) {
	for _, colNameStr := range colNameSlice {
		if _, exist := colNameGroupBy[colNameStr]; !exist {
			err = fmt.Errorf(`esql: select column %v that not in group by`, colNameStr)
			return err
		}
	}
	return nil
}

func (e *ESql) convertAggFuncExpr(exprs []*sqlparser.FuncExpr) (dsl string, err error) {
	var aggSlice []string
	aggMap := make(map[string]map[string]int) // colName -> AggFunc -> appear time
	for _, funcExpr := range exprs {
		funcNameStr := strings.ToLower(funcExpr.Name.String())
		funcArguStr := sqlparser.String(funcExpr.Exprs)
		funcAggTag := funcNameStr + "(" + funcArguStr + ")"
		if _, exist := aggMap[funcArguStr]; !exist {
			aggMap[funcArguStr] = make(map[string]int)
		}
		switch funcNameStr {
		case "count":
			if _, exist := aggMap[funcArguStr][funcNameStr]; exist {
				continue
			}
			aggMap[funcArguStr][funcNameStr] = 1
			var aggStr string
			if funcArguStr == "*" {
				// no need to handle since the size of bucket is always returned
				continue
			} else if funcExpr.Distinct {
				aggStr = fmt.Sprintf(`"%v": {"cardinality": {"field": "%v"}}`, funcAggTag, funcArguStr)
			} else {
				// * ES SQL translate API just ignore non DISTINCT COUNT since the count of a bucket is always
				// * returned. However, we don't want count null value of a certain field, as a result we count
				// * documents w/ non-null value of the target field by "value_count" keyword
				aggStr = fmt.Sprintf(`"%v": {"value_count": {"field": "%v"}}`, funcAggTag, funcArguStr)
			}
			aggSlice = append(aggSlice, aggStr)
		case "avg", "max", "min", "sum", "stat":
			if _, exist := aggMap[funcArguStr][funcNameStr]; exist {
				continue
			}
			aggMap[funcArguStr][funcNameStr] = 1
			// TODO: optimization: group multiple aggregation on the same colName as stat
			aggStr := fmt.Sprintf(`"%v": {"%v": {"field": "%v"}}`, funcAggTag, funcNameStr, funcArguStr)
			aggSlice = append(aggSlice, aggStr)
		default:
			err = fmt.Errorf(`esql: aggregation function %v not supported`, funcNameStr)
			return "", err
		}
	}
	if len(aggSlice) > 0 {
		dsl = "{" + strings.Join(aggSlice, ",") + "}"
	}
	return dsl, nil
}

func (e *ESql) extractSelectedExpr(expr sqlparser.SelectExprs) ([]*sqlparser.FuncExpr, []string, error) {
	var aggFuncExprSlice []*sqlparser.FuncExpr
	var colNameSlice []string
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
			case *sqlparser.ColName:
				colName := aliasedExpr.Expr.(*sqlparser.ColName)
				colNameSlice = append(colNameSlice, sqlparser.String(colName))
			default:
				err := fmt.Errorf(`esql: %T not supported in select body`, aliasedExpr)
				return nil, nil, err
			}
		default:
		}
	}
	return aggFuncExprSlice, colNameSlice, nil
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
	dsl = fmt.Sprintf(`"composite": {"size": 1000, "sources": [%v]}`, dsl)
	return dsl, colNameSet, nil
}

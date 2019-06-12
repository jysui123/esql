package esql

import (
	"fmt"
	"strings"

	"github.com/xwb1989/sqlparser"
)

func (e *ESql) convertAgg(sel sqlparser.Select) (dsl string, err error) {
	if len(sel.GroupBy) == 0 && sel.Having != nil {
		err = fmt.Errorf(`esql: HAVING used without GROUP BY`)
		return "", err
	}

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
	if err = e.checkSelGroupByCompatibility(colNameSlice, colNameSetGroupBy, aggNameSlice); err != nil {
		return "", err
	}

	// explanations for getAggSelect, getAggOrderBy, getAggHaving:
	// user can introduce aggregation functions from SELECT, ORDER BY and HAVING, for each different
	// aggregation functions, we need to add a tag for it in "aggs" field, which let ES to do the calculation
	// each aggregation's query body is in the form of "<tag>: {"<agg function name>": {"field": "<colName>"}}
	//
	// <tag> is generated by us, the convention in esql is tag = <agg function name>_<colName> to prevent dup tag name
	// <agg function name> can be sum, max, min, count
	// <colName> is the field that agg apply to
	//
	// however, for each source, there can be dups, we don't want to introduce duplicate tags
	// aggTagSet, aggTagOrderBySet, aggTagHavingSet are used to resolve dups, each of them is a map[string]int
	// which maps the tag string to an offset integer which indicates the position of this tag in
	// the corresponding aggxxxSlice
	//
	// aggNamexxxSlice stores agg functions names, aggTargetxxxSlice stores colNames, aggTagxxxSlice stores the tags
	// they are used to generate final json query

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
	script, aggNameHavingSlice, aggTargetHavingSlice, aggTagHavingSlice, aggTagHavingSet, err := e.getAggHaving(sel.Having)
	if err != nil {
		return "", err
	}

	// add necessary aggregations originated from order by and having
	for tag, i := range aggTagOrderBySet {
		if _, exist := aggTagSet[tag]; !exist && tag != "_count" {
			aggTagSet[tag] = len(aggTagSet)
			aggNameSlice = append(aggNameSlice, aggNameOrderBySlice[i])
			aggTargetSlice = append(aggTargetSlice, aggTargetOrderBySlice[i])
			aggTagSlice = append(aggTagSlice, aggTagOrderBySlice[i])
		}
	}
	for tag, i := range aggTagHavingSet {
		if _, exist := aggTagSet[tag]; !exist && tag != "_count" {
			aggTagSet[tag] = len(aggTagSet)
			aggNameSlice = append(aggNameSlice, aggNameHavingSlice[i])
			aggTargetSlice = append(aggTargetSlice, aggTargetHavingSlice[i])
			aggTagSlice = append(aggTagSlice, aggTagHavingSlice[i])
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
			dslAggOrder = fmt.Sprintf(`"bucket_sort": {"bucket_sort": {"sort": [%v], "size": %v}}`, dslAggOrder, e.bucketNumber)
			dslAggSlice = append(dslAggSlice, dslAggOrder)
		}
		if script != "" {
			var bucketPathSlice []string
			for tag := range aggTagHavingSet {
				bucketPathSlice = append(bucketPathSlice, fmt.Sprintf(`"%v": "%v"`, tag, tag))
			}
			bucketPathStr := strings.Join(bucketPathSlice, ",")
			bucketFilterStr := fmt.Sprintf(`"having": {"bucket_selector": {"buckets_path": {%v}, "script": "%v"}}`, bucketPathStr, script)
			dslAggSlice = append(dslAggSlice, bucketFilterStr)
		}
		dslAgg = "{" + strings.Join(dslAggSlice, ",") + "}"
	}

	// generate final dsl for aggs field
	// here "groupby" is just a tag and can be any unreserved word
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

func (e *ESql) checkSelGroupByCompatibility(colNameSlice []string, colNameGroupBy map[string]int, aggNameSlice []string) error {
	for _, aggName := range aggNameSlice {
		colNameGroupBy[aggName] = 1
	}
	if len(colNameGroupBy) == 0 {
		return nil
	}
	for _, colNameStr := range colNameSlice {
		if _, exist := colNameGroupBy[colNameStr]; !exist {
			err := fmt.Errorf(`esql: select column %v that not in group by`, colNameStr)
			return err
		}
	}
	return nil
}

func (e *ESql) getAggOrderBy(orderBy sqlparser.OrderBy) ([]string, []string, []string, []string, map[string]int, error) {
	var aggNameSlice, aggTargetSlice, aggDirSlice, aggTagSlice []string
	aggTagDirSet := make(map[string]string) // tag -> asc / desc
	aggTagSet := make(map[string]int)       // tag -> offset, for compatiblity checking

	aggCnt := 0
	for _, orderExpr := range orderBy {
		switch orderExpr.Expr.(type) {
		case *sqlparser.FuncExpr:
			aggCnt++
			funcExpr := orderExpr.Expr.(*sqlparser.FuncExpr)
			aggNameStr := strings.ToLower(funcExpr.Name.String())
			// ? should we convert funcExpr.Exprs to colname?
			aggTargetStr := sqlparser.String(funcExpr.Exprs)
			aggTargetStr = strings.Trim(aggTargetStr, "`")
			aggTargetStr, err := e.filterOrReplace(aggTargetStr)
			if err != nil {
				return nil, nil, nil, nil, nil, err
			}
			var aggTagStr string
			switch aggNameStr {
			case "count":
				// no need to handle count(*) since the size of bucket is always returned
				if aggTargetStr == "*" {
					aggTagStr = "_count"
				} else if funcExpr.Distinct {
					aggTagStr = aggNameStr + "_distinct_" + aggTargetStr
					aggNameStr = "cardinality"
				} else {
					aggTagStr = aggNameStr + "_" + aggTargetStr
					aggNameStr = "value_count"
				}
			case "avg", "sum", "min", "max":
				if funcExpr.Distinct {
					err := fmt.Errorf(`esql: aggregation function %v w/ DISTINCT not supported`, aggNameStr)
					return nil, nil, nil, nil, nil, err
				}
				aggTagStr = aggNameStr + "_" + aggTargetStr
			default:
				err := fmt.Errorf(`esql: aggregation function %v not supported`, aggNameStr)
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
		default:
		}
	}

	if aggCnt > 0 && aggCnt < len(orderBy) {
		err := fmt.Errorf(`esql: mix order by agg functions and column names`)
		return nil, nil, nil, nil, nil, err
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
		aggTargetStr, err := e.filterOrReplace(aggTargetStr)
		if err != nil {
			return nil, nil, nil, nil, err
		}
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
				lhs := aliasedExpr.Expr.(*sqlparser.ColName)
				lhsStr, err := e.convertColName(lhs)
				if err != nil {
					return nil, nil, nil, err
				}
				colNameSlice = append(colNameSlice, strings.Trim(lhsStr, "`"))
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
	dsl = fmt.Sprintf(`"composite": {"size": %v, "sources": [%v]}`, e.bucketNumber, dsl)
	return dsl, colNameSet, nil
}

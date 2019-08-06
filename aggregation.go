package esql

import (
	"fmt"
	"strings"

	"github.com/xwb1989/sqlparser"
)

func (e *ESql) convertAggregation(sel sqlparser.Select) (selectedColNames []string, dsl string, err error) {
	if len(sel.GroupBy) == 0 && sel.Having != nil {
		err = fmt.Errorf(`esql: HAVING used without GROUP BY`)
		return nil, "", err
	}

	aggMaps := make(map[string]string)
	dslGroupBy, err := e.convertGroupBy(sel.GroupBy)
	if err != nil {
		return nil, "", err
	}

	selectedColNames, err = e.convertSelectExpr(sel.SelectExprs, aggMaps)
	if err != nil {
		return nil, "", err
	}

	dslOrderBy, err := e.convertOrderBy(sel.OrderBy, aggMaps)
	if err != nil {
		return nil, "", err
	}

	dslHaving, err := e.convertHaving(sel.Having, aggMaps)
	if err != nil {
		return nil, "", err
	}

	var aggs []string
	for tag, body := range aggMaps {
		if tag != "_count" {
			aggs = append(aggs, fmt.Sprintf(`"%v": {%v}`, tag, body))
		}
	}
	if dslOrderBy != "" {
		aggs = append(aggs, fmt.Sprintf(`"order_by": {%v}`, dslOrderBy))
	}
	if dslHaving != "" {
		aggs = append(aggs, fmt.Sprintf(`"having": {%v}`, dslHaving))
	}
	if len(aggs) > 0 {
		dsl = fmt.Sprintf(`{%v}`, strings.Join(aggs, ","))
	}

	if dslGroupBy != "" && len(aggMaps) == 0 {
		dsl = fmt.Sprintf(`{"groupby": {%v}}`, dslGroupBy)
	} else if dslGroupBy != "" && len(aggMaps) != 0 {
		dsl = fmt.Sprintf(`{"groupby": {%v, "aggs": %v}}`, dslGroupBy, dsl)
	}
	return selectedColNames, dsl, nil
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

func (e *ESql) convertOrderBy(orderBy sqlparser.OrderBy, aggMaps map[string]string) (dsl string, err error) {
	if orderBy == nil {
		return "", nil
	}
	var dslOrderSlice []string
	for _, orderExpr := range orderBy {
		switch expr := orderExpr.Expr.(type) {
		case *sqlparser.FuncExpr:
			tag, body, err := e.convertFuncExpr(*expr)
			if err != nil {
				return "", err
			}
			if _, exist := aggMaps[tag]; !exist {
				aggMaps[tag] = body
			}
			dslOrder := fmt.Sprintf(`{"%v": {"order": "%v"}}`, tag, orderExpr.Direction)
			dslOrderSlice = append(dslOrderSlice, dslOrder)
		case *sqlparser.ColName:
		default:
			err = fmt.Errorf(`esql: %T not supported in ORDER BY`, expr)
			return "", err
		}
	}
	if (len(dslOrderSlice) > 0) {
		dsl = strings.Join(dslOrderSlice, ",")
		dsl = fmt.Sprintf(`"bucket_sort": {"sort": [%v], "size": %v}`, dsl, e.bucketNumber)
	}
	return dsl, nil
}

func (e *ESql) convertGroupConcatExpr(concatExpr sqlparser.GroupConcatExpr) (tag string, body string, err error) {
	var colNameStrSlice, unitStrSlice []string
	for _, selExpr := range concatExpr.Exprs {
		colName, ok := selExpr.(*sqlparser.AliasedExpr).Expr.(*sqlparser.ColName)
		if !ok {
			err := fmt.Errorf(`esql: fail to parse group concat`)
			return "", "", err
		}
		colNameStr, err := e.convertColName(colName)
		if err != nil {
			return "", "", err
		}
		colNameStrSlice = append(colNameStrSlice, colNameStr)
		unitStrSlice = append(unitStrSlice, fmt.Sprintf(`doc['%v'].value`, colNameStr))
	}

	sep := concatExpr.Separator[12 : len(concatExpr.Separator)-1]

	unitStr := strings.Join(unitStrSlice, ` + '`+sep+`' + `)
	if len(colNameStrSlice) > 1 {
		unitStr = fmt.Sprintf(`'(' + %v + ')'`, unitStr)
	}

	init := `"init_script": "state.strs = []"`
	mapping := fmt.Sprintf(`"map_script": "state.strs.add(%v)"`, unitStr)
	combine := fmt.Sprintf(`"combine_script": "return String.join('%v', state.strs);"`, sep)
	reduce := fmt.Sprintf(`"reduce_script": "return String.join('%v', states);"`, sep)
	body = fmt.Sprintf(`"scripted_metric": {%v, %v, %v, %v}`, init, mapping, combine, reduce)

	tag = "group_concat_"+strings.Join(colNameStrSlice, "_")
	return tag, body, nil
}

func (e *ESql) convertSelectExpr(exprs sqlparser.SelectExprs, aggMaps map[string]string) (colNameSlice []string, err error) {
	for _, selectExpr := range exprs {
		if sqlparser.String(selectExpr) == "*" {
			return nil, nil
		}
		aliasedExpr, ok := selectExpr.(*sqlparser.AliasedExpr)
		if !ok {
			err = fmt.Errorf(`esql: %T not supported in SELECT`, selectExpr)
			return nil, err
		}
		aggTagStr := sqlparser.String(aliasedExpr.As)
		if _, exist := aggMaps[aggTagStr]; exist {
			continue
		}
		switch expr := aliasedExpr.Expr.(type) {
		case *sqlparser.FuncExpr:
			tag, body, err := e.convertFuncExpr(*expr)
			if err != nil {
				return nil, err
			}
			if aggTagStr == "" {
				aggTagStr = tag
			}
			if _, exist := aggMaps[aggTagStr]; !exist {
				aggMaps[aggTagStr] = body
			}
		case *sqlparser.ColName:
			lhsStr, err := e.convertColName(expr)
			if err != nil {
				return nil, err
			}
			colNameSlice = append(colNameSlice, lhsStr)
		case *sqlparser.GroupConcatExpr:
			tag, body, err := e.convertGroupConcatExpr(*expr)
			if err != nil {
				return nil, err
			}
			if aggTagStr == "" {
				aggTagStr = tag
			}
			if _, exist := aggMaps[aggTagStr]; !exist {
				aggMaps[aggTagStr] = body
			}
		case *sqlparser.BinaryExpr, *sqlparser.UnaryExpr, *sqlparser.ParenExpr:
			script, err := e.convertToScript(expr, aggMaps)
			if err != nil {
				return nil, err
			}
			if aggTagStr == "" {
				aggTagStr = fmt.Sprintf(`expr_%v`, len(aggMaps))
			}
			var bucketPathSlice []string
			for tag := range aggMaps {
				bucketPathSlice = append(bucketPathSlice, fmt.Sprintf(`"%v": "%v"`, tag, tag))
			}
			bucketPathStr := strings.Join(bucketPathSlice, ",")
			body := fmt.Sprintf(`"bucket_script": {"buckets_path": {%v}, "script": "return %v;"}`, bucketPathStr, script)
			if _, exist := aggMaps[aggTagStr]; !exist {
				aggMaps[aggTagStr] = body
			}
		default:
			err = fmt.Errorf(`esql: %T not supported in SELECT`, expr)
			return nil, err
		}
	}
	return colNameSlice, nil
}

func (e *ESql) convertGroupBy(expr sqlparser.GroupBy) (dsl string, err error) {
	if expr == nil {
		return "", nil
	}
	var groupByStrSlice []string
	colNameSet := make(map[string]int)
	for _, groupByExpr := range expr {
		switch groupByItem := groupByExpr.(type) {
		case *sqlparser.ColName:
			colNameStr, err := e.convertColName(groupByItem)
			if err != nil {
				return "", err
			}
			if _, exist := colNameSet[colNameStr]; !exist {
				colNameSet[colNameStr] = 1
				groupByStr := fmt.Sprintf(`{"group_%v": {"terms": {"field": "%v", "missing_bucket": true}}}`, colNameStr, colNameStr)
				groupByStrSlice = append(groupByStrSlice, groupByStr)
			}
		default:
			err = fmt.Errorf(`esql: GROUP BY %T not supported`, groupByExpr)
			return "", err
		}
	}
	if len(groupByStrSlice) > 0 {
		dsl = strings.Join(groupByStrSlice, ",")
		dsl = fmt.Sprintf(`"composite": {"size": %v, "sources": [%v]}`, e.bucketNumber, dsl)
	}
	return dsl, nil
}

func (e *ESql) convertFuncExpr(funcExpr sqlparser.FuncExpr) (tag string, body string, err error) {
	aggNameStr := strings.ToLower(funcExpr.Name.String())
	switch aggNameStr {
	case "count":
		tag, body, err = e.convertCount(funcExpr)
	case "avg", "sum", "min", "max":
		tag, body, err = e.convertStandardArithmetic(funcExpr)
	case "histogram":
		tag, body, err = e.convertHistogram(funcExpr)
	case "date_histogram":
		tag, body, err = e.convertDateHistogram(funcExpr)
	case "range":
		tag, body, err = e.convertRange(funcExpr)
	case "date_range":
		tag, body, err = e.convertDateRange(funcExpr)
	default:
		err := fmt.Errorf(`esql: aggregation function %v not supported`, aggNameStr)
		return "", "", err
	}
	if err != nil {
		return "", "", err
	}
	tag = strings.Trim(tag, "'")
	return tag, body, nil
}


package esql

import (
	"fmt"
	"strings"

	"github.com/xwb1989/sqlparser"
)

func (e *ESql) convertCount(funcExpr sqlparser.FuncExpr) (tag string, body string, err error) {
	funcName := strings.ToLower(funcExpr.Name.String())
	argument := sqlparser.String(funcExpr.Exprs)
	argument = strings.Trim(argument, "`")
	argument, err = e.keyProcess(argument)
	if err != nil {
		return "", "", err
	}
	if argument == "*" {
		tag = "_count"
	} else if funcExpr.Distinct {
		tag = funcName + "_distinct_" + argument
		funcName = "cardinality"
	} else {
		tag = funcName + "_" + argument
		funcName = "value_count"
	}
	tag = strings.Replace(tag, ".", "_", -1)
	if argument == "*" {
		body = fmt.Sprintf(`"%v": "%v"`, tag, tag)
	} else {
		body = fmt.Sprintf(`"%v": {"field": "%v"}`, funcName, argument)
	}
	return tag, body, nil
}

func (e *ESql) convertStandardArithmetic(funcExpr sqlparser.FuncExpr) (tag string, body string, err error) {
	funcName := strings.ToLower(funcExpr.Name.String())
	argument := sqlparser.String(funcExpr.Exprs)
	argument = strings.Trim(argument, "`")
	argument, err = e.keyProcess(argument)
	if err != nil {
		return "", "", err
	}
	if funcExpr.Distinct {
		err := fmt.Errorf(`esql: aggregation function %v w/ DISTINCT not supported`, funcName)
		return "", "", err
	}
	tag = funcName + "_" + argument
	tag = strings.Replace(tag, ".", "_", -1)
	body = fmt.Sprintf(`"%v": {"field": "%v"}`, funcName, argument)
	return tag, body, nil
}

func (e *ESql) convertHistogram(funcExpr sqlparser.FuncExpr) (tag string, body string, err error) {
	funcName := strings.ToLower(funcExpr.Name.String())
	if funcName != "histogram" {
		err = fmt.Errorf("fail to convert histogram")
		return "", "", err
	}

	arguments := make(map[string]string)
	for i, expr := range funcExpr.Exprs {
		if i > 3 {
			err = fmt.Errorf("fail to convert histogram")
			return "", "", err
		} 
		aliasedExpr, ok := expr.(*sqlparser.AliasedExpr)
		if !ok {
			err = fmt.Errorf("fail to convert date_histogram")
			return "", "", err
		}
		if i < 3 {
			arguments[histogramTags[i]] = fmt.Sprintf(`"%v"`, strings.Trim(sqlparser.String(aliasedExpr.Expr), "'"))
		} else {
			bounds := strings.Split(strings.Trim(sqlparser.String(aliasedExpr.Expr), "'"), ",")
			arguments[histogramTags[i]] = fmt.Sprintf(`{"min": %v, "max": %v}`, bounds[0], bounds[1])
		}
	}

	tag = funcName + "_" + arguments["field"]

	var aggBodys []string
	for k, v := range arguments {
		aggBodys = append(aggBodys, fmt.Sprintf(`"%v": %v`, k, v))
	}
	body = strings.Join(aggBodys, ",")
	body = fmt.Sprintf(`"histogram": {%v}`, body)
	tag = strings.Replace(tag, ".", "_", -1)
	return tag, body, nil
}

// TODO: sanity checks
func (e *ESql) convertDateHistogram(funcExpr sqlparser.FuncExpr) (tag string, body string, err error) {
	funcName := strings.ToLower(funcExpr.Name.String())
	if funcName != "date_histogram" {
		err = fmt.Errorf("fail to convert date_histogram")
		return "", "", err
	}

	arguments := make(map[string]string)
	for i, expr := range funcExpr.Exprs {
		if i > 2 {
			err = fmt.Errorf("fail to convert date_histogram")
			return "", "", err
		}
		aliasedExpr, ok := expr.(*sqlparser.AliasedExpr)
		if !ok {
			err = fmt.Errorf("fail to convert date_histogram")
			return "", "", err
		}
		arguments[dateHistogramTags[i]] = strings.Trim(sqlparser.String(aliasedExpr.Expr), "'")
	}

	tag = funcName + "_" + arguments["field"]
	var aggBodys []string
	for k, v := range arguments {
		aggBodys = append(aggBodys, fmt.Sprintf(`"%v": "%v"`, k, v))
	}
	body = strings.Join(aggBodys, ",")
	body = fmt.Sprintf(`"date_histogram": {%v}`, body)
	tag = strings.Replace(tag, ".", "_", -1)
	return tag, body, nil
}

func (e *ESql) convertRange(funcExpr sqlparser.FuncExpr) (tag string, body string, err error) {
	funcName := strings.ToLower(funcExpr.Name.String())
	if funcName != "range" {
		err = fmt.Errorf("fail to convert range aggregation")
		return "", "", err
	}

	arguments := make(map[string]string)
	var ranges []string
	for i, expr := range funcExpr.Exprs {
		aliasedExpr, ok := expr.(*sqlparser.AliasedExpr)
		if !ok {
			err = fmt.Errorf("fail to convert date_histogram")
			return "", "", err
		}
		if i == 0 {
			arguments["field"] = fmt.Sprintf(`"%v"`, strings.Trim(sqlparser.String(aliasedExpr.Expr), "'"))
		} else {
			ranges = append(ranges, strings.Trim(sqlparser.String(aliasedExpr.Expr), "'"))
		}
	}

	tag = funcName + "_" + arguments["field"]
	var rangeBodies []string
	if len(ranges) > 1 {
		for i := range ranges {
			if i == len(ranges) - 1 {
				break
			}
			rangeBodies = append(rangeBodies, fmt.Sprintf(`{"from": "%v", "to": "%v"}`, ranges[i], ranges[i+1]))
		}
	}
	rangeBodies = append(rangeBodies, fmt.Sprintf(`{"to": "%v"}`, ranges[0]))
	rangeBodies = append(rangeBodies, fmt.Sprintf(`{"from": "%v"}`, ranges[len(ranges)-1]))
	arguments["ranges"] = fmt.Sprintf(`[%v]`, strings.Join(rangeBodies, ","))
	var aggBodys []string
	for k, v := range arguments {
		aggBodys = append(aggBodys, fmt.Sprintf(`"%v": %v`, k, v))
	}
	body = strings.Join(aggBodys, ",")
	body = fmt.Sprintf(`"range": {%v}`, body)
	tag = strings.Replace(tag, ".", "_", -1)
	return tag, body, nil
}

func (e *ESql) convertDateRange(funcExpr sqlparser.FuncExpr) (tag string, body string, err error) {
	funcName := strings.ToLower(funcExpr.Name.String())
	if funcName != "date_range" {
		err = fmt.Errorf("fail to convert date_range aggregation")
		return "", "", err
	}

	arguments := make(map[string]string)
	var ranges []string
	for i, expr := range funcExpr.Exprs {
		aliasedExpr, ok := expr.(*sqlparser.AliasedExpr)
		if !ok {
			err = fmt.Errorf("fail to convert date_range")
			return "", "", err
		}
		if i < 2 {
			arguments[dateRangeTags[i]] = fmt.Sprintf(`"%v"`, strings.Trim(sqlparser.String(aliasedExpr.Expr), "'"))
		} else {
			ranges = append(ranges, strings.Trim(sqlparser.String(aliasedExpr.Expr), "'"))
		}
	}
	tag = funcName + "_" + strings.Trim(arguments["field"], "\"")
	var rangeBodies []string
	if len(ranges) > 1 {
		for i := range ranges {
			if i == len(ranges) - 1 {
				break
			}
			rangeBodies = append(rangeBodies, fmt.Sprintf(`{"from": "%v", "to": "%v"}`, ranges[i], ranges[i+1]))
		}
	}
	rangeBodies = append(rangeBodies, fmt.Sprintf(`{"to": "%v"}`, ranges[0]))
	rangeBodies = append(rangeBodies, fmt.Sprintf(`{"from": "%v"}`, ranges[len(ranges)-1]))
	arguments["ranges"] = fmt.Sprintf(`[%v]`, strings.Join(rangeBodies, ","))
	var aggBodys []string
	for k, v := range arguments {
		aggBodys = append(aggBodys, fmt.Sprintf(`"%v": %v`, k, v))
	}
	body = strings.Join(aggBodys, ",")
	body = fmt.Sprintf(`"date_range": {%v}`, body)
	tag = strings.Replace(tag, ".", "_", -1)
	return tag, body, nil
}

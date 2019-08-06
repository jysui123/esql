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
		body = fmt.Sprintf(`%v": "%v"`, tag, tag)
	} else {
		body = fmt.Sprintf(`"%v": {"%v": "%v"}`, tag, funcName, argument)
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
	body = fmt.Sprintf(`"%v": {"%v": "%v"}`, tag, funcName, argument)
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
	for _, expr := range funcExpr.Exprs {
		aliasedExpr, ok := expr.(*sqlparser.AliasedExpr)
		if !ok {
			err = fmt.Errorf("fail to convert date_histogram")
			return "", "", err
		}
		comparisonExpr, ok := aliasedExpr.Expr.(*sqlparser.ComparisonExpr)
		if !ok || comparisonExpr.Operator != "=" {
			err = fmt.Errorf("fail to convert date_histogram")
			return "", "", err
		}
		lhsStr, rhsStr := sqlparser.String(comparisonExpr.Left), sqlparser.String(comparisonExpr.Right)
		arguments[lhsStr] = rhsStr
	}

	tag = funcName + "_" + arguments["field"]

	var aggBodys []string
	for k, v := range arguments {
		aggBodys = append(aggBodys, fmt.Sprintf(`"%v": "%v"`, k, v))
	}
	body = strings.Join(aggBodys, ",")
	tag = strings.Replace(tag, ".", "_", -1)
	body = fmt.Sprintf(`{%v}`, body)
	return tag, body, nil
}

func (e *ESql) convertDateRange(funcExpr sqlparser.FuncExpr) (tag string, body string, err error) {
	return tag, body, nil
}

func (e *ESql) convertRange(funcExpr sqlparser.FuncExpr) (tag string, body string, err error) {
	return tag, body, nil
}

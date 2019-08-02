package esql

import (
	"fmt"
	"strings"

	"github.com/xwb1989/sqlparser"
)

func (e *ESql) convertCount(funcExpr sqlparser.FuncExpr, as string) (aggTagStr string, aggBodyStr string, err error) {
	return aggTagStr, aggBodyStr, nil
}

func (e *ESql) convertStandardArithmetic(funcExpr sqlparser.FuncExpr, as string) (aggTagStr string, aggBodyStr string, err error) {
	return aggTagStr, aggBodyStr, nil
}

// TODO: sanity checks
func (e *ESql) convertDateHistogram(funcExpr sqlparser.FuncExpr, as string) (aggTagStr string, aggBodyStr string, err error) {
	aggNameStr := strings.ToLower(funcExpr.Name.String())
	if aggNameStr != "date_histogram" {
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

	if as != "" {
		aggTagStr = as
	} else {
		aggTagStr = aggNameStr + "_" + arguments["field"]
	}

	var aggBodys []string
	for k, v := range arguments {
		aggBodys = append(aggBodys, fmt.Sprintf(`"%v": "%v"`, k, v))
	}
	aggBodyStr = strings.Join(aggBodys, ",")
	aggBodyStr = fmt.Sprintf(`{%v}`, aggBodyStr)
	return aggTagStr, aggBodyStr, nil
}

func (e *ESql) convertDateRange(funcExpr sqlparser.FuncExpr) (aggTagStr string, aggBodyStr string, err error) {
	return aggTagStr, aggBodyStr, nil
}

func (e *ESql) convertRange(funcExpr sqlparser.FuncExpr) (aggTagStr string, aggBodyStr string, err error) {
	return aggTagStr, aggBodyStr, nil
}

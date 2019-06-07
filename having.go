package esql

import (
	"fmt"
	"strings"

	"github.com/xwb1989/sqlparser"
)

func (e *ESql) getAggHaving(having *sqlparser.Where) (string, []string, []string, []string, map[string]int, error) {
	var aggNameSlice, aggTargetSlice, aggTagSlice []string
	aggTagSet := make(map[string]int)
	var script string
	var err error
	if having != nil {
		script, err = e.convertHavingExpr(having.Expr, &aggNameSlice, &aggTargetSlice, &aggTagSlice, aggTagSet)
		if err != nil {
			return "", nil, nil, nil, nil, err
		}
	}
	return script, aggNameSlice, aggTargetSlice, aggTagSlice, aggTagSet, nil
}

func (e *ESql) convertHavingExpr(expr sqlparser.Expr, aggNameSlice *[]string, aggTargetSlice *[]string,
	aggTagSlice *[]string, aggTagSet map[string]int) (string, error) {

	switch expr.(type) {
	case *sqlparser.ComparisonExpr:
		return e.convertHavingComparisionExpr(expr, aggNameSlice, aggTargetSlice, aggTagSlice, aggTagSet)
	default:
		err := fmt.Errorf(`esql: %T expression in HAVING no supported`, expr)
		return "", err
	}
}

func (e *ESql) convertHavingComparisionExpr(expr sqlparser.Expr, aggNameSlice *[]string, aggTargetSlice *[]string,
	aggTagSlice *[]string, aggTagSet map[string]int) (string, error) {

	comparisonExpr := expr.(*sqlparser.ComparisonExpr)
	var funcExprs []*sqlparser.FuncExpr
	op := comparisonExpr.Operator

	// lhs
	leftFuncExpr, ok := comparisonExpr.Left.(*sqlparser.FuncExpr)
	if !ok {
		err := fmt.Errorf("esql: found %v in HAVING which is not aggregation function", sqlparser.String(comparisonExpr.Left))
		return "", err
	}
	funcExprs = append(funcExprs, leftFuncExpr)

	// rhs, can be a value or an aggregation function
	var rhsStr, script string
	switch comparisonExpr.Right.(type) {
	case *sqlparser.SQLVal:
		rhsStr = sqlparser.String(comparisonExpr.Right)
		rhsStr = strings.Trim(rhsStr, `'`)
	case *sqlparser.FuncExpr:
		rightFuncExpr := comparisonExpr.Right.(*sqlparser.FuncExpr)
		funcExprs = append(funcExprs, rightFuncExpr)
	default:
		err := fmt.Errorf("esql: %T in HAVING rhs not supported", comparisonExpr.Right)
		return "", err
	}

	for _, funcExpr := range funcExprs {
		aggNameStr := strings.ToLower(funcExpr.Name.String())
		aggTargetStr := sqlparser.String(funcExpr.Exprs)
		aggTargetStr = strings.Trim(aggTargetStr, "`")
		var aggTagStr string
		switch aggNameStr {
		case "count":
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
				err := fmt.Errorf(`esql: HAVING: aggregation function %v w/ DISTINCT not supported`, aggNameStr)
				return "", err
			}
			aggTagStr = aggNameStr + "_" + aggTargetStr
		default:
			err := fmt.Errorf(`esql: HAVING: aggregation function %v not supported`, aggNameStr)
			return "", err
		}
		aggTagSet[aggTagStr] = len(*aggNameSlice)
		*aggNameSlice = append(*aggNameSlice, aggNameStr)
		*aggTargetSlice = append(*aggTargetSlice, aggTargetStr)
		*aggTagSlice = append(*aggTagSlice, aggTagStr)
	}

	n := len(*aggTagSlice)
	if rhsStr == "" {
		script = fmt.Sprintf(`params.%v %v params.%v`, (*aggTagSlice)[n-2], op, (*aggTagSlice)[n-1])
	} else {
		script = fmt.Sprintf(`params.%v %v %v`, (*aggTagSlice)[n-1], op, rhsStr)
	}
	return script, nil
}

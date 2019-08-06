package esql

import (
	"fmt"
	"strings"

	"github.com/xwb1989/sqlparser"
)

func (e *ESql) convertHaving(having *sqlparser.Where, aggMaps map[string]string) (dsl string, err error) {
	if having != nil {
		script, err := e.convertHavingExpr(having.Expr, aggMaps)
		if err != nil {
			return "", err
		}
		var bucketPathSlice []string
		for tag := range aggMaps {
			bucketPathSlice = append(bucketPathSlice, fmt.Sprintf(`"%v": "%v"`, tag, tag))
		}
		bucketPathStr := strings.Join(bucketPathSlice, ",")
		dsl = fmt.Sprintf(`"bucket_selector": {"buckets_path": {%v}, "script": "%v"}`, bucketPathStr, script)
	}
	return dsl, err
}

func (e *ESql) convertHavingExpr(expr sqlparser.Expr, aggMaps map[string]string) (string, error) {
	switch expr.(type) {
	case *sqlparser.ComparisonExpr:
		return e.convertHavingComparisionExpr(expr, aggMaps)
	case *sqlparser.AndExpr:
		return e.convertHavingAndExpr(expr, aggMaps)
	case *sqlparser.OrExpr:
		return e.convertHavingOrExpr(expr, aggMaps)
	case *sqlparser.NotExpr:
		return e.convertHavingNotExpr(expr, aggMaps)
	case *sqlparser.ParenExpr:
		return e.convertHavingParenExpr(expr, aggMaps)
	case *sqlparser.RangeCond:
		return e.convertHavingBetweenExpr(expr, aggMaps)
	// TODO: case *sqlparser.BinaryExpr
	default:
		err := fmt.Errorf(`esql: %T expression in HAVING no supported`, expr)
		return "", err
	}
}

func (e *ESql) convertHavingBetweenExpr(expr sqlparser.Expr, aggMaps map[string]string) (string, error) {
	rangeCond := expr.(*sqlparser.RangeCond)
	lhs := rangeCond.Left
	from, to := rangeCond.From, rangeCond.To
	var expr1 sqlparser.Expr = &sqlparser.ComparisonExpr{Left: lhs, Right: from, Operator: ">="}
	var expr2 sqlparser.Expr = &sqlparser.ComparisonExpr{Left: lhs, Right: to, Operator: "<="}
	var expr3 sqlparser.Expr = &sqlparser.AndExpr{Left: expr1, Right: expr2}

	script, err := e.convertHavingAndExpr(expr3, aggMaps)
	if err != nil {
		return "", err
	}
	// here parenthesis is to deal with the case when an not(!) operator out side
	// if no parenthesis, NOT xxx BETWEEN a and b -> !xxx > a && xxx < b
	script = fmt.Sprintf(`(%v)`, script)
	return script, nil
}

func (e *ESql) convertHavingAndExpr(expr sqlparser.Expr, aggMaps map[string]string) (string, error) {

	andExpr := expr.(*sqlparser.AndExpr)
	leftExpr := andExpr.Left
	rightExpr := andExpr.Right
	scriptLeft, err := e.convertHavingExpr(leftExpr, aggMaps)
	if err != nil {
		return "", err
	}
	scriptRight, err := e.convertHavingExpr(rightExpr, aggMaps)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(`%v && %v`, scriptLeft, scriptRight), nil
}

func (e *ESql) convertHavingOrExpr(expr sqlparser.Expr, aggMaps map[string]string) (string, error) {

	orExpr := expr.(*sqlparser.OrExpr)
	leftExpr := orExpr.Left
	rightExpr := orExpr.Right
	scriptLeft, err := e.convertHavingExpr(leftExpr, aggMaps)
	if err != nil {
		return "", err
	}
	scriptRight, err := e.convertHavingExpr(rightExpr, aggMaps)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(`%v || %v`, scriptLeft, scriptRight), nil
}

func (e *ESql) convertHavingParenExpr(expr sqlparser.Expr, aggMaps map[string]string) (string, error) {

	parenExpr := expr.(*sqlparser.ParenExpr)
	script, err := e.convertHavingExpr(parenExpr.Expr, aggMaps)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(`(%v)`, script), nil
}

func (e *ESql) convertHavingNotExpr(expr sqlparser.Expr, aggMaps map[string]string) (string, error) {

	notExpr := expr.(*sqlparser.NotExpr)
	script, err := e.convertHavingExpr(notExpr.Expr, aggMaps)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(`!%v`, script), nil
}

func (e *ESql) convertHavingComparisionExpr(expr sqlparser.Expr, aggMaps map[string]string) (script string, err error) {
	comparisonExpr := expr.(*sqlparser.ComparisonExpr)
	if _, exist := op2PainlessOp[comparisonExpr.Operator]; !exist {
		err := fmt.Errorf(`esql: %s operator not supported in having comparison clause`, comparisonExpr.Operator)
		return "", err
	}
	// convert SQL operator format to equivalent painless operator
	op := op2PainlessOp[comparisonExpr.Operator]

	lhsScript, err := e.convertToScript(comparisonExpr.Left, aggMaps)
	if err != nil {
		return "", err
	}
	rhsScript, err := e.convertToScript(comparisonExpr.Right, aggMaps)
	if err != nil {
		return "", err
	}
	
	script = fmt.Sprintf(`%v %v %v`, lhsScript, op, rhsScript)
	return script, nil
}

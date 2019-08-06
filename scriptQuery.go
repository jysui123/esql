package esql

import (
	"fmt"

	"github.com/xwb1989/sqlparser"
)

func (e *ESql) convertToScript(exprToConvert sqlparser.Expr, aggMaps map[string]string) (script string, err error) {
	switch expr := exprToConvert.(type) {
	case *sqlparser.ColName:
		script, err = e.convertColName(expr)
		script = fmt.Sprintf(`doc['%v'].value`, script)
	case *sqlparser.SQLVal:
		script, err = e.convertValExpr(expr, true)
	case *sqlparser.BinaryExpr:
		script, err = e.convertBinaryExprToScript(expr, aggMaps)
	case *sqlparser.ParenExpr:
		script, err = e.convertToScript(expr.Expr, aggMaps)
		script = fmt.Sprintf(`(%v)`, script)
	case *sqlparser.UnaryExpr:
		script, err = e.convertUnaryExprToScript(expr, aggMaps)
	case *sqlparser.FuncExpr:
		tag, body, err := e.convertFuncExpr(*expr)
		if err != nil {
			return "", err
		}
		script = fmt.Sprintf(`params.%v`, tag)
		// here we suppose aggMaps is initialized
		if _, exist := aggMaps[tag]; !exist {
			aggMaps[tag] = body
		}
	default:
		err = fmt.Errorf("esql: invalid expression type for scripting")
	}
	if err != nil {
		return "", err
	}
	return script, nil
}

func (e *ESql) convertUnaryExprToScript(expr sqlparser.Expr, aggMaps map[string]string) (script string, err error) {
	unaryExpr, ok := expr.(*sqlparser.UnaryExpr)
	if !ok {
		err = fmt.Errorf("esql: invalid unary expression")
		return "", err
	}
	op, ok := opUnaryExpr[unaryExpr.Operator]
	if !ok {
		err = fmt.Errorf("esql: not supported binary expression operator")
		return "", err
	}
	script, err = e.convertToScript(unaryExpr.Expr, aggMaps)
	if err != nil {
		return "", err
	}

	script = fmt.Sprintf(`%v%v`, op, script)
	return script, nil
}

func (e *ESql) convertBinaryExprToScript(expr sqlparser.Expr, aggMaps map[string]string) (script string, err error) {
	var lhsScript, rhsScript string
	binExpr, ok := expr.(*sqlparser.BinaryExpr)
	if !ok {
		err = fmt.Errorf("esql: invalid binary expression")
		return "", err
	}
	lhsExpr, rhsExpr := binExpr.Left, binExpr.Right
	op, ok := opBinaryExpr[binExpr.Operator]
	if !ok {
		err = fmt.Errorf("esql: not supported binary expression operator")
		return "", err
	}

	lhsScript, err = e.convertToScript(lhsExpr, aggMaps)
	if err != nil {
		return "", err
	}
	rhsScript, err = e.convertToScript(rhsExpr, aggMaps)
	if err != nil {
		return "", err
	}

	script = fmt.Sprintf(`%v %v %v`, lhsScript, op, rhsScript)
	return script, nil
}

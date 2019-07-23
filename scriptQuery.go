package esql

import (
	"fmt"

	"github.com/xwb1989/sqlparser"
)

func (e *ESql) convertToScript(expr sqlparser.Expr) (script string, err error) {
	switch expr.(type) {
	case *sqlparser.ColName:
		exprColName := expr.(*sqlparser.ColName)
		script, err = e.convertColName(exprColName)
		script = fmt.Sprintf(`doc['%v'].value`, script)
	case *sqlparser.SQLVal:
		script, err = e.convertValExpr(expr, true)
	case *sqlparser.BinaryExpr:
		script, err = e.convertBinaryExpr(expr)
	case *sqlparser.ParenExpr:
		parenExpr := expr.(*sqlparser.ParenExpr)
		script, err = e.convertToScript(parenExpr.Expr)
		script = fmt.Sprintf(`(%v)`, script)
	case *sqlparser.UnaryExpr:
		script, err = e.convertUnaryExpr(expr)
	default:
		err = fmt.Errorf("esql: invalid expression type for scripting")
	}
	if err != nil {
		return "", err
	}
	return script, nil
}

func (e *ESql) convertUnaryExpr(expr sqlparser.Expr) (string, error) {
	var script, expStr string
	var err error
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
	expStr, err = e.convertToScript(unaryExpr.Expr)
	if err != nil {
		return "", err
	}

	script = fmt.Sprintf(`%v%v`, op, expStr)
	return script, nil
}

func (e *ESql) convertBinaryExpr(expr sqlparser.Expr) (string, error) {
	var script, lhsStr, rhsStr string
	var err error
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

	lhsStr, err = e.convertToScript(lhsExpr)
	rhsStr, err = e.convertToScript(rhsExpr)
	if err != nil {
		return "", err
	}

	script = fmt.Sprintf(`%v %v %v`, lhsStr, op, rhsStr)
	return script, nil
}

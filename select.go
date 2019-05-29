package esql

import (
	"errors"
	"fmt"
	"strings"

	"github.com/xwb1989/sqlparser"
)

// ESql is used to hold necessary information that required in parsing
type ESql struct {
	whiteList map[string]interface{}
}

func (e *ESql) init(whiteListArg map[string]interface{}) {
	e.whiteList = whiteListArg
}

func (e *ESql) convertSelect(sel *sqlparser.Select) (dsl string, err error) {
	var rootParent sqlparser.Expr
	// defaultdsl that user does not pass in where clause, just select all
	var defaultDslStr = `{"match_all": {}}`
	var dslStr = defaultDslStr

	// check whether user passes in where clause
	if sel.Where != nil {
		dslStr, err = e.convertWhereExpr(&sel.Where.Expr, true, &rootParent)
		if err != nil {
			return "", err
		}
	}

	// check whether user passes only 1 from clause
	if len(sel.From) != 1 {
		if len(sel.From) == 0 {
			err = errors.New("esql: invalid from expressino: no from expression specified")
		} else {
			err = errors.New("esql: multiple from select clause not supported")
		}
		return "", err
	}

	// check whther user passes aggregation clause
	if len(sel.GroupBy) != 0 {
		err = errors.New("esql: group by not supported")
		return "", err
	}

	// check whether user passes in limit clause
	var dslFrom, dslSize string
	if sel.Limit != nil {
		if sel.Limit.Offset != nil {
			dslFrom = sqlparser.String(sel.Limit.Offset)
		}
		dslSize = sqlparser.String(sel.Limit.Rowcount)
	}

	// check whether user passes in order by clause
	if len(sel.OrderBy) != 0 {
		err = errors.New("esql: order by not supported")
		return "", err
	}

	// build the final dsl
	dslMap := make(map[string]interface{})
	dslMap["query"] = dslStr
	if dslFrom != "" {
		dslMap["from"] = dslFrom
	}
	if dslSize != "" {
		dslMap["size"] = dslSize
	}

	var dslKeySlice = []string{"query", "from", "size", "sort", "aggregations"}
	var dslQuerySlice []string
	for _, k := range dslKeySlice {
		if v, exist := dslMap[k]; exist {
			dslQuerySlice = append(dslQuerySlice, fmt.Sprintf(`"%v" : %v`, k, v))
		}
	}

	dsl = "{" + strings.Join(dslQuerySlice, ",") + "}"
	return dsl, nil
}

func (e *ESql) convertWhereExpr(expr *sqlparser.Expr, topLevel bool, parent *sqlparser.Expr) (string, error) {
	var err error
	if expr == nil {
		err = errors.New("esql: invalid where expression, where expression should not be nil")
	}

	switch (*expr).(type) {
	case *sqlparser.ComparisonExpr:
		return e.convertComparisionExpr(expr, topLevel, parent)
	case *sqlparser.AndExpr:
		return e.convertAndExpr(expr, topLevel, parent)
	case *sqlparser.OrExpr:
		return e.convertOrExpr(expr, topLevel, parent)
	case *sqlparser.ParenExpr:
		boolExpr := (*expr).(*sqlparser.ParenExpr).Expr
		return e.convertWhereExpr(&boolExpr, topLevel, expr)
	case *sqlparser.NotExpr:
		return e.convertNotExpr(expr, topLevel, parent)
	default:
		err = fmt.Errorf(`esql: %T expression not supported in where clause`, *expr)
		return "", err
	}
}

func (e *ESql) convertNotExpr(expr *sqlparser.Expr, topLevel bool, parent *sqlparser.Expr) (string, error) {
	notExpr := (*expr).(*sqlparser.NotExpr)
	exprInside := notExpr.Expr
	dsl, err := e.convertWhereExpr(&exprInside, false, expr)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(`{"bool": {"must_not" : [%v]}}`, dsl), nil
}

func (e *ESql) convertAndExpr(expr *sqlparser.Expr, topLevel bool, parent *sqlparser.Expr) (string, error) {
	andExpr := (*expr).(*sqlparser.AndExpr)
	lhsExpr := andExpr.Left
	rhsExpr := andExpr.Right

	lhsStr, err := e.convertWhereExpr(&lhsExpr, false, expr)
	if err != nil {
		return "", err
	}
	rhsStr, err := e.convertWhereExpr(&rhsExpr, false, expr)
	if err != nil {
		return "", err
	}
	var dsl string
	if lhsStr == "" || rhsStr == "" {
		dsl = lhsStr + rhsStr
	} else {
		dsl = lhsStr + `,` + rhsStr
	}

	// merge chained AND expression
	if _, ok := (*parent).(*sqlparser.AndExpr); ok {
		return dsl, nil
	}
	return fmt.Sprintf(`{"bool" : {"filter" : [%v]}}`, dsl), nil
}

func (e *ESql) convertOrExpr(expr *sqlparser.Expr, topLevel bool, parent *sqlparser.Expr) (string, error) {
	andExpr := (*expr).(*sqlparser.OrExpr)
	lhsExpr := andExpr.Left
	rhsExpr := andExpr.Right

	lhsStr, err := e.convertWhereExpr(&lhsExpr, false, expr)
	if err != nil {
		return "", err
	}
	rhsStr, err := e.convertWhereExpr(&rhsExpr, false, expr)
	if err != nil {
		return "", err
	}
	var dsl string
	if lhsStr == "" || rhsStr == "" {
		dsl = lhsStr + rhsStr
	} else {
		dsl = lhsStr + `,` + rhsStr
	}

	// merge chained OR expression
	if _, ok := (*parent).(*sqlparser.OrExpr); ok {
		return dsl, nil
	}
	return fmt.Sprintf(`{"bool" : {"should" : [%v]}}`, dsl), nil
}

func (e *ESql) convertComparisionExpr(expr *sqlparser.Expr, topLevel bool, parent *sqlparser.Expr) (string, error) {
	// extract lhs, and check lhs is a colName
	comparisonExpr := (*expr).(*sqlparser.ComparisonExpr)
	colName, ok := comparisonExpr.Left.(*sqlparser.ColName)
	if !ok {
		return "", errors.New("esql: invalid comparison expression, lhs must be a column name")
	}

	lhsStr := sqlparser.String(colName)
	lhsStr = strings.Replace(lhsStr, "`", "", -1)

	// extract rhs
	// ? pass by pointer?
	rhsStr, err := e.convertValExpr(&comparisonExpr.Right)
	if err != nil {
		return "", err
	}

	// generate dsl according to operator
	var dsl string
	switch comparisonExpr.Operator {
	case "=":
		dsl = fmt.Sprintf(`{"match_phrase" : {"%v" : {"query" : "%v"}}}`, lhsStr, rhsStr)
	case "<":
		dsl = fmt.Sprintf(`{"range" : {"%v" : {"lt" : "%v"}}}`, lhsStr, rhsStr)
	case "<=":
		dsl = fmt.Sprintf(`{"range" : {"%v" : {"lte" : "%v"}}}`, lhsStr, rhsStr)
	case ">":
		dsl = fmt.Sprintf(`{"range" : {"%v" : {"gt" : "%v"}}}`, lhsStr, rhsStr)
	case ">=":
		dsl = fmt.Sprintf(`{"range" : {"%v" : {"gte" : "%v"}}}`, lhsStr, rhsStr)
	case "!=":
		dsl = fmt.Sprintf(`{"bool" : {"must_not" : {"match_phrase" : {"%v" : {"query" : "%v"}}}}}`, lhsStr, rhsStr)
	default:
		err := fmt.Errorf(`esql: %s operator not supported in comparison clause`, comparisonExpr.Operator)
		return "", err
	}

	if topLevel {
		dsl = fmt.Sprintf(`{"bool" : {"filter" : [%v]}}`, dsl)
	}
	return dsl, nil
}

func (e *ESql) convertValExpr(expr *sqlparser.Expr) (dsl string, err error) {
	switch (*expr).(type) {
	case *sqlparser.SQLVal:
		dsl = sqlparser.String(*expr)
		dsl = strings.Trim(dsl, `'`)
	default:
		err = errors.New("esql: not supported rhs expression")
		return "", err
	}
	return dsl, nil
}

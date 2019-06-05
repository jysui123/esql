package esql

import (
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

func (e *ESql) convertSelect(sel sqlparser.Select) (dsl string, err error) {
	var rootParent sqlparser.Expr
	// a map that contains the main components of a query
	dslMap := make(map[string]interface{})

	// check whether user passes in where clause
	if sel.Where != nil {
		dslQuery, err := e.convertWhereExpr(sel.Where.Expr, rootParent)
		if err != nil {
			return "", err
		}
		dslMap["query"] = dslQuery
	}

	// check whether user passes only 1 from clause
	if len(sel.From) != 1 {
		if len(sel.From) == 0 {
			err = fmt.Errorf("esql: invalid from expressino: no from expression specified")
		} else {
			err = fmt.Errorf("esql: join not supported")
		}
		return "", err
	}

	// check whther user passes groupby clause
	// TODO: raise error when SELECT colName and GROUP BY colName does not match
	// TODO: avoid returning all documents if unnecessary
	if len(sel.GroupBy) != 0 {
		dslGroupBy, err := e.convertGroupByExpr(sel.GroupBy)
		if err != nil {
			return "", err
		}
		dslMap["aggs"] = dslGroupBy
	}

	// check whether user passes in limit clause
	if sel.Limit != nil {
		if sel.Limit.Offset != nil {
			dslMap["from"] = sqlparser.String(sel.Limit.Offset)
		}
		dslMap["size"] = sqlparser.String(sel.Limit.Rowcount)
	}

	// check whether user passes in order by clause
	if len(sel.OrderBy) != 0 {
		err = fmt.Errorf("esql: order by not supported")
		return "", err
	}

	var dslKeySlice = []string{"query", "from", "size", "sort", "aggs"}
	var dslQuerySlice []string
	for _, k := range dslKeySlice {
		if v, exist := dslMap[k]; exist {
			dslQuerySlice = append(dslQuerySlice, fmt.Sprintf(`"%v" : %v`, k, v))
		}
	}

	dsl = "{" + strings.Join(dslQuerySlice, ",") + "}"
	return dsl, nil
}

func (e *ESql) convertWhereExpr(expr sqlparser.Expr, parent sqlparser.Expr) (string, error) {
	var err error
	if expr == nil {
		err = fmt.Errorf("esql: invalid where expression, where expression should not be nil")
	}

	switch expr.(type) {
	case *sqlparser.ComparisonExpr:
		return e.convertComparisionExpr(expr, parent, false)
	case *sqlparser.AndExpr:
		return e.convertAndExpr(expr, parent)
	case *sqlparser.OrExpr:
		return e.convertOrExpr(expr, parent)
	case *sqlparser.ParenExpr:
		boolExpr := expr.(*sqlparser.ParenExpr).Expr
		return e.convertWhereExpr(boolExpr, expr)
	case *sqlparser.NotExpr:
		return e.convertNotExpr(expr, parent)
	case *sqlparser.RangeCond:
		rangeCond := expr.(*sqlparser.RangeCond)
		lhs, ok := rangeCond.Left.(*sqlparser.ColName)
		if !ok {
			return "", fmt.Errorf("esql: invalid range column name")
		}
		lhsStr := sqlparser.String(lhs)
		fromStr := strings.Trim(sqlparser.String(rangeCond.From), `'`)
		toStr := strings.Trim(sqlparser.String(rangeCond.To), `'`)

		dsl := fmt.Sprintf(`{"range" : {"%v" : {"from" : "%v", "to" : "%v"}}}`, lhsStr, fromStr, toStr)
		return dsl, nil
	case *sqlparser.IsExpr:
		return e.convertIsExpr(expr, parent, false)
	default:
		err = fmt.Errorf(`esql: %T expression not supported in WHERE clause`, expr)
		return "", err
	}
}

// ! dsl must_not is not an equivalent to sql NOT, should convert the inside expression accordingly
func (e *ESql) convertNotExpr(expr sqlparser.Expr, parent sqlparser.Expr) (string, error) {
	notExpr := expr.(*sqlparser.NotExpr)
	exprInside := notExpr.Expr
	switch (exprInside).(type) {
	case *sqlparser.NotExpr:
		expr1 := exprInside.(*sqlparser.NotExpr)
		expr2 := expr1.Expr
		return e.convertWhereExpr(expr2, parent)
	case *sqlparser.AndExpr:
		expr1 := exprInside.(*sqlparser.AndExpr)
		var exprLeft sqlparser.Expr = &sqlparser.NotExpr{Expr: expr1.Left}
		var exprRight sqlparser.Expr = &sqlparser.NotExpr{Expr: expr1.Right}
		var expr2 sqlparser.Expr = &sqlparser.OrExpr{Left: exprLeft, Right: exprRight}
		return e.convertOrExpr(expr2, parent)
	case *sqlparser.OrExpr:
		expr1 := exprInside.(*sqlparser.OrExpr)
		var exprLeft sqlparser.Expr = &sqlparser.NotExpr{Expr: expr1.Left}
		var exprRight sqlparser.Expr = &sqlparser.NotExpr{Expr: expr1.Right}
		var expr2 sqlparser.Expr = &sqlparser.AndExpr{Left: exprLeft, Right: exprRight}
		return e.convertAndExpr(expr2, parent)
	case *sqlparser.ParenExpr:
		expr1 := exprInside.(*sqlparser.ParenExpr)
		exprBody := expr1.Expr
		var expr2 sqlparser.Expr = &sqlparser.NotExpr{Expr: exprBody}
		return e.convertNotExpr(expr2, parent)
	case *sqlparser.ComparisonExpr:
		return e.convertComparisionExpr(exprInside, parent, true)
	case *sqlparser.IsExpr:
		return e.convertIsExpr(exprInside, parent, true)
	default:
		// for BETWEEN expr
		dsl, err := e.convertWhereExpr(exprInside, expr)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf(`{"bool": {"must_not" : [%v]}}`, dsl), nil
	}
}

func (e *ESql) convertAndExpr(expr sqlparser.Expr, parent sqlparser.Expr) (string, error) {
	andExpr := expr.(*sqlparser.AndExpr)
	lhsExpr := andExpr.Left
	rhsExpr := andExpr.Right

	lhsStr, err := e.convertWhereExpr(lhsExpr, expr)
	if err != nil {
		return "", err
	}
	rhsStr, err := e.convertWhereExpr(rhsExpr, expr)
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
	if _, ok := parent.(*sqlparser.AndExpr); ok {
		return dsl, nil
	}
	return fmt.Sprintf(`{"bool" : {"filter" : [%v]}}`, dsl), nil
}

func (e *ESql) convertOrExpr(expr sqlparser.Expr, parent sqlparser.Expr) (string, error) {
	orExpr := expr.(*sqlparser.OrExpr)
	lhsExpr := orExpr.Left
	rhsExpr := orExpr.Right

	lhsStr, err := e.convertWhereExpr(lhsExpr, expr)
	if err != nil {
		return "", err
	}
	rhsStr, err := e.convertWhereExpr(rhsExpr, expr)
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
	if _, ok := parent.(*sqlparser.OrExpr); ok {
		return dsl, nil
	}
	return fmt.Sprintf(`{"bool" : {"should" : [%v]}}`, dsl), nil
}

func (e *ESql) convertIsExpr(expr sqlparser.Expr, parent sqlparser.Expr, not bool) (string, error) {
	isExpr := expr.(*sqlparser.IsExpr)
	colName, ok := isExpr.Expr.(*sqlparser.ColName)
	if !ok {
		return "", fmt.Errorf("esql: is expression only support colname missing check")
	}
	lhsStr := sqlparser.String(colName)
	lhsStr = strings.Replace(lhsStr, "`", "", -1)
	dsl := ""
	op := isExpr.Operator
	if not {
		switch isExpr.Operator {
		case sqlparser.IsNullStr:
			op = sqlparser.IsNotNullStr
		case sqlparser.IsNotNullStr:
			op = sqlparser.IsNullStr
		default:
			return "", fmt.Errorf("esql: is expression only support is null and is not null")
		}
	}
	switch op {
	case sqlparser.IsNullStr:
		dsl = fmt.Sprintf(`{"bool": {"must_not": {"exists": {"field": "%v"}}}}`, lhsStr)
	case sqlparser.IsNotNullStr:
		dsl = fmt.Sprintf(`{"exists": {"field": "%v"}}`, lhsStr)
	default:
		return "", fmt.Errorf("esql: is expression only support is null and is not null")
	}
	return dsl, nil
}

func (e *ESql) convertComparisionExpr(expr sqlparser.Expr, parent sqlparser.Expr, not bool) (string, error) {
	// extract lhs, and check lhs is a colName
	comparisonExpr := expr.(*sqlparser.ComparisonExpr)
	colName, ok := comparisonExpr.Left.(*sqlparser.ColName)
	if !ok {
		return "", fmt.Errorf("esql: invalid comparison expression, lhs must be a column name")
	}

	lhsStr := sqlparser.String(colName)
	lhsStr = strings.Replace(lhsStr, "`", "", -1)

	// extract rhs
	rhsStr, err := e.convertValExpr(comparisonExpr.Right)
	if err != nil {
		return "", err
	}
	op := comparisonExpr.Operator
	if not {
		switch comparisonExpr.Operator {
		case "=":
			op = "!="
		case "<":
			op = ">="
		case "<=":
			op = ">"
		case ">":
			op = "<="
		case ">=":
			op = "<"
		case "<>", "!=":
			op = "="
		case "in":
			op = "not in"
		case "not in":
			op = "in"
		case "like":
			op = "not like"
		case "not like":
			op = "like"
		default:
			err := fmt.Errorf(`esql: %s operator not supported in comparison clause`, comparisonExpr.Operator)
			return "", err
		}
	}
	// generate dsl according to operator
	var dsl string
	switch op {
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
	case "<>", "!=":
		dsl = fmt.Sprintf(`{"bool" : {"must_not" : {"match_phrase" : {"%v" : {"query" : "%v"}}}}}`, lhsStr, rhsStr)
	case "in":
		rhsStr = strings.Replace(rhsStr, `'`, `"`, -1)
		rhsStr = strings.Trim(rhsStr, "(")
		rhsStr = strings.Trim(rhsStr, ")")
		dsl = fmt.Sprintf(`{"terms" : {"%v" : [%v]}}`, lhsStr, rhsStr)
	case "not in":
		rhsStr = strings.Replace(rhsStr, `'`, `"`, -1)
		rhsStr = strings.Trim(rhsStr, "(")
		rhsStr = strings.Trim(rhsStr, ")")
		dsl = fmt.Sprintf(`{"bool" : {"must_not" : {"terms" : {"%v" : [%v]}}}}`, lhsStr, rhsStr)
	case "like":
		rhsStr = strings.Replace(rhsStr, `%`, `*`, -1)
		rhsStr = strings.Replace(rhsStr, `_`, `?`, -1)
		//dsl = fmt.Sprintf(`{"wildcard" : {"%v" : {"wildcard": "%v"}}}`, lhsStr, rhsStr)
		dsl = fmt.Sprintf(`{"regexp" : {"%v" : "%v"}}`, lhsStr, rhsStr)
	case "not like":
		rhsStr = strings.Replace(rhsStr, `%`, `*`, -1)
		rhsStr = strings.Replace(rhsStr, `_`, `?`, -1)
		//dsl = fmt.Sprintf(`{"bool" : {"must_not" : {"wildcard" : {"%v" : {"wildcard": "%v"}}}}}`, lhsStr, rhsStr)
		dsl = fmt.Sprintf(`{"bool" : {"must_not" : {"regexp" : {"%v" : "%v"}}}}`, lhsStr, rhsStr)
	default:
		err := fmt.Errorf(`esql: %s operator not supported in comparison clause`, comparisonExpr.Operator)
		return "", err
	}
	return dsl, nil
}

func (e *ESql) convertValExpr(expr sqlparser.Expr) (dsl string, err error) {
	switch expr.(type) {
	case *sqlparser.SQLVal:
		dsl = sqlparser.String(expr)
		dsl = strings.Trim(dsl, `'`)
	// ValTuple is not a pointer from sqlparser
	case sqlparser.ValTuple:
		dsl = sqlparser.String(expr)
	default:
		err = fmt.Errorf("esql: not supported rhs expression %T", expr)
		return "", err
	}
	return dsl, nil
}

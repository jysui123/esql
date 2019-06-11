package esql

import (
	"strconv"

	"github.com/xwb1989/sqlparser"
)

// ExecutionTimeStr ...
// used for special handling in cadence usage
var ExecutionTimeStr = "ExecutionTime"

// oppositeOperator ...
// used for invert operator when NOT is specified
var oppositeOperator = map[string]string{
	"=":                     "!=",
	"!=":                    "=",
	"<":                     ">=",
	"<=":                    ">",
	">":                     "<=",
	">=":                    "<",
	"<>":                    "=",
	"in":                    "not in",
	"like":                  "not like",
	"regexp":                "not regexp",
	"not in":                "in",
	"not like":              "like",
	"not regexp":            "regexp",
	sqlparser.IsNullStr:     sqlparser.IsNotNullStr,
	sqlparser.IsNotNullStr:  sqlparser.IsNullStr,
	sqlparser.BetweenStr:    sqlparser.NotBetweenStr,
	sqlparser.NotBetweenStr: sqlparser.BetweenStr,
}

// op2PainlessOp ...
// used for convert SQL operator to painless operator in HAVING expression
var op2PainlessOp = map[string]string{
	"=":  "==",
	"!=": "!==",
	"<":  "<",
	"<=": "<=",
	">":  ">",
	">=": ">=",
	"<>": "!==",
}

// fromZeroTimeExpr ...
// used for special handling in cadence usage
var fromZeroTimeExpr sqlparser.Expr = &sqlparser.SQLVal{Type: sqlparser.IntVal, Val: []byte(strconv.Itoa(0))}

// default sizes
var defaultPageSize = 1000
var defaultBucketNumber = 1000

var tieBreaker = "runID"

func defaultCadenceColNameReplacePolicy(colNameStr string) string {
	return "Attr." + colNameStr
}

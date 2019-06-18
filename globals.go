package esql

import (
	"strconv"

	"github.com/xwb1989/sqlparser"
)

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

// used for special handling in cadence usage
var fromZeroTimeExpr sqlparser.Expr = &sqlparser.SQLVal{Type: sqlparser.IntVal, Val: []byte(strconv.Itoa(0))}

// default sizes and identifiers used in cadence visibility
const (
	DefaultPageSize      = 1000
	DefaultBucketNumber  = 1000
	ESDefaultMaxPageSize = 10000
	TieBreaker           = "runID"
	RunID                = "runID"
	StartTime            = "StartTime"
	DomainID             = "domainID"
	WorkflowID           = "workflowID"
	ExecutionTime        = "ExecutionTime"
	TieBreakerOrder      = "desc"
	StartTimeOrder       = "desc"
)

func defaultCadenceColNameReplacePolicy(colNameStr string) string {
	return "Attr." + colNameStr
}

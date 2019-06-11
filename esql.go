package esql

import (
	"bytes"
	"encoding/json"
	"errors"

	"github.com/xwb1989/sqlparser"
)

// Replace ...
// esql use replace function to apply user colName replacing policy
type Replace func(string) string

// ESql ...
// ESql is used to hold necessary information that required in parsing
type ESql struct {
	whiteList    map[string]int
	replaceList  map[string]int
	replace      Replace
	cadence      bool
	pageSize     int
	bucketNumber int
}

// init ...
// Initialize ESql struct
// arguments:
//     whiteListArg: the white list that prevent any selection on columns outside it
//                   if pass in nil, esql won't any filtering
//     replaceListArg: any selected column name on this list will be replaced by the replace
//                   policy specified by replaceFuncArg
//     replaceFuncArg: the policy of column name replacement
//     cadenceArg: boolean that indicates whether to apply special handling for cadence visibility
//     pageSizeArg: default number of documents returned for a non-aggregation query
//     bucketNumberArg: default number of buckets returned for an aggregation query
func (e *ESql) init(whiteListArg []string, replaceListArg []string, replaceFuncArg Replace, cadenceArg bool, pageSizeArg int, bucketNumberArg int) {
	for _, colName := range replaceListArg {
		e.whiteList[colName] = 1
	}
	for _, colName := range whiteListArg {
		e.replaceList[colName] = 0
	}

	e.cadence = cadenceArg
	if replaceFuncArg != nil {
		// user specified replacing policy
		e.replace = replaceFuncArg
	} else if cadenceArg {
		// for candence usage, we have default policy
		e.replace = defaultCadenceColNameReplacePolicy
	}

	if pageSizeArg > 0 {
		e.pageSize = pageSizeArg
	} else {
		e.pageSize = DefaultPageSize
	}
	if bucketNumberArg > 0 {
		e.bucketNumber = bucketNumberArg
	} else {
		e.bucketNumber = DefaultBucketNumber
	}
}

// ConvertPretty ...
// Transform sql to elasticsearch dsl, and prettify the output json
// usage:
//     dsl, err := e.ConvertPretty(sql, pageParam1, pageParam2, ...)
// arguments:
//     sql: the sql query needs conversion in string format
//     domainID: used for cadence visibility. for non-cadence usage it is not used
// 	   pagination: variadic arguments that indicates es search_after for pagination
func (e *ESql) ConvertPretty(sql string, domainID string, pagination ...interface{}) (dsl string, err error) {
	dsl, err = e.Convert(sql, domainID, pagination)
	if err != nil {
		return dsl, err
	}

	var prettifiedDSLBytes bytes.Buffer
	err = json.Indent(&prettifiedDSLBytes, []byte(dsl), "", "  ")
	if err != nil {
		return "", err
	}

	return string(prettifiedDSLBytes.Bytes()), err
}

// Convert ...
// Transform sql to elasticsearch dsl string
// usage:
//     dsl, err := e.Convert(sql, pageParam1, pageParam2, ...)
// arguments:
//     sql: the sql query needs conversion in string format
//     domainID: used for cadence visibility. for non-cadence usage it is not used
//     pagination: variadic arguments that indicates es search_after for pagination
func (e *ESql) Convert(sql string, domainID string, pagination ...interface{}) (dsl string, err error) {
	stmt, err := sqlparser.Parse(sql)
	if err != nil {
		return "", err
	}

	//sql valid, start to handle
	switch stmt.(type) {
	case *sqlparser.Select:
		dsl, err = e.convertSelect(*(stmt.(*sqlparser.Select)), domainID, pagination)
	default:
		err = errors.New("esql: Queries other than select not supported")
	}

	if err != nil {
		return "", err
	}

	return dsl, nil
}

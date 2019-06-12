package esql

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/xwb1989/sqlparser"
)

// Replace ...
// esql use replace function to apply user colName replacing policy
type Replace func(string) string

// Filter ...
// esql use filter function to prevent user to select certain columns
// only accept column names that filter(colName) == true
type Filter func(string) bool

// ESql ...
// ESql is used to hold necessary information that required in parsing
type ESql struct {
	filter       Filter
	replace      Replace
	cadence      bool
	pageSize     int
	bucketNumber int
}

// Init ... Initialize ESql struct
// all members goes to default
func (e *ESql) Init() {
	e.pageSize = DefaultPageSize
	e.bucketNumber = DefaultBucketNumber
	e.cadence = false
	e.replace = nil
	e.filter = nil
}

// SetFilter ... set up user specified column name filter policy
func (e *ESql) SetFilter(filterArg Filter) {
	e.filter = filterArg
}

// SetReplace ... set up user specified column name replacement policy
func (e *ESql) SetReplace(replaceArg Replace) {
	e.replace = replaceArg
}

// SetCadence ... specify whether do special handling for cadence visibility
func (e *ESql) SetCadence(cadenceArg bool) {
	e.cadence = cadenceArg
}

// SetPageSize ... set the number of documents returned in a non-aggregation query
func (e *ESql) SetPageSize(pageSizeArg int) {
	e.pageSize = pageSizeArg
}

// SetBucketNum ... set the number of bucket returned in an aggregation query
func (e *ESql) SetBucketNum(bucketNumArg int) {
	e.bucketNumber = bucketNumArg
}

// ConvertPretty ...
// Transform sql to elasticsearch dsl, and prettify the output json
// usage:
//     dsl, err := e.ConvertPretty(sql, pageParam1, pageParam2, ...)
// arguments:
//     sql: the sql query needs conversion in string format
//     domainID: used for cadence visibility. for non-cadence usage just pass in empty string
// 	   pagination: variadic arguments that indicates es search_after for pagination
func (e *ESql) ConvertPretty(sql string, domainID string, pagination ...interface{}) (dsl string, err error) {
	dsl, err = e.Convert(sql, domainID, pagination...)
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
//     domainID: used for cadence visibility. for non-cadence usage just in pass empty string
//     pagination: variadic arguments that indicates es search_after for pagination
func (e *ESql) Convert(sql string, domainID string, pagination ...interface{}) (dsl string, err error) {
	stmt, err := sqlparser.Parse(sql)
	if err != nil {
		return "", err
	}

	//sql valid, start to handle
	switch stmt.(type) {
	case *sqlparser.Select:
		dsl, err = e.convertSelect(*(stmt.(*sqlparser.Select)), domainID, pagination...)
	default:
		err = fmt.Errorf(`esql: Queries other than select not supported`)
	}

	if err != nil {
		return "", err
	}

	return dsl, nil
}

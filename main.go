package esql

import (
	"bytes"
	"errors"

	"encoding/json"

	"github.com/xwb1989/sqlparser"
)

// ConvertPretty will transform sql to elasticsearch dsl, and prettify the output json
func (e *ESql) ConvertPretty(sql string) (dsl string, err error) {
	dsl, err = e.Convert(sql)
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

// Convert will transform sql to elasticsearch dsl string
func (e *ESql) Convert(sql string) (dsl string, err error) {
	stmt, err := sqlparser.Parse(sql)
	if err != nil {
		return "", err
	}

	//sql valid, start to handle
	switch stmt.(type) {
	case *sqlparser.Select:
		dsl, err = e.convertSelect(stmt.(*sqlparser.Select))
	default:
		err = errors.New("esql: Queries other than select not supported")
	}

	if err != nil {
		return "", err
	}

	return dsl, nil
}

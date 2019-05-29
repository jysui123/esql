package esql

import (
	"bufio"
	"os"
	"strconv"
	"testing"
)

var tableName = `inspections`
var testCases = `sqls.txt`
var testDsls = `dsls.txt`
var testDslsPretty = `dslsPretty.txt`
var groundTruth = ``

var whiteList = map[string]interface{}{
	"business_code":  []int{91111, 94102, 33309},
	"business_state": []string{"GA", "NH"},
}

func TestGenDSL(t *testing.T) {
	var e ESql
	e.init(whiteList)
	f, err := os.Open(testCases)
	if err != nil {
		t.Error("Fail to open testcase file")
	}
	scanner := bufio.NewScanner(f)
	scanner.Split(bufio.ScanLines)
	var sqls []string
	for scanner.Scan() {
		sqls = append(sqls, scanner.Text())
	}
	f.Close()
	f, err = os.Create(testDsls)
	fp, err := os.Create(testDslsPretty)
	if err != nil {
		t.Error("Fail to create dsl file")
	}
	for i, sql := range sqls {
		dsl, err := e.Convert(sql)
		if err != nil {
			t.Error(err)
		}
		f.WriteString(dsl)
		f.WriteString("\n")
		dslPretty, err := e.ConvertPretty(sql)
		if err != nil {
			t.Error(err)
		}
		fp.WriteString("\n**************************\n" + strconv.Itoa(i) + "th query\n")
		fp.WriteString(dslPretty)
	}
	f.Close()
	fp.Close()
}

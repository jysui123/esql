package esql

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
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
	fmt.Println("Start generating DSL ...")
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

	// optional: provide expected dsl to compare
	compareGroundTruthDsl := false
	var groundTruthDsls []string
	fg, err := os.Open(groundTruth)
	if err == nil {
		compareGroundTruthDsl = true
		gscanner := bufio.NewScanner(fg)
		gscanner.Split(bufio.ScanLines)
		for gscanner.Scan() {
			groundTruthDsls = append(groundTruthDsls, scanner.Text())
		}
		if len(groundTruthDsls) != len(sqls) {
			t.Error("number of ground truth dsl and sql test cases not match")
		}
	}

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

		// check ground truth dsls if provided
		if compareGroundTruthDsl {
			var dsljson, gtDsljson map[string]interface{}
			err = json.Unmarshal([]byte(dsl), &dsljson)
			if err != nil {
				t.Error(err)
			}
			err = json.Unmarshal([]byte(groundTruthDsls[i]), &gtDsljson)
			if err != nil {
				t.Error(err)
			}
			if !reflect.DeepEqual(dsljson, gtDsljson) {
				t.Error("generated dsl does not match with ground truth dsl", i)
			}
		}

		f.WriteString(dsl)
		f.WriteString("\n")
		dslPretty, err := e.ConvertPretty(sql)
		if err != nil {
			t.Error(err)
		}
		fp.WriteString("\n**************************\n" + strconv.Itoa(i+1) + "th query\n")
		fp.WriteString(dslPretty)
		fmt.Printf("query %d dsl generated\n", i+1)
	}
	f.Close()
	fp.Close()
	fmt.Println("DSL generated\n---------------------------------------------------------------------")
}

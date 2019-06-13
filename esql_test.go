package esql

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/olivere/elastic"
)

var tableName = `test`
var testCases = `sqls.txt`
var testCasesCadence = `sqlsCadence.txt`
var testDsls = `dsls.txt`
var testDslsCadence = `dslsCadence.txt`
var testDslsPretty = `dslsPretty.txt`
var testDslsPrettyCadence = `dslsPrettyCadence.txt`
var groundTruth = ``
var urlES = "http://localhost:9200"
var url = "http://localhost:9200/test0/_search"
var urlSQL = "http://localhost:9200/_xpack/sql/translate"
var index = "test0"
var notTestedKeywords = []string{"HAVING", "LIMIT", "LIKE", "REGEX"}

type TestDoc struct {
	ColA          string  `json:"colA,omitempty"`
	ColB          string  `json:"colB,omitempty"`
	ColC          string  `json:"colC,omitempty"`
	ColD          int64   `json:"colD,omitempty"`
	ColE          float64 `json:"colE,omitempty"`
	ExecutionTime int64   `json:"ExecutionTime,omitempty"`
	DomainID      string  `json:"DomainID,omitempty"`
	RunID         string  `json:"runID,omitempty"`
}

func compareRespGroup(respES *elastic.SearchResult, resp *elastic.SearchResult) error {
	var groupES, group map[string]interface{}
	if _, exist := respES.Aggregations["groupby"]; !exist {
		fmt.Printf("\tagg without group by, not covered in test module\n")
		return nil
	}
	err := json.Unmarshal(*respES.Aggregations["groupby"], &groupES)
	if err != nil {
		return err
	}
	err = json.Unmarshal(*resp.Aggregations["groupby"], &group)
	if err != nil {
		return err
	}
	bucketCounts := make(map[int]int)
	for _, bucket := range groupES["buckets"].([]interface{}) {
		if b, ok := bucket.(map[string]interface{}); ok {
			bucketCounts[int(b["doc_count"].(float64))]++
		} else {
			err = fmt.Errorf("parsing json error")
			return err
		}
	}
	for _, bucket := range group["buckets"].([]interface{}) {
		if b, ok := bucket.(map[string]interface{}); ok {
			bucketCounts[int(b["doc_count"].(float64))]--
		} else {
			err = fmt.Errorf("parsing json error")
			return err
		}
	}
	for _, v := range bucketCounts {
		if v != 0 {
			err = fmt.Errorf("bucket size not match")
			return err
		}
	}
	return nil
}

func compareResp(respES *elastic.SearchResult, resp *elastic.SearchResult) error {
	if respES.Hits.TotalHits != resp.Hits.TotalHits {
		err := fmt.Errorf(`get %v documents, but %v expected`, resp.Hits.TotalHits, respES.Hits.TotalHits)
		return err
	}
	fmt.Printf("\tget %v documents, document number matches\n", respES.Hits.TotalHits)

	docIDES := make(map[string]int)
	if respES.Hits.TotalHits > 0 {
		for _, hit := range respES.Hits.Hits {
			docIDES[hit.Id] = 0
		}
		// if orderby is specified, es sql dsl won't return document id
		if int64(len(docIDES)) < respES.Hits.TotalHits {
			return nil
		}
		for _, hit := range resp.Hits.Hits {
			if _, exist := docIDES[hit.Id]; !exist {
				err := fmt.Errorf(`document id not match`)
				return err
			}
			docIDES[hit.Id] = 1
		}
		for _, v := range docIDES {
			if v == 0 {
				err := fmt.Errorf(`document id not match`)
				return err
			}
		}
	}

	return nil
}

func TestSQL(t *testing.T) {
	fmt.Println("Test SQLs ...")

	// initilizations
	var e ESql
	e.Init()
	clientHTTP := &http.Client{Timeout: time.Second * 5}
	client, err := elastic.NewClient(elastic.SetURL(urlES))
	if err != nil {
		t.Error("Fail to create elastic client")
	}
	ctx := context.Background()

	// read in sql test cases
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

	f, err = os.Create(testDslsPretty)
	if err != nil {
		t.Error("Fail to create dslPretty file")
	}

	for i, sql := range sqls {
		fmt.Printf("testing %vth query...\n", i+1)
		// use es sql translate api to convert sql to dsl
		sqlQuery := fmt.Sprintf(`{"query": "%v"}`, sql)
		sqlReq, err := http.NewRequest("GET", urlSQL, bytes.NewBuffer([]byte(sqlQuery)))
		if err != nil {
			t.Errorf(`esql test: %vth sql translate query failed to create request struct: %v`, i+1, err)
		}
		sqlReq.Header.Add("Content-type", "application/json")
		sqlResp, err := clientHTTP.Do(sqlReq)
		if err != nil {
			t.Errorf(`esql test: %vth sql translate query failed: %v`, i+1, err)
		}
		sqlRespBody, err := ioutil.ReadAll(sqlResp.Body)
		if err != nil {
			t.Errorf(`esql test: %vth sql translate query failed to read body: %v`, i+1, err)
		}

		// use esql to translate sql to dsl
		dsl, err := e.Convert(sql, "0")
		if err != nil {
			t.Errorf(`esql test: %vth query convert fail: %v`, i+1, err)
		}
		dslPretty, err := e.ConvertPretty(sql, "0")
		if err != nil {
			t.Errorf(`esql test: %vth query convert pretty fail: %v`, i+1, err)
		}
		f.WriteString("\n**************************\n" + strconv.Itoa(i+1) + "th query\n")
		f.WriteString(dslPretty)
		fmt.Printf("\tquery dsl generated\n")

		// query with esql dsl
		resp, err := client.Search(index).Source(dsl).Do(ctx)
		if err != nil {
			t.Errorf(`esql test: %vth ESQL DSL query fail: %v`, i+1, err)
		}
		if resp.Error != nil {
			t.Errorf(`esql test: %vth ESQL DSL query fail with error: %v`, i+1, resp.Error)
		}
		fmt.Printf("\tgenerated dsl is syntactically correct\n")

		skip := false
		for _, k := range notTestedKeywords {
			if strings.Contains(sql, k) {
				skip = true
				fmt.Printf("\tquery contains %v, not covered in test module\n", k)
				break
			}
		}
		if skip {
			continue
		}

		sqlDsl := string(sqlRespBody)
		respES, err := client.Search(index).Source(sqlDsl).Do(ctx)
		if err != nil || respES.Error != nil {
			fmt.Printf("\tquery not covered in test module\n")
			continue
		}

		// compare query results
		if strings.Contains(dsl, "aggs") {
			if strings.Contains(dsl, "groupby") {
				err = compareRespGroup(respES, resp)
			} else {
				fmt.Printf("\tquery contains aggregations without group by, not covered in test module\n")
				continue
			}
		} else {
			err = compareResp(respES, resp)
		}
		if err != nil {
			t.Errorf(`esql test: %vth query results not match, %v`, i+1, err)
		} else {
			fmt.Printf("\tpassed\n")
		}
	}
}

func myfilter(s string) bool {
	sz := len(s)
	return sz == 4 && (s[sz-1] == 'A' || s[sz-1] == 'C' || s[sz-1] == 'B' || s[sz-1] == 'D' || s[sz-1] == 'E')
}

func TestGenCadenceDSL(t *testing.T) {
	fmt.Println("Test Generating DSL for Cadence...")
	var e ESql
	e.Init()
	e.SetCadence(true)
	e.SetPageSize(100)
	e.SetBucketNum(100)
	//e.SetFilter(myfilter)
	//e.SetReplace(defaultCadenceColNameReplacePolicy)

	f, err := os.Open(testCasesCadence)
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

	fp, err := os.Create(testDslsPrettyCadence)
	if err != nil {
		t.Error("Fail to create dsl file")
	}
	start := time.Now()
	for i, sql := range sqls {
		dslPretty, err := e.ConvertPretty(sql, "1", "1", 123)
		if err != nil {
			t.Error(err)
		}
		fp.WriteString("\n**************************\n" + strconv.Itoa(i+1) + "th query\n")
		fp.WriteString(dslPretty)
		fmt.Printf("query %d dsl generated\n", i+1)
	}
	elapsed := time.Since(start)
	fmt.Printf("Time taken to generate all dsls: %s", elapsed)
	fp.Close()
	fmt.Println("DSL Cadence generated\n---------------------------------------------------------------------")
}

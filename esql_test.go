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
var testCases = `testcases/sqls.txt`
var testCasesBenchmarkAll = `testcases/sqlsBm.txt`
var testCasesBenchmarkAgg = `testcases/sqlsBmAgg.txt`
var testCasesBenchmarkCadence = `testcases/sqlsBmCad.txt`
var testCasesCadence = `testcases/sqlsCadence.txt`
var testDsls = `testcases/dsls.txt`
var testDslsCadence = `testcases/dslsCadence.txt`
var testDslsPretty = `testcases/dslsPretty.txt`
var testDslsPrettyCadence = `testcases/dslsPrettyCadence.txt`
var testBenchmarkDsls = `testcases/dslsBm.txt`
var groundTruth = ``
var urlES = "http://localhost:9200"
var url = "http://localhost:9200/test0/_search"
var urlSQL = "http://localhost:9200/_xpack/sql/translate"
var index = "test0"
var notTestedKeywords = []string{"LIMIT", "LIKE", "REGEX"}

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
	buckNum := 0
	for _, bucket := range groupES["buckets"].([]interface{}) {
		if b, ok := bucket.(map[string]interface{}); ok {
			buckNum++
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
	fmt.Printf("\tquery return %v buckets\n", buckNum)
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
			docIDES[hit.Id]++
		}
		// if orderby is specified, es sql dsl won't return document id
		if int64(len(docIDES)) < respES.Hits.TotalHits {
			return nil
		}
		for _, hit := range resp.Hits.Hits {
			docIDES[hit.Id]--
		}
		for _, v := range docIDES {
			if v != 0 {
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

	sqls, err := readSQLs(testCases)
	if err != nil {
		t.Errorf("Fail to load testcases")
		return
	}

	f, err := os.Create(testDslsPretty)
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
	return s != "colE"
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

	sqls, err := readSQLs(testCases)
	if err != nil {
		t.Errorf("Fail to load testcases")
		return
	}

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

func readSQLs(fileName string) ([]string, error) {
	f, err := os.Open(fileName)
	if err != nil {
		return nil, err
	}
	scanner := bufio.NewScanner(f)
	scanner.Split(bufio.ScanLines)
	var sqls []string
	for scanner.Scan() {
		sqls = append(sqls, scanner.Text())
	}
	f.Close()
	return sqls, nil
}

func testBenchmark(t *testing.T, choice string, round int) {
	var e ESql
	e.Init()
	e.SetCadence(true)
	var fileName string
	//search := false
	switch choice {
	case "conversion":
		fileName = testCasesBenchmarkAll
	case "search":
		fileName = testCasesBenchmarkAll
		//search = true
	default:
		t.Errorf("wrong choice")
		return
	}

	sqls, err := readSQLs(fileName)
	if err != nil {
		t.Errorf("Fail to open testcase file %v", fileName)
		return
	}
	var dslEsql, dslElasticsql []string
	start := time.Now()
	for k := 0; k < round; k++ {
		for i, sql := range sqls {
			dsl, err := e.Convert(sql, "0")
			if err != nil {
				t.Errorf("Esql fail at %vth query: %v", i+1, err)
				return
			}
			if k == 0 {
				dslEsql = append(dslEsql, dsl)
			}
		}
	}
	elapsedEsql := time.Since(start)
	start = time.Now()
	for k := 0; k < round; k++ {
		for i, sql := range sqls {
			// fmt.Println(sql)
			dsl, err := getCustomizedDSLFromSQL(sql, "0")
			if err != nil {
				t.Errorf("Elasticsql fail at %vth query", i+1)
				return
			}
			if k == 0 {
				dslElasticsql = append(dslElasticsql, dsl)
			}
		}
	}
	elapsedElasticsql := time.Since(start)
	fmt.Printf("Convert %v sqls cost: esql: %v, elasticsql: %v\n", len(sqls)*round, elapsedEsql, elapsedElasticsql)
	f, _ := os.Create(testBenchmarkDsls)
	for _, dsl := range dslEsql {
		f.WriteString(dsl + "\n")
	}
}

func TestBenchmark(t *testing.T) {
	fmt.Println("Compare performance between esql and elasticsql  ... ")
	testBenchmark(t, "conversion", 1000)
}

// func TestBenchmarkAgg(t *testing.T) {
// 	fmt.Println("Compare performance between esql and elasticsql aggregations ... ")
// 	testBenchmark(t, "agg", 1000)
// }

// func TestBenchmarkCadence(t *testing.T) {
// 	fmt.Println("Compare performance between esql and elasticsql cadence special ... ")
// 	testBenchmark(t, "cadence", 1000)
// }

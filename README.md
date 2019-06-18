# ESQL: Translate SQL to Elasticsearch DSL

## Motivation
Currently [Cadence](https://github.com/uber/cadence) is using [elasticsql](https://github.com/cch123/elasticsql) to translate sql query. However it only support up to ES V2.x while Cadence is using ES V6.x. Beyond that, Cadence has some specific requirements that not supported by elasticsql yet.

Current Cadence query request processing steps are listed below:
- generate SQL from query
- use elasticsql to translate SQL to DSL
- ES V6.x does not support "missing" field, convert "missing" to "bool","must_not","exist" for ExecutionTime query if any
- complete "range" field for ExecutionTime query by adding {"gt": 0}
- add domain query
- key whitelist filtering
- delete some useless field like "from", "size"
- modify sorting field (add workflow id as sorting tie breaker)
- setup search after for pagination

**This project is based on elasticsql** and aims at dealing all these addtional processing steps and providing an api to generate DSL in one step for visibility usage in Cadence.


## Supported features
- [x] =, !=, <, >, <=, >=, <>, ()
- [x] ANT, OR, NOT
- [x] LIKE, IN, REGEX, IS NULL, BETWEEN
- [x] LIMIT, SIZE, DISTINCT
- [x] COUNT, COUNT(DISTINCT)
- [x] AVG, MAX, MIN, SUM
- [x] GROUP BY, ORDER BY
- [x] HAVING
- [x] column name selection filtering and replacing
- [x] special handling for cadence visibility


## Usage
Please refer to code and comments in `esql.go`. `esql.go` contains all the function apis that an outside user needs. Below shows a simple usage example:
~~~~
sql := "SELECT colA FROM myTable WHERE colB < 10"
// custom filter that only allows user to select columns start with "col"
func myfilter(colName string) bool {
    return strings.HasPrefix(colName, "col")
}
var e ESql
e.Init()                                // initialize
e.SetFilter(myfilter)                   // set up filtering policy
dsl, err := e.ConvertPretty(sql, "")    // convert sql to dsl
if err == nil {
    fmt.Println(dsl)
}
~~~~


## Testing Module
We are using elasticsearch's SQL translate API as a reference in testing. Testing contains 3 basic steps:
- using elasticsearch's SQL translate API to translate sql to dsl
- using our library to convert sql to dsl
- query local elasticsearch server with both dsls, check the results are identical

There are some specific features not covered in testing yet:
- `LIMIT` keyword: when order is not specified, identical queries with LIMIT can return different results
- `LIKE`, `REGEX` keyword: ES V6.5's sql api does not support regex search but only wildcard (only support shell wildcard `%` and `_`)
- Most aggregations are not thoroughly tested

Testing steps:
- download elasticsearch v6.5 (optional: kibana v6.5) and unzip
- run `chmod u+x start_service.sh test.sh`
- run `./start_service.sh <elasticsearch_path> <kibana_path>` to start a local elasticsearch server (by default, elasticsearch listens port 9200, kibana listens port 5600)
- optional: modify `sqls.txt` to add custom SQL queries as test cases
- optional: run `python gen_test_date.py -dcmi <number of documents> <missingRate>` to customize testing data set
- run `./test.sh` to run all the test cases
- generated dsls are stored in `dslsPretty.txt` for reference


## Improvement from elasticsql
|Item|detail|
|:-:|:-:|
|keyword IS|support standard SQL keywords IS NULL, IS NOT NULL for missing check|
|keyword NOT|support NOT, convert NOT recursively since elasticsearch's must_not is not the same as boolean operator NOT in sql|
|keyword LIKE|using "wildcard" tag, support SQL wildcard '%' and '_'|
|keyword REGEX|using "regexp" tag, support standard regular expression syntax|
|keyword GROUP BY|using "composite" tag to flatten multiple grouping|
|keyword ORDER BY|using "bucket_sort" to support order by aggregation functions|
|keyword HAVING|using "bucket_selector" and painless scripting language to support HAVING|
|aggregations|allow introducing aggregation functions from all HAVING, SELECT, ORDER BY|
|column name filtering|allow user pass an white list, when the sql query tries to select column out side white list, refuse the converting|
|column name replacing|allow user pass an function as initializing parameter, the matched column name will be replaced upon the policy|
|optimization|using "filter" tag rather than "must" tag to avoid scoring analysis and save time|
|optimization|no redundant {"bool": {"filter": xxx}} wrapped|all queries wrapped by {"bool": {"filter": xxx}}|
|optimization|does not return document contents in aggregation query|
|optimization|only return fields user specifies after SELECT|


## ES V2.x vs ES V6.5
|Item|ES V2.x|ES v6.5|
|:-:|:-:|:-:|
|missing check|{"missing": {"field": "xxx"}}|{"must_not": {"exist": {"field": "xxx"}}}|
|group by multiple columns|nested "aggs" field|"composite" flattened grouping|


## Attentions
- We assume all the text are store in type `keyword`, i.e., ES does not break the text into separated words. And we use tag `term` to match the whole text rather than a word in the text.
- `must_not` in ES does not share the same logic as `NOT` in sql
- If you want to apply aggregation on some fields, they should be in type `keyword` in ES (set type of a field by put mapping to your table)
- `COUNT(colName)` will include documents w/ null values in that column in ES SQL API, while in esql we exclude null valued documents
- ES SQL API and esql do not support `SELECT DISTINCT`, but we can achieve the same result by `SELECT * FROM table GROUP BY colName`
- ES SQL API does not support `ORDER BY aggregation`, esql support it by applying bucket_sort
- ES SQL API does not support `HAVING aggregation` that not show up in `SELECT`, esql support it
- To use regex query, the column should be `keyword` type, otherwise the regex is applied to all the terms produced by tokenizer from the original text rather than the original text

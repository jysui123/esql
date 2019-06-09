# ESQL: Translate SQL to Elasticsearch DSL

## Milestones

### M1
- [x] comparison operators: =, !=, <, >, <=, >=
- [x] boolean operators: AND, OR, NOT
- [x] parenthesis: ()
- [x] auto testing
- [x] setup git branch for pull request and code review
- [x] keyword: LIMIT, SIZE
- [x] depedencies management and golint checking

### M2
- [x] keyword: IS NULL, IS NOT NULL (missing check)
- [x] keyword: BETWEEN
- [x] keyword: IN, NOT IN
- [x] keyword: LIKE, NOT LIKE

### M3
- [x] aggregations
    - [x] COUNT
    - [x] AVG, SUM
    - [x] MIN, MAX
    - [x] COUNT DISTINCT
- [x] keyword: GROUP BY (column name)
- [x] resolve conflict between aggregations and GROUP BY
- [x] keyword: ORDER BY
    - [x] ORDER BY column name
    - [x] ORDER BY aggregation function
- [x] select specific columns
- [x] keyword: HAVING

### M4
- [ ] special handling: ExecutionTime field
- [ ] key whitelist filtering
- [ ] column name filtering
- [ ] pagination, search after
- [ ] select regex column names
- [ ] workflow ID as sorting tie breaker
- [ ] domain ID search

### M5
- [ ] TBD

### Misc
- [ ] optimization: docvalue_fields, term&keyword
- [ ] documentation
- [ ] test cases for unsupported and invalid queries
- [ ] ES functions (not in sql standard)
    - [ ] ES aggregation functions
    - [ ] GROUP BY ES aggregation functions: date_histogram, range, date_range
- [ ] ES pipeline aggregations


## Motivation
Currently we are using [elasticsql](https://github.com/cch123/elasticsql). However it only support up to ES V2.x while [Cadence](https://github.com/uber/cadence) is using ES V6.x. Beyond that, Cadence has some specific requirements that not supported by elasticsql yet.

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

**This project is based on [elasticsql](https://github.com/cch123/elasticsql)** and aims at dealing all these addtional processing steps and providing an api to generate DSL in one step for visibility usage in Cadence.


## Testing Module
We are using elasticsearch's SQL translate API as a reference in testing. Testing contains 3 basic steps:
- using elasticsearch's SQL translate API to translate sql to dsl
- using our library to convert sql to dsl
- query local elasticsearch server with both dsls, check the results are identical

There are some specific features not covered in testing yet:
- `LIMIT` keyword: when order is not specified, identical queries with LIMIT can return different results
- `LIKE` keyword: ES V6.5's sql api does not support regex search but only wildcard (only support shell wildcard `%` and `_`)

Testing steps:
- download elasticsearch v6.5 (optional: kibana v6.5) and unzip
- run `chmod u+x start_service.sh test_all.sh`
- run `./start_service.sh <elasticsearch_path> <kibana_path>` to start a local elasticsearch server
- optional: modify `sqls.txt` to add custom SQL queries as test cases
- optional: run `python gen_test_date.py -dcmi <number of documents> <missingRate>` to customize testing data set
- run `./test_all.sh` to run all the test cases
- generated dsls are stored in `dsls.txt` and `dslsPretty.txt` for reference


## esql vs elasticsql
|Item|esql|elasticsql|
|:-:|:-:|:-:|
|scoring|using "filter" to avoid scoring analysis and save time|using "must" which calculates scores|
|missing check|support IS NULL, IS NOT NULL|does not support IS NULL, using colName = missing which is not standard sql|
|NOT expression|support NOT, convert NOT recursively since elasticsearch's must_not is not the same as boolean operator NOT in sql|not supported|
|LIKE expression|using "regexp", support standard regex syntax|using "match_phrase", only support '%' and the smallest match unit is space separated word|
|group by multiple columns|"composite" flattened grouping|nested "aggs" field|
|order by aggregation function|use "bucket_sort" to order by aggregation functions, also do validation check|not supported|
|HAVING expression|supported, using "bucket_selector" and painless scripting language|not supported|
|optimization|no redundant {"bool": {"filter": xxx}} wrapped|all queries wrapped by {"bool": {"filter": xxx}}|
|optimization|does not return document contents in aggregation query|return all document contents|
|optimization|only return fields user specifies after SELECT|return all fields no matter what user specifies|


## ES V2.x vs ES V6.5
|Item|ES V2.x|ES v6.5|
|:-:|:-:|:-:|
|missing check|{"missing": {"field": "xxx"}}|{"must_not": {"exist": {"field": "xxx"}}}|
|group by multiple columns|nested "aggs" field|"composite" flattened grouping|


## Attentions
- `must_not` in ES does not share the same logic as `NOT` in sql
- if you want to apply aggregation on some fields, they should be in type `keyword` in ES (set type of a field by put mapping to your table)
- `COUNT(colName)` will include documents w/ null values in that column in ES SQL API, while in esql we exclude null valued documents
- ES SQL API does not support `SELECT DISTINCT`, but we can achieve the same result by `COUNT(DISTINCT colName)`
- ES SQL API does not support `ORDER BY aggregation`, esql support it by applying bucket_sort
- ES SQL API does not support `HAVING aggregation` that not show up in `SELECT`, esql support it

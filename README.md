# ESQL: Translate SQL to Elasticsearch DSL

## Milestones

### M1
- [x] comparison operators: =, !=, <, >, <=, >=
- [x] boolean operators: AND, OR, NOT
- [x] parenthesis: ()
- [x] auto testing
- [x] setup git branch for pull request and code review
- [x] keyword: LIMIT, SIZE
- [ ] depedencies management and golint checking

### M2
- [ ] keyword: IS NULL, IS NOT NULL (missing check)
- [ ] keyword: BETWEEN
- [ ] keyword: LIKE, NOT LIKE
- [ ] keyword: IN, NOT IN

### M3
- [ ] select specific columns
- [ ] keyword: ORDER BY
- [ ] keyword: GROUP BY

### M4
- [ ] special handling: ExecutionTime field
- [ ] key whitelist filtering
- [ ] column name filtering
- [ ] pagination, search after

### Misc
- [ ] optimization: docvalue_fields, term&keyword
- [ ] documentation
- [ ] test cases for unsupported and invalid queries


## Installing and Testing
- download elasticsearch v6.5 (optional: kibana v6.5) and unzip
- run `chmod u+x start_service.sh test_all.sh`
- run `./start_service.sh` to start a local elasticsearch server (may need modification to specify the path to your elasticseach executable)
- optional: modify `sqls.txt` to add custom SQL queries as test cases
- run `./test_all.sh` to run all the test cases
- generated dsls are stored in `dsls.txt` and `dslsPretty.txt`


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
- modify sorting field
- setup search after for pagination

This project aims at dealing all these addtional processing steps and providing an api to generate DSL in one step for visibility usage in Cadence.


## Testing Module
We are using elasticsearch's SQL translate API as a reference in testing. Testing contains 3 basic steps:
- using elasticsearch's SQL translate API to translate sql to dsl
- using our library to convert sql to dsl
- query local elasticsearch server with both dsls, check the results are identical

There are some specific features not covered in testing yet:
- `LIMIT` keyword: when order is not specified, identical queries with LIMIT can return different resutls


## ES V2.x vs ES V6.5
|Item|ES V2.x|ES v6.5|
|:-:|:-:|:-:|
|missing check|||
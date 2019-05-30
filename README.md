# ESQL: Translate SQL to Elasticsearch DSL

## Milestones

### M1
- [x] comparison operators: =, !=, <, >, <=, >=
- [x] boolean operators: AND, OR, NOT
- [x] parenthesis: ()
- [x] auto testing
- [ ] setup git branch for pull request and code review
- [ ] special handling: time format
- [ ] keyword: LIMIT, SIZE

### M2
- [ ] missing check
- [ ] keyword: BETWEEN
- [ ] keyword: LIKE, NOT LIKE
- [ ] keyword: IN, NOT IN

### M3
- [ ] select specific columns
- [ ] keyword: ORDER BY
- [ ] keyword: GROUP BY

### M4
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
- run `python gen_test_data.py -cmi` to create a test index and feed in test data
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

This project aims at dealing all these addtional processing steps and provides an api to generate DSL in one step for visibility usage in Cadence.


## ES V2.x vs ES V6.5
|Item|ES V2.x|ES v6.5|
|:-:|:-:|:-:|
|missing check|||
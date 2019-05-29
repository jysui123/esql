# ESQL: Translate SQL to Elasticsearch DSL

## Milestones

### M1
- [x] comparison operators: =, !=, <, >, <=, >=
- [x] boolean operators: AND, OR, NOT
- [x] parenthesis: ()
- [x] auto testing
- [ ] special handling: time format
- [ ] keyword: LIMIT, SIZE
- [ ] pagination, search after

### M2
- [ ] missing check
- [ ] keyword: BETWEEN
- [ ] keyword: LIKE, NOT LIKE
- [ ] keyword: IN, NOT IN

### M3
- [ ] keyword: FROM
- [ ] keyword: ORDER BY
- [ ] keyword: GROUP BY

### M4
- [ ] key whitelist filtering
- [ ] column name filtering

### Misc
- [ ] optimization: docvalue_fields, term&keyword
- [ ] documentation
- [ ] test cases for unsupported and invalid queries

## Install and Testing
- download elasticsearch v6.5 (optional: kibana v6.5) and unzip
- run `chmod u+x start_service.sh test_all.sh`
- run `./start_service.sh` to start a local elasticsearch server (may need modification to specify the path to your elasticseach executable)
- run `python gen_test_data.py -cmi` to create a test index and feed in test data
- optional: modify `sqls.txt` to add custom SQL queries as test cases
- run `./test_all.sh` to run all the test cases
- generated dsls are stored in `dsls.txt` and `dslsPretty.txt`

## ES V2.x vs ES V6.5
|Item|ES V2.x|es v6.5|
|:-:|:-:|:-:|
|missing check|||

# ESQL: Translate SQL to Elasticsearch DSL

## Milestones

### M1
Support following features:
- [x] comparison operators: =, !=, <, >, <=, >=
- [x] boolean operators: AND, OR, NOT
- [x] parenthesis: ()
- [ ] special handling: time format
- [ ] survey: docvalue_fields in elastic search api

### M2
- [ ] missing check
- [ ] keyword: BETWEEN
- [ ] keyword: LIKE, NOT LIKE
- [ ] keyword: IN, NOT IN

### M3
- [ ] keyword: ORDER BY
- [ ] keyword: GROUP BY
- [ ] keyword: LIMIT

### M4
- [ ] key whitelist filtering
- [ ] column name filtering

### Misc
- [ ] optimization
- [ ] documentation

## Install and Testing
- download elasticsearch v6.5 (optional: kibana v6.5) and unzip
- run `chmod u+x start_service.sh test_all.sh`
- run `./start_service.sh` to start a local elasticsearch server
- feed in some data to the server by either kibana or cli
- optional: modify `sqls.txt` to add custom SQL queries as test cases
- run `./test_all.sh` to run all the test cases
- generated dsl are stored in `dsls.txt` and `dslsPretty.txt`

## ESV2.x to ESV6.5
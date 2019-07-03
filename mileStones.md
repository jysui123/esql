
# Milestones

## M1 Basic sql - 1
- [x] comparison operators: =, !=, <, >, <=, >=
- [x] boolean operators: AND, OR
- [x] parenthesis: ()
- [x] auto testing
- [x] setup git branch for pull request and code review
- [x] keyword: LIMIT, SIZE
- [x] depedencies management and golint checking

## M2 Basic sql - 2
- [x] keyword: IS NULL, IS NOT NULL (missing check)
- [x] keyword: NOT
- [x] keyword: BETWEEN
- [x] keyword: IN, NOT IN
- [x] keyword: LIKE, NOT LIKE
- [x] keyword: REGEX, NOT REGEX

## M3 Aggregation
- [x] aggregation functions
    - [x] COUNT, COUNT DISTINCT
    - [x] AVG, SUM, MIN, MAX
- [x] keyword: GROUP BY (column name)
- [x] resolve conflict between aggregations and GROUP BY
- [x] keyword: ORDER BY
    - [x] ORDER BY column name
    - [x] ORDER BY aggregation function
- [x] select specific columns
- [x] keyword: HAVING

## M4 Cadence Special Handling
- [x] add lower bound to ExecutionTime field
- [x] column name filtering and replacing
- [x] pagination, search after (avoid using magic "size"=1000)
- [x] run ID as sorting tie breaker
- [x] domain ID search
- [x] test cadence special handlings

## M5 Benchmark
- [x] measure the dsl generation speed
- [x] measure the generated dsl processing speed in ES
- [x] compare the speed between esql and elasticsql

## M6 Integration w/ Cadence
- [x] fork cadence code and run locally
- [x] replace elasticsql by esql (mainly list workflow api)
- [x] remove redundant code

## M7 Cadence Report
- [ ] use snapshot to pull cadence data to local fork
- [ ] add a new command to Cadence CLI
- [ ] use aggregation of esql to process some straightforward report query
- [ ] show report result in frontend in markdown

## Misc
- [ ] optimization: docvalue_fields, term&keyword
- [ ] documentation
- [ ] test cases for unsupported and invalid queries
- [ ] ES functions (not in sql standard)
    - [ ] ES aggregation functions
    - [ ] GROUP BY ES aggregation functions: date_histogram, range, date_range
- [ ] ES pipeline aggregations
- [ ] aggregation report - what could be interesting in report


## temp
record some of the testing results
zookeeper, kafka, cassandra, es, kibana
template: schema/es/visibility/index_template.json
config:
    config/development.yaml: elasticsearch:
                             enable: true
    service/frontend/service.go: newconfig: EnableReadVisibilityFromES:          dc.GetBoolPropertyFnWithDomainFilter(dynamicconfig.EnableReadVisibilityFromES, true)




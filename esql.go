package esql

// Replace ...
// esql use replace function to apply user colName replacing policy
type Replace func(string) string

// ESql ...
// ESql is used to hold necessary information that required in parsing
type ESql struct {
	whiteList    map[string]int
	replace      Replace
	cadence      bool
	pageSize     int
	bucketNumber int
}

func (e *ESql) init(whiteListArg []string, replaceListArg []string, replaceFuncArg Replace, cadenceArg bool, pageSizeArg int, bucketNumberArg int) {
	for _, colName := range replaceListArg {
		e.whiteList[colName] = 1
	}
	for _, colName := range whiteListArg {
		e.whiteList[colName] = 0
	}

	e.cadence = cadenceArg
	if replaceFuncArg != nil {
		// user specified replacing policy
		e.replace = replaceFuncArg
	} else if cadenceArg {
		// for candence usage, we have default policy
		e.replace = defaultCadenceColNameReplacePolicy
	}

	if pageSizeArg > 0 {
		e.pageSize = pageSizeArg
	} else {
		e.pageSize = defaultPageSize
	}
	if bucketNumberArg > 0 {
		e.bucketNumber = bucketNumberArg
	} else {
		e.bucketNumber = defaultBucketNumber
	}
}

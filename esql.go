package esql

// Replace ...
// esql use replace function to apply user colName replacing policy
type Replace func(string) string

// ESql ...
// ESql is used to hold necessary information that required in parsing
type ESql struct {
	whiteList map[string]int
	replace   Replace
	cadence   bool
}

func (e *ESql) init(whiteListArg []string, replaceArg Replace, cadenceArg bool) {
	for _, colName := range whiteListArg {
		e.whiteList[colName] = 1
	}
	e.replace = replaceArg
	e.cadence = cadenceArg
}

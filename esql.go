package esql

// ESql is used to hold necessary information that required in parsing
type ESql struct {
	whiteList map[string]interface{}
	cadence   bool
}

func (e *ESql) init(whiteListArg map[string]interface{}, cadenceArg bool) {
	e.whiteList = whiteListArg
	e.cadence = cadenceArg
}

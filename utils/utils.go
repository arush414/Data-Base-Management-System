package utils

import (
	"fmt"
)

//assert function, prints message when condition fails
func Assert(condition bool, message string) {
	if !condition {
		panic(fmt.Sprintf("Condition Failed %s\n", message))
	}
}
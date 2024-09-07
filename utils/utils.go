package utils

import (
	"fmt"
	"testing"
)

//assert function, prints message when condition fails
func Assert(condition bool, message string) {
	if !condition {
		panic(fmt.Sprintf("Condition Failed %s\n", message))
	}
}

// Equal checks if two values are equal.
func Equal(t *testing.T, got, want interface{}) {
    if got != want {
        t.Helper()
        t.Fatalf("got %v, want %v", got, want)
    }
}
package util

import "fmt"

type ArrayFlags []string

func (a *ArrayFlags) String() string {
	return fmt.Sprint(*a)
}

func (a *ArrayFlags) Set(value string) error {
	*a = append(*a, value)
	return nil
}

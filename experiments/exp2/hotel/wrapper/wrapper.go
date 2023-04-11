package wrapper

import (
  "github.com/eniac/Beldi/pkg/beldilib"
  "math/rand"
  "strconv"
)

var FAILURE_RATE = "0.0"

type OutputWrapper struct {
	Status string
	Output interface{}
}

func Wrapper(f func(env *beldilib.Env) interface{}) func(iw interface{}) (OutputWrapper, error) {
	return func(raw interface{}) (OutputWrapper, error) {
		failure_rate, err := strconv.ParseFloat(FAILURE_RATE, 64)
		if err != nil {
			panic(err)
		}
  	if rand.Float64() < failure_rate {
  		 return OutputWrapper{
			 	 Status: "Failure",
			 	 Output: "Intentional failure",
			 }, nil
  	}
		iw := beldilib.ParseInput(raw)
		env := beldilib.PrepareEnv(iw)
		output := f(env)
		return OutputWrapper{
			Status: "Success",
			Output: output,
		}, nil
	}
}

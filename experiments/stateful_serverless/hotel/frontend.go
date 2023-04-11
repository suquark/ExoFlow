package main

import (
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/eniac/Beldi/internal/hotel-workflow/main/wrapper"
	"github.com/eniac/Beldi/pkg/beldilib"
)

func Handler(env *beldilib.Env) interface{} {
  env.TxnId = env.InstanceId
	return *env
}

func main() {
	lambda.Start(wrapper.Wrapper(Handler))
}

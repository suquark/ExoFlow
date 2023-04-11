package main

import (
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/eniac/Beldi/internal/hotel/main/data"
	"github.com/eniac/Beldi/internal/hotel-workflow/main/wrapper"
	"github.com/eniac/Beldi/pkg/beldilib"
)

func Handler(env *beldilib.Env) interface{} {
  req := env.Input.(map[string]interface{})
  ok := wrapper.TxnLock("hotel", env.TxnId, data.Thotel(), req["hotelId"].(string))
  return wrapper.TxnResult{TxnId: env.TxnId, Ok: ok}
}

func main() {
	lambda.Start(wrapper.Wrapper(Handler))
}

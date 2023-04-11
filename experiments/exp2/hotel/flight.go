package main

import (
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/eniac/Beldi/internal/hotel/main/data"
	"github.com/eniac/Beldi/internal/hotel-workflow/main/wrapper"
	"github.com/eniac/Beldi/pkg/beldilib"
	"github.com/mitchellh/mapstructure"
)

type Flight struct {
	FlightId  string
	Cap       int32
	Customers []string
}

func ReserveFlight(env *beldilib.Env, flightId string, userId string) bool {
	ok, item := wrapper.TPLRead(env, data.Tflight(), flightId)
	if !ok {
		return false
	}
	var flight Flight
	beldilib.CHECK(mapstructure.Decode(item, &flight))
	if flight.Cap == 0 {
		return false
	}
	ok = beldilib.TPLWrite(env, data.Tflight(), flightId,
		aws.JSONValue{"V.Cap": flight.Cap})
	return ok
}

func Handler(env *beldilib.Env) interface{} {
  req := env.Input.(map[string]interface{})
	ok := ReserveFlight(env, req["flightId"].(string), req["userId"].(string))
	return wrapper.TxnResult{TxnId: env.TxnId, Ok: ok}
}

func main() {
	lambda.Start(wrapper.Wrapper(Handler))
}

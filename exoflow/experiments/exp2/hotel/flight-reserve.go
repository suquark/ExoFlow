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

func ReserveFlight(txnId string, flightId string, userId string) bool {
  item := wrapper.Read(data.Tflight(), flightId)
	var flight Flight
	beldilib.CHECK(mapstructure.Decode(item, &flight))
	if flight.Cap == 0 {
		return false
	}
	wrapper.TxnWrite("flight", txnId, data.Tflight(), flightId, aws.JSONValue{"V.Cap": flight.Cap})
	return true
}

func Handler(env *beldilib.Env) interface{} {
  req := env.Input.(map[string]interface{})
  var acquired wrapper.TxnResult
  beldilib.CHECK(mapstructure.Decode(req["acquired"], &acquired))
  if !acquired.Ok {
    return acquired
  }
  acquired.Ok = ReserveFlight(acquired.TxnId, req["flightId"].(string), req["userId"].(string))
  return acquired
}

func main() {
	lambda.Start(wrapper.Wrapper(Handler))
}

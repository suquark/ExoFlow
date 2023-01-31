package main

import (
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
	"github.com/eniac/Beldi/internal/hotel/main/data"
	"github.com/eniac/Beldi/internal/hotel-workflow/main/wrapper"
	"github.com/eniac/Beldi/pkg/beldilib"
	"github.com/lithammer/shortuuid"
	"github.com/mitchellh/mapstructure"
)

type Order struct {
	OrderId  string
	FlightId string
	HotelId  string
	UserId   string
}

func PlaceOrder(env *beldilib.Env, userId string, flightId string, hotelId string) {
	orderId := shortuuid.New()
	beldilib.Write(env, data.Torder(), orderId,
		map[expression.NameBuilder]expression.OperandBuilder{expression.Name("V"): expression.Value(Order{
			OrderId: orderId, FlightId: flightId, HotelId: hotelId, UserId: userId,
		})})
}

func Handler(env *beldilib.Env) interface{} {
	req := env.Input.(map[string]interface{})

	var reserveHotel wrapper.TxnResult
	var reserveFlight wrapper.TxnResult
  beldilib.CHECK(mapstructure.Decode(req["reserveHotel"], &reserveHotel))
  beldilib.CHECK(mapstructure.Decode(req["reserveFlight"], &reserveFlight))

	if !reserveHotel.Ok || !reserveFlight.Ok {
		wrapper.CommitOrAbortTxn("hotel", reserveHotel.TxnId, false)
		wrapper.CommitOrAbortTxn("flight", reserveFlight.TxnId, false)
		return "Place Order Fails"
	}
	wrapper.CommitOrAbortTxn("hotel", reserveHotel.TxnId, true)
	wrapper.CommitOrAbortTxn("flight", reserveFlight.TxnId, true)
	PlaceOrder(env, req["userId"].(string), req["flightId"].(string), req["hotelId"].(string))
	return "Place Order Success"
}

func main() {
	lambda.Start(wrapper.Wrapper(Handler))
}

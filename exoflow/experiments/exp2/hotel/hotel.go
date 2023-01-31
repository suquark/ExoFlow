package main

import (
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/eniac/Beldi/internal/hotel/main/data"
	"github.com/eniac/Beldi/internal/hotel-workflow/main/wrapper"
	"github.com/eniac/Beldi/pkg/beldilib"
	"github.com/mitchellh/mapstructure"
)

type Hotel struct {
	HotelId   string
	Cap       int32
	Customers []string
}

func ReserveHotel(env *beldilib.Env, hotelId string, userId string) bool {
	ok, item := wrapper.TPLRead(env, data.Thotel(), hotelId)
	if !ok {
		return false
	}
	var hotel Hotel
	beldilib.CHECK(mapstructure.Decode(item, &hotel))
	if hotel.Cap == 0 {
		return false
	}
	ok = beldilib.TPLWrite(env, data.Thotel(), hotelId,
		aws.JSONValue{"V.Cap": hotel.Cap})
	return ok
}

func Handler(env *beldilib.Env) interface{} {
  req := env.Input.(map[string]interface{})
	ok := ReserveHotel(env, req["hotelId"].(string), req["userId"].(string))
	return wrapper.TxnResult{TxnId: env.TxnId, Ok: ok}
}

func main() {
	lambda.Start(wrapper.Wrapper(Handler))
}

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

func ReserveHotel(txnId string, hotelId string, userId string) bool {
  item := wrapper.Read(data.Thotel(), hotelId)
	var hotel Hotel
	beldilib.CHECK(mapstructure.Decode(item, &hotel))
	if hotel.Cap == 0 {
		return false
	}
	wrapper.TxnWrite("hotel", txnId, data.Thotel(), hotelId, aws.JSONValue{"V.Cap": hotel.Cap})
	return true
}

func Handler(env *beldilib.Env) interface{} {
  req := env.Input.(map[string]interface{})
  var acquired wrapper.TxnResult
  beldilib.CHECK(mapstructure.Decode(req["acquired"], &acquired))
  if !acquired.Ok {
    return acquired
  }
	acquired.Ok = ReserveHotel(acquired.TxnId, req["hotelId"].(string), req["userId"].(string))
  return acquired
}

func main() {
	lambda.Start(wrapper.Wrapper(Handler))
}

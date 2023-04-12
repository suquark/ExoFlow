package main

import (
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/eniac/Beldi/internal/hotel/main/data"
	"github.com/eniac/Beldi/internal/hotel-workflow/main/wrapper"
	"github.com/eniac/Beldi/pkg/beldilib"
	"github.com/mitchellh/mapstructure"
)

type Request struct {
	HotelIds []string
	Locale   string
}

type Result struct {
	Hotels []data.Hotel
}

func GetProfiles(req Request) Result {
	var hotels []data.Hotel
	for _, i := range req.HotelIds {
		hotel := data.Hotel{}
		res := wrapper.Read(data.Tprofile(), i)
		err := mapstructure.Decode(res, &hotel)
		beldilib.CHECK(err)
		hotels = append(hotels, hotel)
	}
	return Result{Hotels: hotels}
}

func Handler(env *beldilib.Env) interface{} {
	req := Request{}
	err := mapstructure.Decode(env.Input, &req)
	beldilib.CHECK(err)
	return GetProfiles(req)
}

func main() {
	lambda.Start(wrapper.Wrapper(Handler))
}

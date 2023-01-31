package main

import (
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/eniac/Beldi/pkg/beldilib"
	"github.com/eniac/Beldi/internal/hotel/main/rate"
	"github.com/eniac/Beldi/internal/hotel-workflow/main/wrapper"
	"github.com/mitchellh/mapstructure"
)

type Result struct {
	HotelIds []string
}

func Handler(env *beldilib.Env) interface{} {
	req := rate.Result{}
	err := mapstructure.Decode(env.Input, &req)
	beldilib.CHECK(err)
	var hts []string
	for _, r := range req.RatePlans {
		hts = append(hts, r.HotelId)
	}
	return aws.JSONValue{"search": Result{HotelIds: hts}}
}

func main() {
	lambda.Start(wrapper.Wrapper(Handler))
}

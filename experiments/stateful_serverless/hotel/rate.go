package main

import (
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/eniac/Beldi/internal/hotel/main/data"
	"github.com/eniac/Beldi/internal/hotel-workflow/main/wrapper"
	"github.com/eniac/Beldi/pkg/beldilib"
	"github.com/mitchellh/mapstructure"
	"sort"
)

type RatePlans []data.RatePlan

func (r RatePlans) Len() int {
	return len(r)
}

func (r RatePlans) Swap(i, j int) {
	r[i], r[j] = r[j], r[i]
}

func (r RatePlans) Less(i, j int) bool {
	return r[i].RoomType.TotalRate > r[j].RoomType.TotalRate
}

type Request struct {
	HotelIds []string
	Indate   string
	Outdate  string
}

type Result struct {
	RatePlans []data.RatePlan
}

func GetRates(req Request) Result {
	var plans RatePlans
	for _, i := range req.HotelIds {
		plan := data.RatePlan{}
		res := wrapper.Read(data.Trate(), i)
		err := mapstructure.Decode(res, &plan)
		beldilib.CHECK(err)
		if plan.HotelId != "" {
			plans = append(plans, plan)
		}
	}
	sort.Sort(plans)
	return Result{RatePlans: plans}
}

func Handler(env *beldilib.Env) interface{} {
	req := Request{}
	err := mapstructure.Decode(env.Input, &req)
	beldilib.CHECK(err)
	return GetRates(req)
}

func main() {
	lambda.Start(wrapper.Wrapper(Handler))
}

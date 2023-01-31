package main

import (
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/eniac/Beldi/internal/hotel/main/data"
	"github.com/eniac/Beldi/internal/hotel-workflow/main/wrapper"
	"github.com/eniac/Beldi/pkg/beldilib"
	"github.com/hailocab/go-geoindex"
	"github.com/mitchellh/mapstructure"
	"math"
)

type Request struct {
	Require string
	Lat     float64
	Lon     float64
}

type Result struct {
	HotelIds []string
}

func LoadRecommendations() []data.Recommend {
	var recommends []data.Recommend
	res := wrapper.Scan(data.Trecommendation())
	err := mapstructure.Decode(res, &recommends)
	beldilib.CHECK(err)
	return recommends
}

func GetRecommendations(req Request) Result {
	hotels := LoadRecommendations()
	res := Result{HotelIds: []string{}}
	switch req.Require {
	case "dis":
		p1 := &geoindex.GeoPoint{
			Pid:  "",
			Plat: req.Lat,
			Plon: req.Lon,
		}
		min := math.MaxFloat64
		for _, hotel := range hotels {
			tmp := float64(geoindex.Distance(p1, &geoindex.GeoPoint{
				Pid:  "",
				Plat: hotel.HLat,
				Plon: hotel.HLon,
			})) / 1000
			if tmp < min {
				min = tmp
			}
		}
		for _, hotel := range hotels {
			tmp := float64(geoindex.Distance(p1, &geoindex.GeoPoint{
				Pid:  "",
				Plat: hotel.HLat,
				Plon: hotel.HLon,
			})) / 1000
			if tmp == min {
				res.HotelIds = append(res.HotelIds, hotel.HId)
			}
		}
	case "rate":
		max := 0.0
		for _, hotel := range hotels {
			if hotel.HRate > max {
				max = hotel.HRate
			}
		}
		for _, hotel := range hotels {
			if hotel.HRate == max {
				res.HotelIds = append(res.HotelIds, hotel.HId)
			}
		}
	case "price":
		min := math.MaxFloat64
		for _, hotel := range hotels {
			if hotel.HPrice < min {
				min = hotel.HPrice
			}
		}
		for _, hotel := range hotels {
			if hotel.HPrice == min {
				res.HotelIds = append(res.HotelIds, hotel.HId)
			}
		}
	default:
		panic("no such requirement")
	}
	return res
}

func Handler(env *beldilib.Env) interface{} {
	req := Request{}
	err := mapstructure.Decode(env.Input, &req)
	beldilib.CHECK(err)
	res := GetRecommendations(req)
	return aws.JSONValue{"recommend": res}
}

func main() {
	lambda.Start(wrapper.Wrapper(Handler))
}

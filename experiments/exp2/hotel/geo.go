package main

import (
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/eniac/Beldi/internal/hotel/main/data"
	"github.com/eniac/Beldi/internal/hotel-workflow/main/wrapper"
	"github.com/eniac/Beldi/pkg/beldilib"
	"github.com/hailocab/go-geoindex"
	"github.com/mitchellh/mapstructure"
)

type Request struct {
	Lat float64
	Lon float64
}

type Result struct {
	HotelIds []string
}

func newGeoIndex() *geoindex.ClusteringIndex {
	var ps []data.Point
	res := wrapper.Scan(data.Tgeo())
	err := mapstructure.Decode(res, &ps)
	beldilib.CHECK(err)
	index := geoindex.NewClusteringIndex()
	for _, e := range ps {
		index.Add(e)
	}
	return index
}

func getNearbyPoints(lat float64, lon float64) []geoindex.Point {
	center := &geoindex.GeoPoint{
		Pid:  "",
		Plat: lat,
		Plon: lon,
	}
	index := newGeoIndex()
	res := index.KNearest(
		center,
		5,
		geoindex.Km(10), func(p geoindex.Point) bool {
			return true
		},
	)
	return res
}

func Nearby(req Request) Result {
	var (
		points = getNearbyPoints(req.Lat, req.Lon)
	)
	res := Result{HotelIds: []string{}}
	for _, p := range points {
		res.HotelIds = append(res.HotelIds, p.Id())
	}
	return res
}

func Handler(env *beldilib.Env) interface{} {
	req := Request{}
	err := mapstructure.Decode(env.Input, &req)
	beldilib.CHECK(err)
	return Nearby(req)
}

func main() {
	lambda.Start(wrapper.Wrapper(Handler))
}

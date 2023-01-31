package main

import (
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/eniac/Beldi/internal/hotel/main/data"
	"github.com/eniac/Beldi/internal/hotel-workflow/main/wrapper"
	"github.com/eniac/Beldi/pkg/beldilib"
	"github.com/mitchellh/mapstructure"
)

type Request struct {
	Username string
	Password string
}

type Result struct {
	Correct bool
}

func CheckUser(req Request) Result {
	var user data.User
	item := wrapper.Read(data.Tuser(), req.Username)
	err := mapstructure.Decode(item, &user)
	beldilib.CHECK(err)
	return Result{Correct: req.Password == user.Password}
}

func Handler(env *beldilib.Env) interface{} {
	req := Request{}
	err := mapstructure.Decode(env.Input, &req)
	beldilib.CHECK(err)
	return CheckUser(req)
}

func main() {
	lambda.Start(wrapper.Wrapper(Handler))
}

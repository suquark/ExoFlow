require "socket"
local JSON = require("JSON")
local UUID = require("uuid")
UUID.randomseed(socket.gettime() * 1000)
math.randomseed(socket.gettime() * 1000)

local function uuid()
    return UUID():gsub('-', '')
end

local gatewayPath = os.getenv("ENDPOINT")
local apiName = os.getenv("API")

local function reserve_all()
    local method = "GET"
    local path = gatewayPath

    local param = {
        InstanceId = uuid(),
        CallerName = "",
        Async = true,
        Input = {
            Function = apiName,
            Input = {
                userId = "user1",
                hotelId = tostring(math.random(0, 99)),
                flightId = tostring(math.random(0, 99)),
            }
        }
    }
    local body = JSON:encode(param)

    local headers = {}
    headers["Content-Type"] = "application/json"
    return wrk.format(method, path, headers, body)
end

request = function()
    return reserve_all()
end

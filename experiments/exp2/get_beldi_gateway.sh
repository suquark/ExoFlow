#!/bin/bash

aws apigatewayv2 get-apis --query 'Items[0].ApiEndpoint' --output text

#!/bin/bash

aws apigatewayv2 get-apis --query 'Items[-1].ApiEndpoint' --output text

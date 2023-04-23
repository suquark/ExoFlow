#!/bin/bash

cmd () {
  # https://linuxize.com/post/bash-functions/
  docker exec -w /root/beldi -it beldi bash -ic "$*"
}

echo '========= Setting up Docker ========='
# cleanup
docker stop beldi &> /dev/null
docker rm beldi &> /dev/null
# pull the docker and keep it running in the background
docker run -td -v /exoflow/experiments/stateful_serverless:/stateful_serverless --name=beldi tauta/beldi:latest
container_id=$(docker ps -aqf "name=beldi")
# docker exec beldi <command>
echo

echo '========= Setting up Credentials ========='
cmd "mkdir -p ~/.aws"
docker cp ~/.aws/credentials $container_id:/root/.aws/credentials
cmd "chmod 600 ~/.aws/credentials"
echo

echo '========= Compiling ========='
cmd "make clean && make hotel"
cmd make -f /stateful_serverless/Makefile hotel
echo

echo "========= Deploying ========="
cmd sls deploy -c hotel.yml
cmd make -f /stateful_serverless/Makefile deploy
echo

echo "========= Initializing Database (DynamoDB) ========="
cmd make -f /stateful_serverless/Makefile init_data
python /exoflow/experiments/stateful_serverless/init_dynamodb_table.py
echo

# increase the concurrent execution quota (by default 10, Beldi requires 1000)
# https://us-east-1.console.aws.amazon.com/servicequotas/home/services/lambda/quotas
# make sure dynamodb autoscaling (by default yes)
# https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/AutoScaling.Console.html

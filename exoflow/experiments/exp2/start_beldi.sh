#!/bin/bash

cmd () {
  # https://linuxize.com/post/bash-functions/
  docker exec -w /root/beldi -it beldi bash -ic "$*"
}

docker run -v ~/efs/exp2:/exp2 --name=beldi -it tauta/beldi:latest /bin/bash
# docker exec beldi <command>
cmd "mkdir -p ~/.aws && vim ~/.aws/credentials && chmod 600 ~/.aws/credentials"
cmd "make clean && make hotel"
cmd make -f /exp2/Makefile hotel
cmd make -f /exp2/Makefile deploy
cmd make -f /exp2/Makefile init_data

# increase the concurrent execution quota (by default 10, Beldi requires 1000)
# https://us-east-1.console.aws.amazon.com/servicequotas/home/services/lambda/quotas
# make sure dynamodb autoscaling (by default yes)
# https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/AutoScaling.Console.html

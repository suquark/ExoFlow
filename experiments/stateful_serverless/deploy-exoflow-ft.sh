#!/bin/bash

cmd () {
  # https://linuxize.com/post/bash-functions/
  docker exec -w /root/beldi -it beldi bash -ic "$*"
}

echo '========= Compiling ========='
cmd make -f /stateful_serverless/Makefile hotel_failure
echo -e 'Done!🎉\n'

echo "========= Deploying ExoFlow Fault Tolerance Lambda Functions ========="
cmd make -f /stateful_serverless/Makefile deploy
echo -e 'Done!🎉\n'

#!/bin/bash

aresConfig(){
   cd $BASE_DEPLOY_PATH/nodeConf/service_config/data/ares
   bits=`getconf LONG_BIT`
   if [ $bits = 64 ]
   then
       cp libWordSegJni64.so libWordSegJni.so
   else
       cp libWordSegJni32.so libWordSegJni.so
   fi
}


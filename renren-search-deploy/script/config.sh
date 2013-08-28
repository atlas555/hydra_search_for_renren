#/bin/bash

BASE_PATH=`dirname $PWD`
source ${BASE_PATH}/bin/config.sh
SCRIPT_PATH=${BASE_DEPLOY_PATH}/manager/script

serverManagerConf(){
    cp settings.py.tp settings.py
    sed -i "s#USERNAME_TP#$USERNAME#g" settings.py
    sed -i "s#REGISTRY_TP#$REGISTRY#g" settings.py
    sed -i "s#SCRIPT_PATH_TP#$SCRIPT_PATH#g" settings.py
    sed -i "s#BROKERS_TP#$BROKERS#g" settings.py
    sed -i "s#SEARCHERS_TP#$SEARCHERS#g" settings.py
    sed -i "s#NUM_PARTITION_TP#$KFK_COMMON_PATITION_COUNT#g" settings.py
    sed -i "s#NODE_TP#$NODE#g" settings.py
    sed -i "s#PARTITIONS_TP#$PARTITIONS#g" settings.py
    sed -i "s#BASE_DEPLOY_PATH_TP#$BASE_DEPLOY_PATH#g" settings.py
    sed -i "s#USERNAME_TP#$USERNAME#g" settings.py
    sed -i "s#BUSINESS_TP#$BUSINESS#g" settings.py
    sed -i "s#ZKADDRESS_TP#$ZKADDRESS#g" settings.py
    sed -i "s#LIB_PATH_TP#$CLASSPATH#g" settings.py
}

serverManagerConf

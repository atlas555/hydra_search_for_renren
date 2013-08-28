#!/bin/bash

source ./config.sh
source ./util.sh

aresConfig
cd $BASE_DEPLOY_PATH
#pid=`ps -ef | icegridnode | grep -i "node.cfg" | grep -v grep | awk '{print $2}'`
#if [ "$pid" != "" ]
#then
#	echo -n "icegridnode process already exists. Do you wish to kill that old icegridnode process. pid:" $pid " <y or n>?"
#	read WISH
#	echo
#	if [ $WISH = "y"]; then 
#		ps -ef |grep icegridnode | grep -i "node.cfg" | grep -v grep | awk '{print $2}' | xargs kill -9
#		#kill $pid
#	fi  
#fi

#ps -ef | grep -i "node.cfg" | grep -v grep | awk '{print $2}'


echo 'starting icegrid'
nohup ${ICE_HOME}/bin/icegridnode --Ice.Config=node.cfg 2>&1 | nodeConf/cronolog `pwd`/log/node.%Y%m%d.log &	


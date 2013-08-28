#!/bin/bash
BASE_PATH=`dirname $PWD`
source "${BASE_PATH}/bin/config.sh"

ICE_CONFIG_PATH=${BASE_PATH}/config
USERNAME=admin
PASSWORD=beijing


do_command(){
	result=`${ICE_HOME}/bin/icegridadmin --Ice.Config=${ICE_CONFIG_PATH}/admin.cfg -u ${USERNAME} -p ${PASSWORD} -e "$1"`	
	echo $result
	return 0
}

start(){
	for i in ${SERVER_LIST[@]} ; do
		echo "start server $i"
		do_command "server start $i"
		ret=($(do_command "server state $i"))
		if [ ${ret[0]} == "active" ]; then 
			echo "server $i start success!"; 
		else
			echo "server $i start error!";
		fi
	done	
}


stop(){
	for i in ${SERVER_LIST[@]} ; do
		echo "stop server $i"
		do_command "server stop $i"
		ret=($(do_command "server state $i"))
		if [ ${ret[0]} == "inactive" ]; then 
			echo "server $i stop success!"; 
		else
			echo "server $i stop error!";
		fi
	done	
}

ret=$(do_command "server list")
SERVER_LIST=(${ret})

case $1 in 
startall) 
	start
	;;
stopall)  
	stop
	;;
restartall)
	stop
	start
	;;
help)
	echo "startall|stopall|restartall|other operations"
	;;
*)
	do_command "$1"
	;;
esac

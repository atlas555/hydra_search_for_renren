#!/bin/bash

BASE_PATH=`dirname $PWD`
source ${BASE_PATH}/bin/config.sh

SEARCHERALLLIST=""
SEARCHERLIST=""
BROKERALLLIST=""
BROKERLIST=""
NODELIST=""
SCRIPT_PATH=${BASE_PATH}/manager/script

SSH_CMD=/bin/bash

for BROKERPART in $BROKERS
do
	BROKER=`echo $BROKERPART | awk -F: '{print $2}'`
	BROKERALLLIST="$BROKERALLLIST $BROKER "
done
array_broker=($BROKERALLLIST)
searcher_num=${#array_broker[*]}
for SEARCHERPART in $SEARCHERS
do
	SEARCHER=`echo $SEARCHERPART | awk -F: '{print $2}'`
	SEARCHERALLLIST="$SEARCHERALLLIST $SEARCHER "
done
array_searcher=($SEARCHERALLLIST)
searcher_num=${#array_searcher[*]}
SEARCHERIDLIST=${!array_searcher[*]}

function searcher_start()
{
	for searcher in $@
	do
		if [ -z $(echo $searcher | sed 's/[[:digit:]]//g') ]; then
			if [ "$searcher" -ge 0 -a "$searcher" -lt "$searcher_num" ]; then
				SEARCHER=${array_searcher[$searcher]}
				echo "Starting searcher: "$SEARCHER
				ssh ${USERNAME}@$REGISTRY "cd ${SCRIPT_PATH}; ${SSH_CMD} icegridadmin.sh \"server start ${SEARCHER}\"; exit"
				while true
				do
					state=$(ssh ${USERNAME}@$REGISTRY "cd ${SCRIPT_PATH}; ${SSH_CMD} icegridadmin.sh \"server state ${SEARCHER}\"; exit")
					if [ "${state/"active"/}" != "$state" -o "${state/"timed out"/}" != "$state" ]; then
						break
					fi
					sleep 3
				done
				return 0
			else
				echo "the searcher name you enter is too big!!! exit" >&2 
				return 1
			fi
		else
			echo "the searcher name you enter is not num!!! exit" >&2 
			return 1
		fi
	done
}   

function searcher_stop()
{
	for searcher in $@
	do
		if [ -z $(echo $searcher | sed 's/[[:digit:]]//g') ]; then
			if [ "$searcher" -ge 0 -a "$searcher" -lt "$searcher_num" ]; then
				SEARCHER=${array_searcher[$searcher]}
				echo "Stoping searcher: "$SEARCHER
				ssh ${USERNAME}@$REGISTRY "cd ${SCRIPT_PATH}; ${SSH_CMD} icegridadmin.sh \"server stop ${SEARCHER}\"; exit"
			else
				echo "the searcher name you enter is too big!!! exit" >&2 
				return 1
			fi
		else
			echo "the searcher name you enter is not num!!! exit" >&2 
			return 1
		fi
	done
	return 0
}

function broker_start()
{
	for broker in $@
	do
		echo "Starting broker: "$broker
		ssh ${USERNAME}@$REGISTRY "cd ${SCRIPT_PATH}; ${SSH_CMD} icegridadmin.sh \"server start ${broker}\"; exit"
	done
	return 0
}

function broker_stop()
{
	for broker in $@
	do
		echo "Stoping broker: "$broker
		ssh ${USERNAME}@$REGISTRY "cd ${SCRIPT_PATH}; ${SSH_CMD} icegridadmin.sh \"server stop ${broker}\"; exit"
	done
	return 0
}

function server_list()
{
	echo "Searchers are: "
	len=${#array_searcher[*]}
	i=0
	while [ "$i" -lt "$len" ]; do
		echo "${i}) ${array_searcher[$i]}"
		let i++
	done
	echo "Brokers are: "
	len=${#array_broker[*]}
	i=0
	while [ "$i" -lt "$len" ]; do
		echo "${i}) ${array_broker[$i]}"
		let i++
	done
}

case $1 in
	--start)
		shift
		if [ "$1" == "all_brokers" ]; then
			broker_start $BROKERALLLIST
		elif [ "$1" == "all_searchers" ]; then 
			searcher_start $SEARCHERIDLIST
		elif [ "$1" == "all_servers" ]; then
			broker_start $BROKERALLLIST
			searcher_start $SEARCHERIDLIST
		else
			while [[ -n $1 ]]; do
				echo "Start ${array_searcher[$1]} ..."
				NODELIST="$NODELIST $1"
				shift
			done
			if [ x"$NODELIST" == "x" ]; then
				broker_start $BROKERALLLIST
				searcher_start $SEARCHERIDLIST
			else
				searcher_start $NODELIST
			fi
		fi
		echo "Started !" >&2
		;;
	--stop)
		shift
		if [ "$1" == "all_brokers" ]; then
			broker_stop $BROKERALLLIST
		elif [ "$1" == "all_searchers" ]; then 
			searcher_stop $SEARCHERIDLIST
		elif [ "$1" == "all_servers" ]; then
			broker_stop $BROKERALLLIST
			searcher_stop $SEARCHERIDLIST
		else
			while [[ -n $1 ]]; do
				echo "Stop ${array_searcher[$1]} ..."
				NODELIST="$NODELIST $1"
				shift
			done
			if [ x"$NODELIST" == "x" ]; then
				broker_stop $BROKERALLLIST
				searcher_stop $SEARCHERIDLIST
			else
				searcher_stop $NODELIST
			fi
		fi
		echo "Stoped !" >&2
		;;
	--restart)
		shift
		if [ "$1" == "all_brokers" ]; then
			broker_stop $BROKERALLLIST
			broker_start $BROKERALLLIST
		elif [ "$1" == "all_searchers" ]; then 
			searcher_stop $SEARCHERIDLIST
			searcher_start $SEARCHERIDLIST
		elif [ "$1" == "all_servers" ]; then
			broker_stop $BROKERALLLIST
			searcher_stop $SEARCHERIDLIST
			broker_start $BROKERALLLIST
			searcher_start $SEARCHERIDLIST
		else
			while [[ -n $1 ]]; do
				echo "Restart ${array_searcher[$1]} ..."
				NODELIST="$NODELIST $1"
				shift
			done
		fi
		echo "Restarted !" >&2
		;;
	--help)
		echo "./hydra-server.sh --start all_brokers/all_searchers/all_servers/0 1 2 ... 
		--stop
		--restart" >&2
		;;
	*)
		echo "./hydra-server.sh --start all_brokers/all_searchers/all_servers/0 1 2 ... 
		--stop
		--restart" >&2
		server_list
		;;
esac

exit 0

#!/bin/bash - 
#set -o nounset                              # Treat unset variables as an error

BASE_PATH=`dirname $PWD`
source ${BASE_PATH}/bin/config.sh

declare -A array_searcher_part
declare -A array_searcher_nodeid
SEARCHERALLLIST=""
SEARCHERLIST=""
BROKERALLLIST=""
BROKERLIST=""
NODELIST=""

INDEX_DIR=""
OFFLINE_DIR=""
JAR_DIR=""
SCHEMA_DIR=""


for BROKERPART in $BROKERS
do
	BROKER=`echo $BROKERPART | awk -F: '{print $1}'`
	BROKERALLLIST="$BROKERALLLIST $BROKER"
done
for SEARCHERPART in $SEARCHERS
do
	SEARCHER=`echo $SEARCHERPART | awk -F: '{print $1}'`
	SEARCHERALLLIST="$SEARCHERALLLIST $SEARCHER"
done
NODEID=0
for PARTITIONPART in $PARTITIONS
do
	SEARCHER_PART=`echo $PARTITIONPART | awk -F: '{print $1}'`
	PARTITION=`echo $PARTITIONPART | awk -F: '{print $2}'`
	array_searcher_part[$SEARCHER_PART]=$PARTITION
	array_searcher_nodeid[$SEARCHER_PART]=$NODEID
	NODEID=`expr $NODEID + 1`
done
searcher_num=$NODEID
array_searcher=($SEARCHERALLLIST)

function dispatch_index_online()
{
	index_dir=$INDEX_DIR
	if [ ! -d $index_dir ]; then
		echo "there is no index dir!!!" >&2
		return 1
	fi
       
	base_backup_dir=$INDEX_BACKUP_DIR
        now=`date +%Y_%m_%d_%H_%M_%S`
	for searcher in $@
	do
		if [ -z $(echo $searcher | sed 's/[[:digit:]]//g') ]; then
			if [ "$searcher" -ge 0 -a "$searcher" -lt "$searcher_num" ]; then
				SEARCHER=${array_searcher[$searcher]}
				echo "Dispatching index and onlineAttributeData to searcher: "$SEARCHER
				if [ ! -e hydra-server.sh ];then
					echo "hydra-server.sh is not exist!!!" >&2
					return 1
				fi
                                echo "stop server"
				./hydra-server.sh --stop $searcher 
				PART="${array_searcher_part[$SEARCHER]}"
				NODEID="${array_searcher_nodeid[$SEARCHER]}"
				oldIFS=$IFS
				IFS=","
                                backup_dir="${base_backup_dir}/index/${now}"                  
				for PARTID in $PART
				do	
					shard_dir=shard${PARTID}
					dir=$BASE_DEPLOY_PATH/index/node${NODEID}/
                                        echo "backup to ${backup_dir}"
					#ssh -f ${USERNAME}@${NODE} "if [ ! -d ${backup_dir} ];then mkdir -p ${backup_dir} ; fi; exit"
					ssh -f ${USERNAME}@${NODE} "cd $dir; if [ -e ${shard_dir} ]; then if [ ! -d ${backup_dir} ];then mkdir -p ${backup_dir} ; fi; mv ${shard_dir} ${backup_dir}; fi; exit"
                                        sleep 2
                                        echo "copy index to searcher"
					scp -r ${INDEX_DIR}/${shard_dir} ${USERNAME}@${NODE}:${dir}/
                                        sleep 2
				done
				IFS=$oldIFS
                                echo "start server"
				./hydra-server.sh --start $searcher
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

function dispatch_offline()
{
	offline_dir=$OFFLINE_DIR
	if [ ! -d offline_dir ]; then
		echo "there is no offlineAttributeData dir!!!" >&2
		return 1
	fi
	time=`date +%s`
	for searcher in $@
	do
		if [ -z $(echo $searcher | sed 's/[[:digit:]]//g') ]; then
			if [ "$searcher" -ge 0 -a "$searcher" -lt "$searcher_num" ]; then
				SEARCHER=${array_searcher[$searcher]}
				echo "Dispatching offlineAttributeData to searcher: "$SEARCHER
				PART="${array_searcher_part[$SEARCHER]}"
				NODEID="${array_searcher_nodeid[$SEARCHER]}"
				oldIFS=$IFS
				IFS=","
				for PARTID in $PART
				do	
					dir=$BASE_DEPLOY_PATH/index/node${NODEID}/shard${PARTID}/
					ssh -f ${USERNAME}@${SEARCHER} "cd $dir; if [ ! -d off_attr ]; then mkdir off_attr; fi; exit"
					scp $PARTID ${USERNAME}@${SEARCHER}:${dir}/off_attr/off_attr.inc.$time
				done
				IFS=$oldIFS
			else
				echo "the searcher name you enter is too big!!! exit" >&2 
				return 1
			fi
		else
			echo "the searcher name you enter is not num!!! exit" >&2 
			return 1
		fi
	done
	java -cp /home/jin.shang/newJob/offDistrbute/tt-0.0.1-SNAPSHOT-jar-with-dependencies.jar OfflineNotification 10.11.18.120 2181

	return 0
}

function dispatch_schema()
{
	cd $SCHEMA_DIR
	for searcher in $@
	do
		if [ -z $(echo $searcher | sed 's/[[:digit:]]//g') ]; then
			if [ "$searcher" -ge 0 -a "$searcher" -lt "$searcher_num" ]; then
				SEARCHER=${array_searcher[$searcher]}
				echo "Dispatching schema.xml to searcher: "$SEARCHER
				dir=$BASE_DEPLOY_PATH/nodeConf/service_config
				echo $dir
				scp schema.xml ${USERNAME}@${SEARCHER}:${dir}/
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

function dispatch_schema_with_brokers()
{
	cd $SCHEMA_DIR
	for broker in $BROKERALLLIST
	do
		echo "Dispatching schema.xml to broker: " $broker
		dir=$BASE_DEPLOY_PATH/nodeConf/service_config
		scp schema.xml ${USERNAME}@${broker}:${dir}/
	done
	return 0
}

function dispatch_jar_with_brokers()
{
	cd $JAR_DIR
	for broker in $BROKERALLLIST
	do
		echo "Dispatching jars to broker: " $broker
		dir=$BASE_DEPLOY_PATH/nodeConf/lib
		scp *hydra*.jar ${USERNAME}@${broker}:${dir}/
	done
	return 0
}

function dispatch_jar()
{
	time=`date +%Y_%m_%d_%H_%M_%S`
	cd $JAR_DIR
	for searcher in $@
	do
		if [ -z $(echo $searcher | sed 's/[[:digit:]]//g') ]; then
			if [ "$searcher" -ge 0 -a "$searcher" -lt "$searcher_num" ]; then
				SEARCHER=${array_searcher[$searcher]}
				echo "Dispatching jar to searcher: "$SEARCHER
				dir=$BASE_DEPLOY_PATH/nodeConf/lib
				ssh -f ${USERNAME}@${SEARCHER} "cd /tmp; mkdir backup_jar_$time; cd $dir; cp *hydra*.jar /tmp/backup_jar_$time; exit"
				if [ $? ]; then
					scp ${JAR_DIR}/*hydra*.jar ${USERNAME}@${SEARCHER}:${dir}/
				fi
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

function searcher_list()
{
	echo "Searchers are: "
	len=${#array_searcher[*]}
	i=0
	while [ "$i" -lt "$len" ]; do
		echo "${i}) ${array_searcher[$i]}"
		let i++
	done
}

case $1 in
	--index)
		shift
		arg_first=true
		while [[ -n $1 ]]; do
			echo "Dispatch index and onlineAttributeData to $1 ..."
			if [ "x$1" = "x--input" ]; then
				shift
				if [ -n "$1" ]; then
					INDEX_DIR=$1
					shift
					arg_first=false
				else
					echo "Input dir is not exist!" >&2
					break
				fi
			else	
				NODELIST="$NODELIST $1"
				shift
			fi
		done
		if [ "x$arg_first" = "xtrue" ]; then
        	echo "you didn't enter the input dir!!!" >&2
		else
			if [ x"$NODELIST" == "x" ]; then
				dispatch_index_online  ${!array_searcher[*]}
			else
				dispatch_index_online $NODELIST 
			fi
		fi
		;;
	--offline)
		shift
		arg_first=true
		while [[ -n $1 ]]; do
			echo "Dispatch offlineAttributeData to $1 ..."
			if [ "x$1" = "x--input" ]; then
				shift
				if [ -n "$1" ]; then
					OFFLINE_DIR=$1
					shift
					arg_first=false
				else
					echo "Input dir is not exist!" >&2
					break
				fi
			else	
				NODELIST="$NODELIST $1"
				shift
			fi

		done
		if [ "x$arg_first" = "xtrue" ]; then
        	echo "you didn't enter the input dir!!!" >&2
		else
			if [ x"$NODELIST" == "x" ]; then
				dispatch_offline ${!array_searcher[*]}
			else
				dispatch_offline $NODELIST 
			fi
		fi
		;;
	--schema)
		shift
		arg_first=true
		while [[ -n $1 ]]; do
			echo "Dispatch schema.xml to $1 ..."
			if [ "x$1" = "x--input" ]; then
				shift
				if [ -n "$1" ]; then
					SCHEMA_DIR=$1
					shift
					arg_first=false
				else
					echo "Input dir is not exist!" >&2
					break
				fi
			else	
				NODELIST="$NODELIST $1"
				shift
			fi
		done
		if [ "x$arg_first" = "xtrue" ]; then
        	echo "you didn't enter the input dir!!!" >&2
		else
			if [ x"$NODELIST" == "x" ]; then
				dispatch_schema ${!array_searcher[*]}
				dispatch_schema_with_brokers
			else
				dispatch_schema $NODELIST 
				dispatch_schema_with_brokers
			fi
		fi
		;;
	--jar)
		shift
		arg_first=true
		while [[ -n $1 ]]; do
			echo "Dispatch jars to $1 ..."
			if [ "x$1" = "x--input" ]; then
				shift
				if [ -n "$1" ]; then
					JAR_DIR=$1
					shift
					arg_first=false
				else
					echo "Input dir is not exist!" >&2
					break
				fi
			else	
				NODELIST="$NODELIST $1"
				shift
			fi

			#NODELIST="$NODELIST $1"
			#shift
		done
		if [ "x$arg_first" = "xtrue" ]; then
            echo "you didn't enter the input dir!!!" >&2
        else
			if [ x"$NODELIST" == "x" ]; then
				dispatch_jar ${!array_searcher[*]}
				dispatch_jar_with_brokers
			else
				dispatch_jar $NODELIST 
				dispatch_jar_with_brokers
			fi
		fi
		;;
	--help)
		echo "./hydra-dispatch --index 0 1 2 ... 
		--offline
		--schema
		--jar" >&2
		;;
	*)
		searcher_list
		;;
esac

exit 0

#!/bin/bash

source ./icegrid.cfg
source ./config.sh

ICE_USERNAME=admin
ICE_PASSWORD=beijing 

CHECK_TIME=180

do_command(){
	result=`icegridadmin --Ice.Config=admin.cfg -u ${ICE_USERNAME} -p ${ICE_PASSWORD} -e "$1"`	
	echo $result
	return 0
}

get_node_server(){
	servers=($(do_command "server list"))	
	for item in ${servers[@]}; do 
		str=`icegridadmin --Ice.Config=admin.cfg -u ${ICE_USERNAME} -p ${ICE_PASSWORD} -e "server describe $item" | awk '{if($1=="node") print $3}'` 	
		len=`expr ${#str} - 2`
		node_name=`expr substr ${str} 2 ${len}`
		if [ $node_name == $1 ] ; then 
			echo $item
		fi
	done
}

update_file (){
	NODE_HOST=$1
	echo "---update node: $NODE_HOST---"
	ssh -f $USERNAME@$NODE_HOST "if [ -d $BASE_DEPLOY_PATH/data/$NODE_HOST ]; then rm -rf $BASE_DEPLOY_PATH/data/$NODE_HOST; fi; mkdir $BASE_DEPLOY_PATH/data/$NODE_HOST;"
        sleep 2s
	ssh -f $USERNAME@$NODE_HOST "NODENAME=\"node_${NICKNAME/\.*/}\" && source ~/.bash_profile && cd $BASE_DEPLOY_PATH/ && if [ ! -d log -a ! -d data ]; then mkdir log; mkdir data; fi && cd data && if [ ! -d $NODE_HOST ]; then mkdir $NODE_HOST; fi;"
        sleep 2s
	scp -r $PWD/nodeConf $USERNAME@$NODE_HOST:$BASE_DEPLOY_PATH
        sleep 2s
	scp $PWD/node.cfg $USERNAME@$NODE_HOST:$BASE_DEPLOY_PATH
	scp $PWD/config.sh $USERNAME@$NODE_HOST:$BASE_DEPLOY_PATH
	scp $PWD/start.sh $USERNAME@$NODE_HOST:$BASE_DEPLOY_PATH
        sleep 2s
	ssh -f $USERNAME@$NODE_HOST "cd $BASE_DEPLOY_PATH && sed -i \"s/NODENAME/$NODE_HOST/g\" node.cfg && sed -i \"s/NODEIP/$NODE_HOST/g\" node.cfg"
	sleep 5s

}

restart_node(){
	NODE_HOST=$1
	ssh -f $USERNAME@$NODE_HOST "cd $BASE_DEPLOY_PATH && source start.sh"
}

restart_server(){
	servers=(`echo "$@"`)
	index=`expr ${#servers[@]} - 1` 
	node_name=${servers[$index]}
	unset servers[$index]
	servers=(`echo "${servers[@]}"`)

	for item in ${servers[@]}; do
		#clean the started flag

		ssh -f $USERNAME@$node_name "rm -rf /tmp/xce/_started"

		do_command "server start $item"
		ret=($(do_command "server state $item"))
		search_ret=`echo "$item" | grep 'searcher' -i`
		if [ "${search_ret}" == "${item}" ]; then 
			count=0
			while [ $count -lt $CHECK_TIME ]; do
				count=`expr ${count} + 1`	
				check_ret=`ssh $USERNAME@$node_name "if [ ! -d /tmp/xce/_started ]; then echo 'started' ; fi"`	
				if [ "$check_ret"=="started" ]; then break; fi
				sleep 10s
			done

			check_ret=`ssh $USERNAME@$node_name "if [ ! -d /tmp/xce/_started ]; then echo 'started' ; fi"`	
			if [ ${ret[0]}=="active" -a "$check_ret"=="started" ]; then 
				echo "searcher server $item start successful!"
				mkdir -p $DEPLOY_DIR/$node_name/$item			
				ssh -f $USERNAME@$node_name "rm -rf /tmp/xce/_started"
			else
				echo "searcher server $item start fail!"
				echo "$item" >> server_error.txt
			fi
		else
			if [ ${ret[0]}=="active" ]; then 
				echo "broker server $item start successful!"
				mkdir -p $DEPLOY_DIR/$node_name/$item			
			else
				echo "broker server $item start fail!"
				echo "$item" >> server_error.txt
			fi
		fi
	done
}

cp ${PWD}/node.cfg.example node.cfg  #此处要改过来，因为将来模板是从svn下载下来的，不需要传过来
#替代node.cfg中的配置
sed -i "s/REGISTRYNAME/registry_data/g" node.cfg
sed -i "s/REGISTRYIP/${registry}/g" node.cfg
sed -i "s/INSTANCENAME/$INSTANCE_NAME/g" node.cfg
sed -i "s%BASEDEPLOYPATH%$BASE_DEPLOY_PATH%g" node.cfg

DEPLOY_DIR=${PWD}/_deploy

if [ -d ${DEPLOY_DIR} ] ; then 
	echo "-----------------------------------warning--------------------------------------"
	echo "--------------deploy目录已经存在，该目录用于记录升级过程中的信息----------------"
	echo "建议每次升级前确保该目录不存在，升级中途错误中断不需删除，除非确认完全重新部署！"
	if [ -f ${DEPLOY_DIR}/server_error.txt ]; then
		rm ${DEPLOY_DIR}/server_error.txt
	fi
else
	mkdir ${DEPLOY_DIR}
fi

NODE_LIST=(${node})

for item in ${NODE_LIST[@]}; do
	total_servers=($(get_node_server $item))
	ready_servers=()
	if [ -d ${DEPLOY_DIR}/$item ]; then 
		for server_item in ${total_servers[@]}; do
			if [ ! -d ${DEPLOY_DIR}/$item/$server_item ]; then 
				ready_servers[${#ready_servers[@]}]="$server_item"	
				echo ${ready_servers[@]}
			fi
		done		
	else		
		ready_servers=(`echo "${total_servers[@]}"`)
	fi

	echo "$item 中需要更新的服务：${ready_servers[@]}"
	server_num=${#ready_servers[@]}
	if [ ${server_num} -eq 0 ]; then continue; fi #没有server需要更新则继续下一节点
	#shutdown all servers of this node and the node itself
	if [ ! -d ${DEPLOY_DIR}/$item ] ; then    #该node的代码与配置文件还没有更新
		do_command "node shutdown $item"
		update_file $item	
		restart_node $item
		sleep 5s
		mkdir -p ${DEPLOY_DIR}/$item
	fi
	
	#重启需要更新的服务
	restart_server ${ready_servers[@]} $item

	count=0
	for server_item in ${ready_servers[@]}; do
			if [ -d ${DEPLOY_DIR}/$item/$server_item ]; then 
				count=`expr $count + 1`
			fi
	done	
	
	if [ ! $count==$server_num ] ; then 
		echo "$item update error!"
		echo "please check the error information in ${DEPLOY_DIR}/server_error.txt"
		exit
	fi
done

rm node.cfg 
	

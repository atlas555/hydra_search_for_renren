#!/bin/bash

#awk 获取第x列的内容
#sed 类似于vi 中的替换功能，

source ./config.sh
source ./util.sh
CURRENT_PATH=`pwd`
ROOT_PATH=`echo ${PWD%/*}`
CONFIG_PATH=$ROOT_PATH/config
BIN_PATH=$ROOT_PATH/bin
SCRIPT_PATH=$ROOT_PATH/script


icegridConf(){
	cd $CONFIG_PATH
	cp icegrid.cfg.example icegrid.cfg
    sed -i "s/LOCATER/$LOCATOR/g" icegrid.cfg
    sed -i "s/REGISTRY/$REGISTRY/g" icegrid.cfg
    sed -i "s/NODE/$NODE/g" icegrid.cfg
    sed -i "s/PARTITIONS/$PARTITIONS/g" icegrid.cfg
}

#生成kafka_redis客户端的配置文件
kafkaRedisConf(){
	cd $CONFIG_PATH
    cp kafka_redis_client.properties.example kafka_redis_client.properties
	sed -i "s/TOPIC/$KFK_COMMON_TOPIC/g" kafka_redis_client.properties
	sed -i "s/PARTITION_COUNT/$KFK_COMMON_PATITION_COUNT/g" kafka_redis_client.properties
	sed -i "s/THREAD_COUNT/$KFK_COMMON_THREAD_COUNT/g" kafka_redis_client.properties
	sed -i "s/VERSION_CHECKPOINT/$KFK_REDIS_VER_CHECKPOINT/g" kafka_redis_client.properties
	sed -i "s/VERSION_DIR/$KFK_REDIS_VER_DIR/g" kafka_redis_client.properties
	sed -i "s/REDIS_NAME/$KFK_REDIS_NAME/g" kafka_redis_client.properties
	sed -i "s/REDIS_ZKSERVERS/$KFK_REDIS_REDIS_ZKADDRESS/g" kafka_redis_client.properties
	sed -i "s/KAFKA_ZKSERVERS/$KFK_COMMON_ZKADDRESS/g" kafka_redis_client.properties
        sed -i "s#SEARCH_SCHEMA_FILE#$SEARCH_SCHEMA_FILE#g" kafka_redis_client.properties
        sed -i "s#INDEX_SCHEMA_FILE#$INDEX_SCHEMA_FILE#g" kafka_redis_client.properties
        sed -i "s#LOG_PATH#$LOG4J_FILE#g" kafka_redis_client.properties
}

kafkaHadoopConf(){
	cd $CONFIG_PATH
	cp kafka_hadoop_client.properties.example kafka_hadoop_client.properties
	sed -i "s/PARTITION_COUNT/$KFK_COMMON_PATITION_COUNT/g" kafka_hadoop_client.properties 
	sed -i "s/THREAD_COUNT/$KFK_COMMON_THREAD_COUNT/g" kafka_hadoop_client.properties 
	sed -i "s/DATA_DIR/$KFK_HADOOP_DATA_DIR/g" kafka_hadoop_client.properties 
        sed -i "s#LOG_PATH#$LOG4J_FILE#g" kafka_hadoop_client.properties
	sed -i "s/ZKSERVERS/$KFK_COMMON_ZKADDRESS/g" kafka_hadoop_client.properties 
	sed -i "s/TOPIC/$KFK_COMMON_TOPIC/g" kafka_hadoop_client.properties 
	sed -i "s/VERSION_CHECKPOINT/$KFK_HADOOP_VER_CHECKPOINT/g" kafka_hadoop_client.properties 
        sed -i "s#INDEX_SCHEMA_FILE#$INDEX_SCHEMA_FILE#g" kafka_hadoop_client.properties
}

kafkaClientConf(){
	cd $CONFIG_PATH
    cp producer.properties.example producer.properties
	sed -i "s/SERIALIZER_CLASS/$KFK_CLIENT_SERIALIZER_CLASS/g" producer.properties
	sed -i "s/ZKADDRESS/$KFK_COMMON_ZKADDRESS/g" producer.properties
	sed -i "s/PRODUCER_TYPE/$KFK_CLIENT_PRODUCER_TYPE/g" producer.properties
	sed -i "s/COMPRESSION_TYPE/$KFK_CLIENT_COMPRESSION_CODEC/g" producer.properties
	sed -i "s/QUEUE_TIME/$KFK_CLIENT_QUEUE_TIME/g" producer.properties
	sed -i "s/QUEUE_SIZE/$KFK_CLIENT_QUEUE_SIZE/g" producer.properties
	sed -i "s/BATCH_SIZE/$KFK_CLIENT_BATCH_SIZE/g" producer.properties
}


#生成icegridadmin 的配置文件
locater(){
	cd $CONFIG_PATH
	cp admin.cfg.example admin.cfg
	sed -i "s/LOCATERIP/$LOCATER_HOST/g" admin.cfg
	sed -i "s/INSTANCENAME/$INSTANCE_NAME/g" admin.cfg

}

#启动icegridregistry, 如果ice程序已经运行，则关闭已有进程
newregistry(){
    cd $CONFIG_PATH
	pid=`ps -ef | grep icegridregistry | grep -i "node.cfg" | grep -v grep | awk '{print $2}'`
	if [ "$pid" != "" ]
	then
                echo -n "icegridregistry process already exists. Do you wish to kill that old icegridregistry process. pid:" $pid " <y or n>?"
		read WISH
		echo
		if [ $WISH = "y" ]; then	
			ps -ef |grep icegridregistry| grep -i "node.cfg" | grep -v grep | awk '{print $2}' | xargs kill -9
			#kill -9 $pid
		fi
	fi
	echo "---registry: $REGISTRY_HOST"

        #清除已经存在的文件夹
	if [ -d $BASE_DEPLOY_PATH ]
	then
		echo $BASE_DEPLOY_PATH already exists
		rm -rf $BASE_DEPLOY_PATH
	fi

        #创建新文件夹和自目录
	mkdir $BASE_DEPLOY_PATH
	cd $CONFIG_PATH
	cp passwd $BASE_DEPLOY_PATH
	#source ~/.bash_profile
	cd $BASE_DEPLOY_PATH/
        if [ ! -d manager ]
        then
            mkdir manager
        fi
         
       
        cp -r $BIN_PATH $BASE_DEPLOY_PATH/manager
        cp -r $SCRIPT_PATH $BASE_DEPLOY_PATH/manager
        

        if [ ! -d manager/config ]
        then
            mkdir manager/config
        fi 
        cp  $CONFIG_PATH/admin.cfg $BASE_DEPLOY_PATH/manager/config
	if [ ! -d log -a ! -d data ]
	then
		mkdir log
		mkdir data
	#	mkdir lib64
	fi
	cd data
	if [ ! -d registry_data ]
	then
		mkdir registry_data
	fi

        
        #更改log目录
        cd $CONFIG_PATH/nodeConf/service_config
        cp log4j.properties.example log4j.properties
        sed -i "s%BASEDEPLOYPATH%$BASE_DEPLOY_PATH%g" log4j.properties

        #复制配置文件
	cd $BASE_DEPLOY_PATH
	if [ ! -d nodeConf ]
	then
		cp -r $CONFIG_PATH/nodeConf nodeConf
	fi

        cd $CONFIG_PATH	
	cp $CONFIG_PATH/node.cfg.example node.cfg  #此处要改过来，因为将来模板是从svn下载下来的，不需要传过来

        #替代node.cfg中的配置
	sed -i "s/REGISTRYNAME/registry_data/g" node.cfg
	sed -i "s/REGISTRYIP/$REGISTRY_HOST/g" node.cfg
	sed -i "s/INSTANCENAME/$INSTANCE_NAME/g" node.cfg
	sed -i "s%BASEDEPLOYPATH%$BASE_DEPLOY_PATH%g" node.cfg

        cd $BASE_DEPLOY_PATH
        cp $CONFIG_PATH/node.cfg node.cfg

	nohup ${ICE_HOME}/bin/icegridregistry --Ice.Config=`pwd`/node.cfg 2>&1 | nodeConf/cronolog `pwd`/../log/registry.%Y%m%d.log &	
}

#启动icegrid node 
#需要设置JAVA_HOME, PATH, 将Ice.jar 添加到CLASSPATH中
newLocalNode(){	
	cd $BASE_DEPLOY_PATH/data
	if [ ! -d $NODE_HOST ]
	then 
		mkdir $NODE_HOST
	fi
        aresConfig
	cd $BASE_DEPLOY_PATH
	sed -i "s/NODENAME/$NODE_HOST/g" node.cfg
	sed -i "s/NODEIP/$NODE_HOST/g" node.cfg
	nohup ${ICE_HOME}/bin/icegridnode --Ice.Config=`pwd`/node.cfg 2>&1 | nodeConf/cronolog `pwd`/log/node.%Y%m%d.log & 
}

#启动远程icegrid node
#需要keytab支持，还需要测试
newRemoteNode(){
	echo "---remote node: $NODE_HOST"
	ssh -f $USERNAME@$NODE_HOST "if [ -d $BASE_DEPLOY_PATH ]; then rm -rf $BASE_DEPLOY_PATH; fi; mkdir $BASE_DEPLOY_PATH;"
        sleep 2s
	ssh -f $USERNAME@$NODE_HOST "NODENAME=\"node_${NICKNAME/\.*/}\" && source ~/.bash_profile && cd $BASE_DEPLOY_PATH/ && if [ ! -d log -a ! -d data ]; then mkdir log; mkdir data; fi && cd data && if [ ! -d $NODE_HOST ]; then mkdir $NODE_HOST; fi;"
        sleep 2s
	scp -r $CONFIG_PATH/nodeConf $USERNAME@$NODE_HOST:$BASE_DEPLOY_PATH
        sleep 2s
	scp $CONFIG_PATH/node.cfg $USERNAME@$NODE_HOST:$BASE_DEPLOY_PATH
	scp $BIN_PATH/config.sh $USERNAME@$NODE_HOST:$BASE_DEPLOY_PATH
	scp $BIN_PATH/util.sh $USERNAME@$NODE_HOST:$BASE_DEPLOY_PATH
	scp $BIN_PATH/start.sh $USERNAME@$NODE_HOST:$BASE_DEPLOY_PATH
        sleep 2s
	ssh -f $USERNAME@$NODE_HOST "cd $BASE_DEPLOY_PATH && sed -i \"s/NODENAME/$NODE_HOST/g\" node.cfg && sed -i \"s/NODEIP/$NODE_HOST/g\" node.cfg"
	ssh -f $USERNAME@$NODE_HOST "cd $BASE_DEPLOY_PATH && source start.sh"
	#ssh -f $USERNAME@$NODE_HOST "cd $BASE_DEPLOY_PATH && source config.sh && sed -i \"s/NODENAME/$NODE_HOST/g\" node.cfg && sed -i \"s/NODEIP/$NODE_HOST/g\" node.cfg && nohup ${ICE_HOME}/bin/icegridnode --Ice.Config=`pwd`/node.cfg 2>&1 | nodeConf/cronolog `pwd`/log/node.%Y%m%d.log &"
        sleep 5s
}

#searcher 配置
partitions(){
        cd $CONFIG_PATH/nodeConf/service_config
        cp hydra.properties.example hydra.properties
        sed -i "s/BUSINESS/$BUSINESS/g" hydra.properties
        sed -i "s/INDEX_ZKADDRESS/$ZKADDRESS/g" hydra.properties
        sed -i "s%INDEXDIR%$INDEX_DIR%g" hydra.properties
        sed -i "s%INDEXMODE%$INDEX_DIR_MODE%g" hydra.properties
        sed -i "s/NODENAME/$NODEID/g" hydra.properties
        sed -i "s/NODEPART/$PART/g" hydra.properties
        sed -i "s/KFK_ZKADDRESS/$KFK_COMMON_ZKADDRESS/g" hydra.properties
        sed -i "s/TOPIC/$KFK_COMMON_TOPIC/g" hydra.properties
        sed -i "s/REDIS_NAME/$KFK_REDIS_NAME/g" hydra.properties
        sed -i "s/REDIS_ZKADDRESS/$KFK_REDIS_REDIS_ZKADDRESS/g" hydra.properties
        sed -i "s/PARTITION_SIZE/$KFK_COMMON_PATITION_COUNT/g" hydra.properties
        if [ $NODE = $REGISTRY_HOST ]
        then
        	cp hydra.properties $BASE_DEPLOY_PATH/nodeConf/service_config
        else
        	scp hydra.properties $USERNAME@$NODE:$BASE_DEPLOY_PATH/nodeConf/service_config
	fi
	rm hydra.properties
}
#生成application.xml
insertBrokerApplication()
{
	cd $CONFIG_PATH
	linenum=`awk '/\/server-template/ {print NR}' application.xml`
	nodetext='<node name=\"'${IP}'\"> \
            <server-instance template=\"SearchService\" Name=\"'${NAME}'\" ServiceName=\"BrokerService\" JavaXms=\"100m\" JavaXmx=\"4g\" BusinessName=\"'${BUSINESS}'\" Port=\"50399\"/> \
        </node>'
	sed -i "${linenum} a\ $nodetext" application.xml
	cd ${CURRENT_PATH}
}
insertSearcherApplication()
{
	cd $CONFIG_PATH
	linenum=`awk '/\/server-template/ {print NR}' application.xml`
	nodetext='<node name=\"'${IP}'\"> \
            <server-instance template=\"SearchService\" Name=\"'${NAME}'\" ServiceName=\"MainSearcherService\" JavaXms=\"100m\" JavaXmx=\"4g\" BusinessName=\"'${BUSINESS}'\" Port=\"50388\"/> \
        </node>'
	sed -i "${linenum} a\ $nodetext" application.xml
	cd ${CURRENT_PATH}
}


#从ice grid cluster配置文件中读取registry/node的配置信息
#icegridConf
#按行读取
#cd $CONFIG_PATH
cd $CONFIG_PATH
cp application.xml.example application.xml
cd ${CURRENT_PATH}

LOCATER_HOST=$LOCATOR
locater
REGISTRY_HOST=$REGISTRY
newregistry
NODELIST=$NODE
for NODE_HOST in $NODELIST
do
	echo "node---------$NODE_HOST"
	if [ "x$NODE_HOST" = "x$REGISTRY_HOST" ]; then
		newLocalNode
	else
		newRemoteNode
	fi
done
partitions=$PARTITIONS
NODEID=0
for NODEPART in $partitions
do
	NODE=`echo $NODEPART | awk -F: '{print $1}'`
        PART=`echo $NODEPART | awk -F: '{print $2}'`
	partitions
	NODEID=`expr $NODEID + 1`
done
for IPNAME in $BROKERS
do
	IP=`echo $IPNAME | awk -F: '{print $1}'`
	NAME=`echo $IPNAME | awk -F: '{print $2}'`
	insertBrokerApplication
done
for IPNAME in $SEARCHERS
do
	IP=`echo $IPNAME | awk -F: '{print $1}'`
	NAME=`echo $IPNAME | awk -F: '{print $2}'`
	insertSearcherApplication
done


#设置service config 路径
cd $CONFIG_PATH
#cp application.xml.example application.xml
sed -i "s%BASEDEPLOYPATH%$BASE_DEPLOY_PATH%g" application.xml
rm node.cfg


kafkaRedisConf
kafkaHadoopConf
kafkaClientConf


#!/bin/bash

#deploy path
#部署文件的根目录，其他文件都在该文件夹之下
BASE_DEPLOY_PATH=/data/xce/xiaobing/deploy2


#ice grid 
#ice 集群的名称
INSTANCE_NAME='xiaobing.liu'
#ice application.xml server confg 路径 
SERVER_CONFIG_PATH=$BASE_DEPLOY_PATH/nodeConf/service_config

#locator 和 registry 设置为ice registry 节点的hostname 或者ip地址
LOCATOR=127.0.0.1
REGISTRY=127.0.0.1
#node节点实际运行ice service,多个node之间使用空格分割,例如node＝hostname1 hostname2
NODE='127.0.0.1'
#多个节点之间通过空格分割，每一个节点的partitions配置为 nodename:partitionid1,partitionid2,......partitionidn
PARTITIONS='127.0.0.1:0,1'
BROKERS='127.0.0.1:broker1'
SEARCHERS='127.0.0.1:searcher1'

#user config
#username ssh 登录远程服务器
USERNAME='xiaobing'
BUSINESS='functional_test'


#enviroment config
#设置环境变量，Ice.jar 需要添加到CLASSPATH中
JAVA_HOME=/data/xce/local/jdk
ICE_HOME=/opt/Ice-3.3
PATH=.:${JAVA_HOME}/bin:${PATH}:${ICE_HOME}/bin
CLASSPATH=./*:${BASE_DEPLOY_PATH}/nodeConf/lib/*:${ICE_HOME}/lib/Ice.jar:/usr/share/java/Ice.jar
LD_LIBRARY_PATH=${HOME}/XiaoNei/lib64

export CLASSPATH JAVA_HOME PATH LD_LIBRARY_PATH ICE_HOME


#zookeeper config
ZKADDRESS=127.0.0.1:2181


#index dir
INDEX_DIR=$BASE_DEPLOY_PATH/index

#index dir mode
INDEX_DIR_MODE=MMAP


#backup dir, 保存历史索引数据
INDEX_BACKUP_DIR=$BASE_DEPLOY_PATH/backup

SCHEMA_FILE=$SERVER_CONFIG_PATH/schema.xml
LOG4J_FILE=$SERVER_CONFIG_PATH/log4j.properties

#kafka relevant client config
KFK_COMMON_TOPIC=test
KFK_COMMON_ZKADDRESS=127.0.0.1:2181
KFK_COMMON_PATITION_COUNT=2
KFK_COMMON_THREAD_COUNT=1


#kafka-redis-online config
KFK_REDIS_NAME=
KFK_REDIS_VER_CHECKPOINT=9
KFK_REDIS_VER_DIR=version
#redis zookeeper
KFK_REDIS_REDIS_ZKADDRESS=


#kafka-hadoop-client config
KFK_HADOOP_VER_CHECKPOINT=9
KFK_HADOOP_DATA_DIR=hadoop_data


#search-kafka-client config
KFK_CLIENT_SERIALIZER_CLASS=kafka.serializer.StringEncoder
KFK_CLIENT_PRODUCER_TYPE=async
KFK_CLIENT_COMPRESSION_CODEC=0
KFK_CLIENT_QUEUE_TIME=5000
KFK_CLIENT_QUEUE_SIZE=10000
KFK_CLIENT_BATCH_SIZE=200



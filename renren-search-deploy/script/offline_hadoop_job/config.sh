#!/bin/bash

source ../../bin/config.sh


#offline_hadoop_job
#------------------------------------------
HDFS_KAFKA_DIR="/xiaobing/guangjie/kafka"
NUM_KAFKA_SERVER=1
NUM_PARTITION=1

#CLEAN_DATA
#clean data的输入为history dir 和kafka dir
CLEAN_DATA_OUTPUT_DIR="/xiaobing/guangjie/cleandataoutput"

#dedup
DEDUP_INPUT_DIR="/xiaobing/guangjie/cleandataoutput"
DEDUP_OUTPUT_DIR="/xiaobing/guangjie/dedupoutput"
DEDUP_NUM_REDUCER=2

#build index
BUILD_INDEX_INPUT_DIR="/xiaobing/guangjie/dedupoutput"
BUILD_INDEX_OUTPUT_DIR="/xiaobing/guangjie/output"
HDFS_INDEX_DIR="xiaobing/guangjie/index"

#build attribute
BUILD_ATTRIBUTE_INPUT_DIR="/xiaobing/guangjie/dedupoutput"
BUILD_ATTRIBUTE_OUTPUT_DIR="/xiaobing/guangjie/attribute"

#back dir
HDFS_BACKUP_DIR="/xiaobing/guangjie/backup"

#hdfs history dir
HDFS_HISTORY_DIR=

#local offline index/attribute dir, 运行完job之后，从hdfs拷贝到本地
LOCAL_OFFLINE_DATA_DIR="offline_data"

#kafka back 目录
KAFKA_BACKUP_DATA_DIR="hadoop_data"

SCHEMA_FILE_URL="/xiaobing/functional/schema.xml"
ANALYZER_NAME="mmseg"
ANALYZER_DIC_DIR="/xiaobing/guangjie/dic"
CLEANDATA_NUM_REDUCER=4
BUSINESSNAME="guangjie_goods"

autorunConf(){
    cp settings.py.tp settings.py
    sed -i "s#KAFKA_BACKUP_DATA_DIR_TP#$KAFKA_BACKUP_DATA_DIR#g" settings.py
    sed -i "s#HDFS_KAFKA_DIR_TP#$HDFS_KAFKA_DIR#g" settings.py
    sed -i "s#CLEAN_DATA_OUTPUT_DIR_TP#$CLEAN_DATA_OUTPUT_DIR#g" settings.py
    sed -i "s#DEDUP_INPUT_DIR_TP#$DEDUP_INPUT_DIR#g" settings.py
    sed -i "s#DEDUP_OUTPUT_DIR_TP#$DEDUP_OUTPUT_DIR#g" settings.py
    sed -i "s#BUILD_INDEX_INPUT_DIR_TP#$BUILD_INDEX_INPUT_DIR#g" settings.py
    sed -i "s#BUILD_INDEX_OUTPUT_DIR_TP#$BUILD_INDEX_OUTPUT_DIR#g" settings.py
    sed -i "s#HDFS_INDEX_DIR_TP#$HDFS_INDEX_DIR#g" settings.py
    sed -i "s#BUILD_ATTRIBUTE_INPUT_DIR_TP#$BUILD_ATTRIBUTE_INPUT_DIR#g" settings.py
    sed -i "s#BUILD_ATTRIBUTE_OUTPUT_DIR_TP#$BUILD_ATTRIBUTE_OUTPUT_DIR#g" settings.py
    sed -i "s#HDFS_BACKUP_DIR_TP#$HDFS_BACKUP_DIR#g" settings.py
    sed -i "s#HDFS_HISTORY_DIR_TP#$HDFS_HISTORY_DIR#g" settings.py
    sed -i "s#LOCAL_OFFLINE_DATA_DIR_TP#$LOCAL_OFFLINE_DATA_DIR#g" settings.py
    sed -i "s#INDEX_BACKUP_DIR_TP#$INDEX_BACKUP_DIR#g" settings.py
    
    sed -i "s#NUM_PARTITION_TP#$NUM_PARTITION#g" settings.py
    sed -i "s#NUM_KAFKA_SERVER_TP#$NUM_KAFKA_SERVER#g" settings.py
    sed -i "s#NODE_TP#$NODE#g" settings.py
    sed -i "s#PARTITIONS_TP#$PARTITIONS#g" settings.py
    sed -i "s#BASE_DEPLOY_PATH_TP#$BASE_DEPLOY_PATH#g" settings.py
    sed -i "s#USERNAME_TP#$USERNAME#g" settings.py
    sed -i "s#BUSINESSNAME_TP#$BUSINESSNAME#g" settings.py
}

jobPropertyConf(){
    cp job.property.tp job.property
    sed -i "s#HDFS_KAFKA_DIR_TP#$HDFS_KAFKA_DIR#g" job.property
    sed -i "s#CLEAN_DATA_OUTPUT_DIR_TP#$CLEAN_DATA_OUTPUT_DIR#g" job.property
    sed -i "s#CLEANDATA_NUM_REDUCER_TP#$CLEANDATA_NUM_REDUCER#g" job.property
    sed -i "s#DEDUP_INPUT_DIR_TP#$DEDUP_INPUT_DIR#g" job.property
    sed -i "s#DEDUP_OUTPUT_DIR_TP#$DEDUP_OUTPUT_DIR#g" job.property
    sed -i "s#DEDUP_NUM_REDUCER_TP#$DEDUP_NUM_REDUCER#g" job.property
    sed -i "s#BUILD_INDEX_INPUT_DIR_TP#$BUILD_INDEX_INPUT_DIR#g" job.property
    sed -i "s#BUILD_INDEX_OUTPUT_DIR_TP#$BUILD_INDEX_OUTPUT_DIR#g" job.property
    sed -i "s#HDFS_INDEX_DIR_TP#$HDFS_INDEX_DIR#g" job.property
    sed -i "s#BUILD_ATTRIBUTE_INPUT_DIR_TP#$BUILD_ATTRIBUTE_INPUT_DIR#g" job.property
    sed -i "s#BUILD_ATTRIBUTE_OUTPUT_DIR_TP#$BUILD_ATTRIBUTE_OUTPUT_DIR#g" job.property
    sed -i "s#HDFS_HISTORY_DIR_TP#$HDFS_HISTORY_DIR#g" job.property
    sed -i "s#SCHEMA_FILE_URL_TP#$SCHEMA_FILE_URL#g" job.property
    sed -i "s#ANALYZER_NAME_TP#$ANALYZER_NAME#g" job.property
    sed -i "s#ANALYZER_DIC_DIR_TP#$ANALYZER_DIC_DIR#g" job.property
    sed -i "s#NUM_PARTITION_TP#$NUM_PARTITION#g" job.property
    sed -i "s#BUSINESSNAME_TP#$BUSINESSNAME#g" job.property
}

autorunConf
jobPropertyConf

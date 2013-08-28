
hadoop job 
clean_delete_data_job
输入包括两部分：
   1) 历史数据，一般为用户从数据库中导出， 这一部分一般不包括delete信息
   2）从kafka 获取到的用户数据， 这一部分包括一些delete信息
用户首先需要将历史数据添加到HDFS上，同时运行kakfa_hadoop_client获取kafka上的数据
该job将两部分的输入数据组合，删除需要delete的文档，生成build index/build attribute的输入数据

uid_dedup_job
对于同一个uid的JsonObject,保留version最大的（也就是最后推送到kafka的，最新的数据)
输入数据：
   1）一般为clean delete data 之后的输出

build_index_job
输入数据：
   1) 可以直接使用历史数据
   2) 也可以使用clean_delete_data_job的输出作为输入

输出：
   离线索引

build_attribute_job
输入数据：
   1) 可以直接使用历史数据
   2) 也可以使用clean_delete_data_job的输出作为输入

输出：
   离线索引的online attribute data



运行先决条件：
1)schema文件需要copy到HDFS上
2)选择Analyzer,Analyzer对应的dict文件需要复制到HDFS上
  如果使用AresAnalyzer, 修改config.sh 中ANALYZER_NAME="ares"
  如果使用MMSegAnalyzer, 修改config.sh中ANALYZER_NAME="mmseg"
 
  修改ANALYZER_DIC_DIR 为HDFS上词典的位置

  mmseg 需要将nodeConf/service_config/data 文件夹复制到HDFS
  ares 需要将nodeConf/service_config/ares 文件夹复制到HDFS

运行说明


1.修改config.sh里面的路径
2.运行./config.sh
3.编译hadoop job
  在renren-search-hadoop 下面
  主要编译
  clean_delete_data_job
  build_index_job
  build_attribute_job
  这三个job依赖renren-search-hadoop/util 
  先编译util, mvn clean install
  然后编译三个hadoop job, mvn package assembly:assembly
4. 将生成jar文件拷贝到该目录
5. 使用python autorun.py 运行hadoop job
   可以使用python autorun.py --help 获取帮助信息

   python autorun.py --command all --withCleanData 
   运行: cleanDataJob, dedup,buildIndex, buildAttribute, 之后将建好的数据复制到本地目录
   
   python autorun.py --command prepareDispatch
   生成shard 文件夹，拷贝index和attribute 文件，为分发做准备 

   python autorun.py --command dispatch
   将数据分发到不同的searcher目录下
   注意：分发之前会删除searcher目录下已有的index数据




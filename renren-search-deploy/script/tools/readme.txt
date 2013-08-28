工具列表
1)JSONTxt2SequenceFile.java
一般要求应用将历史数据导出为文本文件，一行是一个文档，文档的表示形式为JSONString,该工具将输入的JSON文件转换为SequenceFile
后续hadoop job 可以使用转换后的SequenceFile

使用方法：
 java JSONTxt2SequenceFile jsontxtfile idfield outputfile

参数：
 jsontxtfile: 为输入文件
 idfield: 表示JsonObject中的id域， 使用optLong 获取该域的值作为SequenceFile的key
 outputfile: 输出文件位置

返回值：
  0 表示成功
  -1 表示失败

依赖：
  hadoop, json

2)OfflineAttributePartition
功能： 分隔离线数据到各个Partition, 没个Partition生成一个SequenceFile
用户提供离线数据的JSON文件，或者SequenceFile, 设置Partition 个数，将离线数据分割到各个Partition

使用方法：
  java OfflineAttributeParition inputformat([sequence|json] inputfile partitionNum outputFolder [idfield]

参数0：
  inputformat:  sequence/json 输入文件格式，可以为SequenceFile 也可以是一行一个JSONString的文本文件
  inputfile: 输入文件路径
  partitionNum: partition 个数
  outputFolder: 输出文件夹，输出文件命名为0,1,2...--partitionNum
  idfield: 如果inputformat 是json， 需要指出要作为SequenceFile key的域名
  

返回值：
  0 表示成功
  -1 表示失败

依赖：
  hadoop, json

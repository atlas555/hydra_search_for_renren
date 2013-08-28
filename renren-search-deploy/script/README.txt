-------------------------------ServerManager.py-------------------------------------
管理服务器，broker/searcher的启动和关闭，需要首先运行./config.sh
运行python ServerManager.py 获取帮助信息


-------------------------------OfflineDataManger.py---------------------------------
离线数据的分发，输入为离线数据文件，可以是SequenceFile也可以是Json的文本文件，一行一个数据
需要首先运行./config.sh 生成配置文件setttings.py, 运行python OfflineDataManger.py -h 获取使用帮助信息
OfflineDataManager.py --all 运行:
                                 1) 将输入根据uid分割到不同的partition
                                 2) 将相应的partition复制到searcher目录下
                                 3）通知zookeeper 加载数据

--------------------------------hydra-dispatch.sh----------------------------------
向searcher,broker分发文件
./hydra-dispatch.sh 默认情况下显示searcher列表，前面有0,1,2标识
--help 显示帮助，即该脚本有哪些参数
--index --input /index_dir 0 1 2 ... 该选项用于向指定的searcher发送索引数据和onlineAttributeData，如果后面不加标识的话会向所有searcher发送数据,--input选项后是输入数据存在的目录
--offline         该选项用于向指定的searcher发送offlineAttributeData，如果后面不加标识的话会向所有searcher发送数据
--schema          该选项用于向指定的searcher发送schema.xml，如果后面不加标识的话会向所有searcher发送数据
--jar             该选项用于向指定的searcher发送通用架构搜索的相关jar包，如果后面不加标识的话会向所有searcher发送数据


--------------------------------hydra-server.sh------------------------------------
管理服务器启动和关闭
./hydra-server.sh 默认情况下显示searcher列表，前面有0,1,2标识
--help 显示帮助，即该脚本有哪些参数
--start all_brokers/all_searchers/all_servers/0 1 2 ...该选项用于启动指定的broker或searcher，如果后面不加选项则会启动所有broker和searcher
--stop 该选项用于停掉指定的broker或searcher，如果后面不加选项则会停掉所有broker和searcher
--restart 该选项用于重启指定的broker或searcher，如果后面不加选项则会重启所有broker和searcher

------------------------------------------------------------------------------------
icegridadmin.sh 与 update_application.sh脚本介绍

相关约束：
命名：所有searcher服务命时必须包含searcher，大小写不做约束
	  所有node统一使用主机host，因此单一主机只能配置一个node
	  该目录下_deploy被保留使用
	  该目录下node.cfg.example被保留使用，需要提供icegridnode的配置模版
	  以下脚本都需要在application add application.xml 或者application update application.xml成功之后执行
	  脚本需要指定icegridregistry的用户名与密码
	  
-----------------------------icegridadmin.sh---------------------------------------
该脚本用于封装icegridadmin的相关命令，增加了正对server的批量操作命令。

该脚本依赖与当前目录下的admin.ccfg指定iceregistry的地址与端口

startall 启动application.xml中描述的所有server
stopall  关闭application.xml中描述的所有server
命令的格式分为两种：1，批量命令；2，icegridadmin 原生命令
1：./icegridadmin.sh [startall|stopall]
2: ./icegridadmin.sh ["server list" | "application add application.xml"|.....]等，将命令作为字符串传递给icegridadmin.sh


-------------------------update_application.sh--------------------------------------
依赖：
该脚本依赖于当前目录下icegrid.cfg和config.sh
其中icegrid.cfg中node 属性被使用，其格式必须为目标主机的host，registry属性配置为icegridregistry的host地址
依赖与config.sh中所有配置

配置：
	对于searcher,因为需要延时检查启动是否成功，需要设定一个检查次数，每次检查间隔为30s

功能：
根据node配置node列表依次更新每个node上的可执行程序与配置，并且自动启动该节点上相应的server

执行：
./update_application.sh

输出:
_deploy文件用于记录升级过程中的升级信息，目录结构如下：
_deploy \
         node1\                          #代表该目录的可执行程序与配置文件已更新
		       server1                   #代表node1下的server1已经成功启动
			   server2
			   ...
			   servern
         node2\
		 
		 
		 server_error.txt                 #启动失败的server

升级成功后应该将_deploy去除

注意事项：
一旦一个节点中有server没有启动成功，升级将会终端。解决问题后执行脚本会继续完成更新，因错误的中断不要删除_deploy文件夹


#coding:utf-8
import os

baseConfigPath="/data/xce/zookeeper/config"
baseBinPath="/data/xce/zookeeper/server"


for i in xrange(1,4):
    configPath=baseConfigPath+os.path.sep+str(i)+os.path.sep+"zoo.cfg"
    binPath=baseBinPath+os.path.sep+str(i)
    cDir=os.getcwd()
    cmd = "zkServer.sh start %s" % configPath
    os.chdir(binPath)
    print cmd
    os.system(cmd)
    os.chdir(cDir)







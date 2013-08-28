#coding:utf-8

from settings import *
import os
import sys
sys.path.append("../")
from util import *
from hdfsUtil import *
from datetime import datetime
import glob
import traceback
from optparse import OptionParser
import shutil

def runHadoopJob(jarfile,classfile,*args):
    cmd="hadoop jar {0} {1} ".format(jarfile,classfile)
    if args:
        cmd=cmd+" "+" ".join(args)
    return execute(cmd)

def runIndexHadoopJob(**params):
    cmd="hadoop jar {jarfile} {class} {args} ".format(**params)
    return execute(cmd)



def backUpOfflineIndex():
    print 'backup offline index'
    dateStr=datetime.now().strftime(dateformat)
    curBackDir=HDFS_BACKUP_DIR+os.path.sep+dateStr
    HdfsMkdir(curBackDir)
    HdfsCopy(HDFS_INDEX_DIR,curBackDir)
    HdfsCopy(BUILD_ATTRIBUTE_OUTPUT_DIR,curBackDir)     
    HdfsCopy(HDFS_KAFKA_DIR,curBackDir)
    HdfsCopy(HDFS_HISTORY_DIR,curBackDir)    



def getMaxVersion(partFolder):
    maxVersion=-1
    versionFiles=glob.glob(partFolder+os.path.sep+"version.*")
    for versionFile in versionFiles:
        numPos=versionFile.rfind(".")
        version=int(versionFile[numPos+1:])
        if(version>maxVersion):
            maxVersion=version

    loginfo("for folder "+partFolder+" max version is "+str(maxVersion))
    return maxVersion 

#离线备份的数据中，通常会有一个Sequence文件，序号最大的那个，由于没有close，在copy到hdfs时会出错，这里先吧它删除
def deleteUnCorrectData(dataFolder):
    partFolders=os.listdir(dataFolder)
    for partFolder in partFolders: 
        folderpath=dataFolder+os.path.sep+partFolder
        loginfo("delete uncomplete data for folder:"+folderpath)
        maxVersion=getMaxVersion(folderpath)
        dfile=folderpath+os.path.sep+str(maxVersion+1)
        if(os.path.exists(dfile)):
            loginfo('remove file :'+dfile)
            os.remove(dfile)

copyKafkaData2Hdfs=lambda : copyLocal2Hdfs(KAFKA_BACKUP_DATA_DIR,HDFS_KAFKA_DIR)
cleanDataJob=lambda:runIndexHadoopJob(**JobSetting["job.cleanjob"])
buildIndexJob=lambda:runIndexHadoopJob(**JobSetting["job.buildindexjob"])
buildAttributeJob=lambda:runIndexHadoopJob(**JobSetting["job.buildattributejob"])
dedupJob=lambda:runIndexHadoopJob(**JobSetting["job.dedupjob"])

copyIndex2Local=lambda:copyHdfs2Local(HDFS_INDEX_DIR,LOCAL_OFFLINE_DATA_DIR)
copyAttribute2Local=lambda:copyHdfs2Local(BUILD_ATTRIBUTE_OUTPUT_DIR,LOCAL_OFFLINE_DATA_DIR)


def prepareShard(tmpDir,indexFolder,attributeFile,index):
    shardDir=tmpDir+os.path.sep+"shard"+str(index)
    shutil.copytree(indexFolder,shardDir)
    shutil.copy(attributeFile,shardDir+os.path.sep+ATTR_DATA_FILE_NAME)
    offsetFile=shardDir+os.path.sep+ZOIE_OFFSET_FILE_NAME
    versionFile=shardDir+os.path.sep+ATTR_VERSION_FILE_NAME
    if(os.path.exists(offsetFile) and os.path.isfile(offsetFile)):
        pass
    else:
       f=open(offsetFile,'w')
       offsetStr=""
       for i in xrange(0,NUM_KAFKA_SERVER):
           offsetStr=offsetStr+"%d:0_" % i
       loginfo("offset is %s" % offsetStr[:-1])
       f.write(offsetStr[:-1]+"\n")
       f.close()
    shutil.copy(offsetFile,versionFile)
     


def prepareDispatch():
    tmpDir=LOCAL_OFFLINE_DATA_DIR+os.path.sep+DISPATCH_TEMP_DIR
    indexPath=LOCAL_OFFLINE_DATA_DIR+os.path.sep+os.path.split(HDFS_INDEX_DIR)[1]
    attributePath=LOCAL_OFFLINE_DATA_DIR+os.path.sep+os.path.split(BUILD_ATTRIBUTE_OUTPUT_DIR)[1]
    
    if(os.path.exists(indexPath) and os.path.isdir(indexPath) and os.path.exists(attributePath) and os.path.isdir(attributePath)):
        if(os.path.exists(tmpDir) and os.path.isdir(tmpDir)):
            dF=raw_input("tmp dir %s already exists delete or use the old one, delete?[y/n]:" % tmpDir)
            if(dF.lower()=='y'):
                shutil.rmtree(tmpDir)
            else:
                return 1
        indexFolders=getSubFolders(indexPath)
        attributeFiles=glob.glob(attributePath+os.path.sep+"part-*")
        if(len(indexFolders)!=NUM_PARTITION or len(attributeFiles)!=NUM_PARTITION):
            logerr('partition error')
            return -1
        indexFolders.sort(key=lambda index:int(index))
        indexFolders=map(lambda v:indexPath+os.path.sep+v,indexFolders)
        attributeFiles.sort()
        os.mkdir(tmpDir) 
        for i in xrange(0,NUM_PARTITION):
            loginfo("prepare shard for partition %d with folder %s and attribute file %s" %(i,indexFolders[i],attributeFiles[i]))
            prepareShard(tmpDir,indexFolders[i],attributeFiles[i],i)
        return 0
    else:
        logerr('path error, please check index file path and attribute file path (%s,%s)' % (indexPath,attributePath))
        return -1   

def dispatch():
    status=prepareDispatch()
    if(status==1):
        loginfo('use old offline data')
    elif(status==-1):
        logerr("dispatch prepare failed")
        sys.exit(-1)
    else:
        loginfo("dispatch prepare ok")
    node2id,id2partition=getNode2Partition(NODE,PARTITIONS)
    print node2id
    print id2partition
    nodes=node2id.keys()
    tmpDir=LOCAL_OFFLINE_DATA_DIR+os.path.sep+DISPATCH_TEMP_DIR
    tStr=datetime.now().strftime(dateformat) 
    backupDir=INDEX_BACKUP_DIR+os.path.sep+"index"+os.path.sep+tStr
    for node in nodes:
        nodeid=node2id[node]
        partitions=id2partition[nodeid]
        indexPath=SEARCHER_INDEX_DIR+os.path.sep+"node"+str(nodeid)+os.path.sep
        #remoteDirDelete(SSH_USERNAME,node,indexPath+os.path.sep+"*")  
        for partition in partitions:
            print 'backup to ', backupDir
            status=backRemoteDir(SSH_USERNAME,node,indexPath+os.path.sep+"shard"+str(partition),backupDir)
            if(status!=0):
                logerr("back up remote folder failed")
                sys.exit(-1)
        remoteDirDelete(SSH_USERNAME,node,indexPath)
        makeRemoteDir(SSH_USERNAME,node,indexPath) 
        for partition in partitions:
            #remoteDirDelete(SSH_USERNAME,node,indexPath+os.path.sep+"shard"+str(partition))  
            src=tmpDir+os.path.sep+"shard"+str(partition)
            copyLocal2Remote(SSH_USERNAME,node,src,indexPath) 


#删除已有目录
def removeOutput():
    HdfsRmr(CLEAN_DATA_OUTPUT_DIR)
    HdfsRmr(DEDUP_OUTPUT_DIR)
    HdfsRmr(BUILD_INDEX_OUTPUT_DIR)
    HdfsRmr(HDFS_INDEX_DIR)
    HdfsRmr(BUILD_ATTRIBUTE_OUTPUT_DIR)
       
    

def initAll():
    backUpFlag=raw_input("back up history data?[y/n]:")  
    if(backUpFlag.lower()=='y'):
        backUpOfflineIndex()
    try:
        removeOutput()
    except Exception,e:
        logerr("init error:"+str(traceback.format_exc()))
        sys.exit(-1) 

def prepareKafkaData():
    dataFolder=KAFKA_BACKUP_DATA_DIR
    status=-1
    if(os.path.exists(dataFolder) and os.path.isdir(dataFolder)):
        deleteUnCorrectData(dataFolder)
        HdfsRmr(HDFS_KAFKA_DIR)
        status=copyKafkaData2Hdfs()
    
    return status

def cleanDataInit():
    HdfsRmr(CLEAN_DATA_OUTPUT_DIR)

def dedupInit():
    HdfsRmr(DEDUP_OUTPUT_DIR)
    

def buildIndexInit():
    HdfsRmr(BUILD_INDEX_OUTPUT_DIR)
    HdfsRmr(HDFS_INDEX_DIR)
     

def buildAttributeInit():
    HdfsRmr(BUILD_ATTRIBUTE_OUTPUT_DIR)
 

def runCleanDataJob():
    status=0
    if(os.path.exists(KAFKA_BACKUP_DATA_DIR) and os.path.isdir(KAFKA_BACKUP_DATA_DIR)):
        pF=raw_input("copy new kafka data to hdfs ?[y/n] : ")
        if(pF.lower()=='y'):
            loginfo("using new kafka data as input")
            status=prepareKafkaData()
        else:
            loginfo("using old kafka data as input")
    if(status!=0):
        logerr('prepare kafka data error')
    else:
        status=cleanDataJob()
        if(status!=0):
            logerr('error run clean data')
        else:
            loginfo('run clean data job ok')
    return status

def runDedupJob():
    status=dedupJob()
    if(status!=0):
        logerr('error run dedup job')
    else:
        loginfo('run dedup job ok')
    return status
      
 
def runBuildIndexJob():
    status=buildIndexJob()
    if(status!=0):
        logerr('error run build index job')
    else:
        loginfo('run build index job ok')
    return status

def runBuildAttributeJob():
    status=buildAttributeJob()
    if(status!=0):
        logerr('error run build attribute job')
    else:
        loginfo('run build attribute job ok')
    return status


def copyData2Local():
    ret=0
    if(os.path.exists(LOCAL_OFFLINE_DATA_DIR) and os.path.isdir(LOCAL_OFFLINE_DATA_DIR)):
        deleteFlag=raw_input("local dir %s already exists continue will delete it?[y/n]:" % LOCAL_OFFLINE_DATA_DIR)
        if(deleteFlag.lower()=='y'):
            shutil.rmtree(LOCAL_OFFLINE_DATA_DIR)
            os.mkdir(LOCAL_OFFLINE_DATA_DIR)
        else:
            loginfo("please delete local data manually")
            return -1

    else:
        os.mkdir(LOCAL_OFFLINE_DATA_DIR)

    status=copyIndex2Local()
    if(status!=0):
        logerr('error copy index to local');
        ret=status
    else:
        loginfo('copy index to local ok')
    status=copyAttribute2Local()         
    if(status!=0):
        logerr('error copy attribute to local')
        ret=status
    else:
        loginfo('copy attribute to local ok')
    
    return ret
 
def runAllJob(withCleanData=False):
    try:
        status=-1
        if(withCleanData):
            status=runCleanDataJob()
            if(status!=0):
                sys.exit(-1)

        status=runDedupJob()
        if(status!=0):
            sys.exit(-1)

        status=runBuildIndexJob()
        if(status!=0):
            sys.exit(-1)
      
        status=runBuildAttributeJob()
        if(status!=0):
            sys.exit(-1)

        copyData2Local()
    except Exception,e:
        logerr("error:"+str(traceback.format_exc()))  
    




if __name__=='__main__':
    usage = "usage: %prog [options]"
    parser=OptionParser(usage)
    parser.add_option("", "--command", action="store",dest="command",choices=["all","cleanData","dedup","buildIndex","buildAttribute","copyData2Local","prepareDispatch","dispatch","cleanHDFSOutput"],
                  help="command[all|cleanData|dedup|buildIndex|buildAttribute|copyData2Local|prepareDispatch|dispatch|cleanHDFSOutput")
    parser.add_option("", "--withCleanData", action="store_true",dest="withCleanData",default=False,
                  help="run clean data job in all")

    (options, args) = parser.parse_args()
 
    numArgs=len(sys.argv)
    if(numArgs<=1):
        parser.print_help() 
        sys.exit(-1)
    
    command=options.command
    if(command=="all"): 
        print 'run cleanDataJob, buildIndexJob, buildAttributeJob and then copy data to local'
        print 'need dispatch by yourself' 
        initAll()
        runAllJob(options.withCleanData)
    elif(command=="cleanData"):
        cleanDataInit()
        runCleanDataJob()
    elif(command=="dedup"):
        dedupInit()
        runDedupJob()
    elif(command=="buildIndex"):
        buildIndexInit()
        runBuildIndexJob()
    elif(command=="buildAttribute"):
        buildAttributeInit()
        runBuildAttributeJob()
    elif(command=="copyData2Local"):
        copyData2Local()
    elif(command=="prepareDispatch"):
        prepareDispatch()
    elif(command=="dispatch"):
        dispatch() 
    elif(command=='cleanHDFSOutput'):
       removeOutput()
    else:
        parser.print_help() 

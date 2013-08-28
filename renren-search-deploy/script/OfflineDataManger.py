#coding:utf-8
from util import *
import os
import time
from optparse import OptionParser
import sys
from settings import *
import shutil

cwd=os.getcwd()

def initClassPath():
    if(os.environ.has_key('CLASSPATH')):
        classpath=os.environ['CLASSPATH']
    else:
        classpath="."
    paths=classpath.split(":")
    libPaths=LIB_PATH.split(":")
    extPaths=[]
    for libPath in libPaths:
        if(libPath in paths):
            pass
        else:
            extPaths.append(libPath)
    newClassPath=classpath+":"+":".join(extPaths)
    os.environ['CLASSPATH']=newClassPath.strip()
   
initClassPath() 

def partitionCompile():
    cmd='javac tools/%s.java -d %s' % (OFFLINE_DATA_PARITION_BIN,cwd)
    return execute(cmd)


def notificationCompile():
    cmd='javac tools/%s.java -d %s '  % (OFFLINE_NOTIFICATION_BIN,cwd)
    return execute(cmd)

def compile():
    loginfo('-----------------------starting compiler -------------------------')
    status=partitionCompile()
    if(status!=0):
        return status
    
    status=notificationCompile()   
    loginfo('-----------------------compiler end------------------------------')
    return status 

def checkAllOptions(options):
    status=checkPartitionOptions(options)
    if(status!=0):
        return status

    status=checkDispatchOptions(options)
    if(status!=0):
        return status

    return checkNotificationOptions(options)

def checkPartitionOptions(options):
    inputFile=options.input
    outputFolder=options.output
    fileFormat=options.format
    if(inputFile==None or outputFolder==None or fileFormat==None):
        print "inputfile,outoutfolder, fileformat cann't be none"
        return -1
    if(fileFormat=="json"):
        idField=options.idField
        if(idField==None):
            logerr("uid needed for json input format, please given by -u yourid")
            return -1
    return 0

def checkDispatchOptions(options):
    command=options.command
    if(command=='dispatch'):
        dataFolder=options.dataFolder
        if(dataFolder==None):
            logerr("dataFolder can not be none,given by -d yourfolder")
            return -1
    return 0

def checkNotificationOptions(options):
    zkAddress=options.zkAddress
    business=options.business
    if(zkAddress==None):
        zkAddress=ZKADDRESS
    if(business==None):
        business=BUSINESS
    if(zkAddress=="" or business==""):
        logerr("zkAddress or business can't be empty")
        return -1
    return 0

def partitionPrepare(options):
    status=checkPartitionOptions(options)
    if(status!=0):
        return -3
    
    status=partitionCompile()
    if(status!=0):
        logerr('compile error')
        return -2

    outputFolder=options.output
    if(os.path.isdir(outputFolder)):
        cFlag=raw_input("folder %s already exists, delete and continue?[y/n] : " % outputFolder)
        if(cFlag.lower()=='y'):
            loginfo("delete folder %s" % outputFolder)
            shutil.rmtree(outputFolder)
        else:
            loginfo("please check your outputfolder %s " % outputFolder)
            sys.exit(1)
    try:
        os.mkdir(outputFolder)
    except OSError,e:
        logerr(traceback.format_exc())
        return -1

    return 0

def dispatchPrepare(options):
    return checkDispatchOptions(options) 
   
    
def notificationPrepare(options):
    status=checkNotificationOptions(options)
    if(status!=0):
        return -1

    status=notificationCompile()
    if(status!=0):
        logerr("compile error")
        return -2

    return 0 
 
def prepare(options):
    loginfo('-----------------------starting parpare-------------------------')
    status=partitionPrepare(options)
    if(status!=0):
        return status

    status = dispatchPrepare(options)
    if(status!=0):
        return status
    status=notificationPrepare(options)    
    loginfo('-----------------------parpare end-----------------------------')
    return status


def dataPartition(inputformat,inputpath,partitionNum,outputfolder,idfield):
    print '----------------------starting data partition---------------------------------'
    cmd='java %s %s %s %s %s %s' % (OFFLINE_DATA_PARITION_BIN,inputformat,inputpath,partitionNum,outputfolder,idfield)
    status=execute(cmd)
    print '----------------------data partition   end -----------------------------------'
    return status

def dispatch(datafolder,isAll=False):
    print '----------------------starting dispatch---------------------------------'
    node2id,id2partition=getNode2Partition(NODE,PARTITIONS)
    loginfo(node2id)
    logerr(id2partition)
    nodes=node2id.keys()
    timeStr=int(time.time())
    loginfo('start dispatch for timestamp %s ' % timeStr)
    partName="inc"
    if(isAll):
        partName="all"
    else:
        partName="inc"
    for node in nodes:
        nodeid=node2id[node]
        partitions=id2partition[nodeid]
        indexPath=SEARCHER_INDEX_DIR+os.path.sep+"node"+str(nodeid)+os.path.sep
        for partition in partitions:
            offlineDataFolder=indexPath+os.path.sep+"shard"+str(partition)+os.path.sep+"off_attr"
            offlineDataPath=offlineDataFolder+os.path.sep+"off_attr."+partName+"."+str(timeStr)
            localPath=datafolder+os.path.sep+str(partition)
            status=makeRemoteDir(USERNAME,node,offlineDataFolder)
            time.sleep(1)
            if(status!=0):
                logerr('error for mkdir remote dir %s:%s, please check' % (node,offlineDataFolder))
                sys.exit(-1)
            status=copyLocal2Remote(USERNAME,node,localPath,offlineDataPath)
            if(status!=0):
                logerr('error for copy %s to %s:%s' % (localPath,node,offlineDataPath))
                sys.exit(-1)
            loginfo('dispath %s to %s:%s ok' % (localPath,node,offlineDataPath))
            time.sleep(DISPATCH_INTERVAL)

    print '----------------------dispatch    end---------------------------------'
    return 0

      

def notification(zkAddress,business):
    print '----------------------starting notification---------------------------------'
    cmd='java %s %s %s ' %(OFFLINE_NOTIFICATION_BIN,zkAddress,OFFLINE_PARAENT_PATH+os.path.sep+business)    
    status=execute(cmd)
    print '----------------------notification  end-------------------------------------'
    return status

def clean():
    if(os.path.exists("%s.class" % OFFLINE_DATA_PARITION_BIN)):
        os.remove("%s.class" % OFFLINE_DATA_PARITION_BIN)
    if(os.path.exists("%s.class" % OFFLINE_NOTIFICATION_BIN)):
        os.remove("%s.class" % OFFLINE_NOTIFICATION_BIN)
    return 0

def runAll(options):
    status=prepare(options)
    if(status!=0):
        print 'usage error: usage python %s --c all -i inputfile -f inputformat[json|sequence] -o outputfolder [-u(uidfileld) when input is json] -z zookeeper_address -b business' % sys.argv[0]
        sys.exit(-1)

    time.sleep(1)

    inputFile=options.input
    outputFolder=options.output
    fileFormat=options.format
    idField=options.idField
    isAll=options.isAll
    num=options.num
    if(num==None):
        num=NUM_PARTITION
    
    status=dataPartition(fileFormat,inputFile,num,outputFolder,idField) 
    if(status!=0):
        print 'error for data partition'
        return status
    
    time.sleep(1)
    status=dispatch(outputFolder, isAll)
    if(status!=0):
        print 'dispatch error'
        return status
   
    time.sleep(1)
    zkAddress=options.zkAddress
    business=options.business
    if(zkAddress==None):
        zkAddress=ZKADDRESS
    if(business==None):
        business=BUSINESS
    status=notification(zkAddress,business)
    if(status!=0):
        print 'notification error'
        return status

    time.sleep(1)
    return 0


def runDataPartition(options):
    status=partitionPrepare(options)
    if(status!=0):
        print 'prepare partition failed'
        print 'usage python %s -c parition -i inputfile -o outputfolder -f fileformat[json|sequence] [-u(uid need when format is json)] [-n(partition num)]' % sys.argv[0]
        return status
    
    inputFile=options.input
    outputFolder=options.output
    fileFormat=options.format
    idField=options.idField
    num=options.num
    if(num==None):
        num=NUM_PARTITION
    status=dataPartition(fileFormat,inputFile,num,outputFolder,idField)
    if(status!=0):
        print 'partition failed'
    return status


def runDispatch(options):
    status=dispatchPrepare(options)
    if(status!=0):
        print 'prepare dispatch failed'
        return status
    dataFolder=options.dataFolder
    isAll=options.isAll
    status=dispatch(dataFolder,isAll)
    if(status!=0):
        print 'dispatch failed'
    return status    

def runNotification(options):
    status=notificationPrepare(options)
    if(status!=0):
        print 'prepare notification failed'
        return status
    
    zkAddress=options.zkAddress
    business=options.business
    if(zkAddress==None):
        zkAddress=ZKADDRESS
    if(business==None):
        business=BUSINESS
    status=notification(zkAddress,business)
    if(status!=0):
        print 'notification error'
    return status

if __name__=='__main__':
    usage = "usage: %prog [options]"
    parser=OptionParser(usage)
    parser.add_option("","-c","--command", action="store",dest="command",choices=["all","partition","dispatch","notification"],
                  help="command[all|partition|dispatch|notification]")
    parser.add_option("","-i","--input", action="store",dest="input",help="input file path")
    parser.add_option("","-f","--format", action="store",dest="format",help="input file foramt,[json|sequence]",choices=["json","sequence"])
    parser.add_option("","-o","--output", action="store",dest="output",help="outputfolder")
    parser.add_option("","-u","--uid", action="store",dest="idField",help="id field for json file")
    parser.add_option("","-n","--num", action="store",dest="num",type="int",help="partition num")
    parser.add_option("","-d","--dataFolder", action="store",dest="dataFolder",help="offline data folder")
    parser.add_option("","-z","--zookeeper", action="store",dest="zkAddress",help="zookeeper address")
    parser.add_option("","-b","--business", action="store",dest="business",help="business name")
    parser.add_option("","-a","--all", action="store_true",dest="isAll",default=False,help="true for all data update, false for increment update")
   
    (options, args) = parser.parse_args() 
    numArgs=len(sys.argv)
    if(numArgs<=1):
        parser.print_help()
        sys.exit(-1)

    command=options.command

    if(command=="all"):
        print 'partition----dispatch----notification'
        runAll(options)
    elif(command=="partition"):
        print 'partition'
        runDataPartition(options)
    elif(command=="dispatch"):
        print 'dispatch'
        runDispatch(options)
    elif(command=="notification"):
        print "notification"
        runNotification(options)
    else:
        parser.print_help()

    clean()

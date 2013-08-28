#coding:utf-8
import os

def execute(cmd):
    status=0
    print cmd
    status=os.system(cmd) 
    return status


def getSubFolders(folder):
    files=os.listdir(folder)
    subFolders=[]
    for filepath in files:
        path=folder+os.path.sep+filepath
        if(os.path.isdir(path)):
            subFolders.append(filepath)

    return subFolders


def getNode2Partition(nodeStr,partitionStr):
    nodes=nodeStr.strip().split()
    node2id={}
    id2partition={}
    id=0
    for node in nodes:
        node2id[node]=id
        id=id+1

    nodePartitions=partitionStr.split()
    for nodePartition in nodePartitions:
        infos=nodePartition.split(':')
        assert len(infos)==2,'error format for %s' % nodePartition
        node=infos[0]
        partitions=infos[1].split(",")
        partitions=map(lambda x:int(x),partitions)
        id=node2id[node]
        id2partition[id]=partitions

    return (node2id,id2partition)



def simpleRemoteCommand(username,host,commandStr):
    cmd='ssh {0}@{1} {2} {3}'.format(username,host,commandStr)
    status=execute(cmd)
    return status

def mvRemoteDir(username,host,srcDir,dstDir):
    cmd="""ssh -f {0}@{1} "if [ -e {2} ]; then if [ ! -d {3} ];then mkdir -p {3} ; fi; mv {2} {3}; fi; exit " """.format(username,host,srcDir,dstDir)
    status=execute(cmd)
    return status
     
def makeRemoteDir(username,host,path):
    cmd="""ssh -f {0}@{1} "if [ ! -d {2} ];then mkdir -p {2} ; fi; exit " """.format(username,host,path)
    status=execute(cmd)
    return status

def backRemoteDir(username,host,srcDir,dstDir):
    cmd="""ssh -f {0}@{1} "if [ -e {2} ]; then if [ ! -d {3} ];then mkdir -p {3} ; fi; cp -r {2} {3}; fi; exit" """.format(username,host,srcDir,dstDir)
    status=execute(cmd)
    return status


def remoteCommand(username,host,command,*args):
    cmd='ssh {0}@{1} {2} {3}'.format(username,host,command,' '.join([str(arg) for arg in args]))
    status=execute(cmd)
    return status

def copyLocal2Remote(username,host,src,dst): 
    if(not os.path.exists(src)):
        return -1
    cmd=""
    if(os.path.isfile(src)):
        cmd='scp {0} {1}@{2}:{3}'.format(src,username,host,dst)
    else:
        cmd='scp -r {0} {1}@{2}:{3}'.format(src,username,host,dst)
        
    status=execute(cmd)
    return status

remoteDirDelete=lambda username,host,dst:remoteCommand(username,host,"rm -r",dst)
remoteDirMake=lambda username,host,path:remoteCommand(username,host,"mkdir",path)
remoteFileDelete=lambda username,host,dst:remoteCommand(username,host,"rm",dst)
remoteDirForceMake=lambda username,host,path:remoteCommand(username,host,"mkdir -p",path)


def loginfo(infoStr):
    print infoStr


def logerr(errStr):
    print errStr


if __name__=='__main__':
    import sys
    #mvRemoteDir(sys.argv[1],sys.argv[2],sys.argv[3],sys.argv[4])
    status=makeRemoteDir(sys.argv[1],sys.argv[2],sys.argv[3])
    print status

#coding:utf-8
#管理broker,searcher, 远程控制启动和关闭" 

import os
import time
from settings import *
import sys
from util import *

def server_command(command,serverName,success):
    cmd="""ssh %s@%s "cd %s; %s icegridadmin.sh 'server %s %s'; exit;" """ % (USERNAME,REGISTRY,SCRIPT_PATH,SSH_CMD,command,serverName)
    print '{0}ing {1}'.format(command,serverName)
    os.system(cmd)
  
    status_cmd="""ssh %s@%s "cd %s; %s icegridadmin.sh 'server state %s'; exit;" """ %(USERNAME,REGISTRY,SCRIPT_PATH,SSH_CMD,serverName)
    print 'check server status ...........'
    status = os.popen(status_cmd).readlines()[0]
    if(status.strip().startswith(success)):
        print '%s %s success, status: %s' % (command,serverName,status)
        return True 
    else:
        print '%s %s failed, status: %s' % (command,serverName,status)
        return False



brokers=[]
searchers=[]
server2ip={}
alreadyClearServers={}

server_start_fun = lambda serverName : server_command("start",serverName,"active")
server_stop_fun = lambda serverName : server_command("stop",serverName,"inactive")

def server_clear_fun(serverName):
    ip=server2ip[serverName]
    if(alreadyClearServers.has_key(ip)):
        return True

    status=remoteDirDelete(USERNAME,server2ip[serverName],SEARCHER_INDEX_DIR) 
    if(status!=0 and status!=256):
        print 'error for delete index dir %s for server %s %s with status %s' %(serverName,SEARCHER_INDEX_DIR,server2ip,status)
        return False
    remoteDirDelete(USERNAME,server2ip[serverName],SEARCHER_LOG_DIR)  
    if(status!=0 and status!=256):
        print 'error for delete index dir %s for server %s %s with status %s' %(serverName,SEARCHER_LOG_DIR,server2ip,status)
        return False

    alreadyClearServers[ip]=True
    return True

def server_restart_fun(serverName):
    return server_stop_fun(serverName) and server_start_fun(serverName)

def help():
    print 'usage python ServerManager.py {--start|--stop|--restart|--clear} {broker|searcher|all} {names|index|all} '
    print '---------------------------------'
    print 'brokers:',brokers
    print 'searchers:',searchers
    print '---------------------------------'
    

def getAllBrokers():
    return brokers

def getAllSearchers():
    return searchers

def initServer(serverListStr):
    servers=[]
    serverInfos = serverListStr.strip().split()
    for serverInfo in serverInfos: 
        ip,name = serverInfo.split(":")
        servers.append(name)
        server2ip[name]=ip

    return servers

def getServer(serverList,selectedList):
    if("all" in selectedList):
        return serverList

    retList=[]
    for server in selectedList:
        if(server.isdigit()):
            index=int(server)
            if(index<0 or index>=len(serverList)):
                print 'index error:', index, 'server list is :',serverList
                return []
            else:
                retList.append(serverList[index])
        else:
            if(server in serverList):
                retList.append(server)
            else:
                print 'error, no server for name :', server
                return []            
  
    return list(set(retList))
    

if __name__=='__main__':
    import sys
    brokers=initServer(BROKERS)
    searchers=initServer(SEARCHERS)
    print server2ip
    if(len(sys.argv)<3):
        help()
        sys.exit(-1)

    command=sys.argv[1]
    func=None
    if(command=='--start'):
        func=server_start_fun    
    elif(command=='--stop'):
        func=server_stop_fun
    elif(command=='--restart'):
        func=server_restart_fun
    elif(command=='--clear'):
        func=server_clear_fun
    else:
        print 'usage error'
        help()
        sys.exit(-1)



    serverNames=[]
    serverType=sys.argv[2]
    if(serverType=='broker'):
        serverNames=getServer(brokers,sys.argv[3:])
    elif(serverType=='searcher'):
        serverNames=getServer(searchers,sys.argv[3:])
    elif(serverType=='all'):
        brokerNames=getAllBrokers()
        searcherNames=getAllSearchers()
        serverNames.extend(brokerNames)
        serverNames.extend(searcherNames)
    else:
        help()
        sys.exit(-1)

    

    print '---------------------------------'
    print 'brokers:',brokers
    print 'searchers:',searchers
    print '---------------------------------'
    if(len(serverNames)==0):
        sys.exit(-1)

    state=True
    for serverName in serverNames:
        state=func(serverName)
        if(state==False): 
            break

    print '------------------------------------------'
    if(state):
        print 'successed!'
    else:
        print 'failed!, status is not stable, please login on %s to check ice grid manually ' % REGISTRY

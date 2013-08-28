#coding:utf-8

import os

version="1.0"

sub_projects=["search-kafka-client","kafka-redis-online","kafka-hadoop-client"]
curdir = os.getcwd()


def clean():
    for pro in sub_projects:
        pdir=curdir+os.path.sep+pro
        print pdir
        os.chdir(pdir)
        cmd = 'mvn clean'
        print cmd
        os.system(cmd)
        os.chdir(curdir)

def deploy():
    for pro in sub_projects:
        pdir=curdir+os.path.sep+pro
        print pdir
        os.chdir(pdir)
        cmd = 'mvn deploy'
        print cmd
        os.system(cmd)
        os.chdir(curdir)

def build():
    for pro in sub_projects:
        pdir=curdir+os.path.sep+pro
        print pdir
        os.chdir(pdir)
   
        #cmd = 'mvn clean install -Dmaven.test.skip=true'
        cmd = 'mvn clean install'
        print cmd
        os.system(cmd)
        os.chdir(curdir)

def install(installPath):
    os.chdir(curdir)
    if(os.path.isdir(installPath)==False):
        cFlag=raw_input("folder %s not exists, create and continue?[y|n]: " % installPath)
        if(cFlag.lower()=='y'):
            os.mkdir(installPath)
        else:
            print 'please check installPath %s' % installPath

    cmdbase = "cp -i %s " + installPath
    jarPath = curdir + os.path.sep + "kafka-hadoop-client/target/kafka-hadoop-client-%s-SNAPSHOT.jar" % version
    os.system(cmdbase % jarPath)
    jarPath = curdir + os.path.sep + "kafka-redis-online/target/kafka-redis-online-%s-SNAPSHOT.jar" % version
    os.system(cmdbase % jarPath)
    jarPath = curdir + os.path.sep + "search-kafka-client/target/hydra-kafka-client-%s-SNAPSHOT.jar" % version
    os.system(cmdbase % jarPath)

   
    
if __name__=='__main__':
    import sys
    if(len(sys.argv)<2):
        print 'usage python %s [-c --clean|-b --build|-i --install path] ' % sys.argv[0]
        sys.exit(-1) 
    command=sys.argv[1]
    if(command=='--build' or command =='-b'):
        build()
    elif(command=='--clean' or command=='-c'):
        clean()
    elif(command=='--deploy' or command=='-d'):
        deploy()
    elif(command=='--install' or command=='-i'):
        if(len(sys.argv)<3):
            print 'must specific install path for install'
            sys.exit(-1)
        install(sys.argv[2])
    else:
        print 'error usage'
        print 'usage python %s [-c --clean|-b --build|-i --install path] ' % sys.argv[0]
        sys.exit(-1)

    sys.exit(0) 


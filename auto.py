#coding:utf-8

import os

sub_projects=["renren-search-util","renren-search-searcher","renren-search-broker"]
curdir = os.getcwd()
service_dir=curdir+os.path.sep+"renren-search-service"
hadoop_dir=curdir+os.path.sep+"renren-search-hadoop"
plugin_dir=curdir+os.path.sep+"renren-search-plugin"
version="1.0.0"

def clean():
    for pro in sub_projects:
        pdir=curdir+os.path.sep+pro
        print pdir
        os.chdir(pdir)
        cmd = 'mvn clean'
        print cmd
        os.system(cmd)
        os.chdir(curdir)


def build():
    for pro in sub_projects:
        pdir=curdir+os.path.sep+pro
        print pdir
        os.chdir(pdir)
   
        cmd = 'mvn clean install -Dmaven.test.skip=true'
        #cmd = 'mvn clean install'
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

    utilPath = curdir + os.path.sep + "renren-search-util/target/renren-hydra-util-%s-SNAPSHOT.jar" % version 
    searcherPath = curdir + os.path.sep + "renren-search-searcher/target/renren-hydra-searcher-%s-SNAPSHOT.jar" % version
    brokerPath = curdir + os.path.sep + "renren-search-broker/target/renren-hydra-broker-%s-SNAPSHOT.jar" % version

    cmdbase = "cp -i %s " + installPath
   
    os.system(cmdbase % utilPath)
    os.system(cmdbase % searcherPath)
    os.system(cmdbase % brokerPath)

def cleanAll():
    clean()
    os.chdir(plugin_dir)
    os.system("python auto.py -c")
    os.chdir(service_dir)
    os.system("python auto.py -c")
    os.chdir(hadoop_dir)
    os.system("python auto.py -c")

def buildAll():
    build()
    os.chdir(plugin_dir)
    os.system("python auto.py -b")
    os.chdir(service_dir)
    os.system("python auto.py -b")
    os.chdir(hadoop_dir)
    os.system("python auto.py -b")

def installAll(baseDeployPath):
   deployPath=os.path.abspath(baseDeployPath)
   install(deployPath+os.path.sep+"config/nodeConf/lib")
   os.chdir(plugin_dir)
   os.system("python auto.py -i %s" % (deployPath+os.path.sep+"plugins"))
   os.chdir(service_dir)
   os.system("python auto.py -i %s" % (deployPath+os.path.sep+"services"))
   os.chdir(hadoop_dir)
   os.system("python auto.py -i %s" % (deployPath+os.path.sep+"script/offline_hadoop_job"))
  
def deploy():
    for pro in sub_projects:
        pdir=curdir+os.path.sep+pro
        print pdir
        os.chdir(pdir)
        cmd = 'mvn deploy'
        print cmd
        os.system(cmd)
        os.chdir(curdir)


def deployAll():
    deploy()
    os.chdir(service_dir)
    os.system("python auto.py -d")
    os.chdir(hadoop_dir)
    os.system("python auto.py -d")
    
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
    elif(command=='--install' or command=='-i'):
        if(len(sys.argv)<3):
            print 'must specific install path for install'
            sys.exit(-1)
        install(sys.argv[2])
    elif(command=='--installAll' or command=='-ia'):
        if(len(sys.argv)<3):
            print 'must specific install path for install'
            sys.exit(-1)
        installAll(sys.argv[2])
    elif(command=='--buildAll' or command=='-ba'):
        buildAll()
    elif(command=='--cleanAll' or command=='-ca'):
        cleanAll()
    elif(command=='--deploy' or command=='-d'):
        deploy()
    elif(command=='--deployAll' or command=='-da'):
        deployAll()
    else:
        print 'error usage'
        print 'usage python %s [-c --clean|-b --build|-i --install path] ' % sys.argv[0]
        sys.exit(-1)

    sys.exit(0) 


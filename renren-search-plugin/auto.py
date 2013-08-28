#coding:utf-8

import os

version="1.0"
curdir=os.getcwd()


def clean():
    cmd = 'mvn clean'
    print cmd
    os.system(cmd)

def build():
    cmd = 'mvn clean install'
    print cmd
    os.system(cmd)

def install(installPath):
    os.chdir(curdir)
    if(os.path.isdir(installPath)==False):
        cFlag=raw_input("folder %s not exists, create and continue?[y|n]: " % installPath)
        if(cFlag.lower()=='y'):
            os.mkdir(installPath)
        else:
            print 'please check installPath %s' % installPath

    cmdbase = "cp -i %s " + installPath
    jarPath = curdir + os.path.sep + "target/renren-hydra-plugin-%s-SNAPSHOT.jar" % version
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


#coding:utf-8

import os
version="1.0"

basedir=os.getcwd()

def clean():
    util=basedir+os.path.sep+"util"
    os.chdir(util)
    os.system("mvn clean")
 
    cleandata=basedir+os.path.sep+"clean_delete_data_job"
    os.chdir(cleandata)
    os.system("mvn clean")

    
    dedup=basedir+os.path.sep+"uid_dedup_job"
    os.chdir(dedup)
    os.system("mvn clean")
    
    buildindex=basedir+os.path.sep+"build_index_job"
    os.chdir(buildindex)
    os.system("mvn clean")

    buildattribute=basedir+os.path.sep+"build_attribute_job"
    os.chdir(buildattribute)
    os.system("mvn clean")


def deploy():
    util=basedir+os.path.sep+"util"
    os.chdir(util)
    os.system("mvn deploy")
 
    cleandata=basedir+os.path.sep+"clean_delete_data_job"
    os.chdir(cleandata)
    os.system("mvn deploy")

    
    dedup=basedir+os.path.sep+"uid_dedup_job"
    os.chdir(dedup)
    os.system("mvn deploy")
    
    buildindex=basedir+os.path.sep+"build_index_job"
    os.chdir(buildindex)
    os.system("mvn deploy")

    buildattribute=basedir+os.path.sep+"build_attribute_job"
    os.chdir(buildattribute)
    os.system("mvn deploy")


def build():
    util=basedir+os.path.sep+"util"
    os.chdir(util)
    os.system("mvn clean install")
 
    cleandata=basedir+os.path.sep+"clean_delete_data_job"
    os.chdir(cleandata)
    os.system("mvn clean package assembly:assembly")

    
    dedup=basedir+os.path.sep+"uid_dedup_job"
    os.chdir(dedup)
    os.system("mvn clean package assembly:assembly")
    
    buildindex=basedir+os.path.sep+"build_index_job"
    os.chdir(buildindex)
    os.system("mvn clean package assembly:assembly")

    buildattribute=basedir+os.path.sep+"build_attribute_job"
    os.chdir(buildattribute)
    os.system("mvn clean package assembly:assembly")

def install(installPath):
    os.chdir(basedir)
    if(os.path.isdir(installPath)==False):
        cFlag=raw_input("folder %s not exists, create and continue?[y|n]: " % installPath)
        if(cFlag.lower()=='y'):
            os.mkdir(installPath)
        else:
            print 'please check installPath %s' % installPath
    
    cleandata=basedir+os.path.sep+"clean_delete_data_job/target/clean_delete_data_job-%s-SNAPSHOT-jar-with-dependencies.jar" % version
    cmd="cp %s %s -i" % (cleandata,installPath)
    print cmd
    os.system(cmd)


    dedup=basedir+os.path.sep+"uid_dedup_job/target/uid_dedup_job-%s-SNAPSHOT-jar-with-dependencies.jar" % version
    cmd="cp %s %s -i" % (dedup,installPath)
    print cmd
    os.system(cmd)
    
    buildindex=basedir+os.path.sep+"build_index_job/target/build-index-job-%s-SNAPSHOT-jar-with-dependencies.jar" % version
    cmd="cp %s %s -i" % (buildindex,installPath)
    print cmd
    os.system(cmd)


    buildattribute=basedir+os.path.sep+"build_attribute_job/target/build-attribute-job-%s-SNAPSHOT-jar-with-dependencies.jar" % version
    cmd="cp %s %s -i" % (buildattribute,installPath)
    print cmd
    os.system(cmd)
    


if __name__=='__main__':
    import sys
    if(len(sys.argv)<2):
        print 'usage python %s [-b --build|-i --install path] ' % sys.argv[0]
        sys.exit(-1) 
    command=sys.argv[1]
    if(command=='--build' or command =='-b'):
        build()
    elif(command=='--clean' or command =='-c'):
        clean()
    elif(command=='--deploy' or command =='-d'):
        deploy()
    elif(command=='--install' or command=='-i'):
        if(len(sys.argv)<3):
            print 'must specific install path for install'
            sys.exit(-1)
        install(sys.argv[2])
    else:
        print 'error usage'
        sys.exit(-1)

    sys.exit(0) 

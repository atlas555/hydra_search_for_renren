#coding:utf-8

from util import *
from settings import *

def copyLocal2Hdfs(localDir,hdfsDir):
    cmd="hadoop fs -copyFromLocal {0} {1}".format(localDir,hdfsDir)
    return execute(cmd)

def copyHdfs2Local(hdfsDir,localDir):
    cmd="hadoop fs -copyToLocal {0} {1}".format(hdfsDir,localDir)
    return execute(cmd)

def HdfsMove(src,dst):
    cmd="hadoop fs -mv {0} {1}".format(src,dst)
    return execute(cmd)

def HdfsRmr(folder):
    cmd="hadoop fs -rmr {0}".format(folder)
    return execute(cmd)

def HdfsRm(path):
    cmd="hadoop fs -rm {0}".format(path)
    return execute(cmd)
   
def HdfsCopy(src,dst):
    cmd="hadoop fs -cp {0} {1}".format(src,dst)
    return execute(cmd)


def HdfsMkdir(path):
    cmd="hadoop fs -mkdir {0}".format(path)
    return execute(cmd) 

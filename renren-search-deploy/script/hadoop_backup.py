#!/usr/bin/env python3

import glob,os

locale_data_path="/data/xce/dong.liang/hadoop-data" #本地待备份数据的根目录
tmp_data_path="/home/renren.search/hd_backup/hadoop_data" #远程数据临时存储目录
hadoop_cmd_path="/opt/hadoop/bin/hadoop" #远程hadoop命令所在路径
hadoop_root_path="/user/renren.search/chunfei.yang" #hadoop数据存储根目录
dest_user_host="renren.search@m1.bjcer.hadoop.d.xiaonei.com" #远程目标机器的用户名以及host

fileInfoFromLocale=set()
fileInfoFromHadoop=set()

class FileInfo:
      last_file_index=-1
      max_file_index=-1

def findFilesFromLocale(dir):
   files=os.listdir(dir)
   for file in files:
     file_name=dir+"/"+file
     if(os.path.isdir(file_name)):
        findFilesFromLocale(file_name)
     if(os.path.isfile(file_name) and file.startswith("version.")):
        fileInfo=getPathWithFiles(dir,fileInfoFromLocale)
        seq=file[len("version."):]
        if(fileInfo.max_file_index<int(seq)):
           fileInfo.max_file_index=int(seq)

def getPathWithFiles(dir,fileInfoArray):
    for fileInfo in fileInfoArray:
       if(fileInfo.path==dir):
           return fileInfo
    fileInfo=FileInfo()
    fileInfo.path=dir
    fileInfoArray.add(fileInfo)
    return fileInfo

def findFilesFromHadoop(dir):
    find_hadoop_files="ssh "+dest_user_host+" \""+hadoop_cmd_path+" fs -ls "+dir+" \""
    print ("find_hadoop_files cmd:\t"+find_hadoop_files)
    output=os.popen(find_hadoop_files).readlines()
    for line in output:
        dealLine(dir,line)


def dealLine(dir,line):
    print ("dealLine parameters( dir:\t"+dir+"\tline:\t"+line+" )")
    if (not line.startswith("Found")):
           fileInfos=line.strip("\n").split()
           print ("fileInfos:")
           print ( fileInfos )
           file_type=fileInfos[0][0]
           print("file tpye:\t"+file_type)
           file_name=fileInfos[7]
           print("file name:\t"+file_name)
           
           if ( file_type == "d" ):
              findFilesFromHadoop(file_name)
           else:
              simple_name = file_name[len(dir)+1:]
              print ("simple_name:\t"+simple_name)
              if (simple_name.startswith("version.")):
                 fileInfo=getPathWithFiles(dir,fileInfoFromHadoop)
                 seq=simple_name[len("version."):]
                 if (fileInfo.last_file_index<int(seq)):
                    fileInfo.last_file_index=int(seq)
                  
def printFiles(fileInfoArray):
    for fileInfo in fileInfoArray:
        print("path:%s\tlast_file_index:%d\tmax_file_index:%d" %(fileInfo.path,fileInfo.last_file_index,fileInfo.max_file_index))

def mergTransferInfo():
    for hadoopFileInfo in fileInfoFromHadoop:
        contains=False
        for localeFileInfo in fileInfoFromLocale:
            if (localeFileInfo.path[len(locale_data_path):] == hadoopFileInfo.path[len(hadoop_root_path):]):
               contains=True
               localeFileInfo.last_file_index = hadoopFileInfo.last_file_index
        if (not contains):
           fileInfo = FileInfo()
           fileInfo.path= locale_data_path + hadoopFileInfo.path[len(hadoop_root_path):]
           fileInfo.last_file_index = hadoopFileInfo.last_file_index
           fileInfoFromLocale.add(fileInfo)

def transferData():
    checkRemoteDir()
    copyfiles()

def checkRemoteDir():
    length=len(locale_data_path)
    for localeFileInfo in fileInfoFromLocale:
        path=localeFileInfo.path
        subpath=path[length:]

        if ( not (subpath.strip() == "") ):
           testHadoopDir = "ssh "+dest_user_host+" \""+hadoop_cmd_path+" fs -test -d "+hadoop_root_path+subpath+"\""
           ressult = os.system(testHadoopDir)

           if ( not ressult == 0 ):
              mkdirCMD = "ssh "+dest_user_host+" \""+hadoop_cmd_path+" fs -mkdir "+hadoop_root_path+subpath+"\""
              print ("mkdirCMD:\t"+mkdirCMD)
              os.system(mkdirCMD)
           else:
              print ( "path \""+hadoop_root_path+subpath+"\" already exists on hadoop.")

def copyfiles():
    length=len(locale_data_path)
    for fileInfo in fileInfoFromLocale:
        if (fileInfo.max_file_index > fileInfo.last_file_index):
           path=fileInfo.path
           subpath=path[length:]
           hadooppath=hadoop_root_path+subpath
                   
           for index in range(fileInfo.last_file_index+1,fileInfo.max_file_index+1):
               localeDataFile=path+"/"+str(index)
               localeVersionFile=path+"/version."+str(index)

               if ( os.path.isfile(localeDataFile) and os.path.isfile(localeVersionFile) ):

                  copyDataFileCMD = "scp "+localeDataFile+" "+dest_user_host+":"+tmp_data_path
                  copyVersionFileCMD = "scp "+localeVersionFile+" "+dest_user_host+":"+tmp_data_path
                                  
                  print("copyDataFileCMD:\t"+copyDataFileCMD+"\ncopyVersionFileCMD:\t"+copyVersionFileCMD)
                                  
                  os.system(copyDataFileCMD)
                  os.system(copyVersionFileCMD)
                                  
                  tmpDataFile = tmp_data_path+"/"+str(index)
                  tmpVersionFile = tmp_data_path+"/version."+str(index)
                                  
                  putDataFileCMD = "ssh "+dest_user_host+" \""+hadoop_cmd_path+" fs -put "+tmpDataFile+" "+hadooppath+"\""
                  putVersionFileCMD = "ssh "+dest_user_host+" \""+hadoop_cmd_path+" fs -put "+tmpVersionFile+" "+hadooppath+"\""
                                  
                  print ("putDataFileCMD:\t"+putDataFileCMD+"\nputVersionFileCMD:\t"+putVersionFileCMD)
                          
                  os.system(putDataFileCMD)
                  os.system(putVersionFileCMD)
                                  
                  deleteTmpDataFile = "ssh "+dest_user_host+" \"rm -f "+tmpDataFile+"\""                                  
                  deleteTmpVersionFile = "ssh "+dest_user_host+" \"rm -f "+tmpVersionFile+"\""
                                  
                  print ("deleteTmpDataFile:\t"+deleteTmpDataFile+"\ndeleteTmpVersionFile:\t"+deleteTmpVersionFile)
                                  
                  os.system(deleteTmpDataFile)
                  os.system(deleteTmpVersionFile)
                                  
               else:
                  print("file \""+localeDataFile+"\" or \""+localeVersionFile+"\" dose not exists!")

findFilesFromLocale(locale_data_path)
findFilesFromHadoop(hadoop_root_path)

print("===================fileInfoFromHadoop======================")
printFiles(fileInfoFromHadoop)
print("===================fileInfoFromLocale======================")
printFiles(fileInfoFromLocale)

mergTransferInfo()
print("===================fileInfoFromLocale after mergered======================")
printFiles(fileInfoFromLocale)

transferData()

import subprocess
import time
import os

FILE_PATH = "/".join(os.path.realpath(__file__).split("/")[0:-1])+"/"
TEMP_FILE_PATH = "__temp_working_file"

#
# Subprocess wrapper to exit on errors.
#
def subprocessCall(argsList,quitOnError=True,stdout=None):
    returnCode = subprocess.call(argsList,stdout=stdout)
    if (quitOnError and 0 != returnCode):
        print "Error executing subprocess:\n"
        print " ".join(argsList)
        exit(1)
    return returnCode

#
# Executes hive scrits. 
# Changes to the parent directory and the restores the working directory when finished
# Need so we can use the same derby.log 
#
def hiveScript(script,stdout=None):
    cwd = os.getcwd()
    os.chdir(FILE_PATH)
    os.chdir("..")
    returnCode = subprocessCall(["hive","-e",script],stdout=stdout)
    os.chdir(cwd)
    return returnCode


#
# copy local files to hdfs
#
def copyFromLocal(localPath,hdfsPath):
    subprocessCall(["hadoop","fs","-copyFromLocal",localPath,hdfsPath])   



#
# list the contents of a dir in hdfs
#
def listHdfsDir(path):
    fobj = open(TEMP_FILE_PATH,'w')
    subprocessCall(["hadoop","fs","-ls",path],stdout=fobj)
    fobj.close()

    fobj = open(TEMP_FILE_PATH,'r')
    paths = []
    for line in fobj:
        line = line.strip()
        if (line.find("_") == -1 and line.find("/") != -1 ):
            paths.append(line[line.find("/"):])
    return paths
#
# Print to stdout with 3 N lines of * before and after
#
def obnoxiousPrint(prompt,N=3):
    for i in range(N):
        print"*"
    print "*** "+prompt+" ***"
    for i in range(N):
        print"*"

#
# list the contents of a dir in hdfs
#
def lsrHdfsDir(path):
    fobj = open(TEMP_FILE_PATH,'w')
    subprocessCall(["hadoop","fs","-lsr",path],stdout=fobj)
    fobj.close()

    fobj = open(TEMP_FILE_PATH,'r')
    paths = []
    for line in fobj:
        line = line.strip()
        if (line.find("_") == -1 and line.find("/") != -1 ):
            paths.append(line[line.find("/"):])
    return paths

import subprocess
import os
import shutil
import bigdataUtilities as util
import sys

if __name__ == "__main__":

    # base directory in HDFS.  Make sure you're able to write/delete in this directory
    base_dir = "/analytics/leaf-compression"
    version = "-0.1-SNAPSHOT"

    # Comma seperated list of zookeeper nodes in ip:port format.  2181 is the default port.    
    zkList = "localhost:2181"

    slaves = 1 
    if len(sys.argv) > 1:
        slaves = int(sys.argv[1])

    util.obnoxiousPrint("Uploading data to hdfs")
    util.subprocessCall(["hadoop","fs","-rmr", base_dir + "/input"],False)
    util.subprocessCall(["hadoop","fs","-rmr", base_dir + "/output/leaf"],False)
    util.subprocessCall(["hadoop","fs","-mkdir","-p", base_dir + "/input"],False)
    util.subprocessCall(["hadoop","fs","-copyFromLocal","example_data.csv",base_dir + "/input/small_graph"])

    util.obnoxiousPrint("Running leafcompress job")
    util.subprocessCall(["hadoop","jar","target/leaf-compression"+version+"-jar-with-dependencies.jar",
                    "org.apache.giraph.GiraphRunner",
                    "-Dgiraph.zkList=" + zkList,
                    "com.soteradefense.bsp.KeyDataVertex",
                    "-w", str(slaves),
                    "-vif", "com.soteradefense.bsp.KeyDataVertexInputFormat",
                    "-of", "com.soteradefense.bsp.KeyDataVertexOutputFormat",
                    "-vip", base_dir + "/input/small_graph",
                    "-op", base_dir + "/output/leaf"])

    # From this point down we are simply copying the output
    # and formating it for local reading.


    util.obnoxiousPrint("Copying results from hdfs to local output directory")
    try:
        shutil.rmtree("output")
    except:
        pass

    util.subprocessCall(["hadoop","fs","-get",base_dir + "/output","."])

    # read in results and move the smaller key to the left
    # this ensures consistent outputs from different methods.
    results = []
    files = filter(lambda x: x[0][0] != "_" and x[0][0] != '.' ,os.listdir("output/leaf"))
    for file in files:
        fobj = open("output/leaf/"+file,"r")
        for line in fobj:
            temp_array = line.strip().split("\t")
            
            if (temp_array[0] > temp_array[1]):
                temp = temp_array[0]
                temp_array[0] = temp_array[1]
                temp_array[1] = temp
            results.append(temp_array)
        fobj.close()

    # first pass sort, sort by first ip
    results = sorted(results,key=lambda x: x[1])
    results = sorted(results,key=lambda x: x[0])
    

    # put the results in a local file
    fobj = open("output/sorted_out.txt","w")
    for result in results:
        fobj.write("\t".join(result)+"\n")


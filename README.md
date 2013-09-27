Distributed Leaf Compression
============================

This project is a Giraph/Hadoop implementation of a leaf compression algorithm.  The leaf compression algorithm will remove any nodes from a Network Graph who only has one or zero edges.


Build Instructions
------------------

Prior to building you must first download appache giraph and build a version for your cluster.  
Then install the giraph-core-with-dependencies.jar into your local mvn repository.  Building Giraph and Leaf Compression require maven to be installed and executable on the command line.

These are instructions for building Giraph 1.0 against CDH 4.2.0.

1. Download Giraph (http://giraph.apache.org/) -> (http://www.apache.org/dyn/closer.cgi/giraph/giraph-1.0.0)

2. Extract.

3. Find the hadoop_cdh4.1.2 profile within pom.xml and copy the entire section and paste below.

4. Edit the new section changing instances of 4.1.2 to 4.2.0 within the section.

5. From the command line at the top level type 'mvn -Phadoop_cdh4.2.0 -DskipTests clean install'

6. This will install giraph-core-1.0.0.jar in your local maven repository specifically usable for CDH 4.2.0

7. You should now be able to build the leaf-compression by running mvn assembly:assembly This will create the executable jar in the target folder.

Example Run
-----------
A small example is included to verify installation and the general concept.  It is configured to run on your local machine hadoop cluster that has Apache Zookeeper installed and running.

To run, execute 'python Driver.py'

The output is also stored on output/sorted_out.txt


Other Information
-----------------

The graph must be stored as a bi-directional graph with one vertex represented below in a 
tab-delimited file stored on hdfs.  The columns required are node id and the edge list.  The edge list should be a comma-separated list of other nodes.

For example...

> 12345	1|2|9<br>
1	12345<br>
2	12345<br>
9	12345<br> 

In this case node 12345 has edges to nodes 1,2, and 9.  Remember that the data must be bi-directional.

The Driver.py script can also take in one argument, to change the number of Giraph Workers.  The number of Giraph workers is 1 LESS than the number of mappers your cluster will launch.  The default value for this analytic is 1.  

This analytic will write data to and from hdfs.  The default directory is '/analytics/leaf-compression/' .  This value can be set by modifying Driver.py . 




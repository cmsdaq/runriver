elasticsearch-river-RunRiver service
==========================

##Requirements

Maven3, java-8 (Oracle or OpenJDK), elasticsearch

##Compile and Install

Maven is used for building the service jar. If appropriate version is not available on the host OS, a custom version can be installed.
These are instructions for installing the tool in /opt as root user:

cd /opt
wget http://mirrors.gigenet.com/apache/maven/maven-3/3.3.9/binaries/apache-maven-3.3.9-bin.tar.gz
tar xzf apache-maven-3.3.9-bin.tar.gz
ln -s apache-maven-3.3.9-bin maven
rm -rf apache-maven-3.3.9-bin.tar.gz

edit /etc/profile.d/maven.sh and add the following lines:

export M2_HOME=/opt/maven
export M2=$M2_HOME/bin
PATH=$M2:$PATH

##Building jar (on a first run, Maven will pull all depencies):

mvn package

##Building rpm:


##Adding the river for the subsystem (cdaq):

curl -XPUT es-cdaq:9200/river/instance/river_cdaq_main -d '{
    "es_central_cluster":"es-cdaq",
    "es_tribe_host" : "es-tribe",
    "es_tribe_cluster" : "es-tribe",
    "polling_interval" : 15,
    "fetching_interval" : 5,
    "runIndex_read" : "runindex_cdaq_read",
    "runIndex_write" : "runindex_cdaq_write",
    "boxinfo_write" : "boxinfo_cdaq_write",
    "enable_stats" : false,
    "node":{"status":"created"},
    "subsystem":"cdaq", 
    "instance_name":"river_cdaq_main"
}'

##Adding the river for the subsystem (minidaq):

curl -XPUT es-cdaq:9200/river/instance/river_cdaq_main -d '{
    "es_central_cluster":"es-cdaq",
    "es_tribe_host" : "es-tribe",
    "es_tribe_cluster" : "es-tribe",
    "polling_interval" : 15,
    "fetching_interval" : 5,
    "runIndex_read" : "runindex_cdaq_read",
    "runIndex_write" : "runindex_cdaq_write",
    "boxinfo_write" : "boxinfo_cdaq_write",
    "enable_stats" : false,
    "node":{"status":"created"},
    "subsystem":"cdaq", 
    "instance_name":"river_cdaq_main"


Restart river service on es-cdaq nodes in case there was another river instance for the modified subsystem which was previously running:

sudo /sbin/service riverd restart

##Deleting the cdaq river:

curl -XDELETE localhost:9200/river/instance/river_cdaq_main

And restart the river service.

##Adding or deleting the river (minidaq)

curl -XPUT es-cdaq:9200/river/instance/river_minidaq_main -d '{
    "es_central_cluster":"es-cdaq",
    "es_tribe_host" : "es-tribe",
    "es_tribe_cluster" : "es-tribe",
    "polling_interval" : 15,
    "fetching_interval" : 5,
    "runIndex_read" : "runindex_minidaq_read",
    "runIndex_write" : "runindex_minidaq_write",
    "boxinfo_write" : "boxinfo_minidaq_write",
    "enable_stats" : false,
    "node":{"status":"created"},
    "subsystem":"minidaq", 
    "instance_name":"river_minidaq_main"
}'

curl -XDELETE localhost:9200/river/instance/runriver_minidaq_main


elasticsearch-river-RunRiver service
==========================

##Requirements

Maven3, java-8 (Oracle or OpenJDK), elasticsearch 2.2.0 or higher

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

Target version of the River application and elasticsearch version are set in pom.xml file. Building command:

source build.sh #contains:mvn clean compile assembly:single

If building is successful, jar file will be found in "target" subdirectory.

##Building rpm:

Adjust "riverfile" name to the compiled jar version and set RPM target version in scripts/elastic-metarpm.sh. Then run the script:

scripts/elastic-metarpm.sh

##cleaning up river instance index for specific subsystem (caution, this requires inserting "main" documents after riverd restart on es-cdaq hosts):

curl -XDELETE es-cdaq:9200/river/_query -d'{query:{prefix:{_id:"river_$SUBSYSTEM"}}}'

Deleting individual documents:

curl -XDELETE localhost:9200/river/instance/river_cdaq_main

After cleanup, restart riverd service on all es-cdaq machines

sudo /etc/init.d/riverd restart

##Adding/modifying river for the subsystem (cdaq):

curl -XPUT es-cdaq:9200/river/instance/river_cdaq_main -d '{
    "es_central_cluster":"es-cdaq",
    "es_tribe_host" : "es-local",
    "es_tribe_cluster" : "es-local",
    "polling_interval" : 15,
    "fetching_interval" : 5,
    "runIndex_read" : "runindex_cdaq_read",
    "runIndex_write" : "runindex_cdaq_write",
    "boxinfo_write" : "boxinfo_cdaq_write",
    "boxinfo_read" : "boxinfo_cdaq_read",
    "enable_stats" : false,
    "node":{"status":"created"},
    "subsystem":"cdaq", 
    "instance_name":"river_cdaq_main",
    "close_indices" : true
}'

##Adding the river for the subsystem (minidaq):

curl -XPUT es-cdaq:9200/river/instance/river_minidaq_main -d '{
    "es_central_cluster":"es-cdaq",
    "es_tribe_host" : "es-local",
    "es_tribe_cluster" : "es-local",
    "polling_interval" : 15,
    "fetching_interval" : 5,
    "runIndex_read" : "runindex_minidaq_read",
    "runIndex_write" : "runindex_minidaq_write",
    "boxinfo_write" : "boxinfo_minidaq_write",
    "boxinfo_read" : "boxinfo_minidaq_read",
    "enable_stats" : false,
    "node":{"status":"created"},
    "subsystem":"minidaq", 
    "instance_name":"river_minidaq_main",
    "close_indices" : true
}'

Equivalent subsystem name for daq2val is "dv".

Restart river service on es-cdaq nodes in case another version of the document was existing previously (i.e. it was updated):

sudo /sbin/service riverd restart

Alternatively, use "node":{"status":"restart"} to make river service handle restart of the instance (see restarting section).

##Deleting the cdaq or minidaq river:

curl -XDELETE localhost:9200/river/instance/river_cdaq_main

And restart the river service.

##Injecting run instance manually (with appropriate run number and subsystem):

curl -XPUT es-cdaq:9200/river/instance/river_cdaq_111222 {
    "instance_name" : "river_cdaq_111222",
    "subsystem" : "cdaq",
    "runNumber" : 111222,
    "es_tribe_host" : "es-local",
    "es_tribe_cluster" : "es-local",
    "fetching_interval" : 5,
    "runIndex_read" : "runindex_minidaq_read",
    "runIndex_write" : "runindex_minidaq_write",
    "boxinfo_read" : "boxinfo_minidaq_read",
    "boxinfo_write" : "boxinfo_minidaq_read",
    "enable_stats" : false,
    "close_indices" : true,
    "es_central_cluster" : "es-cdaq",
    "node" : { "status" : "created" }
}


##Restarting existing instance manually (from riverd 1.9.6):
Same method applies to either run or main nstance
curl -XPUT es-cdaq:9200/river/instance/river_cdaq_main d'{"doc":{"node":{"status":"restart"}}}'

curl -XPUT es-cdaq:9200/river/instance/river_cdaq_111222 d'{"doc":{"node":{"status":"restart"}}}'


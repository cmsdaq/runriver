elasticsearch-river-RunRiver
==========================

##Requirements

Maven3, java-7, elasticsearch

##Compile and Install

Compile:

cd 'packagefolder'

mvn3 package

Install:
PLEASE NOTE: If you are upgrading from a previous version, please remove all existing runriver documents before to install. Then remove the old version and install the new one. (See below for the related commands).

sudo /usr/share/elasticsearch/bin/plugin -url file:'packagefolder'/target/releases/river-runriver-1.3.2-plugin.zip -i river-runriver

Check:

sudo /usr/share/elasticsearch/bin/plugin  -l

Remove:

sudo /usr/share/elasticsearch/bin/plugin  -r river-runriver



#setting dynamic mapping (no more needed if mapping exists)
curl -XPUT localhost:9200/_river/_mapping/runriver -d '{"dynamic":true}'  #for index.mapper.dynamic false

##Adding the river (central)

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

##Deleting the river (central) - not yet working..

curl -XDELETE localhost:9200/_river/runriver/

##Adding the river (minidaq)


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

##Deleting the river (minidaq) - not yet working..

#curl -XDELETE localhost:9200/_river/runriver_minidaq/


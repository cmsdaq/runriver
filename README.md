elasticsearch-river-RunRiver
==========================

##Requirements

Maven3, java-7, elasticsearch

##Compile and Install

Compile:

cd 'packagefolder'

mvn3 package

Install:

sudo /usr/share/elasticsearch/bin/plugin -url file:'packagefolder'/target/releases/river-runriver-1.3.2-plugin.zip -i river-runriver

Check:

sudo /usr/share/elasticsearch/bin/plugin  -l

Remove:

sudo /usr/share/elasticsearch/bin/plugin  -r river-runriver


##Adding the river

curl -XPUT localhost:9200/_river/_mapping/runriver -d '{"dynamic":true}'  #for index.mapper.dynamic false

curl -XPUT localhost:9200/_river/runriver/_meta -d '{
    "type": "runriver",
    "es_tribe_host" : "es-tribe",
    "es_tribe_cluster" : "srv-c2a11-33-01",
    "polling_interval" : 30,
    "fetching_interval" : 5,
    "runIndex_read" : "runindex_cdaq_read",
    "runIndex_write" : "runindex_cdaq_write",
    "boxinfo_write" : "boxinfo_cdaq_write"
}'

##Deleting the river

curl -XDELETE localhost:9200/_river/runriver/



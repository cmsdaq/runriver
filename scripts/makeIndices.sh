#!/bin/bash

subsystem="cdaq"
newdate="20170112"
#subsystem="minidaq"
#newdate="20170113"
#subsystem="dv"
#newdate="20170114"

#make indices
echo "make ${subsystem}_${newdate}"
curl -XPUT es-cdaq:9200/runindex_${subsystem}_${newdate}?pretty -d'{"settings":{"index":{"number_of_shards" : "8","number_of_replicas" : "2","codec" : "best_compression"},"mapper" : { "dynamic" : "false"}, "translog" : {"durability" : "async"},"analysis" : {"analyzer" : {"default" : {"type" : "keyword"}}}}}'
curl -XPUT es-cdaq:9200/merging_${subsystem}_${newdate}?pretty  -d'{"settings":{"index":{"number_of_shards" : "8","number_of_replicas" : "1","codec" : "best_compression"},"mapper" : { "dynamic" : "false"}, "translog" : {"durability" : "async"},"analysis" : {"analyzer" : {"default" : {"type" : "keyword"}}}}}'
curl -XPUT es-cdaq:9200/boxinfo_${subsystem}_${newdate}?pretty  -d'{"settings":{"index":{"number_of_shards" : "8","number_of_replicas" : "1","codec" : "best_compression"},"mapper" : { "dynamic" : "false"}, "translog" : {"durability" : "async"},"analysis" : {"analyzer" : {"default" : {"type" : "keyword"}}}}}'
curl -XPUT es-cdaq:9200/hltdlogs_${subsystem}_${newdate}?pretty -d'{"settings":{"index":{"number_of_shards" : "8","number_of_replicas" : "1","codec" : "best_compression"},"mapper" : { "dynamic" : "false"}, "translog" : {"durability" : "async"},"analysis" : {"analyzer" : {"default" : {"type" : "keyword"}}}}}'

#run river jar with mapping as first parameter. Last parameter should specify name of the index, to inject the mapping.
echo
echo "#execute this to inject mapping:"
echo "/usr/bin/java -jar river-runriver-1.4.6-jar-with-dependencies.jar mapping ${subsystem} es-cdaq es-cdaq runindex_${subsystem}_${newdate} 0"
echo "./updatemappings.py es-cdaq auto runindex_${subsystem}_${newdate} boxinfo_${subsystem}_${newdate} hltdlogs_${subsystem}_${newdate}"
echo "./updatemappings.py es-cdaq copy runindex_${subsystem}_${newdate} merging_${subsystem}_${newdate}"
echo

echo "add set up aliases:"
echo ./updatemappings.py es-cdaq aliases ${subsystem} runindex_${subsystem}_${newdate} boxinfo_${subsystem}_${newdate} hltdlogs_${subsystem}_${newdate} merging_${subsystem}_${newdate}

#/usr/bin/java -jar river-runriver-1.4.6-jar-with-dependencies.jar mapping cdaq es-cdaq es-cdaq runindex_${subsystem}_${newdate} 0

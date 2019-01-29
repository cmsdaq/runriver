#!/bin/bash

subsystem="dv"
#3newdate="20181126"
newdate=`./getdate.py`

#make indices
echo "make ${subsystem}_${newdate}"
curl -H "Content-Type:application/json" -XPUT localhost:9200/runindex_${subsystem}_${newdate}?pretty -d'{"settings":{"index":{"number_of_shards" : "2","number_of_replicas" : "2","codec" : "best_compression"},"mapper" : { "dynamic" : "false"}, "translog" : {"durability" : "async","flush_threshold_size":"4g"},"analysis" : {"analyzer" : {"default" : {"type" : "keyword"}}}}}'
curl -H "Content-Type:application/json" -XPUT localhost:9200/merging_${subsystem}_${newdate}?pretty  -d'{"settings":{"index":{"number_of_shards" : "2","number_of_replicas" : "1","codec" : "best_compression"},"mapper" : { "dynamic" : "false"}, "translog" : {"durability" : "async","flush_threshold_size":"4g"},"analysis" : {"analyzer" : {"default" : {"type" : "keyword"}}}}}'
curl -H "Content-Type:application/json" -XPUT localhost:9200/boxinfo_${subsystem}_${newdate}?pretty  -d'{"settings":{"index":{"number_of_shards" : "2","number_of_replicas" : "1","codec" : "best_compression"},"mapper" : { "dynamic" : "false"}, "translog" : {"durability" : "async","flush_threshold_size":"4g"},"analysis" : {"analyzer" : {"default" : {"type" : "keyword"}}}}}'
curl -H "Content-Type:application/json" -XPUT localhost:9200/hltdlogs_${subsystem}_${newdate}?pretty -d'{"settings":{"index":{"number_of_shards" : "2","number_of_replicas" : "1","codec" : "best_compression"},"mapper" : { "dynamic" : "false"}, "translog" : {"durability" : "async","flush_threshold_size":"4g"},"analysis":{"analyzer":{"prefix-test-analyzer":{"type":"custom","tokenizer":"prefix-test-tokenizer"}},"tokenizer":{"prefix-test-tokenizer":{"type": "path_hierarchy","delimiter":" "}}}}}'

#run river jar with mapping as first parameter. Last parameter should specify name of the index, to inject the mapping.
echo
echo "#execute this to inject mapping:"
/usr/bin/java -jar river.jar mapping ${subsystem} localhost es-cdaq runindex_${subsystem}_${newdate} 0
/opt/fff/updatemappings.py localhost auto runindex_${subsystem}_${newdate} boxinfo_${subsystem}_${newdate} hltdlogs_${subsystem}_${newdate}
/opt/fff/updatemappings.py localhost copy runindex_${subsystem}_${newdate} merging_${subsystem}_${newdate}
echo

echo "#add set up aliases:"
echo /opt/fff/updatemappings.py localhost aliases ${subsystem} runindex_${subsystem}_${newdate} boxinfo_${subsystem}_${newdate} hltdlogs_${subsystem}_${newdate} merging_${subsystem}_${newdate}

#/usr/bin/java -jar river-runriver-1.4.6-jar-with-dependencies.jar mapping cdaq es-cdaq es-cdaq runindex_${subsystem}_${newdate} 0

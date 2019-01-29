#!/bin/bash

#uncomment to select subsystem and a new date

subsystem="cdaq"
#3newdate="20181127"
newdate=`./getdate.py`

#used to add year alias (e.g. runindex_cdaq2017_read)
year=${newdate:0:4}   ## or put explicit year if newdate doesn't start with year

#subsystem="minidaq"
#newdate="20170704"


#make indices
echo "make ${subsystem}_${newdate}"
curl -H "Content-Type:application/json" -XPUT localhost:9200/runindex_${subsystem}_${newdate}?pretty -d'{"settings":{"index":{"number_of_shards" : "2","number_of_replicas" : "0","codec" : "best_compression"},"mapper" : { "dynamic" : "false"}, "translog" : {"durability" : "async","flush_threshold_size":"4g"},"analysis" : {"analyzer" : {"default" : {"type" : "keyword"}}}}}'
curl -H "Content-Type:application/json" -XPUT localhost:9200/merging_${subsystem}_${newdate}?pretty  -d'{"settings":{"index":{"number_of_shards" : "2","number_of_replicas" : "0","codec" : "best_compression"},"mapper" : { "dynamic" : "false"}, "translog" : {"durability" : "async","flush_threshold_size":"4g"},"analysis" : {"analyzer" : {"default" : {"type" : "keyword"}}}}}'
curl -H "Content-Type:application/json" -XPUT localhost:9200/boxinfo_${subsystem}_${newdate}?pretty  -d'{"settings":{"index":{"number_of_shards" : "2","number_of_replicas" : "0","codec" : "best_compression"},"mapper" : { "dynamic" : "false"}, "translog" : {"durability" : "async","flush_threshold_size":"4g"},"analysis" : {"analyzer" : {"default" : {"type" : "keyword"}}}}}'
curl -H "Content-Type:application/json" -XPUT localhost:9200/hltdlogs_${subsystem}_${newdate}?pretty -d'{"settings":{"index":{"number_of_shards" : "1","number_of_replicas" : "0","codec" : "best_compression"},"mapper" : { "dynamic" : "false"}, "translog" : {"durability" : "async","flush_threshold_size":"4g"},"analysis":{"analyzer":{"prefix-test-analyzer":{"type":"custom","tokenizer":"prefix-test-tokenizer"}},"tokenizer":{"prefix-test-tokenizer":{"type": "path_hierarchy","delimiter":" "}}}}}'

#run river jar with mapping as first parameter. Last parameter should specify name of the index, to inject the mapping.
echo
echo "#executing inject mapping:"
/usr/bin/java -jar river.jar mapping ${subsystem} localhost es-cdaq runindex_${subsystem}_${newdate} 0
/opt/fff/updatemappings.py localhost auto runindex_${subsystem}_${newdate} boxinfo_${subsystem}_${newdate} hltdlogs_${subsystem}_${newdate}
/opt/fff/updatemappings.py localhost copy runindex_${subsystem}_${newdate} merging_${subsystem}_${newdate}
echo
echo "#add set up aliases:"
echo /opt/fff/updatemappings.py localhost aliases ${subsystem} runindex_${subsystem}_${newdate} boxinfo_${subsystem}_${newdate} hltdlogs_${subsystem}_${newdate} merging_${subsystem}_${newdate} $year

#TODO: add year alias mapping (like runindex_minidaq2017_read)

#!/bin/bash

if [ -z "$1" ]; then
 echo "please specify one of subsystems: cdaq, minidaq, dv, d3v, VM"
 exit 1
fi

if [ $1 == "cdaq" ] || [ $1 == "minidaq" ]; then
 subsystem=$1
 shards=8
 shards_log=2
 repl=1
fi
if [ $1 == "d3v" ] || [ $1 = "dv" ]; then
 subsystem=$1
 shards=2
 shards_log=2
 repl=1
fi

if [ $1 == "VM" ]; then
 subsystem="cdaq"
 shards=2
 shards_log=2
 repl=0
fi

if [ -z $subsystem ]; then
 echo "invalid subsystem $1"
 exit 1
fi

echo "option is $subsystem"

subsystem="cdaq"
#3newdate="20181127"
newdate=`./getdate.py`

#used to add year alias (e.g. runindex_cdaq2017_read)
year=${newdate:0:4}   ## or put explicit year if newdate doesn't start with year

#subsystem="minidaq"
#newdate="20170704"


#make indices

#TODO: in LS2 number f replicas for runindex_cdaq is 1. With more servers this can be again 2. Also applies to "river" indices

echo "make ${subsystem}_${newdate}"
curl -H "Content-Type:application/json" -XPUT localhost:9200/runindex_${subsystem}_${newdate}?pretty -d'{"settings":{"index":{"number_of_shards" : "8","number_of_replicas" : "1","codec" : "best_compression"}, "translog" : {"durability" : "async","flush_threshold_size":"4g"},"analysis" : {"analyzer" : {"default" : {"type" : "keyword"}}}}}'
curl -H "Content-Type:application/json" -XPUT localhost:9200/merging_${subsystem}_${newdate}?pretty  -d'{"settings":{"index":{"number_of_shards" : "8","number_of_replicas" : "1","codec" : "best_compression"}, "translog" : {"durability" : "async","flush_threshold_size":"4g"},"analysis" : {"analyzer" : {"default" : {"type" : "keyword"}}}}}'
curl -H "Content-Type:application/json" -XPUT localhost:9200/boxinfo_${subsystem}_${newdate}?pretty  -d'{"settings":{"index":{"number_of_shards" : "8","number_of_replicas" : "1","codec" : "best_compression"}, "translog" : {"durability" : "async","flush_threshold_size":"4g"},"analysis" : {"analyzer" : {"default" : {"type" : "keyword"}}}}}'
curl -H "Content-Type:application/json" -XPUT localhost:9200/hltdlogs_${subsystem}_${newdate}?pretty -d'{"settings":{"index":{"number_of_shards" : "2","number_of_replicas" : "1","codec" : "best_compression"}, "translog" : {"durability" : "async","flush_threshold_size":"4g"},"analysis":{"analyzer":{"prefix-test-analyzer":{"type":"custom","tokenizer":"prefix-test-tokenizer"}},"tokenizer":{"prefix-test-tokenizer":{"type": "path_hierarchy","delimiter":" "}}}}}'

#run river jar with mapping as first parameter. Last parameter should specify name of the index, to inject the mapping.
echo
echo "#executing inject mapping:"
/usr/bin/java -jar river.jar mapping ${subsystem} localhost es-cdaq runindex_${subsystem}_${newdate} 0
/opt/fff/updatemappings.py localhost auto runindex_${subsystem}_${newdate} boxinfo_${subsystem}_${newdate} hltdlogs_${subsystem}_${newdate}
/opt/fff/updatemappings.py localhost copy runindex_${subsystem}_${newdate} merging_${subsystem}_${newdate}
echo
echo "#add set up aliases:"
echo /opt/fff/updatemappings.py localhost aliases ${subsystem} runindex_${subsystem}_${newdate} boxinfo_${subsystem}_${newdate} hltdlogs_${subsystem}_${newdate} merging_${subsystem}_${newdate} $year
echo ""
echo '#for particular index (use copy for "merging_*"):'
echo '#'/opt/fff/updatemappings.py localhost alias runindex ${subsystem} runindex_${subsystem}_${newdate} $year
echo '#'/opt/fff/updatemappings.py localhost alias boxinfo ${subsystem} boxinfo_${subsystem}_${newdate} $year
echo '#'/opt/fff/updatemappings.py localhost alias hltdlogs ${subsystem} hltdlogs${subsystem}_${newdate} $year

#TODO: add year alias mapping (like runindex_minidaq2017_read)

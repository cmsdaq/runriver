#!/bin/bash

usr="f3root"
passcmd="sudo -u es-cdaq ./getpwd.py ${usr}"
pass=`${passcmd}`

#modify this if you are using custom version of river.jar or updatemappings.py script
UPDSCRIPTPREFIX="/opt/fff"
RIVERJARPREFIX="/opt/fff"

if [ -z "$1" ]; then
 echo "please specify one of subsystems: cdaq, minidaq, dv, d3v, VM"
 exit 1
fi

if [ $1 == "cdaq" ] || [ $1 == "minidaq" ]; then
 subsystem=$1
 shards=8
 shardsbox=4
 shardslog=2
 repl=1
fi
if [ $1 == "d3v" ] || [ $1 = "dv" ]; then
 subsystem=$1
 shards=2
 shardsbox=2
 shardslog=2
 repl=1
fi

if [ $1 == "VM" ]; then
 subsystem="cdaq"
 shards=2
 shardsbox=2
 shardslog=2
 repl=0
fi

if [ -z $subsystem ]; then
 echo "invalid subsystem $1"
 exit 1
fi

if [ -z $2 ]; then
 echo "using NO ingest processor for new indices"
 ingest=""
else
 echo "using ingest processor with timestamp injection"
 ingest=',"default_pipeline":"dateinjector"'
fi

echo "option is $subsystem"

#subsystem="cdaq"
#3newdate="20181127"
newdate="`./getdate.py`"

#used to add year alias (e.g. runindex_cdaq2017_read)
year=${newdate:0:4}   ## or put explicit year if newdate doesn't start with year

#subsystem="minidaq"
#newdate="20170704"

#------------------------------
#set ingest processor with set field and overwrite with timestamp. Use unix time format.
#echo "#setting ingest processor to apply injection timestamp:"
#curl --user ${usr}:${pass} -H "Content-Type:application/json" -XPOST localhost:9200/_scripts/timestampscript  -d'{"script":{"lang":"painless","source":"ctx.injdate = new Date().getTime()"}}'
#curl --user ${usr}:${pass} -H "Content-Type:application/json" -XPUT localhost:9200/_ingest/pipeline/dateinjector?pretty  -d'{"description":"inject timestamp", "processors":[{"script":{"id":"timestampscript"}}]}'

#TODO: in LS2 number f replicas for runindex_cdaq is 1. With more servers this can be again 2. Also applies to "river" indices

#------------------------------
#make indices
echo "make ${subsystem}_${newdate}"

curl --user ${usr}:${pass} -H "Content-Type:application/json" -XPUT localhost:9200/runindex_${subsystem}_${newdate}?pretty    -d'{"settings":{"index":{"number_of_shards" : "'$shards'",   "number_of_replicas" : "'$repl'","codec" : "best_compression"'${ingest}'}, "translog" : {"durability" : "async","flush_threshold_size":"4g"},"analysis" : {"analyzer" : {"default" : {"type" : "keyword"}}}}}' #-u fffadmin:$2

curl --user ${usr}:${pass} -H "Content-Type:application/json" -XPUT localhost:9200/merging_${subsystem}_${newdate}?pretty     -d'{"settings":{"index":{"number_of_shards" : "'$shards'",   "number_of_replicas" : "'$repl'","codec" : "best_compression"'${ingest}'}, "translog" : {"durability" : "async","flush_threshold_size":"4g"},"analysis" : {"analyzer" : {"default" : {"type" : "keyword"}}}}}' #-u fffadmin:$2

curl --user ${usr}:${pass} -H "Content-Type:application/json" -XPUT localhost:9200/boxinfo_${subsystem}_${newdate}?pretty     -d'{"settings":{"index":{"number_of_shards" : "'$shardsbox'","number_of_replicas" : "'$repl'","codec" : "best_compression"},            "translog" : {"durability" : "async","flush_threshold_size":"4g"},"analysis" : {"analyzer" : {"default" : {"type" : "keyword"}}}}}' #-u fffadmin:$2

curl --user ${usr}:${pass} -H "Content-Type:application/json" -XPUT localhost:9200/reshistory_${subsystem}_${newdate}?pretty  -d'{"settings":{"index":{"number_of_shards" : "'$shards'",   "number_of_replicas" : "'$repl'","codec" : "best_compression"'${ingest}'}, "translog" : {"durability" : "async","flush_threshold_size":"4g"},"analysis" : {"analyzer" : {"default" : {"type" : "keyword"}}}}}' #-u fffadmin:$2

curl --user ${usr}:${pass} -H "Content-Type:application/json" -XPUT localhost:9200/hltdlogs_${subsystem}_${newdate}?pretty    -d'{"settings":{"index":{"number_of_shards" : "'$shardslog'","number_of_replicas" : "'$repl'","codec" : "best_compression"'${ingest}'}, "translog" : {"durability" : "async","flush_threshold_size":"4g"},"analysis":{"analyzer":{"prefix-test-analyzer":{"type":"custom","tokenizer":"prefix-test-tokenizer"}},"tokenizer":{"prefix-test-tokenizer":{"type": "path_hierarchy","delimiter":" "}}}}}' #-u fffadmin:$2

#configure ingest

#run river jar with mapping as first parameter. Last parameter should specify name of the index, to inject the mapping.
#----------------------------------
echo
echo "#executing inject mapping:"
#reads elasticsearch credentials (needs full sudo on the machine)
sudo -u es-cdaq /usr/bin/java -jar ${RIVERJARPREFIX}/river.jar mapping ${subsystem} localhost es-cdaq runindex_${subsystem}_${newdate} 0
#reads es-cdaq credentials (superuser)
sudo -u es-cdaq ${UPDSCRIPTPREFIX}/updatemappings.py localhost auto runindex_${subsystem}_${newdate} boxinfo_${subsystem}_${newdate} hltdlogs_${subsystem}_${newdate}
sudo -u es-cdaq ${UPDSCRIPTPREFIX}/updatemappings.py localhost copy runindex_${subsystem}_${newdate} merging_${subsystem}_${newdate}
sudo -u es-cdaq ${UPDSCRIPTPREFIX}/updatemappings.py localhost copy boxinfo_${subsystem}_${newdate} reshistory_${subsystem}_${newdate}

echo
echo "#add set up aliases:"
echo 'sudo -u es-cdaq '${UPDSCRIPTPREFIX}/updatemappings.py localhost aliases ${subsystem} ${newdate} $year
echo ""
echo '#for particular index (except for "merging_*" and "reshistory_*"):'
echo '#sudo -u es-cdaq '${UPDSCRIPTPREFIX}/updatemappings.py localhost alias runindex ${subsystem} runindex_${subsystem}_${newdate} $yer
echo '#sudo -u es-cdaq '${UPDSCRIPTPREFIX}/updatemappings.py localhost alias boxinfo ${subsystem} boxinfo_${subsystem}_${newdate} $year
echo '#sudo -u es-cdaq '${UPDSCRIPTPREFIX}/updatemappings.py localhost alias hltdlogs ${subsystem} hltdlogs${subsystem}_${newdate} $year

#TODO: add year alias mapping (like runindex_minidaq2017_read)

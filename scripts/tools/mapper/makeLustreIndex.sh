#!/bin/bash

#3newdate="20180511v1"
newdate=`./getdate.py`

cluster="es-cdaq"

#make indices
echo "make lustre_info_${newdate}"
curl -XPUT ${cluster}:9200/lustre_info_${newdate}?pretty -d'{"settings":{"index":{"number_of_shards" : "4","number_of_replicas" : "1","codec" : "best_compression"}, "translog" : {"durability" : "async"},"analysis" : {"analyzer" : {"default" : {"type" : "keyword"}}}}}'

curl -XPOST ${cluster}:9200/_aliases -d'{"actions":[{"remove":{"index":"lustre_info_*","alias":"lustre_info"}},{"add":{"index":"lustre_info_'${newdate}'","alias":"lustre_info"}}]}'

#TODO: add year alias mapping (like runindex_minidaq2017_read)
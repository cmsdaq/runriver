#!/bin/env python

import sys
import httplib
import datetime

sys.path.append('/opt/fff')

from insertRiver import insertRiver

from riverMapping import riverInstMapping

riverInstSettings = {
    "index" : {
            "codec" : "best_compression",
            "number_of_shards" : "1",
            "mapper" : {
                    "dynamic" : "true"
            },
            "number_of_replicas" : "1", #1 replica for 2-host setup. Later will increase to 2
    }
}

conn = httplib.HTTPConnection(host='localhost',port=9200)


now = datetime.datetime.now()
newdate = str(now.year)+str(now.month).zfill(2)+str(now.day).zfill(2)

print("creating index: river_"+newdate)
success,st,ret = conn.request("PUT","/river_"+newdate,json.dumps({"mappings":{"instance":riverInstMapping},"settings":riverInstSettings}))
print success,st,ret

from updatemappings import elasticUpdatesr

elasticUpdater(["localhost","riveralias","river_"+newdate])

eslocal = "es-local"
if os.uname()[-1]=="es-vm-cdaq-01": eslocal="es-vm-local-01"

insertRiver(["system","cdaq",eslocal])
insertRiver(["system","minidaq",eslocal])
insertRiver(["system","dv",eslocal])
insertRiver(["system","d3v",eslocal])
insertRiver(["script", "mon_cpustats", "nodejs", "/cmsnfses-web/es-web/prod/daemons/lastcpu.js", "append_db_mon", "cdaq"])
insertRiver(["script", "index_del", "python", "/cmsnfses-web/es-web/prod/daemons/eslocal_index_cleaner.py", "admin", "all"])

print "injected river documents..."

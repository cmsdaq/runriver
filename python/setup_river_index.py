#!/bin/env python3.4
from __future__ import print_function

import sys
import os
import requests
import datetime
import json

sys.path.append('/opt/fff')

from insertRiver import insertRiver,parse_esweb_pwd

from riverMapping import riverInstMapping

riverInstSettings = {
    "index" : {
            "codec" : "best_compression",
            "number_of_shards" : "1",
            "number_of_replicas" : "1", #1 replica for 2-host setup. Later will increase to 2
            "priority":21
    }
}

now = datetime.datetime.now()
newdate = str(now.year)+str(now.month).zfill(2)+str(now.day).zfill(2)

print("creating index: river_"+newdate)
ret = requests.put("http://localhost:9200/river_"+newdate,json.dumps({"mappings":riverInstMapping,"settings":riverInstSettings}),headers={'Content-Type':'application/json'},auth=("f3root",parse_esweb_pwd("f3root")))
print(ret.content)

from updatemappings import elasticUpdater

elasticUpdater(["","localhost","riveralias","river_"+newdate])

eslocal = "es-local"
if os.uname()[1].startswith("es-vm-cdaq-01"): eslocal="es-vm-local-01"

insertRiver(["","system","cdaq",eslocal])

if eslocal!="es-vm-local-01":
  insertRiver(["","system","minidaq",eslocal])
  insertRiver(["","system","dv",eslocal])
  insertRiver(["","system","d3v",eslocal])

insertRiver(["","script", "mon_cpustats", "nodejs", "/cmsnfses-web/es-web/prod/daemons/lastcpu.js", "append_db_mon", "cdaq"])
insertRiver(["","script", "index_del", "python", "/cmsnfses-web/es-web/prod/daemons/eslocal_index_cleaner.py", "admin", "all"])

print("injected river documents...")

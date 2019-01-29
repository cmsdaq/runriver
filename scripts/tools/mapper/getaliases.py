#!/bin/env python

from __future__ import print_function 

import sys#,traceback
import os

import mappings

import requests
try:
  import simplejson as json
except:
  import json
import datetime

class elasticUpdater:

    def __init__(self):

        self.url="localhost"
        #update 3 explicitly specified indices
	system=sys.argv[1]
	try:
          year=sys.argv[2]
	except:
	  year=""
        res = requests.get('http://'+self.url+':9200/_aliases')
        aliases = json.loads(res.content)
#        old_idx_list = []
#        actions = []
        selected = {}
        for idx in aliases:
	    for k in aliases[idx]["aliases"].keys():
	      if "_"+system+year in k or k.startswith(system+year):
		selected[idx]={}

        #!/bin/env python
        now = datetime.datetime.now()
        newdate=str(now.year)+"-"+str(now.month).zfill(2)+"-"+str(now.day).zfill(2)
	print("curl -XPUT es-cdaq:9200/_snapshot/es_cdaq_run2_backup/snapshot_"+system+year+"_"+newdate,"-d'"+'{"indices":"'+','.join(selected.keys())+'"}'+"'")
#            for alias in aliases[idx]["aliases"]:
#              if alias in ['runindex_'+sys.argv[3],'runindex_'+sys.argv[3]+'_read','runindex_'+sys.argv[3]+'_write',
#                           'boxinfo_'+sys.argv[3],'boxinfo_'+sys.argv[3]+'_read','boxinfo_'+sys.argv[3]+'_write',
#                           'hltdlogs_'+sys.argv[3],'hltdlogs_'+sys.argv[3]+'_read','hltdlogs_'+sys.argv[3]+'_write',
#                           'merging_'+sys.argv[3],'merging_'+sys.argv[3]+'_write']:
#                actions.append({"remove":{"index":idx,"alias":alias}})
#                old_idx_list.append(idx)

if __name__ == "__main__":

    es = elasticUpdater()

    os._exit(0)

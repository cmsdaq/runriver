#!/bin/env python3.4
from __future__ import print_function
import sys
#allow importing files from running directory (mapping)
sys.path.append('/opt/fff')
sys.path.append(".")
import os

import requests
try:
  import simplejson as json
except:
  import json

from insertRiver import parse_esweb_pwd

f3_root="f3root"
f3_pwd=parse_esweb_pwd(f3_root)
authset=(f3_root,f3_pwd)

headers={'Content-Type':'application/json'}



try:
  import mappings
except:
  print("running without mappings")

class elasticUpdater:

    def __init__(self,argArray):
        if argArray==None:
          argArray = sys.argv
        self.execUpdater(argArray)

    def execUpdater (self,argv):
        if len(argv)<2:
          print("Usage:")
          print("  for runindex, boxinfo and hltdlogs indices:")
          print("       python updatemappings.py central-es-hostname subsystem")
          print("       (es-vm-cdaq-01 cdaq)")
          print("  or use this syntax to specify exact index names for runindex,boxinfo and hltdlogs:")
          print("       python updatemappings.py central-es-hostname 'null' runindex_index boxinfo_index hltdlogs_index")
          print("       (es-vm-cdaq-01 auto runindex_cdaq_20170101 boxinfo_cdaq_20170101 hltdlogs_cdaq_20170101)")
          print("  or for copying mapping from one index to another:")
          print("       python updatemappings.py central-es-hostname 'subsystem' input_index_mapping target_index")
          print("       (es-vm-cdaq-01 copy runindex_cdaq merging_cdaq)")
          print(" or for setting up subsystem aliases:")
          print("       python updatemappings.py central-es-hostname 'aliases' subsystem runindex_index boxinfo_index hltdlogs_index merging_index")
          print("       (es-vm-cdaq-01 aliases cdaq runindex_cdaq_20170101 boxinfo_cdaq_20170101 hltdlogs_cdaq_20170101 merging_cdaq_20170101 2017)")
          print(" or for setting up river alias:")
          print("       python updatemappings.py central-es-hostname 'riveralias' river_index")
          print("       (es-vm-cdaq-01 riveralias river_20190129)")
          os._exit(0)

        self.url=argv[1]

        if argv[2]=="auto":
          #update 3 explicitly specified indices
          self.updateIndexMappingMaybe(argv[3],mappings.central_runindex_mapping)
          self.updateIndexMappingMaybe(argv[4],mappings.central_boxinfo_mapping)
          self.updateIndexMappingMaybe(argv[5],mappings.central_hltdlogs_mapping)
        elif argv[2]=="copy":
          #copy single index mapping to another
          res = requests.get('http://'+self.url+':9200/'+argv[3]+'/_mapping',headers=headers) #NOTE: in es7 will drop type name (but this is just copy)
          if res.status_code==200:
            res_j = json.loads(res.content)
            for idx in res_j:
              #else:alias
              new_mapping = res_j[idx]['mappings']
              print(idx)
              self.updateIndexMappingMaybe(argv[4],new_mapping)
              break
        elif argv[2]=="aliases":
          res = requests.get('http://'+self.url+':9200/_aliases',headers=headers)
          aliases = json.loads(res.content)
          old_idx_list = []
          actions = []
          sub =  argv[3]
          isuf = "_"+sub+"_"+argv[4]
          for idx in aliases:
            #print idx
            for alias in aliases[idx]["aliases"]:
              if alias in ['runindex_'+sub,'runindex_'+sub+'_read','runindex_'+sub+'_write',
                           'boxinfo_'+sub,'boxinfo_'+sub+'_read','boxinfo_'+sub+'_write',
                           'reshistory_'+sub,'reshistory_'+sub+'_read','reshistory_'+sub+'_write',
                           'hltdlogs_'+sub,'hltdlogs_'+sub+'_read','hltdlogs_'+sub+'_write',
                           'merging_'+sub,'merging_'+sub+'_read','merging_'+sub+'_write']:
                actions.append({"remove":{"index":idx,"alias":alias}})
                old_idx_list.append(idx)

          for itype in ["runindex","boxinfo","reshistory","hltdlogs","merging"]:
            actions.append({"add":{"alias":itype+"_"+sub,         "index":itype+isuf}})
            actions.append({"add":{"alias":itype+"_"+sub+"_read", "index":itype+isuf}}) #not relevant
            actions.append({"add":{"alias":itype+"_"+sub+"_write","index":itype+isuf}})

          #set common aliases (will be removed at some point)
          actions.append({"add":{"alias":"runindex_"+sub+"_read","index":"merging"+isuf}})
          actions.append({"add":{"alias":"boxinfo_"+sub+"_read","index":"reshistory"+isuf}})

          #adding all-year index if required
          if len(argv)>5 and argv[5].isdigit():
            year_sub = sub+argv[5]

            for itype in ["runindex","boxinfo","reshistory","hltdlogs","merging"]:
              actions.append({"add":{"alias":itype+"_"+year_sub+"_read","index":itype+isuf}})

            #these are still under other index:
            actions.append({"add":{"alias":"runindex_"+year_sub+"_read","index":"merging"+isuf}})
            actions.append({"add":{"alias":"boxinfo_"+year_sub+"_read","index":"reshistory"+isuf}})

          data = json.dumps({"actions":actions})
          print(data)
          res = requests.post('http://'+self.url+':9200/_aliases',data,headers=headers,auth=authset)
          print(res.status_code)

          print("current",sub,"alias removed from indices: ",",".join(set(old_idx_list)).strip('"'))
          
          pass

        elif argv[2]=="alias":
         sub =  argv[3]
         if not sub=="merging": #exclude this special case
          res = requests.get('http://'+self.url+':9200/_aliases',headers=headers)
          aliases = json.loads(res.content)
          old_idx_list = []
          actions = []
          for idx in aliases:
            #print idx
            for alias in aliases[idx]["aliases"]:
              if alias in [sub+'_'+argv[4],sub+'_'+argv[4]+'_read',sub+'_'+argv[4]+'_write']:
                actions.append({"remove":{"index":idx,"alias":alias}})
                old_idx_list.append(idx)

          actions.append({"add":{"alias":sub+'_'+argv[4],"index":argv[5]}})
          actions.append({"add":{"alias":sub+'_'+argv[4]+"_read","index":argv[5]}})
          actions.append({"add":{"alias":sub+'_'+argv[4]+"_write","index":argv[5]}})

          #adding all-year index if required
          if len(argv)>6 and argv[6].isdigit():
              year_suffix = argv[4]+argv[6]
              actions.append({"add":{"alias":sub+'_'+year_suffix+"_read","index":argv[5]}})

          data = json.dumps({"actions":actions})
          print(data)
          res = requests.post('http://'+self.url+':9200/_aliases',data,headers=headers,auth=authset)
          print(res.status_code)

          print("current",argv[4],"alias removed from indices: ",",".join(set(old_idx_list)).strip('"'))

        elif argv[2]=="riveralias":

          res = requests.get('http://'+self.url+':9200/_aliases',headers=headers)
          aliases = json.loads(res.content)
          old_idx_list = []
          actions = []
          for idx in aliases:
            #print idx
            for alias in aliases[idx]["aliases"]:
              if alias == 'river':
                actions.append({"remove":{"index":idx,"alias":alias}})
                old_idx_list.append(idx)

          actions.append({"add":{"alias":"river","index":sub}})

          data = json.dumps({"actions":actions})
          print(data)
          res = requests.post('http://'+self.url+':9200/_aliases',data,headers=headers,auth=authset)
          print(res.status_code,res.content)

          print("current",sub,"alias removed from indices: ",",".join(set(old_idx_list)).strip('"'))
          
          pass
 
        else:
          self.runindex_name="runindex_"+argv[2]
          self.boxinfo_name="boxinfo_"+argv[2]
          self.hltdlogs_name="hltdlogs_"+argv[2]
          #update by alias
          self.updateIndexMappingMaybe(self.runindex_name,mappings.central_runindex_mapping)
          self.updateIndexMappingMaybe(self.boxinfo_name,mappings.central_boxinfo_mapping)
          self.updateIndexMappingMaybe(self.hltdlogs_name,mappings.central_hltdlogs_mapping)

    def updateIndexMappingMaybe(self,index_name,mapping):
        #update in case of new documents added to mapping definition
        res = requests.post('http://'+self.url+':9200/'+index_name+'/_mapping/',json.dumps(mapping),headers=headers,auth=authset)
        if res.status_code==200:
            print(index_name)
        else:
            print("FAILED",index_name,res.status_code,res.content)

if __name__ == "__main__":

    es = elasticUpdater(None)

    os._exit(0)

#!/bin/env python
from __future__ import print_function
import sys
#allow importing files from running directory (mapping)
sys.path.append(".")
import os

import requests
try:
  import simplejson as json
except:
  import json



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
          for idx in aliases:
            #print idx
            for alias in aliases[idx]["aliases"]:
              if alias in ['runindex_'+argv[3],'runindex_'+argv[3]+'_read','runindex_'+argv[3]+'_write',
                           'boxinfo_'+argv[3],'boxinfo_'+argv[3]+'_read','boxinfo_'+argv[3]+'_write',
                           'hltdlogs_'+argv[3],'hltdlogs_'+argv[3]+'_read','hltdlogs_'+argv[3]+'_write',
                           'merging_'+argv[3],'merging_'+argv[3]+'_write']:
                actions.append({"remove":{"index":idx,"alias":alias}})
                old_idx_list.append(idx)

          actions.append({"add":{"alias":"runindex_"+argv[3],"index":argv[4]}})
          actions.append({"add":{"alias":"runindex_"+argv[3]+"_read","index":argv[4]}})
          actions.append({"add":{"alias":"runindex_"+argv[3]+"_write","index":argv[4]}})

          actions.append({"add":{"alias":"boxinfo_"+argv[3],"index":argv[5]}})
          actions.append({"add":{"alias":"boxinfo_"+argv[3]+"_read","index":argv[5]}})
          actions.append({"add":{"alias":"boxinfo_"+argv[3]+"_write","index":argv[5]}})

          actions.append({"add":{"alias":"hltdlogs_"+argv[3],"index":argv[6]}})
          actions.append({"add":{"alias":"hltdlogs_"+argv[3]+"_read","index":argv[6]}})
          actions.append({"add":{"alias":"hltdlogs_"+argv[3]+"_write","index":argv[6]}})

          actions.append({"add":{"alias":"merging_"+argv[3],"index":argv[7]}})
          actions.append({"add":{"alias":"runindex_"+argv[3]+"_read","index":argv[7]}}) #!
          actions.append({"add":{"alias":"merging_"+argv[3]+"_write","index":argv[7]}})

          #adding all-year index if required
          if len(argv)>8 and argv[8].isdigit():
              year_suffix = argv[3]+argv[8]
              actions.append({"add":{"alias":"runindex_"+year_suffix+"_read","index":argv[4]}})
              actions.append({"add":{"alias":"boxinfo_" +year_suffix+"_read","index":argv[5]}})
              actions.append({"add":{"alias":"hltdlogs_"+year_suffix+"_read","index":argv[6]}})
              actions.append({"add":{"alias":"runindex_"+year_suffix+"_read","index":argv[7]}}) #!

          data = json.dumps({"actions":actions})
          print(data)
          res = requests.post('http://'+self.url+':9200/_aliases',data,headers=headers)
          print(res.status_code)

          print("current",argv[3],"alias removed from indices: ",",".join(set(old_idx_list)).strip('"'))
          
          pass

        elif argv[2]=="alias":
         if not argv[3]=="merging": #exclude this special case
          res = requests.get('http://'+self.url+':9200/_aliases',headers=headers)
          aliases = json.loads(res.content)
          old_idx_list = []
          actions = []
          for idx in aliases:
            #print idx
            for alias in aliases[idx]["aliases"]:
              if alias in [argv[3]+'_'+argv[4],argv[3]+'_'+argv[4]+'_read',argv[3]+'_'+argv[4]+'_write']:
                actions.append({"remove":{"index":idx,"alias":alias}})
                old_idx_list.append(idx)

          actions.append({"add":{"alias":argv[3]+'_'+argv[4],"index":argv[5]}})
          actions.append({"add":{"alias":argv[3]+'_'+argv[4]+"_read","index":argv[5]}})
          actions.append({"add":{"alias":argv[3]+'_'+argv[4]+"_write","index":argv[5]}})

          #adding all-year index if required
          if len(argv)>6 and argv[6].isdigit():
              year_suffix = argv[4]+argv[6]
              actions.append({"add":{"alias":argv[3]+'_'+year_suffix+"_read","index":argv[5]}})

          data = json.dumps({"actions":actions})
          print(data)
          res = requests.post('http://'+self.url+':9200/_aliases',data,headers=headers)
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

          actions.append({"add":{"alias":"river","index":argv[3]}})

          data = json.dumps({"actions":actions})
          print(data)
          res = requests.post('http://'+self.url+':9200/_aliases',data,headers=headers)
          print(res.status_code,res.content)

          print("current",argv[3],"alias removed from indices: ",",".join(set(old_idx_list)).strip('"'))
          
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
        res = requests.post('http://'+self.url+':9200/'+index_name+'/_mapping/',json.dumps(mapping),headers=headers)
        if res.status_code==200:
            print(index_name)
        else:
            print("FAILED",index_name,res.status_code,res.content)

if __name__ == "__main__":

    es = elasticUpdater(None)

    os._exit(0)

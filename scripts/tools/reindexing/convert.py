#!/bin/env python

import json
import sys
import requests

reply = requests.get("http://es-cdaq:9200/"+sys.argv[1]+"?pretty")
req_idx_keys = json.loads(reply.content).keys()
if len(req_idx_keys)>1:
  print "can't handle more than one index",req_idx_keys
  sys.exit(2)
elif len(req_idx_keys)==0:
  print "no index found"
  sys.exit(2)


fileorig = req_idx_keys[0]
print "#source",fileorig

with open(fileorig+".jsn",'w') as fi:
  fi.write(reply.content)

vpos = fileorig.find('_v')
if vpos!=-1:
  vdigit = fileorig[fileorig.find('_v')+2:]
  if vdigit.isdigit():
    target_name = fileorig[:vpos]+"_v"+str(int(vdigit)+1)
  else:
    target_name = fileorig+"_v1"
else:
  target_name = fileorig+"_v1"

#override if parameter
try:
  target_name = sys.argv[2]
except:pass

#with open(sys.argv[1],'r') as fi:
with open(fileorig+".jsn",'r') as fi:
    filejsn = json.load(fi)

keys = filejsn.keys()
if len(keys)>1:
    print "only one index at the time supported"
    sys.exit(2)
index_src = keys[0]

target = filejsn[index_src]

with open("tempsrc.jsn",'w') as fp:
  json.dump(target,fp,indent=4)

aliases = target["aliases"]
del target["aliases"]


settings = target["settings"]
mappings = target["mappings"]

target2 = {}
target2["mappings"]=mappings
target2["settings"]=settings


with open("tempdest_orig.jsn",'w') as fp:
  json.dump(target2,fp,indent=4)


del settings["index"]["version"]
del settings["index"]["uuid"]
del settings["index"]["creation_date"]
#5x only:
try:del settings["index"]["provided_name"]
except:pass
if "codec" not in settings["index"].keys():
  settings["index"]["codec"]="best_compression"
if "translog" not in settings["index"].keys():
  settings["index"]["translog"]= {"flush_threshold_size" : "4g", "durability" : "async"}
if "mapper" not in settings["index"].keys():
  settings["index"]["mapper"]= {"dynamic" : "false"}
try: del settings["archived"]
except:pass

for doc in mappings.keys():
    try:
      del  mappings[doc]["_timestamp"]
      print "removed _timestamp from",doc
    except:pass
    for prop in mappings[doc]["properties"].keys():
        if "type" not in mappings[doc]["properties"][prop].keys():continue
        if mappings[doc]["properties"][prop]["type"]=="string":
	    if "index" not in mappings[doc]["properties"][prop].keys() or mappings[doc]["properties"][prop]["index"]=="analyzed":
	        mappings[doc]["properties"][prop]["type"]="text"
	    elif mappings[doc]["properties"][prop]["index"]=="no" or mappings[doc]["properties"][prop]["index"]==False:
	        mappings[doc]["properties"][prop]["type"]="keyword"
		continue
	    elif mappings[doc]["properties"][prop]["index"]=="not_analyzed":
		try:
		    del mappings[doc]["properties"][prop]["fielddata"]
	        except:
		    pass
	        mappings[doc]["properties"][prop]["type"]="keyword"

        #check for 5X indices
	#elif mappings[doc]["properties"][prop]["type"]=="keyword":
	#  if "index" in mappings[doc]["properties"][prop].keys():
	#    if mappings[doc]["properties"][prop]["index"]=="no" or mappings[doc]["properties"][prop]["index"]==False:
	#      continue


            #print mappings[doc]["properties"][prop]
            try:del mappings[doc]["properties"][prop]["index"]
            except:pass

target["mappings"]=mappings
target["settings"]=settings

print "#dest  ",target_name
with open(target_name+".jsn",'w') as fp:
  json.dump(target,fp,indent=4)

with open("tempdest.jsn",'w') as fp:
  json.dump(target,fp,indent=4)

#TODO drop archived settings parameters
print "before creating new index, you can inspect target jsn file on a local disk for any additional changes needed (diff tempdest_orig.jsn tempdest.jsn)"
print "curl -XPUT es-cdaq:9200/"+target_name+" -d@"+target_name+".jsn"
print 'curl -XPOST es-cdaq:9200/_reindex?pretty -d\'{"source":{"index":"'+fileorig+'"},"dest":{"index":"'+target_name+'"}}\''

actions = {"actions":[]}

for alias in aliases:
  actions["actions"].append({"remove":{"index":fileorig,"alias":alias}})
  actions["actions"].append({"add":{"index":target_name,"alias":alias}})

print 'curl -XPOST es-cdaq:9200/_aliases?pretty -d\''+json.dumps(actions)+'\''

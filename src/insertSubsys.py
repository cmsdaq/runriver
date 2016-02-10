#!/bin/env python

import sys
import httplib
import json

try:
  subsys=sys.argv[1]
except:
  print "Must specify subsystem (and optionally tribe server hostname)"
  sys.exit(1)

try:
  trhost = sys.argv[2]
except:
  trhost = "es-tribe"

conn = httplib.HTTPConnection(host='localhost',port=9200)

q = { "es_central_cluster":"es-cdaq","es_tribe_host" : "es-tribe", "es_tribe_cluster" : "es-tribe", "polling_interval" : 15, "fetching_interval" : 5, "runIndex_read" : "runindex_"+subsys+"_read", "runIndex_write" : "runindex_"+subsys+"_write", "boxinfo_read" : "boxinfo_"+subsys+"_read", "enable_stats" : False, "node":{"status":"created"},"subsystem":subsys, "instance_name":"river_"+subsys+"_main","close_indices": True }

print json.dumps(q),'\n'

creq = conn.request('PUT','/river/instance/river_'+subsys+'_main',json.dumps(q))
cresp = conn.getresponse()
cstatus = cresp.status
cdata = cresp.read()
print cdata,'\n' 
 

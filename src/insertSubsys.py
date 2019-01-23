#!/bin/env python

import sys
import httplib
import json

try:
  subsys=sys.argv[1]
except:
  print "Must specify subsystem (and optionally es local server hostname). Injection will fail if document exists."
  sys.exit(1)

try:
  trhost = sys.argv[2]
except:
  trhost = "es-local"

conn = httplib.HTTPConnection(host='localhost',port=9200)

q = {
        "instance_name":"river_"+subsys+"_main",
        "subsystem":subsys,
        "es_central_cluster":"es-cdaq",
        "es_local_host" : trhost,
        "es_local_cluster" : "es-local",
        "polling_interval" : 15,
        "fetching_interval" : 5,
        "runindex_read" : "runindex_"+subsys+"_read",
        "runindex_write" : "runindex_"+subsys+"_write",
        "boxinfo_read" : "boxinfo_"+subsys+"_read",
        "enable_stats" : False,
        "node":{"status":"created"},
        "close_indices": True }

print json.dumps(q),'\n'

creq = conn.request('PUT','/river/instance/river_'+subsys+'_main?op_type=create',json.dumps(q))
cresp = conn.getresponse()
cstatus = cresp.status
cdata = cresp.read()
print cdata,'\n' 
 

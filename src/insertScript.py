#!/bin/env python

import sys
import httplib
import json

try:
  name=sys.argv[1]
  itype=sys.argv[2]
  path=sys.argv[3]
  role=sys.argv[4]
  subsys=sys.argv[5]
except:
  print "Must specify: name, type (nodejs or python), path, role and subsystem. Injection will fail if document exists."
  print "Example usage:"
  print "    insertScript.py mon_cpustats nodejs /cmsnfses-web/es-web/prod/daemons/lastcpu.js append_db_mon cdaq"
  print "    insertScript.py index_del python /cmsnfses-web/es-web/prod/daemons/eslocal_index_cleaner.py admin all"
  sys.exit(1)

conn = httplib.HTTPConnection(host='localhost',port=9200)

q= {
        "instance_name" : name,
        "process_type" : itype,
        "path" : path,
        "role" : role,
        "subsystem" : subsys
        "node" : { "status" : "created" }
}


print json.dumps(q),'\n'

creq = conn.request('PUT','/river/instance/'+name'?op_type=create',json.dumps(q))
cresp = conn.getresponse()
cstatus = cresp.status
cdata = cresp.read()
print cdata,'\n' 
 

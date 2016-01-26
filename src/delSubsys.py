#!/bin/env python

import sys
import httplib
import json

try:
  subsys=sys.argv[1]
except:
  print "Must specify subsystem"
  sys.exit(1)

conn = httplib.HTTPConnection(host='localhost',port=9200)

creq = conn.request('DELETE','/river/instance/river_'+subsys+'_main')
cresp = conn.getresponse()
cstatus = cresp.status
cdata = cresp.read()
print cdata,'\n' 


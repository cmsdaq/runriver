#!/bin/env python

import sys
import httplib
import json

def insertScript():
  try:
    rivertype=sys.argv[1]
  except:
    print "Please specify system or script as arg[1]"
    return 1
  if rivertype=="system":
      try:
        subsys=sys.argv[2]
        name = "river_"+subsys+"_main"
        try:
          trhost = sys.argv[3]
        except:
           trhost = "es-local"
      except:
        print "Must specify for arg[1] system: system (and optionally es local server hostname). Injection will fail if document exists."

        print "Example usage:"
        print "    insertRiver.py system cdaq es-local"
        return 1

  elif rivertypr=="script":
    try:
      name=sys.argv[2]
      itype=sys.argv[3]
      path=sys.argv[4]
      role=sys.argv[5]
      subsys=sys.argv[6]
    except:
      print "Must specify for arg[1] script   : name, type (nodejs or python), path, role and subsystem. Injection will fail if document exists."
      print "Example usage:"
      print "    insertRiver.py script mon_cpustats nodejs /cmsnfses-web/es-web/prod/daemons/lastcpu.js append_db_mon cdaq"
      print "    insertRiver.py script index_del python /cmsnfses-web/es-web/prod/daemons/eslocal_index_cleaner.py admin all"
      return 1
  else:
    print "unknown river type"
    return 1


  conn = httplib.HTTPConnection(host='localhost',port=9200)

  if rivertype == 'system':

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
    }

  elif rivertype == 'script':
    q= {
        "instance_name" : name,
        "process_type" : itype,
        "path" : path,
        "role" : role,
        "subsystem" : subsys
        "node" : { "status" : "created" }
    }
  else:
    print "invalid river type!"
    return 1


  print json.dumps(q),'\n'

  creq = conn.request('PUT','/river/instance/'+name'?op_type=create',json.dumps(q))
  cresp = conn.getresponse()
  cstatus = cresp.status
  cdata = cresp.read()
  print cdata,'\n' 
  return 0
 
if __name__ == "__main__":
  return(insertScript())

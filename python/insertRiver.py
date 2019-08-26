#!/bin/env python3.4
from __future__ import print_function

import sys
import json
import base64
import requests

escdaq_user_conf = "/cmsnfses-web/es-web/AUTH/river-users.jsn"
elastic_user_conf = "/etc/elasticsearch/users"
elastic_username = "riverwriter"
def parse_elastic_pwd():
  with open(elastic_user_conf,'r') as fp:
    for line in fp.readlines():
      tok = line.strip("\n").split(':')
      if len(tok)>1:
        if tok[0]==elastic_username:
          elasticvar = {
                         "user":elastic_username,
                         "pass":tok[1],
                         "encoded":"Basic %s" % (base64.standard_b64encode((elastic_username+":"+tok[1]).encode('ascii')).decode())
                       }
          return elasticvar
  return None

def parse_esweb_pwd(pwd):
  with open(escdaq_user_conf) as f:
    return json.load(f)[pwd]['pwd']

def insertRiver(args):

  elasticinfo = parse_elastic_pwd()
  headers={'Content-Type':'application/json','Authorization':elasticinfo["encoded"]}

  try:
    rivertype=args[1]
  except:
    print("Please specify system or script as arg[1]")
    return 1
  if rivertype=="system":
      try:
        subsys=args[2]
        name = "river_"+subsys+"_main"
        try:
          trhost = args[3]
        except:
           trhost = "es-local"
      except:
        print("Must specify for arg[1] system: system (and optionally es local server hostname). Injection will fail if document exists.")

        print("Example usage:")
        print("    insertRiver.py system cdaq es-local")
        return 1

  elif rivertype=="script":
    try:
      name=args[2]
      itype=args[3]
      path=args[4]
      role=args[5]
      subsys=args[6]
    except:
      print("Must specify for arg[1] script   : name, type (nodejs or python), path, role and subsystem. Injection will fail if document exists.")
      print("Example usage:")
      print("    insertRiver.py script mon_cpustats nodejs /cmsnfses-web/es-web/prod/daemons/lastcpu.js append_db_mon cdaq")
      print("    insertRiver.py script index_del python /cmsnfses-web/es-web/prod/daemons/eslocal_index_cleaner.py admin all")
      return 1
  elif rivertype=="delete":
    print("delete document invoked...")
    doc_name = args[2]
  else:
    print("unknown river type")
    return 1



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
        "close_indices": True
    }

  elif rivertype == 'script':
    q= {
        "instance_name" : name,
        "process_type" : itype,
        "path" : path,
        "role" : role,
        "subsystem" : subsys,
        "node" : { "status" : "created" }
    }
  elif rivertype == 'delete':
    creq = requests.delete('http://localhost:9200/river/_doc/'+name,headers=headers)
    cstatus = creq.status_code
    cdata = creq.content
    print(cdata,'\n')
    return 0
  else:
    print("invalid river type!")
    return 1


  print(json.dumps(q),'\n')

  creq = requests.put('http://localhost:9200/river/_doc/'+name+'?op_type=create',json.dumps(q),headers=headers)
  cstatus = creq.status_code
  cdata = creq.content
  print(cdata,'\n')
  return 0
 
if __name__ == "__main__":
  exit(insertRiver(sys.argv))

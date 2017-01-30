#!/bin/env python
import sys
import os
import time
import datetime 
import socket
import httplib
import json
import threading
import subprocess
import signal
import syslog

#hltd daemon2
sys.path.append('/opt/fff')
#demote, prctl and other libs
from daemon2 import Daemon2


try:
  import prctl
except:
  pass
try:
  import demote
except:
  pass
socket.setdefaulttimeout(5)
global_quit = False

#thread vector
river_threads = []

host='localhost'
#test:
#host='es-vm-cdaq'
sleep_int=5

#test
#jar_path  = "/opt/fff/river-runriver-1.4.0-jar-with-dependencies.jar"
jar_path  = "/opt/fff/river.jar"
jar_path_dv  = "/opt/fff/river_dv.jar"

#NFS script test version is default path (for now)
query_daemon = "/cmsnfses-web/es-web/prod/lastcpu.js"

keep_running = True
#river doc mapping
riverInstMapping = {
	"properties" : {
                "es_central_cluster": {
			"type" : "string",
			"index":"not_analyzed"
                },
		"boxinfo_write" : {
			"type" : "string",
			"index":"not_analyzed"
		},
		"boxinfo_read" : {
			"type" : "string",
			"index":"not_analyzed"
		},
		"es_tribe_cluster" : {
			"type" : "string",
			"index":"not_analyzed"
		},
		"es_tribe_host" : {
			"type" : "string",
			"index":"not_analyzed"
		},
		"runIndex_read" : {
			"type" : "string",
			"index":"not_analyzed"
		},
		"runIndex_write" : {
			"type" : "string",
			"index":"not_analyzed"
		},
		"polling_interval" : {
			"type" : "integer"
		},
		"fetching_interval" : {
			"type" : "integer"
		},
		"subsystem" : { #cdaq,minidaq, etc.
			"type" : "string",
			"index":"not_analyzed"
		},
		"runNumber": { #0 or unset if main instance
			"type":"integer"
		},
		"instance_name" : { #e.g. river_cdaq_run123456, river_minidaq_main etc. (same as _id?)
			"type" : "string",
			"index":"not_analyzed"
		},
                "process_type" : {
			"type" : "string",
			"index":"not_analyzed"
                },
                "path" : {
			"type" : "string",
			"index":"not_analyzed"
                },
                "role":{
                        "type" : "string",
                        "index":"not_analyzed"
                },
		"node" : {
			"properties" : {
				"name" : { #fqdn
					"type" : "string",
					"index":"not_analyzed"
				},
				"status": { #created, crashed, running, done, stale? ...
					"type":"string",
					"index":"not_analyzed"
				},
				"ping_timestamp":{ #last update (keep alive status?)
					"type":"date"
				},
				"ping_time_fmt":{ #human readable ping timestamp
					"type":"date",
					"format": "YYYY-mm-dd HH:mm:ss"
				}
			}
		},
		"errormsg" : { #crash or exception error
			"type" : "string",
			"index":"not_analyzed"
		},
		"enable_stats" : { #write stats document
			"type" : "boolean"
		},
                "close_indices" : {
                        "type": "boolean"
                }
	}
}



def query(conn,method,path,query=None,retry=False):

  while True:

    try:
      creq = conn.request(method,path,query)
      cresp = conn.getresponse()
      cstatus = cresp.status
      cdata = cresp.read()
      conn_success=True
      break
    except Exception as ex:
      conn_success=False
      cstatus = None
      cdata = None
      syslog.syslog("Exception:"+str(ex))
      time.sleep(.5)
      #restart connection
      #if reconnect or retry:
      #  conn.close()
      #  time.sleep(5)
      #  conn = httplib.HTTPConnection(host=host,port=9200)
      conn.close()
      conn = httplib.HTTPConnection(host=host,port=9200)
      if not retry: 
        break
      syslog.syslog("WARNING:retrying connection with:"+str(method)+' '+str(path))#+' '+str(query))
      #quit if requested globally and stuck in no-connect loop 
      if global_quit:break

  return conn_success,cstatus,cdata



#generate node snipped in river instance doc
def gen_node_doc(status):
    c_time = time.time()
    utcstr = datetime.datetime.utcfromtimestamp(c_time).strftime('%Y-%m-%d %H:%M:%S')
    upd_doc = {
      "node":{
        "name":os.uname()[1],
        "status":status,
        "ping_timestamp" : int(c_time*1000),
        "ping_time_fmt" : utcstr
      } 
    }
    return upd_doc



def preexec_function():
    try:
      dem = demote.demote('elasticsearch')
      dem()
    except:
      pass
    try:
      prctl.set_pdeathsig(signal.SIGKILL) #is this necessary?
    except:pass

def preexec_function2():
    try:
      dem = demote.demote('es-cdaq')
      dem()
    except:
      pass
    try:
      prctl.set_pdeathsig(signal.SIGKILL) #is this necessary?
    except:pass

    #other way (no demote)
    #user_pw = pwd.getpwnam('elasticsearch')
    #user_uid = user_pw[2]
    #user_gid = user_pw[3]
    #os.setuid(user_uid)
    #os.setgid(user_gid)


#todo: tell main thread that should be joined (e.g. - moving to another list)
class river_thread(threading.Thread):

  def __init__(self,riverid,subsys,url,cluster,riverindex,rn,process_type,path):
    threading.Thread.__init__(self)
    #self.logger = logging.getLogger(self.__class__.__name__)
    self.stop_issued=False
    self.stopped=False
    self.proc = None
    self.pid = None
    self.fdo = None
    self.proc_args = [riverid,subsys,url,cluster,riverindex,str(rn)]
    self.riverid = riverid
    self.subsys = subsys
    self.riverindex = riverindex
    self.rn = rn
    self.process_type = process_type
    self.path = path
    self.restart=False
    self.restart_version=None

  def execute(self):
    self.restart=False
    if self.process_type=='java':
      #run Collector
      if self.path: jpath = self.path
      else:
        jpath = jar_path_dv if self.subsys=='dv' else jar_path
      print "running",["/usr/bin/java", "-jar",jpath]+self.proc_args
      self.fdo = os.open('/tmp/'+self.riverid+'.log',os.O_WRONLY | os.O_CREAT | os.O_APPEND)
      self.proc = subprocess.Popen(["/usr/bin/java", "-jar",jpath]+self.proc_args,preexec_fn=preexec_function,close_fds=True,shell=False,stdout=self.fdo,stderr=self.fdo)
      self.start() #start thread to pick up the process
      return True #if success, else False
    elif self.process_type=='nodejs':
      if self.path: qdpath = self.path
      else:
        qdpath = '/dev/null'
      print "running",["/usr/bin/node", qdpath]
      self.fdo = os.open('/tmp/'+self.riverid+'.log',os.O_WRONLY | os.O_CREAT | os.O_APPEND)
      self.proc = subprocess.Popen(["/usr/bin/node",qdpath,self.riverid],preexec_fn=preexec_function2,close_fds=True,shell=False,stdout=self.fdo,stderr=self.fdo)
      self.start() #start thread to pick up the process
      return True #if success, else False
    elif self.process_type=='python':
      if self.path: qdpath = self.path
      else:
        qdpath = '/dev/null'
      print "running",["/usr/bin/python", qdpath]
      self.fdo = os.open('/tmp/'+self.riverid+'.log',os.O_WRONLY | os.O_CREAT | os.O_APPEND)
      self.proc = subprocess.Popen(["/usr/bin/python",qdpath,self.riverid],preexec_fn=preexec_function2,close_fds=True,shell=False,stdout=self.fdo,stderr=self.fdo)
      self.start() #start thread to pick up the process
      return True #if success, else False

  def setRestart(self,version):
    if self.restart:return
    try:
      self.proc.terminate()
      self.restart_version=version
      self.restart=True
    except:
      pass

   
  def run(self):
    self.proc.wait()
    if self.fdo:os.close(self.fdo)
    retcode = self.proc.returncode
    tmp_conn = httplib.HTTPConnection(host=host,port=9200)
    if self.restart:
      st=409 #version mismatch
      tries=100
      while st==409 and tries>0:
        success,st,res = query(gconn,"POST","/river/instance/"+str(self.riverid)+'/_update?version='+str(self.restart_version),json.dumps({'doc':gen_node_doc('restarting')}))
        self.restart_version+=1 #if doc got updated, try again
        tries-=1
        time.sleep(0.05)
      if st != 200:
        syslog.syslog("ERROR updating document "+str(self.riverid)+" status:"+str(st)+" "+str(res))
      else:
        syslog.syslog("terminated instance " +str(self.riverid)+" which was requested by restart state - scheduled for restarting")
      tmp_conn.close()
      return
    if retcode == 0:
      #TODO:make sure exit 0 only happens when plugin is truly finished
      syslog.syslog(str(self.riverid)+" successfully finished. Deleting river document..")
      success,st,res = query(tmp_conn,"DELETE","/river/instance/"+str(self.riverid),retry=True)
    else:
      syslog.syslog("WARNING:"+self.riverid+" exited with code "+str(retcode))
      #crash: change status to crashed
      attempt=0
      while True:
        #update doc 
        success,st,res = query(tmp_conn,"POST","/river/instance/"+str(self.riverid)+'/_update?refresh=true',json.dumps({'doc':gen_node_doc('crashed')}),retry=True)
        if st == 200:
          #ok, given for restarts
          break
        else:
          #TODO:retry this another time...
          syslog.syslog("ERROR updating document "+str(self.riverid)+" status:"+str(st)+" "+str(res))
          time.sleep(int(1+attempt/10.))
          #attempt forever for error 429, else give up after 100 attempts
          if st!=429 and attempt>=100: break
          attempt+=1
          syslog.syslog("retrying with attempt " + str(attempt))

    tmp_conn.close()
    #queue for joining
    self.stopped=True
    return

  def force_stop(self):
    self.stop_issued=True
    if self.proc:
      try:
        self.proc.terminate()
      except:
        pass
 
def runRiver(doc):

  src = doc['_source']
  try:runNumber = src['runNumber']
  except:runNumber = 0
  try:cluster = src['es_central_cluster']
  except:cluster = 'es-cdaq' #default..
  try: process_type=src['process_type']
  except:process_type='java'
  try: path=src['path']
  except:path=None

  #main instance
  doc_id = doc['_id']
  success,st,res = query(gconn,"GET","/river/instance/"+str(doc_id))
  if st!=200:
    syslog.syslog("ERROR:Failed to query!:"+str(doc_id)+" "+str(st)+" "+str(res))
    return
  doc = json.loads(res)
  doc_ver = doc['_version']
  #check again, as we executed run another query
  if doc['_source']['node']['status'] in ['created','crashed','restarting']: #or stale!
    #update doc 
    success,st,res = query(gconn,"POST","/river/instance/"+str(doc_id)+'/_update?version='+str(doc_ver)+'&refresh=true',json.dumps({'doc':gen_node_doc('starting')}))
    if st == 200:
      #success,proceed with fork
      syslog.syslog("successfully updated "+str(doc_id)+" document. will start the instance")
      new_instance = river_thread(doc_id,src['subsystem'],host,cluster,"river",runNumber,process_type,path)
      river_threads.append(new_instance)
      new_instance.execute()
      syslog.syslog("started river thread")
      ###fork river with url, index, type, doc id, some params to identify
    elif st == 409:
      syslog.syslog(str(doc_id)+" update failed. doc was already grabbed.")
    else:
      syslog.syslog("ERROR:Failed to update document; status:"+str(st)+" "+res )
  if doc['_source']['node']['status'] in ['restart']:
    #check if this instance is running here and restart it
    for instance in river_threads:
      if instance.riverid == doc_id:
        instance.setRestart(doc_ver)
        break


def checkRivers():

  #get all plugins running on the same node
  success,st,res = query(gconn,"GET","/river/_search/instance?size=1000",json.dumps({"query":{"term":{"node.name":os.uname()[1]}}}),retry = False)
  if success:
    doc_json = json.loads(res)
    for hit in doc_json['hits']['hits']:
      doc_st = hit["_source"]["node"]["status"]
      doc_id = hit["_id"]
      if doc_st=='running' or doc_st=='starting': #other states are handled
        found_rt = None
        for rt in river_threads:
          if rt.riverid == doc_id:
            found_rt = rt
            break
        if found_rt:continue #river exists
        else:
          #check again the doc and get the version to update
          success,st,res = query(gconn,"GET","/river/instance/"+doc_id,retry = False)
          if success:
            doc_json = json.loads(res)
            doc_st = doc_json["_source"]["node"]["status"]
            doc_ver = doc_json["_source"]['_version']
            syslog.syslog("No mother thread found for river id "+ doc_id + " in state " + doc_st)
            if doc_st == 'running' or doc_st=='starting':
              success,st,res = query(gconn,"POST","/river/instance/"+str(doc_id)+'/_update?version='+str(doc_ver)+'&refresh=true',
                                     json.dumps({'doc':gen_node_doc('crashed')}),retry = False)
              if success:syslog.syslog("reinserted document to restart river instance "+doc_id+" which is not present")
            else:continue #was update in the meantime
          else:continue #can't get doc, don't do anything right until we can
    return True
  else:
    time.sleep(1)
    return False


def runDaemon():
  global gconn
  gconn = httplib.HTTPConnection(host=host,port=9200)

  #require functioning server status before getting to checks and main loop
  while True:
    success,st,res = query(gconn,"GET","/_cluster/health",retry = True)
    if st==200:
      cl_status=json.loads(res)["status"]
      syslog.syslog("cluster status "+cl_status)
      if cl_status=="green" or cl_status=="yellow":
        break
    else:
      syslog.syslog("failed to get cluster status, return code: " + str(st))
    time.sleep(10)

  #put mapping
  success,st,res = query(gconn,"PUT","/river/_mapping/instance",json.dumps(riverInstMapping),retry = True)
  syslog.syslog("attempts to push instance doc mapping:"+str(st)+" "+str(res))

  #recovery if river status is running on this node:
  while True:
    success,st,res = query(gconn,"GET","/river/instance/_search?size=1000", '{"query":{"bool":{"must":[{"term":{"node.status":"running"}},{"term":{"node.name":"'+os.uname()[1]+'"}}] }}}', retry = True)
    if success and st==200:
      jsres = json.loads(res)
      for hit in jsres['hits']['hits']:
        doc_id = hit['_id']
        success,st,res = query(gconn,"POST","/river/instance/"+str(doc_id)+'/_update?refresh=true',json.dumps({'doc':gen_node_doc('crashed')}))
        syslog.syslog('recovering instance ' + doc_id + " success:" + str(success) + " status:" + str(st))
      break
    else:
      time.sleep(3)
      syslog.syslog("will retry recovery check")
      continue

  cnt=0
  while keep_running:
    if cnt%10==0:
      syslog.syslog('running loop...')
      #check if there are any docs running on this host for which there is no active thread
      ### check_res = checkRivers()

    cnt+=1

    #join threads that have finished (needed?)
    for rt in river_threads[:]:
      if rt.stopped:
        try:
          rt.join()
        except:
          pass
        river_threads.remove(rt)

    time.sleep(sleep_int)
    if global_quit:break

    #find instances that need to be started
    success,st,res = query(gconn,"GET","/river/instance/_search?size=1000", '{"query":{"bool":{"should":[{"term":{"node.status":"restart"}},{"term":{"node.status":"restarting"}},{"term":{"node.status":"crashed"}},{"term":{"node.status":"created"}}] }}}')
    #TODO: add detection of stale objects (search for > amount of time since last ping
    if success and st==200:
      jsres = json.loads(res)
      for hit in jsres['hits']['hits']:
        #(try) to instantiate using doc version
        runRiver(hit)
      pass
    else:
      syslog.syslog("ERROR running search query status:"+str(st)+" "+str(res))



#signal handler to allow graceful exit on SIGINT. will be used for control from the main service
def signal_handler(signal, frame):
        print 'Caught sigint!'
        syslog.syslog('Caught sigint...')
        time.sleep(1)
        global global_quit
        global_quit = True
        return
signal.signal(signal.SIGINT, signal_handler)
#--------------------------------------------------------------------
#main code:
class RiverDaemon(Daemon2):

  def __init__(self):
    Daemon2.__init__(self, 'river-daemon', 'main', confname=None, stdin='/dev/null', stdout='/dev/null', stderr='/dev/null')

  def run(self):
    syslog.openlog("river-daemon")
    #logging.basicConfig(level=logging.INFO)
    try:
      dem = demote.demote("elasticsearch")
    except:
      pass

    #main loop
    runDaemon()

    #kill everything
    for rt in river_threads[:]:
      try:
        rt.force_stop()
        rt.join()
      except Exception as ex:
        print ex
        syslog.syslog(str(ex))

    syslog.syslog("quitting")
    #make sure we exit
    syslog.closelog()
    os._exit(0)

if __name__ == "__main__":

    daemon = RiverDaemon()
    runAsDaemon=False
    try:
      if sys.argv[1]=='--daemon':
        runAsDaemon=True
    except:
        pass
    if runAsDaemon:
      try:
        import procname
        procname.setprocname('river-daemon')
      except:
        print "procname not installed"
      daemon.start(req_conf=False)
    else:
      daemon.run()
 

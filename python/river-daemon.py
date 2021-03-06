#!/bin/env python3.4
from __future__ import print_function
import sys
import os
import pwd
import time
import datetime 
import socket
import json
import threading
import subprocess
import signal
import syslog

try:
  from httplib import HTTPConnection
except:
  from http.client import HTTPConnection


#hltd daemon2
sys.path.append('/opt/fff')

#demote, prctl (not essential)
try:
  import prctl
except Exception as ex:
  pass

#local:
import demote
from riverMapping import riverInstMapping
from insertRiver import parse_elastic_pwd

#helper chown
def chown_file(user,fd):
  pw_record = pwd.getpwnam(user)
  os.fchown(fd,pw_record.pw_uid,pw_record.pw_gid)

socket.setdefaulttimeout(5)
global_quit = False

#thread vector
river_threads_map = {}

host='localhost'
sleep_int=5

#cdaq: also includes CPU usage DB injector script
subsys_quota = {"cdaq":10,"dv":3,"d3v":3,"minidaq":10}
quota_default = 3
subsys_heap = {"cdaq":"500m","dv":"100m","d3v":"100m","minidaq":"100m"}
heap_default = "100m"

#quota for VM with reduced memory
if socket.gethostname().startswith('es-vm-cdaq'):
  subsys_heap["cdaq"]="100m"
  subsys_quota["cdaq"]=5

#test
#jar_path  = "/opt/fff/river-runriver-1.4.0-jar-with-dependencies.jar"
jar_path  = "/opt/fff/river.jar"
jar_path_dv  = "/opt/fff/river_dv.jar"
jar_logparam="-Dlog4j.configurationFile=/opt/fff/log4j2.properties"

keep_running = True

#read elasticsearch credentials
elasticinfo = parse_elastic_pwd()
print("user:"+elasticinfo["user"])
headers={'Content-Type':'application/json','Authorization':elasticinfo["encoded"]}

env_copy = os.environ.copy()
env_copy["ELASTICUSER"]=elasticinfo["user"]
env_copy["ELASTICPASS"]=elasticinfo["pass"]

def query(conn,method,path,query=None,retry=False):

  cnt=0

  while True:
    try:
      creq = conn.request(method,path,query,headers=headers)
      cresp = conn.getresponse()
      cstatus = cresp.status
      cdata = cresp.read().decode()
      conn_success=True
      break
    except Exception as ex:
      conn_success=False
      cstatus = None
      cdata = None
      if cnt%200==0:
          syslog.syslog("exception type:"+str(type(ex).__name__)+" msg:"+str(ex))
      time.sleep(.5)
      #restart connection
      conn.close()
      conn = HTTPConnection(host=host,port=9200)
      if not retry: 
        syslog.syslog("will not retry:" +str(type(ex).__name__)+" msg:"+str(ex))
        cstatus=-1
        cdata=ex
        break
      if cnt%200==0:
        #every 10 seconds
        syslog.syslog("WARNING:retrying connection with:"+str(method)+' '+str(path) + ' iteration:'+str(cnt))
      cnt+=1
      #quit if requested globally and stuck in no-connect loop 
      if global_quit:
        syslog.syslog("global quit, interrupted on:" +str(type(ex).__name__)+" msg:"+str(ex))
        break

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

def gen_node_doc_time():
    c_time = time.time()
    utcstr = datetime.datetime.utcfromtimestamp(c_time).strftime('%Y-%m-%d %H:%M:%S')
    upd_doc = {
      "node":{
        "ping_timestamp" : int(c_time*1000),
        "ping_time_fmt" : utcstr
      } 
    }
    return upd_doc


def preexec_function_elasticsearch():
    try:
      dem = demote.demote('elasticsearch')
      dem()
    except Exception as ex:
      pass
    try:
      prctl.set_pdeathsig(signal.SIGKILL) #is this necessary?
    except Exception as ex:
      pass

def preexec_function_escdaq():
    try:
      dem = demote.demote('es-cdaq')
      dem()
    except Exception as ex:
      pass
    try:
      prctl.set_pdeathsig(signal.SIGKILL) #is this necessary?
    except Exception as ex:
      pass


#todo: tell main thread that should be joined (e.g. - moving to another list)
class river_thread(threading.Thread):

  def __init__(self,riverid,subsys,url,cluster,riverindex,rn,process_type,path):
    threading.Thread.__init__(self)
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
    self.watchdogEvent = threading.Event()
    self.give_up=False

  def execute(self):
    self.restart=False
    if self.process_type=='java':
      #run Collector
      if self.path: jpath = self.path
      else:
        jpath = jar_path_dv if self.subsys=='dv' else jar_path
      try:
       jHeap =  subsys_heap[self.subsys]
      except:
       jHeap = heap_default
      print("running",["/usr/bin/java -Xms" + jHeap + " -Xmx" + jHeap, jar_logparam, "-jar",jpath]+self.proc_args)
      self.fdo = os.open('/var/log/river/'+self.riverid+'.log',os.O_WRONLY | os.O_CREAT | os.O_APPEND)
      #tentatively change log file ownership to what the process will be
      chown_file('elasticsearch',self.fdo)
      self.proc = subprocess.Popen(["/usr/bin/java", "-Xms" + jHeap, "-Xmx"+ jHeap, jar_logparam, "-jar",jpath]+self.proc_args,preexec_fn=preexec_function_elasticsearch,close_fds=True,shell=False,stdout=self.fdo,stderr=self.fdo)
      self.start() #start thread to pick up the process
      return True #if success, else False
    elif self.process_type=='nodejs':
      if self.path: qdpath = self.path
      else:
        qdpath = '/dev/null'
      print("running",["/usr/bin/node", qdpath])
      self.fdo = os.open('/var/log/river/'+self.riverid+'.log',os.O_WRONLY | os.O_CREAT | os.O_APPEND)
      #tentatively change log file ownership to what the process will be
      chown_file('es-cdaq',self.fdo)
      self.proc = subprocess.Popen(["/usr/bin/node",qdpath,self.riverid],preexec_fn=preexec_function_escdaq,close_fds=True,shell=False,stdout=self.fdo,stderr=self.fdo,env=env_copy)
      self.start() #start thread to pick up the process
      return True #if success, else False
    elif self.process_type=='python':
      if self.path: qdpath = self.path
      else:
        qdpath = '/dev/null'
      print("running",["/usr/bin/python3.4", qdpath])
      self.fdo = os.open('/var/log/river/'+self.riverid+'.log',os.O_WRONLY | os.O_CREAT | os.O_APPEND)
      #tentatively change log file ownership to what the process will be
      chown_file('es-cdaq',self.fdo)
      self.proc = subprocess.Popen(["/usr/bin/python3.4",qdpath,self.riverid],preexec_fn=preexec_function_escdaq,close_fds=True,shell=False,stdout=self.fdo,stderr=self.fdo,env=env_copy)
      self.start() #start thread to pick up the process
      return True #if success, else False

  def setRestart(self):
    if self.restart:return
    try:
      self.watchdogEvent.set()
      self.restart=True
      time.sleep(.1)
      self.proc.terminate()
    except:
      pass

  def setTerminate(self):
    if self.restart:return
    try:
      self.watchdogEvent.set()
      self.restart=False
      self.give_up=True
      time.sleep(.1)
      self.proc.terminate()
    except:
      pass

  def getSelfDoc(self,conn):
    success,st,res = query(conn,"GET","/river/_doc/"+str(self.riverid))
    ret_success=False
    doc_seqn=None
    doc_pterm=None
    doc=None
    host_changed=False
    if st==200:
      ret_success=True
      doc = json.loads(res)
      doc_seqn=doc['_seq_no']
      doc_pterm=doc['_primary_term']
      if 'node' in doc['_source'] and 'name' in doc['_source']['node']:
        if doc['_source']['node']['name'] != os.uname()[1]:
          host_changed=True
    return ret_success,st,doc_seqn,doc_pterm,host_changed,doc

  def watch(self):
      tmp_conn_w = None
      while not self.stopped:
        try:
          if not tmp_conn_w:
              tmp_conn_w = HTTPConnection(host=host,port=9200)

          success,st,doc_seqn,doc_pterm,host_changed,doc = self.getSelfDoc(tmp_conn_w)
          if st==404 or host_changed:
              try:
                  #doc was manually deleted or taken over, kill the process
                  syslog.syslog('detected hijack or deletion of ' + self.riverid)
                  self.give_up=True
                  self.proc.terminate()
              except Exception as ex:
                  if not self.stopped:
                    syslog.syslog('failed to terminate process ' + str(ex))
              break
          else:
              if doc_seqn and doc_pterm:
                qattribs='?if_seq_no='+str(doc_seqn)+'&if_primary_term='+str(doc_pterm)+'&refresh=true'
                success,st,res = query(tmp_conn_w,"POST","/river/_doc/"+str(self.riverid)+'/_update'+qattribs,json.dumps({'doc':gen_node_doc_time()}))
                if success:
                  if st==409:
                    self.watchdogEvent.wait(5)
                  else:
                    self.watchdogEvent.wait(30)
                else:
                  self.watchdogEvent.wait(5)
              else:
                self.watchdogEvent.wait(5)

        except Exception as ex:
          syslog.syslog('failed to update heartbeat of ' + self.riverid + ':'+str(ex))
        self.watchdogEvent.wait(10)
      if tmp_conn_w:
        tmp_conn_w.close()
  
  def run(self):
    
    watchdog = threading.Thread(target=self.watch)
    watchdog.start()

    self.proc.wait()
    if self.fdo:os.close(self.fdo)
    retcode = self.proc.returncode
    tmp_conn = HTTPConnection(host=host,port=9200)

    try:
      if not self.give_up:
        tries=50
        st=409
        res=None

        #handle updating document for restart
        if not self.restart:
          if retcode==0:
              syslog.syslog(str(self.riverid)+" successfully finished. Deleting river document..")
          else:
              syslog.syslog("WARNING:"+self.riverid+" exited with code "+str(retcode))

        while st not in [200,201] and tries>0:
          success,st,doc_seqn,doc_pterm,host_changed,doc = self.getSelfDoc(tmp_conn)
          #give up if document is no longer there or taken over by another host
          if st==404 or host_changed:break
          #document seq and term should be there
          if doc_seqn and doc_pterm:
              if not self.restart and retcode==0:
                  #delete document
                  qattribs='?if_seq_no='+str(doc_seqn)+'&if_primary_term='+str(doc_pterm)
                  success,st,res = query(tmp_conn,"DELETE","/river/_doc/"+str(self.riverid)+qattribs,retry=True)
              else:
                  qattribs='?if_seq_no='+str(doc_seqn)+'&if_primary_term='+str(doc_pterm)+'&refresh=true'
                  msg = 'crashed' if not self.restart else 'restarting'
                  success,st,res = query(tmp_conn,"POST","/river/_doc/"+str(self.riverid)+'/_update'+qattribs,json.dumps({'doc':gen_node_doc(msg)}))
          else:
              print("could not get doc sequence number or primary term :"+str(doc_seqn) + " " + str(doc_pterm))
          if st==429:
            tries=50 #no finite tries for this error (es is overloaded, keep trying...)

          tries-=1
          if st not in [200,201,409]:
              #start complaining if it starts failing repeatedly.
              if not self.restart and retcode==0:
                  syslog.syslog("ERROR deleting document "+str(self.riverid)+" status:"+str(st)+" "+str(res) + "  tries left: " + str(tries))
              else:
                  syslog.syslog("ERROR updating document "+str(self.riverid)+" status:"+str(st)+" "+str(res) + "  tries left: " + str(tries))
          elif st!=409:
              break
          #sleep period 0.5 to 5 seconds
          time.sleep(0.5*(int(1+(50-tries)/10.)))

        #after loop (print success or failure):
        if st != 200:
            syslog.syslog("river-thread: ERROR - could not update document "+str(self.riverid)+" status:"+str(st)+" "+str(res))
        else:
          if self.restart:
              syslog.syslog("river-thread: terminated instance " + str(self.riverid) + " which was requested by restart state - scheduled for restarting")
          elif retcode!=0:
              syslog.syslog("river-thread: updated instance " + str(self.riverid) + " which has crashed")
          else:
              syslog.syslog("river-thread: deleted instance " + str(self.riverid) + " which has finished")
    except Exception as exc:
      syslog.syslog(str(exc))
    #end threads, connections
    syslog.syslog('closing HTTP connection')
    tmp_conn.close()
    self.stopped=True
    self.watchdogEvent.set()
    watchdog.join()
    return

  def force_stop(self):
    self.stop_issued=True
    if self.proc:
      try:
        self.proc.terminate()
      except:
        pass


#helper: gets sequence and primary term only available with GET by id
def getDocInfo(doc_id,conn):
  success,st,res = query(conn,"GET","/river/_doc/"+str(doc_id))
  if not success or st!=200:
    syslog.syslog("ERROR:Failed to query!:"+str(doc_id)+", HTTP status: "+str(st)+", reply:"+str(res))
    #TODO: return different value for error than for no doc and handle differently
    return False,None
  doc = json.loads(res)
  if doc['found']==False:
    syslog.syslog("Requested document not found: "+str(doc_id)+". Possibly deleted by other instance?")
    return False,None
    
  doc_seqn = doc['_seq_no']
  doc_pterm = doc['_primary_term']
  return True,'?if_seq_no='+str(doc_seqn)+'&if_primary_term='+str(doc_pterm)+'&refresh=true'


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
  success, qattribs = getDocInfo(doc_id,gconn)
  if not success: return

  #check again the state from latest query
  if doc['_source']['node']['status'] in ['created','crashed','restarting','noquota']:

    time.sleep(.1)
    found_copy=False
    ssys = src['subsystem']
    #verify if this river is already active and clean it if not
    try:
      for rsys in river_threads_map:
        river_threads = river_threads_map[rsys]
        for instance in river_threads[:]:
          if instance.riverid == doc_id:
            instance.setTerminate()
            try:
              instance.join()
            except Exception as ex:
              syslog.syslog("error joining river thread (runRiver(1) :" +str(type(ex).__name__)+" msg:"+str(ex))
            found_copy=True

    except Exception as ex:
      syslog.syslog("problem checking rivers:"+str(ex))
    time.sleep(.05)

    #check local quota and clean up if necessary
    if not found_copy and runNumber!=0:
      try:
        for rsys in river_threads_map:
          if rsys!=ssys:continue
          river_threads = river_threads_map[rsys][:]
          try:lim = subsys_quota[ssys]
          except: lim = quota_default
          if len(river_threads)>=lim:
            lim_checked = 0
            min_run = -1
            run_list = []
            for instance in river_threads:
              if instance.rn!=0:
                lim_checked+=1
                run_list.append(instance.rn)
                if min_run==-1 or min_run>instance.rn:
                  min_run=instance.rn
            if lim_checked>=lim:
              if runNumber<min_run:
                #set crashed state before returning
                if doc['_source']['node']['status']!="noquota":
                  syslog.syslog("Instance "+doc_id + " can not run with no remaining slots available for subsystem on this machine. Setting noquota state in river doc")
                  success,st,res = query(gconn,"POST","/river/_doc/"+str(doc_id)+'/_update'+qattribs,json.dumps({'doc':gen_node_doc('noquota')}))
                return
              else:
                run_list.sort()
                for i in range(0,lim_checked+1-lim):
                  for instance in river_threads:
                    if run_list[i]==instance.rn:
                      #do not change state, keep trying (this or other node which could have quota)
                      #success,st,res = query(gconn,"POST","/river/_doc/"+str(instance.riverid)+'/_update'+qattribs,json.dumps({'doc':gen_node_doc('noquota')}))
                      syslog.syslog("killing older over-quota instance "+instance.riverid)
                      instance.setTerminate()
                      try:
                        instance.join()
                      except Exception as ex:
                        syslog.syslog("error joining river thread (runRiver(2) :" +str(type(ex).__name__)+" msg:"+str(ex))
                      river_threads_map[rsys].remove(instance)
      except Exception as ex:
        syslog.syslog("problem clearing rivers over quota:"+str(ex))
      time.sleep(.05)


    #update doc before starting the process
    success,st,res = query(gconn,"POST","/river/_doc/"+str(doc_id)+'/_update'+qattribs,json.dumps({'doc':gen_node_doc('starting')}))
    if success and st == 200:
      #success,proceed with fork
      syslog.syslog("successfully updated "+str(doc_id)+" document. will start the instance")
      new_instance = river_thread(doc_id,src['subsystem'],host,cluster,"river",runNumber,process_type,path)
      river_threads_map.setdefault(src['subsystem'],[]).append(new_instance)
      new_instance.execute()
      syslog.syslog("started river thread")
      ###fork river with url, index, type, doc id, some params to identify
    elif st == 409:
      syslog.syslog(str(doc_id)+" update failed. doc was already grabbed.")
    else:
      syslog.syslog("ERROR:Failed to update document; status:"+str(st)+" error:"+str(res))
  elif doc['_source']['node']['status'] in ['restart']:
    #manual restart was issued, check if this instance is running here and tell the thread to finish and set status to restarting
    for rsys in river_threads_map:
      river_threads = river_threads_map[rsys]
      for instance in river_threads[:]:
        if instance.riverid == doc_id:
          instance.setRestart()
          return
    #if handler was not found, try to set restarting flag to allow someone to pick it up
    success,st,res = query(gconn,"POST","/river/_doc/"+str(doc_id)+'/_update'+qattribs,json.dumps({'doc':gen_node_doc('restarting')}))
    if not (success and st == 200) and st!=409:
      syslog.syslog("error restarting river thread, code "+str(st))


def checkRivers():

  #get all plugins running on the same node
  success,st,res = query(gconn,"GET","/river/_doc/_search?size=1000",json.dumps({"query":{"term":{"node.name":os.uname()[1]}}}),retry = False)
  if success and st in [200,201]:
    doc_json = json.loads(res)
    for hit in doc_json['hits']['hits']:
      doc_st = hit["_source"]["node"]["status"]
      doc_id = hit["_id"]

      if doc_st=='running' or doc_st=='starting': #other states are handled
        found_rt = None
        for rsys in river_threads_map:
          river_threads = river_threads_map[rsys]
          for rt in river_threads[:]:
            if rt.riverid == doc_id:
              found_rt = rt
              break
          if found_rt is not None:
            break
        if not found_rt: #river not handled by thread obj, take over
          syslog.syslog("no mother thread found for river id "+ doc_id + " in state " + doc_st)

          success, qattribs = getDocInfo(doc_id,gconn)
          if not success: continue

          success,st,res = query(gconn,
                                 "POST",
                                 "/river/_doc/"+str(doc_id)+'/_update'+qattribs,
                                 json.dumps({'doc':gen_node_doc('crashed')}),retry = False)
          #if success and st!=409:
          if success and st==200:
            syslog.syslog("reinserted document to restart river instance "+doc_id+" which is not present")

def checkOtherRivers():

    success,st,res = query(gconn,"GET","/river/_doc/_search?size=1000",json.dumps({"query":{"bool":{"must_not":[{"term":{"node.name":os.uname()[1]}}]}}}),retry = False)
    c_time = time.time()
    if success and st in [200,201]:
      doc_json = json.loads(res)
      for hit in doc_json['hits']['hits']:
        doc_st = hit["_source"]["node"]["status"]
        if doc_st in ['running','starting','restart']: #other states are picked up
          doc_id = hit["_id"]
          host_r="null"
          try:
              time_s = (time.time()*1000 - hit['_source']['node']['ping_timestamp'])//1000;
              host_r =  hit['_source']['node']['name']
          except:
            syslog.syslog('could not check document ' + doc_id)
            return
          if time_s>120:
            #stale document
            syslog.syslog("stale river id "+ doc_id + " in state " + doc_st + " host:"+ host_r)

            success, qattribs = getDocInfo(doc_id,gconn)
            if not success: continue

            success,st,res = query(gconn,
                                 "POST",
                                 "/river/_doc/"+str(doc_id)+'/_update'+qattribs,
                                 json.dumps({'doc':gen_node_doc('crashed')}),retry = False)
            #if success and st!=409:
            if success and st==200:
              syslog.syslog("taken over river instance "+doc_id+" which was stale")


def runDaemon():
  global gconn
  gconn = HTTPConnection(host=host,port=9200)

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
  success,st,res = query(gconn,"PUT","/river/_mapping",json.dumps(riverInstMapping),retry = True)
  syslog.syslog("attempts to push instance doc mapping:"+str(st)+" "+str(res))

  #recovery if river status is running on this node:
  while True:
    success,st,res = query(gconn,"GET","/river/_doc/_search?size=1000", '{"query":{"bool":{"must":[{"term":{"node.status":"running"}},{"term":{"node.name":"'+os.uname()[1]+'"}}] }}}', retry = True)
    if success and st==200:
      jsres = json.loads(res)
      for hit in jsres['hits']['hits']:
        doc_id = hit['_id']

        success, qattribs = getDocInfo(doc_id,gconn)
        if not success: continue #should avoid break later if there is error?

        success,st,res = query(gconn,"POST","/river/_doc/"+str(doc_id)+'/_update'+qattribs,json.dumps({'doc':gen_node_doc('crashed')}))
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

    cnt+=1

    try:
      checkRivers()
    except Exception as ex:
      syslog.syslog("checkRivers exception:" + str(ex))
      raise ex
    try:
      checkOtherRivers()
    except Exception as ex:
      syslog.syslog("checkOtherRivers exception:" + str(ex))
      raise ex

    #join threads that have finished (needed?)
    try:
      for rsys in river_threads_map:
        river_threads = river_threads_map[rsys]
        for rt in river_threads[:]:
          if rt.stopped:
            try:
              rt.join()
            except Exception as ex:

              syslog.syslog("error joining river thread (runDaemon loop_ :" +str(type(ex).__name__)+" msg:"+str(ex))
              pass
            river_threads_map[rsys].remove(rt)

      time.sleep(sleep_int)
      if global_quit:break

    except Exception as ex:
      syslog.syslog("handling river threads exception:" + str(ex))
      raise ex
    #find instances that need to be started
    try:
      #success,st,res = query(gconn,"GET","/river/_doc/_search?size=1000", '{"query":{"bool":{"should":[{"term":{"node.status":"restart"}},{"term":{"node.status":"restarting"}},{"term":{"node.status":"crashed"}},{"term":{"node.status":"created"}}] }}}')
      success,st,res = query(gconn,"GET","/river/_doc/_search?size=1000", '{"query":{"terms":{"node.status":["restart","restarting","crashed","created","noquota"]}}}')
      #TODO: add detection of stale objects (search for > amount of time since last ping
      if success and st==200:
        jsres = json.loads(res)
        for hit in jsres['hits']['hits']:
          #(try) to instantiate using doc sequence and term
          runRiver(hit)
        pass
      else:
        syslog.syslog("ERROR running search query status:"+str(st)+" "+str(res))
    except Exception as ex:
      syslog.syslog("exception finding new instances to start+"+str(ex))
      raise ex

class LogCleaner(threading.Thread):

    def __init__(self,period=60*60,maxAgeHours=24*14,path='/var/log/river'):
        threading.Thread.__init__(self)
        self.threadEvent = threading.Event()
        self.period=period
        self.maxAgeHours=maxAgeHours
        self.path=path
        self.stopping=False


    def deleteOldLogs(self):
        existing_logs = os.listdir(self.path)
        current_dt = time.time()
        for f in existing_logs:
            try:
                if self.maxAgeHours>0:
                    file_dt = os.path.getmtime(os.path.join(self.path,f))
                    if (current_dt - file_dt)/3600. > self.maxAgeHours:
                        #delete file
                        os.remove(os.path.join(self.path,f))
                else:
                    os.remove(os.path.join(self.path,f))
            except Exception as ex:
                print("could not delete log file",ex)

    def run(self):
        self.threadEvent.wait(60)
        while True:
            syslog.syslog("running log clean...")
            if self.stopping:
                return
            self.deleteOldLogs()
            self.threadEvent.wait(self.period)

    def stop(self):
        self.stopping=True
        self.threadEvent.set()
        self.join()

      


#signal handler to allow graceful exit on SIGINT. will be used for control from the main service
def signal_handler(signal, frame):
        print('Caught sigint!')
        syslog.syslog('Caught sigint...')
        time.sleep(1)
        global global_quit
        global_quit = True
        return
signal.signal(signal.SIGINT, signal_handler)
#--------------------------------------------------------------------
#main code:
class RiverDaemon():

  def __init__(self):
    self.logCleaner=LogCleaner()

  def run(self):
    syslog.openlog("river-daemon")
    #logging.basicConfig(level=logging.INFO)
    try:
      dem = demote.demote("elasticsearch")
    except:
      pass

    #run log cleaning thread
    self.logCleaner.start()
    #main loop
    try:
      runDaemon()
    except Exception as ex:
     syslog.syslog("error executing runDaemon:" +str(type(ex).__name__)+" msg:"+str(ex))
     #let this sleep for one hour before we restart (should be resilient to errors)
     time.sleep(3600)
     os._exit(0)

    syslog.syslog("exited runDaemon...")
    #kill everything
    for rsys in river_threads_map:
      river_threads = river_threads_map[rsys]
      for rt in river_threads[:]:
        try:
          rt.force_stop()
          rt.join()
        except Exception as ex:
          syslog.syslog("error stopping/joining river thread during shutdown:" +str(type(ex).__name__)+" msg:"+str(ex))
          syslog.syslog(str(ex))

    syslog.syslog("quitting (1)")
    self.logCleaner.stop()
    syslog.syslog("quitting (2)")
    #make sure we exit
    syslog.closelog()
    os._exit(0)

def esClusterName():
    try:
      with open('/etc/elasticsearch/elasticsearch.yml') as fi:
        lines = fi.readlines()
        for line in lines:
          sline = line.strip()
          if line.startswith("cluster.name"):
            return line.split(':')[1].strip()
    except Exception as ex:
      syslog.syslog("could not parse cluster name:" +str(type(ex).__name__)+" msg:"+str(ex))

    return ""

if __name__ == "__main__":

    escname = esClusterName()
    if not (escname.startswith('es-vm-cdaq') or escname.startswith('es-cdaq')) or escname.startswith('es-cdaq-run2'):
      print("Service is disabled on machines which are not es-vm-cdaq or es-cdaq cluster")
      sys.exit(0)

    daemon = RiverDaemon()
    daemon.run()
 

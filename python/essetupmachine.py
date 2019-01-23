#!/bin/env python
from __future__ import print_function
import os,sys,socket
import shutil
import json
import shutil
import syslog
import time

backup_dir = '/opt/fff/backup'
try:
    os.makedirs(backup_dir)
except:pass

elasticsysconf = '/etc/sysconfig/elasticsearch'
elasticconf = '/etc/elasticsearch/elasticsearch.yml'
#elasticlogconf = '/etc/elasticsearch/logging.yml'

es_cdaq_run2_list = ['ncsrv-c2e42-09-02', 'ncsrv-c2e42-11-02', 'ncsrv-c2e42-13-02', 'ncsrv-c2e42-19-02']
es_cdaq_list = ['ncsrv-c2e42-21-02', 'ncsrv-c2e42-23-02']
es_local_list =['ncsrv-c2e42-13-03', 'ncsrv-c2e42-23-03']

myhost = os.uname()[1]

def getmachinetype():

    #print "running on host ",myhost
    if myhost.startswith('ncsrv-'):
        try:
            es_cdaq_run2_list_ip = socket.gethostbyname_ex('es-cdaq-run2')[2]
            es_cdaq_list_ip = socket.gethostbyname_ex('es-cdaq')[2]
            es_local_list_ip = socket.gethostbyname_ex('es-local')[2]

            for es in es_cdaq_run2_list:
                try:
                    es_cdaq_run2_list_ip.append(socket.gethostbyname_ex(es)[2][0])
                except Exception as ex:
                    print(ex)
            for es in es_cdaq_list:
                try:
                    es_cdaq_list_ip.append(socket.gethostbyname_ex(es)[2][0])
                except Exception as ex:
                    print(ex)
            for es in es_local_list:
                try:
                    es_local_list_ip.append(socket.gethostbyname_ex(es)[2][0])
                except Exception as ex:
                    print(ex)

            myaddr = socket.gethostbyname(myhost)

            if myaddr in es_cdaq_run2_list_ip:
                return 'es','escdaqrun2','prod'
            if myaddr in es_cdaq_list_ip:
                return 'es','escdaq','prod'
            elif myaddr in es_local_list_ip:
                return 'es','eslocal','prod'
            else:
                return 'unknown','unknown'
        except socket.gaierror as ex:
            print('dns lookup error ',str(ex))
            raise ex
    elif myhost.startswith('es-vm-cdaq'):
        return 'es','escdaq','vm'
    elif myhost.startswith('es-vm-local'):
        return 'es','eslocal','vm'
    else:
        print("unknown machine type")
        return 'unknown','unknown','unknown'


def getIPs(hostname):
    try:
        ips = socket.gethostbyname_ex(hostname)
    except socket.gaierror as ex:
        print('unable to get ',hostname,'IP address:',str(ex))
        raise ex
    return ips

def getTimeString():
    tzones = time.tzname
    if len(tzones)>1:zone=str(tzones[1])
    else:zone=str(tzones[0])
    return str(time.strftime("%H:%M:%S"))+" "+time.strftime("%d-%b-%Y")+" "+zone


def checkModifiedConfigInFile(file):

    f = open(file)
    lines = f.readlines(2)#read first 2
    f.close()
    tzones = time.tzname
    if len(tzones)>1:zone=tzones[1]
    else:zone=tzones[0]

    for l in lines:
        if l.strip().startswith("#edited by fff meta rpm"):
            return True
    return False



def checkModifiedConfig(lines):
    for l in lines:
        if l.strip().startswith("#edited by fff meta rpm"):
            return True
    return False


class FileManager:
    def __init__(self,file,sep,edited,os1='',os2='',recreate=False):
        self.name = file
        if recreate==False:
            f = open(file,'r')
            self.lines = f.readlines()
            f.close()
        else:
            self.lines=[]
        self.sep = sep
        self.regs = []
        self.remove = []
        self.edited = edited
        #for style
        self.os1=os1
        self.os2=os2

    def reg(self,key,val,section=None):
        self.regs.append([key,val,False,section])

    def removeEntry(self,key):
        self.remove.append(key)

    def commit(self):
        out = []
        #if self.edited  == False:
        out.append('#edited by fff meta rpm at '+getTimeString()+'\n')

        #first removing elements
        for rm in self.remove:
            for i,l in enumerate(self.lines):
                if l.strip().startswith(rm):
                    del self.lines[i]
                    break

        for i,l in enumerate(self.lines):
            lstrip = l.strip()
            if lstrip.startswith('#'):
                continue

            try:
                key = lstrip.split(self.sep)[0].strip()
                for r in self.regs:
                    if r[0] == key:
                        self.lines[i] = r[0].strip()+self.os1+self.sep+self.os2+r[1].strip()+'\n'
                        r[2]= True
                        break
            except:
                continue
        for r in self.regs:
            if r[2] == False:
                toAdd = r[0]+self.os1+self.sep+self.os2+r[1]+'\n'
                insertionDone = False
                if r[3] is not None:
                    for idx,l in enumerate(self.lines):
                        if l.strip().startswith(r[3]):
                            try:
                                self.lines.insert(idx+1,toAdd)
                                insertionDone = True
                            except:
                                pass
                            break
                if insertionDone == False:
                    self.lines.append(toAdd)
        for l in self.lines:
            #already written
            if l.startswith("#edited by fff meta rpm"):continue
            out.append(l)
        #print "file ",self.name,"\n\n"
        #for o in out: print o
        f = open(self.name,'w+')
        f.writelines(out)
        f.close()


def restoreFileMaybe(file):
    try:
        try:
            f = open(file,'r')
            lines = f.readlines()
            f.close()
            shouldCopy = checkModifiedConfig(lines)
        except:
            #backup also if file got deleted
            shouldCopy = True

        if shouldCopy:
            print("restoring ",file)
            backuppath = os.path.join(backup_dir,os.path.basename(file))
            f = open(backuppath)
            blines = f.readlines()
            f.close()
            if  checkModifiedConfig(blines) == False and len(blines)>0:
                shutil.move(backuppath,file)
    except Exception as ex:
        print("restoring problem: " , ex)
        pass

#main function
if __name__ == "__main__":
    if len(sys.argv)>1:
        if 'restore'==sys.argv[1]:
            print("restoring configuration...")
            restoreFileMaybe(elasticsysconf)
            restoreFileMaybe(elasticconf)
            sys.exit(0)

    cluster,type,env = getmachinetype()

    if type == "escdaqrun2":
     print("ERROR: this should NEVER be installed or run on es-cdaq-run2 cluster! Exiting script.")
     exit(1)

    print("running configuration for machine",os.uname()[1],"of type",type,"in cluster",cluster)


    if True:

        if env=="vm":
            es_publish_host=os.uname()[1]
        else:
            es_publish_host=os.uname()[1]+'.cms'

        #print "will modify sysconfig elasticsearch configuration"
        #maybe backup vanilla versions
        essysEdited =  checkModifiedConfigInFile(elasticsysconf)
        if essysEdited == False:
            #print "elasticsearch sysconfig configuration was not yet modified"
            shutil.copy(elasticsysconf,os.path.join(backup_dir,os.path.basename(elasticsysconf)))

        esEdited =  checkModifiedConfigInFile(elasticconf)
        if esEdited == False:
            shutil.copy(elasticconf,os.path.join(backup_dir,os.path.basename(elasticconf)))

        if type == 'eslocal' or type == 'escdaq' or type == 'escdaqrun2':

            essyscfg = FileManager(elasticsysconf,'=',essysEdited)
            if env=='vm':
                essyscfg.reg('ES_JAVA_OPTS','"-Xms1G -Xmx1G"')
            else:
                essyscfg.reg('ES_JAVA_OPTS','"-Xms30G -Xmx30G"')
            #essyscfg.reg('DATA_DIR','/elasticsearch/lib/elasticsearch')
            essyscfg.removeEntry('CONF_FILE')
            essyscfg.removeEntry('ES_HEAP_SIZE')
            essyscfg.commit()

        if type == 'eslocal':
            escfg = FileManager(elasticconf,':',esEdited,'',' ',recreate=True)
            escfg.reg('network.publish_host',es_publish_host)
            escfg.reg('network.bind_host','_local_,'+es_publish_host)
            escfg.reg('cluster.name','es-local')
            escfg.reg('discovery.zen.ping.unicast.hosts',json.dumps(es_local_list))
            if env=='vm':
                escfg.reg('discovery.zen.minimum_master_nodes','1')
            else:
                escfg.reg('discovery.zen.minimum_master_nodes','3')
            escfg.reg('node.master','true')
            escfg.reg('node.data','true')
            escfg.reg('path.data','/elasticsearch/lib/elasticsearch')
            escfg.reg('http.cors.enabled','true')
            escfg.reg('http.cors.allow-origin','"*"')
            escfg.reg('bootstrap.system_call_filter','false')
            escfg.reg('transport.tcp.compress','true')
            escfg.reg('script.max_compilations_rate', '10000/1m')
            escfg.reg('cluster.routing.allocation.disk.watermark.low','92%')
            escfg.reg('cluster.routing.allocation.disk.watermark.high','95%')

            #other optimizations:
            #if env!='vm':
            escfg.reg("indices.recovery.max_bytes_per_sec","100mb") #default:40mb
            escfg.reg('thread_pool.index.queue_size','1000') #default:200
            escfg.reg('thread_pool.bulk.queue_size','3000') #default:50
            escfg.reg('cluster.routing.allocation.node_concurrent_recoveries','5') #default:2
            escfg.reg('cluster.routing.allocation.node_initial_primaries_recoveries', '5') #default:4
            #escfg.reg('index.translog.flush_threshold_size','4g') #default:512 mb, only es-local,must be template
            escfg.commit()
 
            #modify logging.yml --> TODO: adjust /etc/elasticsearch/log4j2.properties
            #eslogcfg = FileManager(elasticlogconf,':',esEdited,'',' ')
            #eslogcfg.reg('es.logger.level','INFO')
            #eslogcfg.commit()

        if type == 'escdaq' or type == 'escdaqrun2':
            escfg = FileManager(elasticconf,':',esEdited,'',' ',recreate=True)
            escfg.reg('network.publish_host',es_publish_host)
            escfg.reg('network.bind_host','_local_,'+es_publish_host)
            if type == 'escdaqrun2': #separate run2 and LS2 operational cluster
                escfg.reg('cluster.name','es-cdaq-run2')
                escfg.reg('discovery.zen.ping.unicast.hosts',json.dumps(es_cdaq_run2_list))
            else:
                escfg.reg('cluster.name','es-cdaq')
                escfg.reg('discovery.zen.ping.unicast.hosts',json.dumps(es_cdaq_list))
            if env=='vm':
                escfg.reg('discovery.zen.minimum_master_nodes','1')
            else:
                escfg.reg('discovery.zen.minimum_master_nodes','3')
            escfg.reg('node.master','true')
            escfg.reg('node.data','true')
            escfg.reg('path.data','/elasticsearch/lib/elasticsearch')
            escfg.reg('http.cors.enabled','true')
            escfg.reg('http.cors.allow-origin','"*"')
            escfg.reg('bootstrap.system_call_filter','false')
            escfg.reg('transport.tcp.compress','true')
            escfg.reg('action.auto_create_index','.marvel-*')
            escfg.reg('script.max_compilations_rate', '10000/1m')
            escfg.reg("action.destructive_requires_name", 'true')
            escfg.reg('cluster.routing.allocation.disk.watermark.low','92%')
            escfg.reg('cluster.routing.allocation.disk.watermark.high','95%')

            #if env!='vm':
            escfg.reg("indices.recovery.max_bytes_per_sec","100mb") #default:40mb
            escfg.reg('thread_pool.index.queue_size','1000') #default:200
            escfg.reg('thread_pool.bulk.queue_size','3000') #default:50
            escfg.reg('cluster.routing.allocation.node_concurrent_recoveries','5') #default:2
            escfg.reg('cluster.routing.allocation.node_initial_primaries_recoveries', '5') #default:4
            escfg.commit()


#!/bin/env python
import os,sys,time,datetime
import threading
from pyelasticsearch.client import ElasticSearch
import json
from ordereddict import OrderedDict

#es_hosts=['http://fuval-c2a11-02:9200','http://fuval-c2a11-03:9200','http://fuval-c2a11-15:9200']
#es_tribe_hosts=['http://fuval-c2a11-28:9200']

es_hosts=['http://dvcu-ccsl6-01:9200']
es_tribe_hosts=['http://dvtu-ccsl6-01:9200']


main_es = ElasticSearch(es_hosts[0])
tribe_es = ElasticSearch(es_tribe_hosts[0])
main_index = 'runindex'
setup='daq2val'

class query_maker(threading.Thread):
    def __init__(self,run):
        threading.Thread.__init__(self)
        self.running = True
        self.hostname = os.uname()[1]
        self.ip = {}
        self.runno = run
        self.known_streams = {}
        app_query = {"query":{"top_children":{"score":"sum","type": "node", "query" : { "match_all" : {}}}},"fields":["name"]}
        node_query = OrderedDict({"query":{"filtered":{"query":{"match_all":{}},"filter":{"has_parent":{"parent_type":"appliance","query":{"term":{"name":""}}}}}},"size":32});
        
        self.state_query =  OrderedDict({"sort":{"_timestamp":"desc"},
                                         "query":{"range":{"_timestamp" : {"gt" :"now-3s", "lt" :"now-1s"}}},
                                         "size":1000,"facets":
                                         {"hmicro": {"histogram" : {"field" : "micro", "interval":"1"}},
                                          "hmini": {"histogram" : {"field" : "mini", "interval":"1"}},
                                          "hmacro": {"histogram" : {"field" : "macro", "interval":"1"}}},
                                         "post_filter":{"term":{"ls":-1}}});

        self.stream_query = OrderedDict({"query":{"term" : {"stream":""}},"size":10000,"sort":{"ls":"desc"},"facets":{"inls": {"terms_stats" : {"key_field" : "ls","value_field" : "data.in", "order":"reverse_term", "size":3000}},"outls": {"terms_stats" : {"key_field" : "ls","value_field" : "data.out", "order":"reverse_term", "size":30}}},"filter":{"term":{"ls":-1}}});

        self.stream_count = OrderedDict({"query":{"match_all" : {}},"size":10000,"sort":{"ls":"desc"},"facets":{"instream": {"terms" : {"field" : "stream", "size":100}}},"filter":{"term":{"ls":-1}}});
        
        node_res = tribe_es.cluster_state()

        for id,node in node_res['nodes'].iteritems():
            print node
            if 'tribe.name' in node['attributes']:
                if node['attributes']['tribe.name'] not in self.ip:
                    self.ip[node['attributes']['tribe.name']]=[]
                self.ip[node['attributes']['tribe.name']].append({'name':node['name'],'address':node['transport_address']})
                print 'appending '+node['name']
    def run(self):
        i=0
        last_doc = {}
        while self.running:
            result={}
            fuinlshist={}
            fuoutlshist={}

            try:
                res=tribe_es.search(self.state_query,index='run'+self.runno.zfill(6)+"*",doc_type="prc-i-state")
                
                for h in res['facets']:
                    result[h] = res['facets'][h]['entries']
            except Exception as ex:
                print ex,'on tribe query'

            if result and len(result['hmacro'])!=0:
                main_es.index(main_index,'state-hist',result,parent=self.runno)

            if(i%1==0):
                try:
                    res=tribe_es.search(self.stream_count,index='run'+self.runno.zfill(6)+"*",doc_type="fu-out") 
                except Exception as ex:
                    print 'EXCEPTION',ex

                print self.runno,res

                for stream in res['facets']['instream']['terms']:
                    name = stream['term']
                    count = stream['count']
                    self.known_streams[name]=count
                    print "stream=",stream
                    if name not in fuoutlshist:
                        fuoutlshist[name]={}
                    if name not in fuinlshist:
                        fuinlshist[name]={}

                for stream in self.known_streams:
                    self.stream_query['query']['term']['stream']=stream
                    res=tribe_es.search(self.stream_query,index='run'+self.runno.zfill(6)+"*",doc_type="fu-out") 
#                    print res

                    for m in range(len(res['facets']['inls']['terms'])):
                        ls = int(res['facets']['inls']['terms'][m]['term'])
                        count = res['facets']['inls']['terms'][m]['count']
                        total = res['facets']['inls']['terms'][m]['total']
                        fuinlshist[stream][ls]=total

                    for m in range(len(res['facets']['outls']['terms'])):
                        ls = int(res['facets']['outls']['terms'][m]['term'])
                        count = res['facets']['outls']['terms'][m]['count']
                        total = res['facets']['outls']['terms'][m]['total']
                        fuoutlshist[stream][ls]=total

                print 'updating streams'
                for stream in fuoutlshist:
                    for ls in fuoutlshist[stream]:
                        if 'ohist' not in last_doc or stream not in last_doc['ohist'] or ls not in last_doc['ohist'][stream] or last_doc['ohist'][stream][ls]!=fuoutlshist[stream][ls] or last_doc['ihist'][stream][ls]!=fuinlshist[stream][ls]:
                            doc={"stream":stream, "ls":ls,"in":fuinlshist[stream][ls],"out":fuoutlshist[stream][ls]}
                            print 'creating doc for '+str(ls)
                            print doc
                            main_es.index(main_index,'stream-hist',doc,id=self.runno.zfill(6)+stream+str(ls),parent=self.runno)
                last_doc={"ohist":fuoutlshist,"ihist":fuinlshist};

            i=i+1
            time.sleep(1.)

    def stop(self):
        self.running=False


class monitor():

    def __init__(self):
        self.running = True
        self.query_threads={}
        self.open_query={"query":{"constant_score":{"filter":{"missing":{"field":"endTime" }}}}}
        self.close_query={"query":{"filtered":{"query":{"match_all":{}},"filter":{"not":{"filter":{"missing":{"field":"stopTime" }}}}}}}
        
        
    def run(self):
        while self.running:
            print 'checking for new run at '+str(datetime.datetime.now())
            result = main_es.search(self.open_query,index=main_index,doc_type='run')
            for runs in result['hits']['hits']:
                if runs['_source']['runNumber'] not in self.query_threads:
                    print 'append new run ',runs['_source']['runNumber']
                    self.query_threads[runs['_source']['runNumber']]=query_maker(runs['_source']['runNumber'])
                    self.query_threads[runs['_source']['runNumber']].start()
            time.sleep(5.)
            
            ended_runs=[]
            for run in self.query_threads:
                result = main_es.get(main_index,'run',run)
                if 'endTime' in result['_source']:
                    print 'run '+run+' ended at '+result['_source']['endTime']
                    self.query_threads[run].stop()
                    ended_runs.append(run)
            for run in ended_runs:
                del self.query_threads[run]
            

if __name__ == "__main__":
    m = monitor()
    m.run()

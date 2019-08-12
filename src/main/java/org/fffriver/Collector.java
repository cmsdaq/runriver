package org.fffriver;

//JAVA
import java.io.IOException;
import java.util.Set;
import java.util.Map;
import java.util.List;
import javafx.util.Pair;
import java.util.HashSet;
import java.util.HashMap;
import java.util.ArrayList;
import java.net.UnknownHostException;
import java.lang.Math;
import org.apache.http.HttpHost;

//ELASTICSEARCH
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.ElasticsearchException;

import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.join.query.JoinQueryBuilders;
import static org.elasticsearch.common.xcontent.XContentFactory.*;

//Remote query stuff
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;

//import java.net.InetSocketAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.search.aggregations.metrics.Sum;
import org.elasticsearch.search.aggregations.metrics.Max;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy;

import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;

//org.json
import net.sf.json.JSONObject;
import net.sf.json.JSONSerializer;
import org.apache.commons.io.IOUtils;


//In java seems to be impossible to inherit/extend from multiple class
//so i need to implement the AbstractRiverComponent compatibility manually
//just to have the logger object. 
//https://github.com/elasticsearch/elasticsearch/blob/master/src/main/java/org/elasticsearch/river/AbstractRiverComponent.java

public class Collector extends AbstractRunRiverThread {

    public class CollectorSet {

    public CollectorSet() {}
    public CollectorSet(String name) {
      appliance=name;
    }

    public String appliance = new String();
    public Map<String, Long> known_streams = new HashMap<String, Long>();
    public Map<String, Map<String, Double>> fuinlshist = new HashMap<String, Map<String, Double>>();
    public Map<String, Map<String, Double>> fuoutlshist = new HashMap<String, Map<String, Double>>();
    public Map<String, Map<String, Double>> fuerrlshist = new HashMap<String, Map<String, Double>>();
    public Map<String, Map<String, Double>> fufilesizehist = new HashMap<String, Map<String, Double>>();
    public Map<String, Map<String, Double>> futimestamplshist = new HashMap<String, Map<String, Double>>();
    public Map<String, Map<String, String>> fumergetypelshist = new HashMap<String, Map<String, String>>();
    public Map<String, Double> eolEventsList = new HashMap<String, Double>();
    public Map<String, Double> eolLostEventsList = new HashMap<String, Double>();
    public Map<String, Double> eolTotalEventsList = new HashMap<String, Double>();
    public long lowestLS = -1;
    }



    CollectorSet fullSet = new CollectorSet();
    Map<String, CollectorSet> buSets=  new HashMap<String,CollectorSet>();

    Map<Integer, Map<String,Double>> incompleteLumis = new HashMap<Integer, Map<String,Double>>();

    RestHighLevelClient remoteClient;
    Boolean EoR=false;
    String eslocalIndex;
    String runStrPrefix;
    Boolean firstTime=true;
    Boolean shouldCloseRun = false;
    Integer ustatesReserved=-1;
    Integer ustatesSpecial=-1;
    Integer ustatesOutput=-1;

    @Inject
    public Collector(String riverName, Map<String, Object> rSettings,RestHighLevelClient client) {
        super(riverName,rSettings,client);
    }

    @Override
    public void beforeLoop() throws UnknownHostException, IOException {
        logger.info("Collector started.");
        this.interval = fetching_interval;
        this.eslocalIndex = "run"+String.format("%06d", Integer.parseInt(runNumber))+"*";
        if (runNumber.length()<6)
            runStrPrefix = "run" + "000000".substring(runNumber.length()) + runNumber + "_ls";
        else
            runStrPrefix = "run" + runNumber + "_ls";

        setRemoteClient();

    }
    @Override
    public void afterLoop() throws Exception, IOException {
        //do a final check on all lumis if there are any incomplete
        if (!incompleteLumis.isEmpty()) {
          firstTime=true;
          collectStreams();
        }
        if (shouldCloseRun) execRunClose();
        remoteClient.close();
        logger.info("Collector stopped.");
    }

    @Override
    public void mainLoop() throws Exception, IOException {
        logger.info("Collector running...");
        collectStates();
        collectStreams();
        checkRunEnd();
        checkBoxInfo();
    }

    private SearchRequest makeStreamsRequest(int size, boolean perbu) {

        SearchSourceBuilder sBuilder = new SearchSourceBuilder()
          .query(QueryBuilders.termQuery("doc_type","fu-out"))
          .size(0)
          .aggregation(
            AggregationBuilders.terms("streams").field("stream").size(200)
              .subAggregation(AggregationBuilders.terms("ls").field("ls").size(size).order(BucketOrder.key(false))
                .subAggregation(AggregationBuilders.sum("in").field("data.in"))
                .subAggregation(AggregationBuilders.sum("out").field("data.out"))
                .subAggregation(AggregationBuilders.sum("error").field("data.errorEvents"))
                .subAggregation(AggregationBuilders.sum("filesize").field("data.fileSize"))
                .subAggregation(AggregationBuilders.max("fm_date").field("fm_date"))
                .subAggregation(AggregationBuilders.terms("mergeType").field("data.MergeType").size(2).order(BucketOrder.key(false)))
          ));
          if (perbu) sBuilder.aggregation(
            AggregationBuilders.terms("appliances").field("appliance").size(200)
             .subAggregation(AggregationBuilders.terms("streams").field("stream").size(200)
              .subAggregation(AggregationBuilders.terms("ls").field("ls").size(size).order(BucketOrder.key(false))
                .subAggregation(AggregationBuilders.sum("in").field("data.in"))
                .subAggregation(AggregationBuilders.sum("out").field("data.out"))
                .subAggregation(AggregationBuilders.sum("error").field("data.errorEvents"))
                .subAggregation(AggregationBuilders.sum("filesize").field("data.fileSize"))
                .subAggregation(AggregationBuilders.max("fm_date").field("fm_date"))
                .subAggregation(AggregationBuilders.terms("mergeType").field("data.MergeType").size(2).order(BucketOrder.key(false)))
          )));
          //.sort(SortBuilders.fieldSort("ls").order(SortOrder.DESC))//not used

        return new SearchRequest().indices(eslocalIndex).source(sBuilder);

    }

    private void fillStreamsSet(CollectorSet set, Terms streams) {

        if(streams.getBuckets().isEmpty()){return;}
        for (Terms.Bucket stream : streams.getBuckets()){
            String streamName = stream.getKeyAsString();
            //logger.info("str:"+streamName+" "+stream.getDocCount);

            set.known_streams.put(streamName,stream.getDocCount());
       
            if (set.fumergetypelshist.get(streamName)==null) {
              set.fuinlshist.put(streamName, new HashMap<String, Double>());
              set.fuoutlshist.put(streamName, new HashMap<String, Double>());
              set.fuerrlshist.put(streamName, new HashMap<String, Double>());
              set.fufilesizehist.put(streamName, new HashMap<String, Double>());
              set.futimestamplshist.put(streamName, new HashMap<String, Double>());
              set.fumergetypelshist.put(streamName, new HashMap<String, String>());
            }

            Terms lss = stream.getAggregations().get("ls");
           
            for (Terms.Bucket ls : lss.getBuckets()) {

                String lsName = ls.getKeyAsString();

                //update last LS found in the query
                Long lsvalLong = (Long)(ls.getKey());
                long lsval = lsvalLong.longValue();
                if (lsval<set.lowestLS || set.lowestLS<0) set.lowestLS=lsval;

                Sum inSum = ls.getAggregations().get("in");
                Sum outSum = ls.getAggregations().get("out");
                Sum errSum = ls.getAggregations().get("error");
                Sum filesizeSum = ls.getAggregations().get("filesize");
                Max fm_date = ls.getAggregations().get("fm_date");

                String mergeType = "";
                Terms mergeTypes = ls.getAggregations().get("mergeType");
                for (Terms.Bucket mTypeBucket : mergeTypes.getBuckets()) {
                 mergeType = mTypeBucket.getKeyAsString();
                 break;
                }
                set.fuinlshist.get(streamName).put(lsName,inSum.getValue());
                set.fuoutlshist.get(streamName).put(lsName,outSum.getValue());
                set.fuerrlshist.get(streamName).put(lsName,errSum.getValue());
                set.fufilesizehist.get(streamName).put(lsName,filesizeSum.getValue());
                set.futimestamplshist.get(streamName).put(lsName,fm_date.getValue());
                set.fumergetypelshist.get(streamName).put(lsName,mergeType);
            } 
        }
    }

    public void collectStreams() throws Exception {
        logger.info("collectStreams");
        //boolean perbu = true;//member
        
        int size=0;
        if(firstTime){
            firstTime = false;
            size=1000000;
        }else{
            size=30;
        }
        SearchResponse sResponse = remoteClient.search(makeStreamsRequest(size,perbu),RequestOptions.DEFAULT);

        collectStats(riverName,"streamQuery",eslocalIndex,sResponse);

        if(sResponse.getHits().getTotalHits().value == 0L){ 
            logger.info("streamQuery returns 0 hits");
            return;
        }

        if(sResponse.getAggregations().asList().isEmpty()){return;}
        
        Terms streams = sResponse.getAggregations().get("streams");
        fillStreamsSet(fullSet,streams);

        int applianceBuckets=0;
        if (perbu) {
          Terms appliances = sResponse.getAggregations().get("appliances");
          for (Terms.Bucket appliance : appliances.getBuckets()){
            applianceBuckets++;
            String buName = appliance.getKeyAsString();

            try {
              buName = buName.substring(0,buName.indexOf("."));
            } catch (Exception exp) {}
 
            if (buSets.get(buName)==null) 
              buSets.put(buName,new CollectorSet(buName));

            Terms ab = appliance.getAggregations().get("streams");
            fillStreamsSet(
            buSets.get(buName),
            ab);
            //appliance.getAggregations().get("streams"));
          }
        }

 
        //create full list of lumisections to check (no need to use SortedMap<int,String>)
        Map<String,Integer> lsMap = new HashMap<String,Integer>();
        Set<String> lsQueried = new HashSet<String>();

        for (String stream : fullSet.known_streams.keySet()){
            for (String ls : fullSet.fuoutlshist.get(stream).keySet())
              lsMap.put(ls,Integer.parseInt(ls));
        }
        //verify with EoLS information
        //sorted needed?
        List<String> removeListAnyStream = new ArrayList<String>();
        for (String ls : lsMap.keySet()) {
                Integer ls_num = lsMap.get(ls);

                //EoLS aggregation for this LS
                Double eventsVal=0.0;
                Double lostEventsVal=0.0;
                Double totalEventsVal=0.0;
                Boolean run_eols_query = true;
                if (fullSet.eolEventsList.get(ls) != null) {

                  eventsVal = fullSet.eolEventsList.get(ls);
                  lostEventsVal = fullSet.eolLostEventsList.get(ls);
                  totalEventsVal = fullSet.eolTotalEventsList.get(ls);

                  //require match, otherwise BU json files might not be written yet
                  if (Math.round(totalEventsVal-eventsVal-lostEventsVal)==0.0) run_eols_query=false;
                }
                if (run_eols_query) lsQueried.add(ls);

                //Integer ls_num = Integer.parseInt(ls); 
                //drop lumis more than 70 LS behind oldest one in the query range (unless EvB information is inconsistent)
                if (ls_num+70<fullSet.lowestLS && run_eols_query==false) {
                  //this is old lumisection, dropping from object maps (even if incomplete)
                  removeListAnyStream.add(ls); //??????!
                  continue;
                }

                if (run_eols_query) {
                    String idStr;
                    int lsStrLen = ls.length(); 
                    Integer delta = 4 - lsStrLen;
                    if (lsStrLen<4)
                      idStr = runStrPrefix + "0000".substring(lsStrLen)+ls;
                    else
                      idStr = runStrPrefix + ls;
 
                    //logger.info("DEBUG: idStr:"+idStr); 
                    int eolsSize =0;
                    if (perbu) eolsSize = 200;

                    SearchSourceBuilder sBuilderEoLS = new SearchSourceBuilder()
                      .query(QueryBuilders.boolQuery()
                        .must(JoinQueryBuilders.parentId("eols",runNumber))
                        .must(QueryBuilders.termQuery("ls",ls))
                      )
                      .size(eolsSize)
                      .aggregation(AggregationBuilders.sum("NEvents").field("NEvents"))
                      .aggregation(AggregationBuilders.sum("NLostEvents").field("NLostEvents"))
                      .aggregation(AggregationBuilders.max("TotalEvents").field("TotalEvents"));

                    SearchRequest sRequestEoLS = new SearchRequest().indices(runindex_write).routing(runNumber).source(sBuilderEoLS);
 
                    SearchResponse sResponseEoLS = client.search(sRequestEoLS,RequestOptions.DEFAULT);
 
                    if(sResponseEoLS.getHits().getTotalHits().value == 0L){ 
                        logger.info("eolsQuery returns 0 hits for LS " + ls + ", skipping collection without EoLS docs");
                        continue;
                    }
                    logger.info("eolsQuery returned " + String.valueOf(sResponseEoLS.getHits().getTotalHits().value) + " hits for LS "+ ls);
                    //logger.info(String.valueOf(sResponseEoLS.getHits().getTotalHits().value));
                    Sum eolEvents = sResponseEoLS.getAggregations().get("NEvents");
                    Sum eolLostEvents = sResponseEoLS.getAggregations().get("NLostEvents");
                    Max eolTotalEvents = sResponseEoLS.getAggregations().get("TotalEvents");
                    eventsVal = eolEvents.getValue();    
                    lostEventsVal = eolLostEvents.getValue();
                    totalEventsVal = eolTotalEvents.getValue();
                    fullSet.eolEventsList.put(ls,eventsVal); 
                    fullSet.eolLostEventsList.put(ls,lostEventsVal); 
                    fullSet.eolTotalEventsList.put(ls,totalEventsVal); 
                    if (Math.round(totalEventsVal-eventsVal-lostEventsVal)==0) {
                        logger.info("eolsQuery match for LS "+ ls + " with total of "+totalEventsVal.toString()+" events");
                    }
                    if (eolsSize>0) {
                      //parse all documents
                      for (SearchHit hit: sResponseEoLS.getHits().getHits()) {
                        //convert to string for now as we don'w know type of the inner object
                        eventsVal = Double.parseDouble(hit.getSourceAsMap().get("NEvents").toString());
                        lostEventsVal = Double.parseDouble(hit.getSourceAsMap().get("NLostEvents").toString());
                        totalEventsVal = Double.parseDouble(hit.getSourceAsMap().get("TotalEvents").toString());
                        String appliance =  hit.getSourceAsMap().get("appliance").toString();
                        try {
                          appliance = appliance.substring(0,appliance.indexOf("."));
                        } catch (Exception exp) {}
                        if  (buSets.get(appliance)==null)
                          buSets.put(appliance, new CollectorSet(appliance));
                        CollectorSet buSet = buSets.get(appliance);
                        buSet.eolEventsList.put(ls,eventsVal); 
                        buSet.eolLostEventsList.put(ls,lostEventsVal); 
                        buSet.eolTotalEventsList.put(ls,totalEventsVal); 
                      }
                    }
                }
        }
        injectStreams(fullSet,lsMap,lsQueried,removeListAnyStream,true); 

        if (perbu) {
          int injectedBuckets=0;
          Terms appliances = sResponse.getAggregations().get("appliances");
          for (Terms.Bucket appliance : appliances.getBuckets()){
            String buName = appliance.getKeyAsString();

            try {
              buName = buName.substring(0,buName.indexOf("."));
            } catch (Exception exp) {}
 
            //refreshing on first and last injected document
            boolean callRefresh = false;
            if (applianceBuckets==injectedBuckets+1) callRefresh = true;
            injectStreams(buSets.get(buName),lsMap,lsQueried,removeListAnyStream,callRefresh); 
            injectedBuckets++;
          }
        }
    }

    //TODO: also we could buffer and bulk inject all of this, as well as get data using search (scales to N_streams x N_BUs) instead of getting by id

    private void injectStreams(CollectorSet set, Map<String,Integer> lsMap, Set<String> lsQueried, List<String> removeListAnyStream, boolean callRefresh) throws IOException {
        //continue with stream aggregation
        String doc_type = "stream-hist";
        if (!set.appliance.isEmpty()) doc_type = "stream-hist-appliance";

        //use multiget!
        List<Pair<String,String>> requestList = new ArrayList<Pair<String,String>>();
        MultiGetRequest greq = new MultiGetRequest();
        int docs_=0;

        for (String stream : set.known_streams.keySet()){
            List<String> removeList = new ArrayList<String>();

            for (String ls : set.fuoutlshist.get(stream).keySet()){
                if (set.eolEventsList.get(ls)==null) continue;

                String id = String.format("%06d", Integer.parseInt(runNumber))+"_"+stream+"_"+ls;

                if (!set.appliance.isEmpty()) id=id+"_"+set.appliance;

                //Check if data is changed (to avoid to update timestamp if not necessary)
                requestList.add(new Pair<String,String>(stream,ls));
                greq.add(new MultiGetRequest.Item(runindex_write,id).routing(runNumber));
                docs_++;
            }
        }
        List<Pair<String,String>> removeList = new ArrayList<Pair<String,String>>();

        if (docs_>0) {
          greq.refresh(callRefresh);
          MultiGetResponse smresponse = client.mget(greq,RequestOptions.DEFAULT);

          BulkRequest bulkRequest = new BulkRequest();
          boolean do_bulk=false;

          for (int i=0;i<smresponse.getResponses().length;i++) {
                Pair<String,String> p = requestList.get(i);
                String stream = p.getKey();
                String ls = p.getValue();

                MultiGetItemResponse mgi = smresponse.getResponses()[i];
                if (mgi.getFailure()!=null) {
                  logger.error("skipping response with failure for stream " + stream + " LS " + ls + " exception:", mgi.getFailure().getFailure());
                  continue;
                }
                GetResponse sresponse = mgi.getResponse();
                if (sresponse == null) {
                  logger.error("skipping response with null response (should not be reached)");
                  continue;
                }
                if (set.eolEventsList.get(ls)==null) continue;

                Integer ls_num = lsMap.get(ls);

                Double eventsVal = set.eolEventsList.get(ls);
                Double lostEventsVal = set.eolLostEventsList.get(ls);
                Double totalEventsVal = set.eolTotalEventsList.get(ls);

                String id = String.format("%06d", Integer.parseInt(runNumber))+"_"+stream+"_"+ls;

                if (!set.appliance.isEmpty()) id=id+"_"+set.appliance;

                //Check if data is changed (to avoid to update timestamp if not necessary)
                Double lastCompletion = 0.;
                boolean dataChanged = true;
                boolean isUpdate= false;
                if (sresponse.isExists()){ 
                    isUpdate=true;
                    Double in = Double.parseDouble(sresponse.getSource().get("in").toString());
                    Double out = Double.parseDouble(sresponse.getSource().get("out").toString());
                    //new field, allow to be missing if log entry updated by the new plugin version
                    Double error = -1.;
                    if (sresponse.getSource().get("err").toString()!=null) {
                      error = Double.parseDouble(sresponse.getSource().get("err").toString());
                    }
                    if (sresponse.getSource().get("completion")!=null) {
                      lastCompletion = Double.parseDouble(sresponse.getSource().get("completion").toString());
                    }
                    //logger.info("old" + in.toString() + " " + out.toString() + " " + error.toString() + " compl "+lastCompletion.toString());
                    //logger.info("new" + set.fuinlshist.get(stream).get(ls).toString() + " " + set.fuoutlshist.get(stream).get(ls).toString() + " " + set.fuerrlshist.get(stream).get(ls).toString() );
                    if (lastCompletion==1.0
                        && in.compareTo(set.fuinlshist.get(stream).get(ls))==0
                        && out.compareTo(set.fuoutlshist.get(stream).get(ls))==0
                        && error.compareTo(set.fuerrlshist.get(stream).get(ls))==0){
                        dataChanged = false;
                    } else {
                        if (set.appliance.isEmpty())
                            logger.info(id+" with completion " + lastCompletion.toString() + " already exists and will be checked for update.");
                    }
                }

                Double newCompletion = 1.;
                //calculate completion as (Nprocessed / Nbuilt) * (Nbuilt+Nlost)/Ntriggered
                Double output_count = set.fuinlshist.get(stream).get(ls) + set.fuerrlshist.get(stream).get(ls);
                //logger.info("output_count:" + output_count.toString() + " eventsVal:"+eventsVal.toString());
                
                if (eventsVal>0 && output_count!=eventsVal)
                    newCompletion = output_count/eventsVal;

                //only calculate completion wrt. Total in case of summed values, not per BU
                if (set.appliance.isEmpty() && (eventsVal +  lostEventsVal != totalEventsVal)) {
                  if (totalEventsVal>0) {
                    Double evb_count = eventsVal +  lostEventsVal;
                    //logger.info("evb_count:" + evb_count.toString() + " totalEventsVal:"+totalEventsVal.toString());
                    if (evb_count != totalEventsVal)
                      newCompletion = newCompletion * ((evb_count) / totalEventsVal);
                  }
                  else //logger.error("This should not happen: mismatch between NEvents + NLostEvents is not zero, but TotalEvents is for ls "+ls);
                    logger.error("This should not happen: mismatch between NEvents + NLostEvents is not zero, but TotalEvents is for ls "+ls
                                 + " values: " + eventsVal.toString() + " " + lostEventsVal.toString()  + " " + totalEventsVal.toString());
                }
                //precision rounding
                if (newCompletion>0.9999999999 && newCompletion<1.0000000001) newCompletion=1.;
                
                //Update Data
                if (dataChanged){ //don't log for each appliance
                  if (set.appliance.isEmpty()) {
                    logger.info(doc_type+" update for ls,stream: "+ls+","+stream+" in:"+set.fuinlshist.get(stream).get(ls).toString()
                                +" out:"+set.fuoutlshist.get(stream).get(ls).toString()+" err:"+set.fuerrlshist.get(stream).get(ls).toString() + " completion " + newCompletion.toString());
                    logger.info("Totals numbers - eventsVal:"+eventsVal.toString() + " lostEventsVal:" + lostEventsVal.toString() + " totalEventsVal:" + totalEventsVal.toString());
                  }
                  else {
                    //if (isUpdate && lastCompletion < newCompletion - 0.00001) {
                    //  logger.info(id+" with completion " + lastCompletion.toString() + " already exists and will be updated with new completion: " + newCompletion.toString());
                    //}
                  }
                  Double retDate = set.futimestamplshist.get(stream).get(ls);
                  Map<String,Double> strcompletion = incompleteLumis.get(ls_num);
                  if (newCompletion<1.) {
                      if (strcompletion!=null)
                          strcompletion.put(stream,newCompletion);
                      else {
                          Map<String,Double> newStreamCompletion = new HashMap<String,Double>();
                          newStreamCompletion.put(stream,newCompletion);
                          incompleteLumis.put(ls_num,newStreamCompletion);
                      }
                  } else {
                      //clean up if completed
                      if (strcompletion!=null) {
                        strcompletion.remove(stream);
                        if (strcompletion.isEmpty()) incompleteLumis.remove(ls_num);
                      }
                  }
                  long start_time_millis = System.currentTimeMillis();

                  XContentBuilder tmpBuild = XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject("runRelation").field("name",doc_type).field("parent",Integer.parseInt(runNumber)).endObject()
                        .field("doc_type",doc_type)
                        .field("runNumber",runNumber)
                        .field("stream", stream)
                        .field("ls", ls_num)
                        .field("in", set.fuinlshist.get(stream).get(ls))
                        .field("out", set.fuoutlshist.get(stream).get(ls))
                        .field("err", set.fuerrlshist.get(stream).get(ls))
                        .field("filesize", set.fufilesizehist.get(stream).get(ls))
                        .field("mergeType", set.fumergetypelshist.get(stream).get(ls))
                        .field("completion", newCompletion)
                        .field("date",  start_time_millis);
 
                  if (!set.appliance.isEmpty())
                      tmpBuild.field("host",set.appliance);

                  //set timestamp. Use file timestamp if available, else set system time
                  if (retDate >  Double.NEGATIVE_INFINITY)
                    tmpBuild.field("fm_date", retDate.longValue()); //convert when injecting into futimestamplshist?
                  else
                    tmpBuild.field("fm_date",start_time_millis);

                  tmpBuild.endObject();

                  IndexRequest indexReq = new IndexRequest(runindex_write,"_doc",id)
                    .routing(runNumber)
                    .source(tmpBuild);
                  bulkRequest.add(indexReq);
                  do_bulk=true;
                }
                
                //drop complete lumis older than query range (keep checking if EvB information is inconsistent)
                if (newCompletion==1 && ls_num<set.lowestLS && !lsQueried.contains(ls)) {
                  //complete,dropping from maps
                  removeList.add(new Pair<String,String>(stream,ls));
                }
          }
          if (do_bulk) {
            if (callRefresh)
              bulkRequest.setRefreshPolicy(RefreshPolicy.IMMEDIATE);

            BulkResponse iResponse = client.bulk(bulkRequest,RequestOptions.DEFAULT);

            if (iResponse.hasFailures()) {
              for (BulkItemResponse bulkItemResponse : iResponse) {

                 BulkItemResponse.Failure failure = bulkItemResponse.getFailure();
                 logger.error("bulk index failure:",failure);
              }
            }
          }
        }

        for (String stream : set.known_streams.keySet()){
            for (String ls : removeListAnyStream) {
              if (set.appliance.isEmpty())
                logger.info("removing old stream" + stream + " Map entries for LS "+ ls);
              set.fuinlshist.get(stream).remove(ls);
              set.fuoutlshist.get(stream).remove(ls);
              set.fuerrlshist.get(stream).remove(ls);
              set.fufilesizehist.get(stream).remove(ls);
              set.futimestamplshist.get(stream).remove(ls);
              set.fumergetypelshist.get(stream).remove(ls);
            }
        }

        for (Pair<String,String> streamls : removeList) {
              String stream = streamls.getKey();
              String ls = streamls.getValue();
              if (set.appliance.isEmpty())
                logger.info("removing stream" + stream + " Map entries for LS "+ ls);
              set.fuinlshist.get(stream).remove(ls);
              set.fuoutlshist.get(stream).remove(ls);
              set.fuerrlshist.get(stream).remove(ls);
              set.fufilesizehist.get(stream).remove(ls);
              set.futimestamplshist.get(stream).remove(ls);
              set.fumergetypelshist.get(stream).remove(ls);
        }
    }
        
    public void collectStates() throws Exception {
        logger.info("collectStates");

        SearchSourceBuilder sBuilder = new SearchSourceBuilder()
          .query(QueryBuilders.boolQuery()
            .must(QueryBuilders.termQuery("doc_type","prc-i-state"))
            .must(QueryBuilders.rangeQuery("fm_date").from("now-4s").to("now-2s"))
          )
          .size(0)
          .aggregation(AggregationBuilders.terms("mclass").field("mclass").size(9999).order(BucketOrder.key(true)).minDocCount(1)
            .subAggregation(AggregationBuilders.terms("hmicro").field("micro").size(9999).order(BucketOrder.key(true)).minDocCount(1))
            .subAggregation(AggregationBuilders.terms("hinput").field("instate").size(9999).order(BucketOrder.key(true)).minDocCount(1))
          )
          .aggregation(AggregationBuilders.terms("hmicro").field("micro").size(9999).order(BucketOrder.key(true)).minDocCount(1))
          .aggregation(AggregationBuilders.terms("hmini").field("mini").size(9999).order(BucketOrder.key(true)).minDocCount(1))
          .aggregation(AggregationBuilders.terms("hmacro").field("macro").size(9999).order(BucketOrder.key(true)).minDocCount(1))
          .aggregation(AggregationBuilders.terms("hinput").field("instate").size(9999).order(BucketOrder.key(true)).minDocCount(1))
          .aggregation(AggregationBuilders.max("fm_date").field("fm_date"));
        
        SearchRequest sRequest = new SearchRequest().indices(eslocalIndex).source(sBuilder);
        SearchResponse sResponse = remoteClient.search(sRequest,RequestOptions.DEFAULT);

        collectStats(riverName,"statesQuery",eslocalIndex,sResponse);
        
        //logger.info(String.valueOf(sResponse.getHits().getTotalHits()));
        if(sResponse.getHits().getTotalHits().value == 0L){ 
            logger.info("statesQuery returns 0 hits");
            return;
        }
        if (ustatesReserved==-1) {
          SearchSourceBuilder sBuilderUstates = new SearchSourceBuilder()
            .query(JoinQueryBuilders.parentId("microstatelegend",runNumber))
            .size(1);
          SearchRequest sRequestUstates = new SearchRequest().indices(runindex_read).routing(runNumber).source(sBuilderUstates);
          SearchResponse sResponseUstates = client.search(sRequestUstates,RequestOptions.DEFAULT);

          SearchHit[] searchHits = sResponseUstates.getHits().getHits();
          if(searchHits.length != 0) {
            logger.info("microstatelegend query returns hits");
            //List<String> keys = new ArrayList<String>(searchHits[0].getSourceAsMap().keySet());
            //for (String key: keys) { logger.info("key:");logger.info(key);}
            if (searchHits[0].getSourceAsMap().get("reserved")!=null) {
              //Integer reservedVal = sResponseUstates.getHits().getHits()[0].getSource().field("reserved");
              Integer reservedVal = (Integer) searchHits[0].getSourceAsMap().get("reserved");
              ustatesReserved = reservedVal;

              if (searchHits[0].getSourceAsMap().get("special")!=null)
                ustatesSpecial = (Integer) searchHits[0].getSourceAsMap().get("special");

              if (searchHits[0].getSourceAsMap().get("output")!=null)
                ustatesOutput = (Integer) searchHits[0].getSourceAsMap().get("output");

              if (ustatesOutput<0 || ustatesSpecial<0) {
                ustatesOutput=-1;
                ustatesSpecial=-1;
              }
            }
            else {
              logger.info("reserved field in microstatelegend is not present. Disabling checks.");
              //use default value
              ustatesReserved = 33;
            }
          }
        }

        Max fm_date_mx = sResponse.getAggregations().get("fm_date");
        Double fm_date = fm_date_mx.getValue();

        if(sResponse.getAggregations().asList().isEmpty()){return;}

        BulkRequest bulkRequest = new BulkRequest();

        XContentBuilder xb = XContentFactory.jsonBuilder().startObject(); 
        XContentBuilder xbSummary = XContentFactory.jsonBuilder().startObject(); 
        
        for (Aggregation agg : sResponse.getAggregations()) {
            String name = agg.getName();
            if (name.equals("fm_date")) continue;
            if (name.equals("mclass")) continue;
            if (name.equals("hinput")) {
              Terms hinput = sResponse.getAggregations().get(name);
              if (hinput!=null) {
                xbSummary.startObject("hinput").startArray("entries");
                for ( Terms.Bucket bucket : hinput.getBuckets()){
                  Long keyl = (Long)(bucket.getKey());
                  long key = keyl.longValue();
                  Long doc_count = bucket.getDocCount();            
                  xbSummary.startObject();
                  xbSummary.field("key",key);
                  xbSummary.field("count",doc_count);
                  xbSummary.endObject();
                }
                xbSummary.endArray();
                xbSummary.endObject();
              }
              continue;
            }
            String namev = name + "v"; //alternate name for non-nested docs
            Boolean doSummary = false;
            Boolean doSummaryOutputs = false;
            if (name.equals("hmicro") && ustatesReserved>=0) {
              doSummary = true;
              if (ustatesOutput>=0)
                doSummaryOutputs=true;
            }
            Long total = 0L;
            Long totalBusy = 0L;
            Long totalOutputs = 0L;
            xb.startObject(namev).startArray("entries"); 
            if (doSummary)
                xbSummary.startObject(name).startArray("entries");
            Terms hist = sResponse.getAggregations().get(name);
            for ( Terms.Bucket bucket : hist.getBuckets() ){
                Long keyl = (Long)(bucket.getKey());
                long key = keyl.longValue();
                Long doc_count = bucket.getDocCount();            
                total =  total + doc_count;
                xb.startObject();
                xb.field("key",key);
                xb.field("count",doc_count);
                xb.endObject();
                if (doSummary) {
                  if (key < ustatesReserved) {
                    if (doSummaryOutputs && key >= ustatesSpecial)
                      totalOutputs+=doc_count; 
                    else {
                      xbSummary.startObject();
                      xbSummary.field("key",key);
                      xbSummary.field("count",doc_count);
                      xbSummary.endObject();
                    }
                  }
                  else
                    totalBusy += doc_count;
                }
            }
            xb.endArray();
            xb.field("total",total);
            xb.endObject();
           
            if (doSummaryOutputs) { 
              xbSummary.startObject();
              Number outputKey = ustatesSpecial;
              xbSummary.field("key",outputKey);
              xbSummary.field("count",totalOutputs);
              xbSummary.endObject();
            }

            if (doSummary) { 
              xbSummary.startObject();
              xbSummary.field("key",ustatesReserved);
              xbSummary.field("count",totalBusy);
              xbSummary.endObject();
              xbSummary.endArray();
              xbSummary.field("total",total);
              xbSummary.endObject();
            }
 
        }
        long start_time_millis = System.currentTimeMillis();
        xb.field("fm_date",fm_date.longValue());
        xb.field("date",start_time_millis);
        xb.field("runNumber",runNumber);
        xb.field("doc_type","state-hist");
        xb.startObject("runRelation").field("name","state-hist").field("parent",Integer.parseInt(runNumber)).endObject();
        xb.endObject();


        IndexRequest indexReqXb = new IndexRequest(runindex_write)
          .routing(runNumber)
          .source(xb);
 
        bulkRequest.add(indexReqXb);

        xbSummary.field("fm_date",fm_date.longValue());
        xbSummary.field("date",start_time_millis);
        xbSummary.field("runNumber",runNumber);
        xbSummary.field("doc_type","state-hist-summary");
        xbSummary.startObject("runRelation").field("name","state-hist-summary").field("parent",Integer.parseInt(runNumber)).endObject();
        xbSummary.endObject();

        IndexRequest indexReqXbSum = new IndexRequest(runindex_write)
          .routing(runNumber)
          .source(xbSummary);

        bulkRequest.add(indexReqXbSum);
 
        //class summary
        Terms cpuclasses = sResponse.getAggregations().get("mclass");
        try {
          for (Terms.Bucket cpuclass : cpuclasses.getBuckets()) {
            String[] cpuinfo = ((String)cpuclass.getKey()).split("_");
            Long total = 0L;
            Long totalBusy = 0L;
            Long totalOutputs = 0L;
            XContentBuilder xbClassSummary = XContentFactory.jsonBuilder().startObject(); 
            Terms hmicro = cpuclass.getAggregations().get("hmicro");
            xbClassSummary.startObject("hmicro").startArray("entries");
            for ( Terms.Bucket bucket : hmicro.getBuckets()){
              Long keyl = (Long)(bucket.getKey());
              long key = keyl.longValue();
              Long doc_count = bucket.getDocCount();            
              total =  total + doc_count;
              if (key < ustatesReserved) {
                if (key >= ustatesSpecial)
                  totalOutputs+=doc_count; 
                else {
                  xbClassSummary.startObject();
                  xbClassSummary.field("key",key);
                  xbClassSummary.field("count",doc_count);
                  xbClassSummary.endObject();
                }
              }
              else
                totalBusy += doc_count;
            }
           
            xbClassSummary.startObject();
            Number outputKey = ustatesSpecial;
            xbClassSummary.field("key",outputKey);
            xbClassSummary.field("count",totalOutputs);
            xbClassSummary.endObject();

            xbClassSummary.startObject();
            xbClassSummary.field("key",ustatesReserved);
            xbClassSummary.field("count",totalBusy);
            xbClassSummary.endObject();
            xbClassSummary.endArray();
            xbClassSummary.field("total",total);
            xbClassSummary.endObject();

            Terms hinput = cpuclass.getAggregations().get("hinput");
            if (hinput!=null) {
              xbClassSummary.startObject("hinput").startArray("entries");
              for ( Terms.Bucket bucket : hinput.getBuckets()){
                Long keyl = (Long)(bucket.getKey());
                long key = keyl.longValue();
                Long doc_count = bucket.getDocCount();            
                xbClassSummary.startObject();
                xbClassSummary.field("key",key);
                xbClassSummary.field("count",doc_count);
                xbClassSummary.endObject();
              }
              xbClassSummary.endArray();
              xbClassSummary.endObject();
            }

            xbClassSummary.field("fm_date",fm_date.longValue());
            xbClassSummary.field("date",start_time_millis);
            xbClassSummary.field("cpuslotsmax",cpuinfo[0]);
            xbClassSummary.field("cpuslots",cpuinfo[1]);
            try {
              xbClassSummary.field("cpuid",cpuinfo[2]);
            } catch (Exception e) {
              //missing last token, no cpuid will be stored in the document
            }

            xbClassSummary.field("runNumber",runNumber);
            xbClassSummary.field("doc_type","state-hist-summary");
            xbClassSummary.startObject("runRelation").field("name","state-hist-summary").field("parent",Integer.parseInt(runNumber)).endObject();

            xbClassSummary.endObject();

            IndexRequest indexReqXbClassSum = new IndexRequest(runindex_write)
              .routing(runNumber)
              .source(xbClassSummary);

            bulkRequest.add(indexReqXbClassSum);
          }

          //bulk inject all docs
          BulkResponse iResponse = client.bulk(bulkRequest,RequestOptions.DEFAULT);

          if (iResponse.hasFailures()) {
            for (BulkItemResponse bulkItemResponse : iResponse) {

               BulkItemResponse.Failure failure = bulkItemResponse.getFailure();
               logger.error("states bulk index failure:",failure);
            }
          }

        } catch (Exception e) {
           logger.error("Error getting process microstate info: ", e);
        }

//        DO NOT DELETE. SNIPPET FOR RESPONSE TO JSON CONVERSION
//        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
//        builder.startObject();
//        sResponse.toXContent(builder, ToXContent.EMPTY_PARAMS);
//        builder.endObject();
//        logger.info("states results: "+builder.string());

    }

    public void checkRunEnd() throws IOException {
        if (EoR){return;}
        GetRequest getRequest = new GetRequest(runindex_write,runNumber).refresh(true);
        GetResponse response = client.get(getRequest, RequestOptions.DEFAULT);
        if (!response.isExists()){
            //no run document. Set EoR.
            logger.error("Run document not found. Assuming end of run condition.");
            EoR = true;
            return;
        }
        if (response.getSource().get("endTime") != null) { 
            Integer activeBUs = (Integer)response.getSource().get("activeBUs");
            if (activeBUs==null) { logger.info("EoR received!"); EoR = true; }
            else {
              //Integer activeBUs = (Integer)activeBUsObj;
              if (activeBUs<=0) { logger.info("EoR received!"); EoR = true; }
              else logger.info("EOR received, but there are still active BUs:" + Integer.toString(activeBUs));
            }
        }
    }

    public void checkBoxInfo() throws IOException {
        if (!EoR){return;}

        SearchSourceBuilder sBuilder = new SearchSourceBuilder()
          .query(QueryBuilders.boolQuery()
            .must(QueryBuilders.rangeQuery("fm_date").from("now-1m"))
            .must(QueryBuilders.termQuery("doc_type","boxinfo"))
            .must(QueryBuilders.termQuery("activeRuns",runNumber)))
          .size(1);

        SearchRequest sRequest = new SearchRequest().indices(boxinfo_read).source(sBuilder);
        SearchResponse response = client.search(sRequest,RequestOptions.DEFAULT);

        collectStats(riverName,"boxinfoQuery",boxinfo_read,response);
        
        logger.info("Boxinfo: "+ String.valueOf(response.getHits().getTotalHits()));
        if (response.getHits().getTotalHits().value == 0 ) {
          //execRunClose();
          shouldCloseRun = true;
          setRunning(false);
        }
    }

    public void setRemoteClient() throws UnknownHostException {
        remoteClient =  new RestHighLevelClient(
          RestClient.builder(
            new HttpHost(es_local_host,9200,"http"))
        );
    }

    /*
    public void resetRemoteClient() {
        client =  new RestHighLevelClient(
          RestClient.build(
            new HttpHost(es_local_host,9200,"http"))
        );
    }
    */

    public void execRunClose() throws IOException {
        if (!closeIndices) {
          logger.info("closing indices is disabled");
          return;
        }
        logger.info("closing index for run "+runNumber.toString() 
                      + " (index " + "run" + runNumber.toString()
                      + "_" + subsystem +" on " + es_local_host + " - " + es_local_cluster + ")");

	//close index using JAVA REST API
        //
        CloseIndexRequest closeRequest = new CloseIndexRequest("run"+runNumber.toString()+"_"+subsystem);
        try {
          if (remoteClient.indices().close(closeRequest, RequestOptions.DEFAULT).isAcknowledged())
            logger.info("executed index close for run "+runNumber.toString());
          else {
            logger.error("index close for run "+runNumber.toString() + " failed ");
            logger.info("reconnect before retrying index close ");
            //was reconnecting with transport client, but now there is no state on client side
            //remoteClient.close();
            try {
              Thread.sleep(2000);
            }
            catch (InterruptedException iex) {}
            //resetRemoteClient();

            logger.info("closing index for run "+runNumber.toString() 
                        + " (index " + "run" + runNumber.toString()
                        + "_" + subsystem +" on " + es_local_host + " - " + es_local_cluster + ")");
            if (client.indices().close(closeRequest, RequestOptions.DEFAULT).isAcknowledged())
              logger.info("executed index close for run "+runNumber.toString());
            else
              logger.error("index close for run "+runNumber.toString() + " failed ");
           }
        } catch (ElasticsearchException esx) {
          if (esx.status() == RestStatus.NOT_FOUND) {
            logger.info("index does not exist, closing skipped");
            return;
          }
          throw esx;
        }
        return;
    }
}

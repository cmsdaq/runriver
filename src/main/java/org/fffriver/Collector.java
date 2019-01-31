package org.fffriver;

//JAVA
import java.io.IOException;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.lang.Math;

//ELASTICSEARCH
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.common.inject.Inject;
//import org.elasticsearch.index.get.GetField;


import org.elasticsearch.index.query.QueryBuilders;
//import org.elasticsearch.index.query.TermQueryBuilders;
import org.elasticsearch.join.query.JoinQueryBuilders;
//import org.elasticsearch.join.query.ParentIdQueryBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.*;

//Remote query stuff
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
//import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
//import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetSocketAddress;
//#import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;
import org.elasticsearch.search.aggregations.metrics.max.Max;
import org.elasticsearch.search.aggregations.Aggregation;
//import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy;

//import org.elasticsearch.search.builder.SearchSourceBuilder;
//import org.elasticsearch.common.ParseFieldMatcher;
//import org.elasticsearch.index.query.QueryParseContext;
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

    Map<String, Long> known_streams = new HashMap<String, Long>();
    Map<String, Map<String, Double>> fuinlshist = new HashMap<String, Map<String, Double>>();
    Map<String, Map<String, Double>> fuoutlshist = new HashMap<String, Map<String, Double>>();
    Map<String, Map<String, Double>> fuerrlshist = new HashMap<String, Map<String, Double>>();
    Map<String, Map<String, Double>> fufilesizehist = new HashMap<String, Map<String, Double>>();
    Map<String, Map<String, Double>> futimestamplshist = new HashMap<String, Map<String, Double>>();
    Map<String, Map<String, String>> fumergetypelshist = new HashMap<String, Map<String, String>>();
    Map<String, Double> eolEventsList = new HashMap<String, Double>();
    Map<String, Double> eolLostEventsList = new HashMap<String, Double>();
    Map<String, Double> eolTotalEventsList = new HashMap<String, Double>();

    Map<Integer, Map<String,Double>> incompleteLumis = new HashMap<Integer, Map<String,Double>>();

    InetSocketAddress remoteAddress;
    Client remoteClient;
    Boolean EoR=false;
    String eslocalIndex;
    String runStrPrefix;
    Boolean firstTime=true;
    Boolean shouldCloseRun = false;
    Integer ustatesReserved=-1;
    Integer ustatesSpecial=-1;
    Integer ustatesOutput=-1;

    //queries
    //JSONObject streamQuery;
    //JSONObject eolsQuery;
    //JSONObject statesQuery;
    //JSONObject boxinfoQuery;
        
    @Inject
    public Collector(String riverName, Map<String, Object> rSettings,Client client) {
        super(riverName,rSettings,client);
    }

    @Override
    public void beforeLoop() throws UnknownHostException {
        logger.info("Collector started.");
        this.interval = fetching_interval;
        this.eslocalIndex = "run"+String.format("%06d", Integer.parseInt(runNumber))+"*";
        if (runNumber.length()<6)
            runStrPrefix = "run" + "000000".substring(runNumber.length()) + runNumber + "_ls";
        else
            runStrPrefix = "run" + runNumber + "_ls";

        setRemoteClient();
        getQueries();

    }
    @Override
    public void afterLoop() throws Exception {
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
    public void mainLoop() throws Exception {
        logger.info("Collector running...");
        collectStates();
        collectStreams();
        checkRunEnd();
        checkBoxInfo();
    }

    public void collectStreams() throws Exception {
        logger.info("collectStreams");
        Boolean dataChanged;
        //long highestLumisection=-1;
        long lowestLumisection=-1;
        
        int size=0;
        if(firstTime){
            //streamQuery.getJSONObject("aggs").getJSONObject("streams")
            //    .getJSONObject("aggs").getJSONObject("ls").getJSONObject("terms")
            //    .put("size",1000000);
            firstTime = false;
            size=1000000;
        }else{
            //streamQuery.getJSONObject("aggs").getJSONObject("streams")
            //    .getJSONObject("aggs").getJSONObject("ls").getJSONObject("terms")
            //    .put("size",30);
            size=30;
        }
        //logger.info("streamquery: "+streamQuery.toString());
        //
        //String restContent = streamQuery.toString();
        //XContentParser parser = XContentFactory.xContent(restContent).createParser(restContent);
        //SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.fromXContent(
        //                                                                           //createParseContext(parser),
        //                                                                           new QueryParseContext(searchRequestParsers.queryParsers, parser, ParseFieldMatcher.STRICT),
        //                                                                           searchRequestParsers.aggParsers,
        //                                                                           searchRequestParsers.suggesters,
        //                                                                           searchRequestParsers.searchExtParsers);
        //SearchSourceBuilder ssBuilder = new SearchSourceBuilder();

        //.setSource(streamQuery)
        SearchResponse sResponse = remoteClient.prepareSearch(eslocalIndex)
                                               //.setTypes("fu-out")
                                               .setTypes("doc")
                                               .setQuery(QueryBuilders.termQuery("doc_type","fu-out"))
                                               .setSize(0)
                                               .addSort(SortBuilders.fieldSort("ls").order(SortOrder.DESC))
                                               .addAggregation(AggregationBuilders.terms("streams").field("stream").size(200)
                                                 .subAggregation(AggregationBuilders.terms("ls").field("ls").size(size).order(BucketOrder.key(false))
                                                   .subAggregation(AggregationBuilders.sum("in").field("data.in"))
                                                   .subAggregation(AggregationBuilders.sum("out").field("data.out"))
                                                   .subAggregation(AggregationBuilders.sum("error").field("data.errorEvents"))
                                                   .subAggregation(AggregationBuilders.sum("filesize").field("data.fileSize"))
                                                   .subAggregation(AggregationBuilders.max("fm_date").field("fm_date"))
                                                   .subAggregation(AggregationBuilders.terms("mergeType").field("data.MergeType").size(2).order(BucketOrder.key(false)))
                                                 )
                                               )
        .execute().actionGet();
        collectStats(riverName,"streamQuery",eslocalIndex,sResponse);
        //logger.info(String.valueOf(sResponse.getHits().getTotalHits()));
        if(sResponse.getHits().getTotalHits() == 0L){ 
            logger.info("streamQuery returns 0 hits");
            return;
        }

        if(sResponse.getAggregations().asList().isEmpty()){return;}
        
        Terms streams = sResponse.getAggregations().get("streams");
        
        if(streams.getBuckets().isEmpty()){return;}
        for (Terms.Bucket stream : streams.getBuckets()){
            String streamName = stream.getKeyAsString();

            known_streams.put(streamName,stream.getDocCount());
       
            if (fumergetypelshist.get(streamName)==null) {
              fuinlshist.put(streamName, new HashMap<String, Double>());
              fuoutlshist.put(streamName, new HashMap<String, Double>());
              fuerrlshist.put(streamName, new HashMap<String, Double>());
              fufilesizehist.put(streamName, new HashMap<String, Double>());
              futimestamplshist.put(streamName, new HashMap<String, Double>());
              fumergetypelshist.put(streamName, new HashMap<String, String>());
            }

            Terms lss = stream.getAggregations().get("ls");
           
            for (Terms.Bucket ls : lss.getBuckets()) {

                String lsName = ls.getKeyAsString();

                //update last LS found in the query
                Long lsvalLong = (Long)(ls.getKey());
                long lsval = lsvalLong.longValue();
                //if (lsval>highestLumisection) highestLumisection=lsval;
                if (lsval<lowestLumisection || lowestLumisection<0) lowestLumisection=lsval;

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
                fuinlshist.get(streamName).put(lsName,inSum.getValue());
                fuoutlshist.get(streamName).put(lsName,outSum.getValue());
                fuerrlshist.get(streamName).put(lsName,errSum.getValue());
                fufilesizehist.get(streamName).put(lsName,filesizeSum.getValue());
                futimestamplshist.get(streamName).put(lsName,fm_date.getValue());
                fumergetypelshist.get(streamName).put(lsName,mergeType);
  
            } 
        }

        for (String stream : known_streams.keySet()){
            List<String> removeList = new ArrayList<String>();

            for (String ls : fuoutlshist.get(stream).keySet()){
       
                //EoLS aggregation for this LS
                Double eventsVal=0.0;
                Double lostEventsVal=0.0;
                Double totalEventsVal=0.0;
                Boolean run_eols_query = true;
                if (eolEventsList.get(ls) != null && eolLostEventsList.get(ls)!=null && eolTotalEventsList.get(ls)!=null) {

                  eventsVal = eolEventsList.get(ls);
                  lostEventsVal = eolLostEventsList.get(ls);
                  totalEventsVal = eolTotalEventsList.get(ls);

                  //require match, otherwise BU json files might not be written yet
                  if (Math.round(totalEventsVal-eventsVal-lostEventsVal)==0) run_eols_query=false;
                }
                Integer ls_num = Integer.parseInt(ls); 
                //drop lumis more than 70 LS behind oldest one in the query range (unless EvB information is inconsistent)
                if (ls_num+70<lowestLumisection && run_eols_query==false) {
                  //this is old lumisection, dropping from object maps (even if incomplete)
                  removeList.add(ls);
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

                    SearchResponse sResponseEoLS = client.prepareSearch(runindex_write)
                                                         //.setTypes("eols")
                                                         .setTypes("doc")
                                                         .setRouting(runNumber)
                                                         .setQuery(QueryBuilders.boolQuery()
                                                           .must(JoinQueryBuilders.parentId("eols",runNumber))
                                                           .must(QueryBuilders.termQuery("ls",ls))
                                                         )
                                                         //.setQuery(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("doc_type","eols")).must(JoinQueryBuilders.hasParentQuery("run",QueryBuilders.termQuery("_id",runNumber),false)).must(QueryBuilders.termQuery("ls",ls)))
                                                         //.setQuery(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("doc_type","eols")).must(JoinQueryBuilders.hasParentQuery("run",QueryBuilders.termQuery("_id",runNumber),false)).must(QueryBuilders.termQuery("ls",ls)))
                                                         .setSize(0)
                                                         .addAggregation(AggregationBuilders.sum("NEvents").field("NEvents"))
                                                         .addAggregation(AggregationBuilders.sum("NLostEvents").field("NLostEvents"))
                                                         .addAggregation(AggregationBuilders.max("TotalEvents").field("TotalEvents"))
                                                         .execute().actionGet();
 


                    if(sResponseEoLS.getHits().getTotalHits() == 0L){ 
                        logger.info("eolsQuery returns 0 hits for LS " + ls + ", skipping collection without EoLS docs");
                        continue;
                    }
                    logger.info("eolsQuery returned " + String.valueOf(sResponseEoLS.getHits().getTotalHits()) + " hits for LS "+ ls);
                    //else logger.info(String.valueOf(sResponseEoLS.getHits().getTotalHits()));
                    Sum eolEvents = sResponseEoLS.getAggregations().get("NEvents");
                    Sum eolLostEvents = sResponseEoLS.getAggregations().get("NLostEvents");
                    Max eolTotalEvents = sResponseEoLS.getAggregations().get("TotalEvents");
                    eventsVal = eolEvents.getValue();    
                    lostEventsVal = eolLostEvents.getValue();
                    totalEventsVal = eolTotalEvents.getValue();
                    eolEventsList.put(ls,eventsVal); 
                    eolLostEventsList.put(ls,lostEventsVal); 
                    eolTotalEventsList.put(ls,totalEventsVal); 
                    if (Math.round(totalEventsVal-eventsVal-lostEventsVal)==0) {
                        run_eols_query=false;
                        logger.info("eolsQuery match for LS "+ ls + " with total of "+totalEventsVal.toString()+" events");
                    }
                }
                //continue with stream aggregation

                String id = String.format("%06d", Integer.parseInt(runNumber))+"_"+stream+"_"+ls;

                //Check if data is changed (to avoid to update timestamp if not necessary)
                GetResponse sresponse = client.prepareGet(runindex_write, "stream-hist", id)
                                            .setRouting(runNumber)
                                            .setRefresh(true).execute().actionGet();

                dataChanged = true;
                if (sresponse.isExists()){ 
                    Double in = Double.parseDouble(sresponse.getSource().get("in").toString());
                    Double out = Double.parseDouble(sresponse.getSource().get("out").toString());
                    //new field, allow to be missing if log entry updated by the new plugin version
                    Double error = -1.;
                    if (sresponse.getSource().get("err").toString()!=null) {
                      error = Double.parseDouble(sresponse.getSource().get("err").toString());
                    }
                    Double lastCompletion = 0.;
                    if (sresponse.getSource().get("completion")!=null) {
                      lastCompletion = Double.parseDouble(sresponse.getSource().get("completion").toString());
                    }
                    //logger.info("old" + in.toString() + " " + out.toString() + " " + error.toString() + " compl "+lastCompletion.toString());
                    //logger.info("new" + fuinlshist.get(stream).get(ls).toString() + " " + fuoutlshist.get(stream).get(ls).toString() + " " + fuerrlshist.get(stream).get(ls).toString() );
                    if (lastCompletion==1.0
                        && in.compareTo(fuinlshist.get(stream).get(ls))==0 
                        && out.compareTo(fuoutlshist.get(stream).get(ls))==0
                        && error.compareTo(fuerrlshist.get(stream).get(ls))==0){
                        dataChanged = false;
                    } else { logger.info(id+" with completion " + lastCompletion.toString() + " already exists and will be updated."); }


                }

                Double newCompletion = 1.;
                //calculate completion as (Nprocessed / Nbuilt) * (Nbuilt+Nlost)/Ntriggered
                Double output_count = fuinlshist.get(stream).get(ls) + fuerrlshist.get(stream).get(ls);
                //logger.info("output_count:" + output_count.toString() + " eventsVal:"+eventsVal.toString());
                
                if (eventsVal>0 && output_count!=eventsVal)
                    newCompletion = output_count/eventsVal;

                if (eventsVal +  lostEventsVal != totalEventsVal) {
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
                if (dataChanged){
                  logger.info("stream-hist update for ls,stream: "+ls+","+stream+" in:"+fuinlshist.get(stream).get(ls).toString()
                              +" out:"+fuoutlshist.get(stream).get(ls).toString()+" err:"+fuerrlshist.get(stream).get(ls).toString() + " completion " + newCompletion.toString());
                  logger.info("Totals numbers - eventsVal:"+eventsVal.toString() + " lostEventsVal:" + lostEventsVal.toString() + " totalEventsVal:" + totalEventsVal.toString());
                  Double retDate = futimestamplshist.get(stream).get(ls);
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
                  if (retDate >  Double.NEGATIVE_INFINITY) {
                    IndexResponse iResponse = client.prepareIndex(runindex_write, "doc").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                    .setRouting(runNumber)
                    .setId(id)
                    .setSource(jsonBuilder()
                        .startObject()
                        .startObject("runRelation").field("name","stream-hist").field("parent",Integer.parseInt(runNumber)).endObject()
                        .field("doc_type","stream-hist")
                        .field("runNumber",runNumber)
                        .field("stream", stream)
                        .field("ls", ls_num)
                        .field("in", fuinlshist.get(stream).get(ls))
                        .field("out", fuoutlshist.get(stream).get(ls))
                        .field("err", fuerrlshist.get(stream).get(ls))
                        .field("filesize", fufilesizehist.get(stream).get(ls))
                        .field("mergeType", fumergetypelshist.get(stream).get(ls))
                        .field("fm_date", retDate.longValue()) //convert when injecting into futimestamplshist?
                        .field("date",  System.currentTimeMillis())
                        .field("completion", newCompletion)
                        .endObject())
                    .execute()
                    .actionGet();
                  }
                  else {
                    //if no date, create indexing date explicitely
                    long start_time_millis = System.currentTimeMillis();
                    IndexResponse iResponse = client.prepareIndex(runindex_write, "doc").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                    .setRouting(runNumber)
                    .setId(id)
                    .setSource(jsonBuilder()
                        .startObject()
                        .startObject("runRelation").field("name","stream-hist").field("parent",Integer.parseInt(runNumber)).endObject()
                        .field("doc_type","stream-hist")
                        .field("runNumber",runNumber)
                        .field("stream", stream)
                        .field("ls", Integer.parseInt(ls))
                        .field("in", fuinlshist.get(stream).get(ls))
                        .field("out", fuoutlshist.get(stream).get(ls))
                        .field("err", fuerrlshist.get(stream).get(ls))
                        .field("filesize", fufilesizehist.get(stream).get(ls))
                        .field("mergeType", fumergetypelshist.get(stream).get(ls))
                        .field("date",start_time_millis)
                        .field("fm_date",start_time_millis)
                        .field("completion", newCompletion)
                        .endObject())
                    .execute()
                    .actionGet();    
                  }
                }
                //drop complete lumis older than query range (keep checking if EvB information is inconsistent)
                if (newCompletion==1 && ls_num<lowestLumisection && run_eols_query==false) {
                  //complete,dropping from maps
                  removeList.add(ls);
                }
            }
            for (String ls : removeList) {
              logger.info("removing stream" + stream + " Map entries for LS "+ ls);
              fuinlshist.get(stream).remove(ls);
              fuoutlshist.get(stream).remove(ls);
              fuerrlshist.get(stream).remove(ls);
              fufilesizehist.get(stream).remove(ls);
              futimestamplshist.get(stream).remove(ls);
              fumergetypelshist.get(stream).remove(ls);
            }
        }
    }
        
    public void collectStates() throws Exception {
        logger.info("collectStates");

        //logger.info("states query: "+statesQuery.toString());

        SearchResponse sResponse = remoteClient.prepareSearch(eslocalIndex)
                                               //.setTypes("prc-i-state")
                                               .setTypes("doc")
                                               .setQuery(QueryBuilders.boolQuery()
                                                 .must(QueryBuilders.termQuery("doc_type","prc-i-state"))
                                                 .must(QueryBuilders.rangeQuery("fm_date").from("now-4s").to("now-2s"))
                                               )
                                               .setSize(0)
                                               .addAggregation(AggregationBuilders.terms("mclass").field("mclass").size(9999).order(BucketOrder.key(true)).minDocCount(1)
                                                 .subAggregation(AggregationBuilders.terms("hmicro").field("micro").size(9999).order(BucketOrder.key(true)).minDocCount(1))
                                                 .subAggregation(AggregationBuilders.terms("hinput").field("instate").size(9999).order(BucketOrder.key(true)).minDocCount(1))
                                               )
                                               .addAggregation(AggregationBuilders.terms("hmicro").field("micro").size(9999).order(BucketOrder.key(true)).minDocCount(1))
                                               .addAggregation(AggregationBuilders.terms("hmini").field("mini").size(9999).order(BucketOrder.key(true)).minDocCount(1))
                                               .addAggregation(AggregationBuilders.terms("hmacro").field("macro").size(9999).order(BucketOrder.key(true)).minDocCount(1))
                                               .addAggregation(AggregationBuilders.terms("hinput").field("instate").size(9999).order(BucketOrder.key(true)).minDocCount(1))
                                               .addAggregation(AggregationBuilders.max("fm_date").field("fm_date"))
                                               .execute().actionGet();

        
        collectStats(riverName,"statesQuery",eslocalIndex,sResponse);
        
        //logger.info(String.valueOf(sResponse.getHits().getTotalHits()));
        if(sResponse.getHits().getTotalHits() == 0L){ 
            logger.info("statesQuery returns 0 hits");
            return;
        }
        if (ustatesReserved==-1) {
          SearchResponse sResponseUstates = client.prepareSearch(runindex_read)
          //.setTypes("microstatelegend")
          .setTypes("doc")
          .setRouting(runNumber)
          //.setQuery(QueryBuilders.boolQuery()
          .setQuery(JoinQueryBuilders.parentId("microstatelegend",runNumber))
            //.must(QueryBuilders.termQuery("doc_type","microstatelegend"))
            //.must(JoinQueryBuilders.hasParentQuery("run", QueryBuilders.termQuery("_id",runNumber),false))
          .setSize(1).execute().actionGet();

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
        client.prepareIndex(runindex_write, "doc")
            .setRouting(runNumber)
            .setSource(xb)
            .execute();

        xbSummary.field("fm_date",fm_date.longValue());
        xbSummary.field("date",start_time_millis);
        xbSummary.field("runNumber",runNumber);
        xbSummary.field("doc_type","state-hist-summary");
        xbSummary.startObject("runRelation").field("name","state-hist-summary").field("parent",Integer.parseInt(runNumber)).endObject();
        xbSummary.endObject();
        client.prepareIndex(runindex_write, "doc")
            .setRouting(runNumber)
            .setSource(xbSummary)
            .execute();                  

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
            client.prepareIndex(runindex_write, "doc")
              .setRouting(runNumber)
              .setSource(xbClassSummary)
              .execute();                  
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

    public void checkRunEnd(){
        if (EoR){return;}
        GetResponse response = client.prepareGet(runindex_write, "run", runNumber).setRefresh(true).execute().actionGet();
        if (!response.isExists()){return;}
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

    public void checkBoxInfo(){
        if (!EoR){return;}
        //boxinfoQuery.getJSONObject("filter").getJSONObject("term")
        //        .put("activeRuns",runNumber);
        SearchResponse response = client.prepareSearch(boxinfo_read)
                                        .setQuery(QueryBuilders.boolQuery().must(QueryBuilders.rangeQuery("fm_date").from("now-1m"))
                                                                           .must(QueryBuilders.termQuery("activeRuns",runNumber)))
                                        .setSize(1)
                                        .execute().actionGet();

        //SearchResponse response = client.prepareSearch(boxinfo_read).setSource(boxinfoQuery)
        //    .execute().actionGet();
        
        collectStats(riverName,"boxinfoQuery",boxinfo_read,response);
        
        logger.info("Boxinfo: "+ String.valueOf(response.getHits().getTotalHits()));
        if (response.getHits().getTotalHits() == 0 ) {
          //execRunClose();
          shouldCloseRun = true;
          setRunning(false);
        }
    }

    public void setRemoteClient() throws UnknownHostException{
        //Settings settings = Settings.settingsBuilder()
        //Settings.Builder settingsBuilder = Settings.builder();
        //Settings settings = settingsBuilder
        Settings settings = Settings.builder()
            .put("cluster.name", es_local_cluster).build();
        //remoteClient = TransportClient.builder().settings(settings).build()
        remoteAddress = new InetSocketAddress(InetAddress.getByName(es_local_host), 9300);
        remoteClient = new PreBuiltTransportClient(settings)
            .addTransportAddress(new TransportAddress(remoteAddress));
    }

    public void resetRemoteClient() {
        Settings settings = Settings.builder()
            .put("cluster.name", es_local_cluster).build();
        remoteClient = new PreBuiltTransportClient(settings)
            .addTransportAddress(new TransportAddress(remoteAddress));
    }

    public void getQueries(){
        try{
            //streamQuery = getJson("streamQuery");
            //eolsQuery = getJson("eolsQuery");
            //statesQuery = getJson("statesQuery");
            //boxinfoQuery = getJson("boxinfoQuery");
        } catch (Exception e) {
           logger.error("Collector getQueries Exception: ", e);
        }
    }

    public void execRunClose(){
        if (!closeIndices) {
          logger.info("closing indices is disabled");
          return;
        }
        logger.info("closing index for run "+runNumber.toString() 
                      + " (index " + "run" + runNumber.toString()
                      + "_" + subsystem +" on " + es_local_host + " - " + es_local_cluster + ")");

	//close index using JAVA API

        if (remoteClient.admin().indices().close(Requests.closeIndexRequest("run"+runNumber.toString()+"_"+subsystem)).actionGet().isAcknowledged())
          logger.info("executed index close for run "+runNumber.toString());
        else {
          logger.error("index close for run "+runNumber.toString() + " failed ");
          logger.info("reconnect before retrying index close ");
          //reconnect before retry closing indices
          remoteClient.close();
          try {
            Thread.sleep(2000);
          }
          catch (InterruptedException iex) {}
          resetRemoteClient();
          logger.info("closing index for run "+runNumber.toString() 
                      + " (index " + "run" + runNumber.toString()
                      + "_" + subsystem +" on " + es_local_host + " - " + es_local_cluster + ")");
          if (remoteClient.admin().indices().close(Requests.closeIndexRequest("run"+runNumber.toString()+"_"+subsystem)).actionGet().isAcknowledged())
            logger.info("executed index close for run "+runNumber.toString());
          else
            logger.error("index close for run "+runNumber.toString() + " failed ");
        }
        return;
    }
}

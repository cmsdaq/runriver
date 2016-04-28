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
import org.elasticsearch.index.get.GetField;


import org.elasticsearch.index.query.QueryBuilders;
import static org.elasticsearch.common.xcontent.XContentFactory.*;

//Remote query stuff
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;
import org.elasticsearch.search.aggregations.metrics.max.Max;
import org.elasticsearch.search.aggregations.Aggregation;
//import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

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
    Map<String, Double> eolEventsList = new HashMap<String, Double>();
    Map<String, Double> eolLostEventsList = new HashMap<String, Double>();
    Map<String, Double> eolTotalEventsList = new HashMap<String, Double>();

    Client remoteClient;
    Boolean EoR=false;
    String tribeIndex;
    String runStrPrefix;
    Boolean firstTime=true;
    Integer ustatesReserved=-1;
    Integer ustatesSpecial=-1;
    Integer ustatesOutput=-1;

    //queries
    JSONObject streamQuery;
    JSONObject eolsQuery;
    JSONObject statesQuery;
    JSONObject boxinfoQuery;
        
    @Inject
    public Collector(String riverName, Map<String, Object> rSettings,Client client) {
        super(riverName,rSettings,client);
    }

    @Override
    public void beforeLoop() throws UnknownHostException {
        logger.info("Collector started.");
        this.interval = fetching_interval;
        this.tribeIndex = "run"+String.format("%06d", Integer.parseInt(runNumber))+"*";
        if (runNumber.length()<6)
            runStrPrefix = "run" + "000000".substring(runNumber.length()) + runNumber + "_ls";
        else
            runStrPrefix = "run" + runNumber + "_ls";

        setRemoteClient();
        getQueries();

    }
    @Override
    public void afterLoop(){
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
        
        if(firstTime){
            streamQuery.getJSONObject("aggs").getJSONObject("streams")
                .getJSONObject("aggs").getJSONObject("ls").getJSONObject("terms")
                .put("size",1000000);
            firstTime = false;
        }else{
            streamQuery.getJSONObject("aggs").getJSONObject("streams")
                .getJSONObject("aggs").getJSONObject("ls").getJSONObject("terms")
                .put("size",30);
        }
        //logger.info("streamquery: "+streamQuery.toString());
        SearchResponse sResponse = remoteClient.prepareSearch(tribeIndex).setTypes("fu-out")
            .setSource(streamQuery).execute().actionGet();
        collectStats(riverName,"streamQuery",tribeIndex,sResponse);
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
        
            fuinlshist.put(streamName, new HashMap<String, Double>());
            fuoutlshist.put(streamName, new HashMap<String, Double>());
            fuerrlshist.put(streamName, new HashMap<String, Double>());
            fufilesizehist.put(streamName, new HashMap<String, Double>());
            futimestamplshist.put(streamName, new HashMap<String, Double>());

            Terms lss = stream.getAggregations().get("ls");
           
            for (Terms.Bucket ls : lss.getBuckets()) {

                String lsName = ls.getKeyAsString();
                
                Sum inSum = ls.getAggregations().get("in");
                Sum outSum = ls.getAggregations().get("out");
                Sum errSum = ls.getAggregations().get("error");
                Sum filesizeSum = ls.getAggregations().get("filesize");
                Max fm_date = ls.getAggregations().get("fm_date");

                fuinlshist.get(streamName).put(lsName,inSum.getValue());
                fuoutlshist.get(streamName).put(lsName,outSum.getValue());
                fuerrlshist.get(streamName).put(lsName,errSum.getValue());
                fufilesizehist.get(streamName).put(lsName,filesizeSum.getValue());
                futimestamplshist.get(streamName).put(lsName,fm_date.getValue());
  
            } 
        }

        for (String stream : known_streams.keySet()){
            for (String ls : fuoutlshist.get(stream).keySet()){
        
                //EoLS aggregation for this LS
                //GetResponse sresponseEoL = client.prepareGet(runIndex_write, "eols", id)
                //                                .setRouting(runNumber)
                //                                .setRefresh(true).execute().actionGet();
                Double eventsVal=0.0;
                Double lostEventsVal=0.0;
                Double totalEventsVal=0.0;
                Boolean run_eols_query = true;
                if (eolEventsList.get(ls) != null && eolLostEventsList.get(ls)!=null && eolTotalEventsList.get(ls)!=null) {

                  eventsVal = eolEventsList.get(ls);
                  lostEventsVal = eolLostEventsList.get(ls);
                  totalEventsVal = eolTotalEventsList.get(ls);

                  //require match, otherwise BU json files might not be written yet
                  if (Math.round(totalEventsVal-eventsVal-totalEventsVal)==0) run_eols_query=false;
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
                    eolsQuery.getJSONObject("query").getJSONObject("prefix").put("_id",idStr);

                    SearchResponse sResponseEoLS = client.prepareSearch(runIndex_write).setTypes("eols")
                                                         .setRouting(runNumber)
                                                         .setSource(eolsQuery).execute().actionGet();
                    if(sResponseEoLS.getHits().getTotalHits() == 0L){ 
                        logger.info("eolsQuery returns 0 hits for LS " + ls + ", skipping collection without EoLS docs");
                        continue;
                    }
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
                }
                //continue with stream aggregation

                String id = String.format("%06d", Integer.parseInt(runNumber))+"_"+stream+"_"+ls;

                //Check if data is changed (to avoid to update timestamp if not necessary)
                GetResponse sresponse = client.prepareGet(runIndex_write, "stream-hist", id)
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
                
                //Update Data
                if (dataChanged){
                  logger.info("stream-hist update for ls,stream: "+ls+","+stream+" in:"+fuinlshist.get(stream).get(ls).toString()
                              +" out:"+fuoutlshist.get(stream).get(ls).toString()+" err:"+fuerrlshist.get(stream).get(ls).toString() + " completion " + newCompletion.toString());
                  logger.info("Totals numbers - eventsVal:"+eventsVal.toString() + " lostEventsVal:" + lostEventsVal.toString() + " totalEventsVal:" + totalEventsVal.toString());
                  Double retDate = futimestamplshist.get(stream).get(ls);
                  if (retDate >  Double.NEGATIVE_INFINITY) {
                    IndexResponse iResponse = client.prepareIndex(runIndex_write, "stream-hist").setRefresh(true)
                    .setParent(runNumber)
                    .setId(id)
                    .setSource(jsonBuilder()
                        .startObject()
                        .field("stream", stream)
                        .field("ls", Integer.parseInt(ls))
                        .field("in", fuinlshist.get(stream).get(ls))
                        .field("out", fuoutlshist.get(stream).get(ls))
                        .field("err", fuerrlshist.get(stream).get(ls))
                        .field("filesize", fufilesizehist.get(stream).get(ls))
                        .field("fm_date", retDate.longValue()) //convert when injecting into futimestamplshist?
                        .field("date",  System.currentTimeMillis())
                        .field("completion", newCompletion)
                        .endObject())
                    .execute()
                    .actionGet();
                  }
                  else {
                    //if no date, create indexing date exlplicitely
                    long start_time_millis = System.currentTimeMillis();
                    IndexResponse iResponse = client.prepareIndex(runIndex_write, "stream-hist").setRefresh(true)
                    .setParent(runNumber)
                    .setId(id)
                    .setSource(jsonBuilder()
                        .startObject()
                        .field("stream", stream)
                        .field("ls", Integer.parseInt(ls))
                        .field("in", fuinlshist.get(stream).get(ls))
                        .field("out", fuoutlshist.get(stream).get(ls))
                        .field("err", fuerrlshist.get(stream).get(ls))
                        .field("filesize", fufilesizehist.get(stream).get(ls))
                        .field("date",start_time_millis)
                        .field("fm_date",start_time_millis)
                        .field("completion", newCompletion)
                        .endObject())
                    .execute()
                    .actionGet();    
                  }
                }
            }
        }
    }
        
    public void collectStates() throws Exception {
        logger.info("collectStates");

        //logger.info("states query: "+statesQuery.toString());

        SearchResponse sResponse = remoteClient.prepareSearch(tribeIndex).setTypes("prc-i-state")
            .setSource(statesQuery).execute().actionGet();
        
        collectStats(riverName,"statesQuery",tribeIndex,sResponse);
        
        //logger.info(String.valueOf(sResponse.getHits().getTotalHits()));
        if(sResponse.getHits().getTotalHits() == 0L){ 
            logger.info("streamQuery returns 0 hits");
            return;
        }
        if (ustatesReserved==-1) {
          SearchResponse sResponseUstates = client.prepareSearch(runIndex_read).setTypes("microstatelegend").setRouting(runNumber).setQuery(QueryBuilders.termQuery("_parent", runNumber)).setSize(1).execute().actionGet();
          SearchHit[] searchHits = sResponseUstates.getHits().getHits();
          if(searchHits.length != 0) {
            logger.info("microstatelegend query returns hits");
            //List<String> keys = new ArrayList<String>(searchHits[0].sourceAsMap().keySet());
            //for (String key: keys) { logger.info("key:");logger.info(key);}
            if (searchHits[0].sourceAsMap().get("reserved")!=null) {
              //Integer reservedVal = sResponseUstates.getHits().getHits()[0].getSource().field("reserved");
              Integer reservedVal = (Integer) searchHits[0].sourceAsMap().get("reserved");
              ustatesReserved = reservedVal;

              if (searchHits[0].sourceAsMap().get("special")!=null)
                ustatesSpecial = (Integer) searchHits[0].sourceAsMap().get("special");

              if (searchHits[0].sourceAsMap().get("output")!=null)
                ustatesOutput = (Integer) searchHits[0].sourceAsMap().get("output");

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
            Histogram hist = sResponse.getAggregations().get(name);
            for ( Histogram.Bucket bucket : hist.getBuckets() ){
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
        xb.endObject();
        client.prepareIndex(runIndex_write, "state-hist")
            .setParent(runNumber)
            .setSource(xb)
            .execute();

        xbSummary.field("fm_date",fm_date.longValue());
        xbSummary.field("date",start_time_millis);
        xbSummary.endObject();
        client.prepareIndex(runIndex_write, "state-hist-summary")
            .setParent(runNumber)
            .setSource(xbSummary)
            .execute();                  

//        DO NOT DELETE. SNIPPET FOR RESPONSE TO JSON CONVERSION
//        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
//        builder.startObject();
//        sResponse.toXContent(builder, ToXContent.EMPTY_PARAMS);
//        builder.endObject();
//        logger.info("states results: "+builder.string());

    }

    public void checkRunEnd(){
        if (EoR){return;}
        GetResponse response = client.prepareGet(runIndex_write, "run", runNumber).setRefresh(true).execute().actionGet();
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
        boxinfoQuery.getJSONObject("filter").getJSONObject("term")
                .put("activeRuns",runNumber);

        SearchResponse response = client.prepareSearch(boxinfo_read).setSource(boxinfoQuery)
            .execute().actionGet();
        
        collectStats(riverName,"boxinfoQuery",boxinfo_read,response);
        
        logger.info("Boxinfo: "+ String.valueOf(response.getHits().getTotalHits()));
        if (response.getHits().getTotalHits() == 0 ) {
          execRunClose();
          setRunning(false);
          //selfDelete();
        }
    }

    public void setRemoteClient() throws UnknownHostException{
        Settings settings = Settings.settingsBuilder()
            .put("cluster.name", es_tribe_cluster).build();
        remoteClient = TransportClient.builder().settings(settings).build()
            .addTransportAddress(new InetSocketTransportAddress(new InetSocketAddress(InetAddress.getByName(es_tribe_host), 9300)));
 
    }

    public void getQueries(){
        try{
            streamQuery = getJson("streamQuery");
            eolsQuery = getJson("eolsQuery");
            statesQuery = getJson("statesQuery");
            boxinfoQuery = getJson("boxinfoQuery");
        } catch (Exception e) {
           logger.error("Collector getQueries Exception: ", e);
        }
    }

    public void execRunClose(){
        if (!closeIndices) {
          logger.info("closing indices is disabled");
          return;
        }
        logger.info("closing indices for run "+runNumber.toString());
	//close index using JAVA API
        remoteClient.admin().indices().close(Requests.closeIndexRequest("run"+runNumber.toString()+"_"+subsystem));
        return;
    }
}

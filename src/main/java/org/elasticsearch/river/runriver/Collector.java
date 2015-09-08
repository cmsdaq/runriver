package org.elasticsearch.river.runriver;

//JAVA
import java.io.IOException;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;


//ELASTICSEARCH
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.common.inject.Inject;

import org.elasticsearch.index.query.QueryBuilders;
import static org.elasticsearch.common.xcontent.XContentFactory.*;

//Remote query stuff
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;
import org.elasticsearch.search.aggregations.Aggregation;
//import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;


//RIVER
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;

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
    Map<String, Map<String, Double>> fufilesizehist = new HashMap<String, Map<String, Double>>();

    Client remoteClient;
    Boolean EoR=false;
    String tribeIndex;
    Boolean firstTime=true;
    Integer ustatesReserved=-1;

    //queries
    JSONObject streamQuery;
    JSONObject statesQuery;
    JSONObject boxinfoQuery;
        
    @Inject
    public Collector(RiverName riverName, RiverSettings settings,Client client) {
        super(riverName,settings,client);
    }

    @Override
    public void beforeLoop(){
        logger.info("Collector started.");
        this.interval = fetching_interval;
        this.tribeIndex = "run"+String.format("%06d", Integer.parseInt(runNumber))+"*";
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
        collectStats(riverName.getName(),"streamQuery",tribeIndex,sResponse);
        //logger.info(String.valueOf(sResponse.getHits().getTotalHits()));
        if(sResponse.getHits().getTotalHits() == 0L){ 
            logger.info("streamQuery returns 0 hits");
            return;
        }

        if(sResponse.getAggregations().asList().isEmpty()){return;}
        
        Terms streams = sResponse.getAggregations().get("streams");            
        
        if(streams.getBuckets().isEmpty()){return;}
        for (Terms.Bucket stream : streams.getBuckets()){
            String streamName = stream.getKey();

            known_streams.put(streamName,stream.getDocCount());
        
            fuinlshist.put(streamName, new HashMap<String, Double>());
            fuoutlshist.put(streamName, new HashMap<String, Double>());
            fufilesizehist.put(streamName, new HashMap<String, Double>());

            Terms lss = stream.getAggregations().get("ls");
            
            for (Terms.Bucket ls : lss.getBuckets()) {

                String lsName = ls.getKey();
                
                Sum inSum = ls.getAggregations().get("in");
                Sum outSum = ls.getAggregations().get("out");
                Sum filesizeSum = ls.getAggregations().get("filesize");

                fuinlshist.get(streamName).put(lsName,inSum.getValue());
                fuoutlshist.get(streamName).put(lsName,outSum.getValue());
                fufilesizehist.get(streamName).put(lsName,filesizeSum.getValue());
  
            } 
        }
        for (String stream : known_streams.keySet()){
            for (String ls : fuoutlshist.get(stream).keySet()){

                String id = String.format("%06d", Integer.parseInt(runNumber))+stream+ls;


                //Check if data is changed (to avoid to update timestamp if not necessary)
                GetResponse sresponse = client.prepareGet(runIndex_write, "stream-hist", id)
                                            .setRouting(runNumber)
                                            .setRefresh(true).execute().actionGet();

                dataChanged = true;
                if (sresponse.isExists()){ 
                    Double in = Double.parseDouble(sresponse.getSource().get("in").toString());
                    Double out = Double.parseDouble(sresponse.getSource().get("out").toString());

                    if (   in.compareTo(fuinlshist.get(stream).get(ls))==0 
                        && out.compareTo(fuoutlshist.get(stream).get(ls))==0){
                        dataChanged = false;
                    } else { logger.info(id+" already exists and will be updated."); }
                }
                
                //Update Data
                if (dataChanged){
                    logger.info("stream-hist update for ls,stream: "+ls+","+stream+" in:"+fuinlshist.get(stream).get(ls).toString()+" out:"+fuoutlshist.get(stream).get(ls).toString());
                    IndexResponse iResponse = client.prepareIndex(runIndex_write, "stream-hist").setRefresh(true)
                    .setParent(runNumber)
                    .setId(id)
                    .setSource(jsonBuilder()
                        .startObject()
                        .field("stream", stream)
                        .field("ls", Integer.parseInt(ls))
                        .field("in", fuinlshist.get(stream).get(ls))
                        .field("out", fuoutlshist.get(stream).get(ls))
                        .field("filesize", fufilesizehist.get(stream).get(ls))
                        .endObject())
                    .execute()
                    .actionGet();    
                }
                
            }

        }
    }
        
    public void collectStates() throws Exception {
        logger.info("collectStates");

        //logger.info("states query: "+statesQuery.toString());

        SearchResponse sResponse = remoteClient.prepareSearch(tribeIndex).setTypes("prc-i-state")
            .setSource(statesQuery).execute().actionGet();
        
        collectStats(riverName.getName(),"statesQuery",tribeIndex,sResponse);
        
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
              Integer reservedVal = (Integer) searchHits[0].sourceAsMap().get("reserved");
              //Integer reservedVal = sResponseUstates.getHits().getHits()[0].getSource().field("reserved");
              ustatesReserved = reservedVal;
              //if (reservedVal>=0) ustatesReserved = reservedVal;
            }
            else {
              logger.info("reserved field in microstatelegend is not present. Disabling checks.");
              //use default value
              ustatesReserved = 33;
            }
          }
        }

        if(sResponse.getAggregations().asList().isEmpty()){return;}

        XContentBuilder xb = XContentFactory.jsonBuilder().startObject(); 
        XContentBuilder xbSummary = XContentFactory.jsonBuilder().startObject(); 
        
        for (Aggregation agg : sResponse.getAggregations()) {
            String name = agg.getName();
            Boolean doSummary = false;
            if (name.equals("hmicro") && ustatesReserved>=0)
              doSummary = true;
            Long total = 0L;
            Long totalBusy = 0L;
            xb.startObject(name).startArray("entries"); 
            if (doSummary)
                xbSummary.startObject(name).startArray("entries"); 
            Histogram hist = sResponse.getAggregations().get(name);
            for ( Histogram.Bucket bucket : hist.getBuckets() ){
                Number key = bucket.getKeyAsNumber();
                Long doc_count = bucket.getDocCount();            
                total =  total + doc_count;
                xb.startObject();
                xb.field("key",key);
                xb.field("count",doc_count);
                xb.endObject();
                if (doSummary) {
                  if (key.intValue() < ustatesReserved) {
                    xbSummary.startObject();
                    xbSummary.field("key",key);
                    xbSummary.field("count",doc_count);
                    xbSummary.endObject();
                  }
                  else
                    totalBusy += doc_count;
                }
            }
            xb.endArray();
            xb.field("total",total);
            xb.endObject();
           
            if (doSummary) { 
              xbSummary.startObject();
              Number maxKey = 33;
              xbSummary.field("key",maxKey);
              xbSummary.field("count",totalBusy);
              xbSummary.endObject();
              xbSummary.endArray();
              xbSummary.field("total",total);
              xbSummary.endObject();
            }
 
        }
        xb.endObject();
        client.prepareIndex(runIndex_write, "state-hist")
            .setParent(runNumber)
            .setSource(xb)
            .execute();                  

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
        if (response.getSource().get("endTime") != null){ logger.info("EoR received!"); EoR = true; }
    }

    public void checkBoxInfo(){
        if (!EoR){return;}
        boxinfoQuery.getJSONObject("filter").getJSONObject("term")
                .put("activeRuns",runNumber);

        SearchResponse response = client.prepareSearch(boxinfo_write).setSource(boxinfoQuery)
            .execute().actionGet();
        
        collectStats(riverName.getName(),"boxinfoQuery",boxinfo_write,response);            
        
        logger.info("Boxinfo: "+ String.valueOf(response.getHits().getTotalHits()));
        if (response.getHits().getTotalHits() == 0 ) { selfDelete(); }
    }

    public void setRemoteClient(){
        
        Settings settings = ImmutableSettings.settingsBuilder()
            .put("cluster.name", es_tribe_cluster).build();
        remoteClient = new TransportClient(settings)
            .addTransportAddress(new InetSocketTransportAddress(es_tribe_host, 9300));
    }

    public void getQueries(){
        try{
            streamQuery = getJson("streamQuery");
            statesQuery = getJson("statesQuery");    
            boxinfoQuery = getJson("boxinfoQuery");    
        } catch (Exception e) {
           logger.error("Collector getQueries Exception: ", e);
        }
        
    }
}

package org.fffriver;


//JAVA
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.*;


//ELASTICSEARCH
import org.elasticsearch.client.Client;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequestBuilder;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
//import org.elasticsearch.index.engine.DocumentAlreadyExistsException;
import org.elasticsearch.index.engine.VersionConflictEngineException;
//jsonBuilder
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import static org.elasticsearch.common.xcontent.XContentFactory.*;

import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;

//org.json
import net.sf.json.JSONObject;
import net.sf.json.JSONSerializer;
import org.apache.commons.io.IOUtils;

public class RunMonitor extends AbstractRunRiverThread {
        
    JSONObject streamHistMapping;
    JSONObject streamHistMappingAlt;
    JSONObject stateHistMapping;
    JSONObject stateHistMappingAlt;
    JSONObject stateHistSummaryMapping;
    JSONObject stateHistSummaryMappingAlt;
    JSONObject statsMapping;
    //JSONObject runQuery;

    public RunMonitor(String riverName, Map<String, Object> rSettings, Client client) {
        super(riverName,rSettings,client);
    }

    @Override
    public void beforeLoop() throws UnknownHostException {
        logger.info("RunMonitor Started v1.4.4");
        getQueries();
        prepareServer(client,runIndex_write);
        this.interval = polling_interval;
        
    }
    public void afterLoop() throws Exception {
        logger.info("RunMonitor Stopped.");
    }

    @Override
    public void mainLoop() throws Exception {     
        runPolling();
    }

    public void runPolling() throws Exception {
        logger.info("runPolling on index: "+runIndex_read);
        SearchResponse response = client.prepareSearch(runIndex_read).setTypes("run")
                                        .setSize(100)
                                        .addSort(SortBuilders.fieldSort("startTime").order(SortOrder.DESC))
                                        .setQuery(QueryBuilders.boolQuery()
                                                               .should(QueryBuilders.boolQuery().mustNot(QueryBuilders.existsQuery("endTime")))
                                                               .should(QueryBuilders.rangeQuery("activeBUs").from(1))
                                                               .minimumShouldMatch("1"))
                                        .execute().actionGet();
        
        //SearchResponse response = client.prepareSearch(runIndex_read).setTypes("run")
        //    .setSource(runQuery).execute().actionGet();
        collectStats(riverName,"runRanger",runIndex_read,response);

        if (response.getHits().getTotalHits() == 0 ) { return; }
        
        for (SearchHit hit : response.getHits().getHits()) {
            String runNumber = hit.getSource().get("runNumber").toString();
            if (!runExists(runNumber)){ createRun(runNumber); }
        }
    }

    public void createRun (String runNumber) throws Exception {

        logger.info("Spawning River instance for run run "+ runNumber );

        String index = "river";
        String type = "instance";
        String river_id = "river_"+subsystem+'_'+runNumber;

        //check if document is already found

        // FOR DYNAMIC MAPPING ISSUE
        //String map = "{\"dynamic\" : true}}";
        //PutMappingRequestBuilder pmrb = client.admin().indices()
        //                .preparePutMapping(index)
        //                .setType(type)//.setSource(map);
        //PutMappingResponse mresponse = pmrb.execute().actionGet();   
        try {
          IndexResponse response = client.prepareIndex(index, type, river_id)
          .setSource(jsonBuilder()
                    .startObject()
                        .field("instance_name",river_id)
                        .field("subsystem",subsystem)
                        .field("runNumber", Integer.parseInt(runNumber))
                        .field("es_tribe_host", es_tribe_host)
                        .field("es_tribe_cluster", es_tribe_cluster)
                        .field("fetching_interval", fetching_interval)
                        .field("runIndex_read", runIndex_read)
                        .field("runIndex_write", runIndex_write)
                        .field("boxinfo_read", boxinfo_read)
                        .field("boxinfo_write", boxinfo_read)//fallback option
                        .field("enable_stats", statsEnabled)
                        .field("close_indices", closeIndices)
                        .field("es_central_cluster", es_central_cluster)
                        .startObject("node").field("status","created").endObject()
                    .endObject()
                        //.field("role", "collector")
                  )
          .setCreate(true)
          .execute()
          .actionGet();
        }
        //catch (DocumentAlreadyExistsException ex) {
        catch (VersionConflictEngineException ex) {
          logger.info("skipping already existing document for "+runNumber);
        }

    }

    public boolean runExists(String runNumber){
        // Check if a document exists
        String index = "river";
        String type = "instance";
        String river_id = "river_"+subsystem+'_'+runNumber;
        //setRefresh is not strictly necessary because op type = create is used (only one can create document)
        //GetResponse response = client.prepareGet(index,type,river_id).setRefresh(true).execute().actionGet();
        GetResponse response = client.prepareGet(index,type,river_id).execute().actionGet();
        //GetResponse response = client.prepareGet(index,type,,river_id).get();
        return response.isExists();
    }

    public void getQueries() {
        try {
                //runQuery = getJson("runRanger");
                stateHistMapping = getJson("stateHistMapping");
                stateHistMappingAlt = getJson("stateHistMappingAlt");
                stateHistSummaryMapping = getJson("stateHistSummaryMapping");
                stateHistSummaryMappingAlt = getJson("stateHistSummaryMappingAlt");
                streamHistMapping = getJson("streamHistMapping"); 
                streamHistMappingAlt = getJson("streamHistMappingAlt"); 
                statsMapping = getJson("statsMapping"); 
            } catch (Exception e) {
                logger.error("RunMonitor getQueries Exception: ", e);
            }
        
    }

    public void prepareServer(Client client, String runIndex) {
        //runindexCheck(client,runIndex);
        createStreamMapping(client,runIndex);
        createStateMapping(client,runIndex);
        createStateSummaryMapping(client,runIndex);
        createStatIndex(client,"runriver_stats"); 
    }

    public void createStateMapping(Client client, String runIndex){
        client.admin().cluster().prepareHealth().setWaitForYellowStatus().execute().actionGet();
        GetMappingsResponse response = client.admin().indices().prepareGetMappings(runIndex_write)
            .setTypes("state-hist").execute().actionGet();
        //if (!response.mappings().isEmpty()){ logger.info("State Mapping already exists"); return; }
        logger.info("create/update StateMapping");
        try {
          client.admin().indices().preparePutMapping()
            .setIndices(runIndex_write)
            .setType("state-hist")
            .setSource(stateHistMappingAlt)
            .execute().actionGet();
        } catch (Exception e) {
          client.admin().indices().preparePutMapping()
            .setIndices(runIndex_write)
            .setType("state-hist")
            .setSource(stateHistMapping)
            .execute().actionGet();
        }

    }

    public void createStateSummaryMapping(Client client, String runIndex){
        client.admin().cluster().prepareHealth().setWaitForYellowStatus().execute().actionGet();
        GetMappingsResponse response = client.admin().indices().prepareGetMappings(runIndex_write)
            .setTypes("state-hist-summary").execute().actionGet();
        //if (!response.mappings().isEmpty()){ logger.info("State Summary Mapping already exists"); return; }
        logger.info("create/update StateSummaryMapping");
        try {
          client.admin().indices().preparePutMapping()
            .setIndices(runIndex_write)
            .setType("state-hist-summary")
            .setSource(stateHistSummaryMappingAlt)
            .execute().actionGet();
        } catch (Exception e) {
          client.admin().indices().preparePutMapping()
            .setIndices(runIndex_write)
            .setType("state-hist-summary")
            .setSource(stateHistSummaryMapping)
            .execute().actionGet();
        }
    }


    public void createStreamMapping(Client client, String runIndex){
        client.admin().cluster().prepareHealth().setWaitForYellowStatus().execute().actionGet();
        GetMappingsResponse response = client.admin().indices().prepareGetMappings(runIndex_write)
            .setTypes("stream-hist").execute().actionGet();
        //if (!response.mappings().isEmpty()){ logger.info("Stream Mapping already exists"); return; }
        logger.info("create/update StreamMapping");
        try {
          client.admin().indices().preparePutMapping()
            .setIndices(runIndex_write)
            .setType("stream-hist")
            .setSource(streamHistMappingAlt)
            .execute().actionGet();
        } catch (Exception e) {
          client.admin().indices().preparePutMapping()
            .setIndices(runIndex_write)
            .setType("stream-hist")
            .setSource(streamHistMapping)
            .execute().actionGet();
        }
    }

    public void createStatIndex(Client client, String index){
        if(!statsEnabled){return;}
        client.admin().cluster().prepareHealth().setWaitForYellowStatus().execute().actionGet();
        Boolean exists = client.admin().indices().prepareExists(index).execute().actionGet().isExists();
        logger.info("statIndex exists: "+exists.toString());
        if (!exists){
            logger.info("createStatIndex"); 
            client.admin().indices().prepareCreate(index).addMapping("stats",statsMapping)
                .execute().actionGet();
            client.admin().indices().prepareAliases().addAlias(index,index+"_read")
                .execute().actionGet();;
            client.admin().indices().prepareAliases().addAlias(index,index+"_write")
                .execute().actionGet();;
        }
    }
}

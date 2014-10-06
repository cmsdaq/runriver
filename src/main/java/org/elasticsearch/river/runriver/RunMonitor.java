package org.elasticsearch.river.runriver;


//JAVA
import java.io.IOException;
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

//RIVER
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;

//jsonBuilder
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import static org.elasticsearch.common.xcontent.XContentFactory.*;

//org.json
import net.sf.json.JSONObject;
import net.sf.json.JSONSerializer;
import org.apache.commons.io.IOUtils;

public class RunMonitor extends AbstractRunRiverThread {
        
    JSONObject streamHistMapping;
    JSONObject stateHistMapping;
    JSONObject statsMapping;
    JSONObject runQuery;

    public RunMonitor(RiverName riverName, RiverSettings settings, Client client) {
        super(riverName,settings,client);
    }

    @Override
    public void beforeLoop(){
        logger.info("RunMonitor Started v1.3.3");
        getQueries();
        prepareServer(client,runIndex_write);
        this.interval = polling_interval;
        
    }
    public void afterLoop(){
        logger.info("RunMonitor Stopped.");
    }

    @Override
    public void mainLoop() throws Exception {     
        runPolling();
    }

    public void runPolling() throws Exception {
        logger.info("runPolling on index: "+runIndex_read);

        
        SearchResponse response = client.prepareSearch(runIndex_read).setTypes("run")
            .setSource(runQuery).execute().actionGet();
        collectStats(riverName.getName(),"runRanger",runIndex_read,response);

        if (response.getHits().getTotalHits() == 0 ) { return; }
        
        for (SearchHit hit : response.getHits().getHits()) {
            String runNumber = hit.getSource().get("runNumber").toString();
            if (!runExists(runNumber)){ createRun(runNumber); } 
        }
    }

    public void createRun (String runNumber) throws Exception {

        logger.info("Started run "+ runNumber );

        String index = "_river";
        String type = "runriver_"+runNumber;

        // FOR DYNAMIC MAPPING ISSUE
        String map = "{\"dynamic\" : true}}";

        PutMappingRequestBuilder pmrb = client.admin().indices()
                        .preparePutMapping(index)
                        .setType(type).setSource(map);
        PutMappingResponse mresponse = pmrb.execute().actionGet();   
        
        IndexResponse response = client.prepareIndex(index, type, "_meta")
        .setSource(jsonBuilder()
                    .startObject()
                        .field("type", "runriver")
                        .field("runNumber", runNumber)
                        .field("role", "collector")
                        .field("es_tribe_host", es_tribe_host)
                        .field("es_tribe_cluster", es_tribe_cluster)
                        .field("fetching_interval", fetching_interval)
                        .field("runIndex_read", runIndex_read)
                        .field("runIndex_write", runIndex_write)
                        .field("boxinfo_write", boxinfo_write)
                        .field("enable_stats", statsEnabled)
                    .endObject()
                  )
        .execute()
        .actionGet();

    }

    public boolean runExists(String runNumber){
        // Check if a document exists
        GetResponse response = client.prepareGet("_river", "runriver_"+runNumber, "_meta").setRefresh(true).execute().actionGet();
        return response.isExists();
    }

    public void getQueries() {
        try {
                runQuery = getJson("runRanger");
                stateHistMapping = getJson("stateHistMapping");
                streamHistMapping = getJson("streamHistMapping"); 
                statsMapping = getJson("statsMapping"); 
            } catch (Exception e) {
                logger.error("RunMonitor getQueries Exception: ", e);
            }
        
    }

    public void prepareServer(Client client, String runIndex) {
        //runindexCheck(client,runIndex);
        createStreamMapping(client,runIndex);
        createStateMapping(client,runIndex);
        createStatIndex(client,"runriver_stats"); 
    }

    public void createStateMapping(Client client, String runIndex){
        client.admin().cluster().prepareHealth().setWaitForYellowStatus().execute().actionGet();
        GetMappingsResponse response = client.admin().indices().prepareGetMappings(runIndex_write)
            .setTypes("state-hist").execute().actionGet();
        if (!response.mappings().isEmpty()){ logger.info("State Mapping already exists"); return; }
        logger.info("createStateMapping");
        client.admin().indices().preparePutMapping()
            .setIndices(runIndex_write)
            .setType("state-hist")
            .setSource(stateHistMapping)
            .execute().actionGet();
    }

    public void createStreamMapping(Client client, String runIndex){
        client.admin().cluster().prepareHealth().setWaitForYellowStatus().execute().actionGet();
        GetMappingsResponse response = client.admin().indices().prepareGetMappings(runIndex_write)
            .setTypes("stream-hist").execute().actionGet();
        if (!response.mappings().isEmpty()){ logger.info("Stream Mapping already exists"); return; }
        logger.info("createStreamMapping"); 
        client.admin().indices().preparePutMapping()
            .setIndices(runIndex_write)
            .setType("stream-hist")
            .setSource(streamHistMapping)
            .execute().actionGet();
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

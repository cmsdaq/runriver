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
//import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
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
        
    JSONObject commonMapping;
    JSONObject statsMapping;
    //JSONObject runQuery;

    public RunMonitor(String riverName, Map<String, Object> rSettings, Client client) {
        super(riverName,rSettings,client);
    }

    @Override
    public void beforeLoop() throws UnknownHostException {
        logger.info("RunMonitor Started v1.4.4");
        getQueries();
        prepareServer(client,runindex_write);
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
        logger.info("runPolling on index: "+runindex_read);
        SearchResponse response = client.prepareSearch(runindex_read).setTypes("doc")
                                        .setSize(100)
                                        .addSort(SortBuilders.fieldSort("startTime").order(SortOrder.DESC))
                                        .setQuery(QueryBuilders.boolQuery()
                                                               .must(QueryBuilders.termQuery("doc_type","run"))
                                                               .should(QueryBuilders.boolQuery().mustNot(QueryBuilders.existsQuery("endTime")))
                                                               .should(QueryBuilders.rangeQuery("activeBUs").from(1))
                                                               .minimumShouldMatch("1"))
                                        .execute().actionGet();
        
        collectStats(riverName,"runRanger",runindex_read,response);

        if (response.getHits().getTotalHits().value == 0 ) { return; }
        
        for (SearchHit hit : response.getHits().getHits()) {
            String runNumber = hit.getSourceAsMap().get("runNumber").toString();
            if (!runExists(runNumber)){ createRun(runNumber); }
        }
    }

    public void createRun (String runNumber) throws Exception {

        logger.info("Spawning River instance for run run "+ runNumber );

        String index = "river";
        String type = "_doc";
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
                        .field("es_local_host", es_local_host)
                        .field("es_local_cluster", es_local_cluster)
                        .field("fetching_interval", fetching_interval)
                        .field("runindex_read", runindex_read)
                        .field("runindex_write", runindex_write)
                        .field("boxinfo_read", boxinfo_read)
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
        String type = "_doc";
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
                commonMapping = getJson("commonMapping"); 
                statsMapping = getJson("statsMapping"); 
            } catch (Exception e) {
                logger.error("RunMonitor getQueries Exception: ", e);
            }
        
    }

    public void prepareServer(Client client, String runindex) {
        //runindexCheck(client,runindex);
        createCommonMapping(client,runindex);
        createStatIndex(client,"runriver_stats"); 
    }

    public void createCommonMapping(Client client, String runindex){
        client.admin().cluster().prepareHealth().setWaitForYellowStatus().execute().actionGet();
        GetMappingsResponse response = client.admin().indices().prepareGetMappings(runindex_write)
            //.setTypes("doc")
            .execute().actionGet();
        //if (!response.mappings().isEmpty()){ logger.info("Stream Mapping already exists"); return; }
        logger.info("create/update CommonMapping");
        client.admin().indices().preparePutMapping()
            .setIndices(runindex_write)
            .setType("doc")
            .setSource(commonMapping)
            .execute().actionGet();
    }

    public void createStatIndex(Client client, String index){
        if(!statsEnabled){return;}
        client.admin().cluster().prepareHealth().setWaitForYellowStatus().execute().actionGet();
        Boolean exists = client.admin().indices().prepareExists(index).execute().actionGet().isExists();
        logger.info("statIndex exists: "+exists.toString());
        if (!exists){
            logger.info("createStatIndex"); 
            client.admin().indices().prepareCreate(index).addMapping("properties",statsMapping.get("properties")) //TODO: this will not work in elasticsearch7
                .execute().actionGet();
            client.admin().indices().prepareAliases().addAlias(index,index+"_read")
                .execute().actionGet();;
            client.admin().indices().prepareAliases().addAlias(index,index+"_write")
                .execute().actionGet();;
        }
    }
}

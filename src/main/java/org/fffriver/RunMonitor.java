package org.fffriver;


//JAVA
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.*;


//ELASTICSEARCH
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
//import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;

import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequestBuilder;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
//import org.elasticsearch.index.engine.DocumentAlreadyExistsException;
import org.elasticsearch.index.engine.VersionConflictEngineException;
//jsonBuilder
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import static org.elasticsearch.common.xcontent.XContentFactory.*;
import org.elasticsearch.common.xcontent.XContentType;

import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;

//org.json
import net.sf.json.JSONObject;
import net.sf.json.JSONSerializer;
import org.apache.commons.io.IOUtils;

public class RunMonitor extends AbstractRunRiverThread {
        
    String commonMapping;
    String statsMapping;
    //JSONObject runQuery;

    public RunMonitor(String riverName, Map<String, Object> rSettings, RestHighLevelClient client) {
        super(riverName,rSettings,client);
    }

    @Override
    public void beforeLoop() throws UnknownHostException, IOException {
        logger.info("RunMonitor Started v1.4.4");
        getQueries();
        prepareServer(client,runindex_write);
        this.interval = polling_interval;
        
    }
    public void afterLoop() throws Exception, IOException {
        logger.info("RunMonitor Stopped.");
    }

    @Override
    public void mainLoop() throws Exception, IOException {     

        logger.info("runPolling on index: "+runindex_read);

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
         .query(QueryBuilders.boolQuery()
                                  .must(QueryBuilders.termQuery("doc_type","run"))
                                  .should(QueryBuilders.boolQuery().mustNot(QueryBuilders.existsQuery("endTime")))
                                  .should(QueryBuilders.rangeQuery("activeBUs").from(1))
                                  .minimumShouldMatch("1"))
         .sort(SortBuilders.fieldSort("startTime").order(SortOrder.DESC))
         .size(100);

        SearchRequest searchRequest = new SearchRequest()
          .indices(runindex_read)
          .source(searchSourceBuilder);
          //.size(100);

        SearchResponse response = client.search(searchRequest,RequestOptions.DEFAULT);


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
        String river_id = "river_"+subsystem+'_'+runNumber;

        try {
          IndexRequest indexReq = new IndexRequest(index,river_id)
            .opType("create")
            .source(jsonBuilder()
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
                  );
          client.index(indexReq,RequestOptions.DEFAULT);
        }
        //catch (DocumentAlreadyExistsException ex) {
        catch (VersionConflictEngineException ex) {
          logger.info("skipping already existing document for "+runNumber);
        }

    }

    public boolean runExists(String runNumber) throws IOException{
        // Check if a document exists
        String index = "river";
        String river_id = "river_"+subsystem+'_'+runNumber;
        //setRefresh is not strictly necessary because op type = create is used (only one can create document)
        GetResponse response = client.get( new GetRequest(index,river_id), RequestOptions.DEFAULT);
        return response.isExists();
    }

    public void getQueries() {
        try {
                //runQuery = getJson("runRanger");
                commonMapping = getJsonAsString("commonMapping"); 
                statsMapping = getJsonAsString("statsMapping"); 
            } catch (Exception e) {
                logger.error("RunMonitor getQueries Exception: ", e);
            }
        
    }

    public void prepareServer(RestHighLevelClient client, String runindex) throws IOException {
        //runindexCheck(client,runindex);
        createCommonMapping(client,runindex);
        createStatIndex(client,"runriver_stats"); 
    }

    public void createCommonMapping(RestHighLevelClient client, String runindex) throws IOException {
        ClusterHealthRequest healthReq = new ClusterHealthRequest().waitForYellowStatus();
        client.cluster().health(healthReq, RequestOptions.DEFAULT);//does it wait?

        GetMappingsRequest getRequest = new GetMappingsRequest().indices(runindex_write);
        client.indices().getMapping(getRequest, RequestOptions.DEFAULT);

        logger.info("create/update CommonMapping");
        PutMappingRequest injRequest = new PutMappingRequest(runindex_write)
          .source(commonMapping,XContentType.JSON);
        client.indices().putMapping(injRequest,RequestOptions.DEFAULT);
    }

    public void createStatIndex(RestHighLevelClient client, String index) throws IOException {
        if(!statsEnabled){return;}

        ClusterHealthRequest healthReq = new ClusterHealthRequest().waitForYellowStatus();
        client.cluster().health(healthReq, RequestOptions.DEFAULT);//does it wait?

        Boolean exists = client.indices().exists(new GetIndexRequest(index),RequestOptions.DEFAULT);
        logger.info("statIndex exists: "+exists.toString());

        if (!exists){
            logger.info("createStatIndex"); 
            CreateIndexRequest createRequest = new CreateIndexRequest(index).mapping(statsMapping,XContentType.JSON);
            client.indices().create(createRequest,  RequestOptions.DEFAULT);

            logger.info("adding aliases");
            AliasActions aliasAction1 =
              new AliasActions(AliasActions.Type.ADD)
                .index(index).alias(index+"_read");
            AliasActions aliasAction2 =
              new AliasActions(AliasActions.Type.ADD)
                .index(index).alias(index+"_read");
            IndicesAliasesRequest aliasRequest = new IndicesAliasesRequest().addAliasAction(aliasAction1).addAliasAction(aliasAction2);
            client.indices().updateAliases(aliasRequest, RequestOptions.DEFAULT);
        }
    }
}

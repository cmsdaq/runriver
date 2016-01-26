package org.fffriver;

//JAVA
import java.io.IOException;
import java.lang.InterruptedException;
import java.util.concurrent.ExecutionException;
import java.net.UnknownHostException;
import java.io.File;
import java.util.Map;
import java.util.*;
import java.text.SimpleDateFormat;

import java.io.InputStream;

//ELASTICSEARCH
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.common.xcontent.XContentFactory.*;
import org.elasticsearch.action.update.*;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.common.xcontent.XContentBuilder;

import static org.elasticsearch.common.xcontent.XContentFactory.*;

//Remote query stuff
//import org.elasticsearch.client.transport.TransportClient;
//import org.elasticsearch.common.transport.InetSocketTransportAddress;
//import org.elasticsearch.common.xcontent.ToXContent;
//import org.elasticsearch.common.xcontent.XContentFactory;



//org.json
import net.sf.json.JSONObject;
import net.sf.json.JSONSerializer;
import org.apache.commons.io.IOUtils;

//In java seems to be impossible to inherit/extend from multiple class
//so i need to implement the AbstractRiverComponent compatibility manually
//just to have the logger object. 
//https://github.com/elasticsearch/elasticsearch/blob/master/src/main/java/org/elasticsearch/river/AbstractRiverComponent.java
public class AbstractRunRiverThread extends Thread  {

    //AbstractRiverComponent
    protected final ESLogger logger;
    protected final String riverName;//todo:name
    protected final Map<String, Object> settings;
    protected final Client client;

    //Runrivercomponent
    public String es_tribe_host;
    public String es_tribe_cluster;
    public String role;
    public int polling_interval;
    public int fetching_interval;
    public String runNumber;
    public String runIndex_read;
    public String runIndex_write;
    public String boxinfo_write;
    public Boolean statsEnabled; 
    public Boolean closeIndices; 
    public String subsystem;
    public String river_esindex;
    public String es_central_cluster;

    public SimpleDateFormat sdf;

    //thread
    public int interval;
    public volatile boolean isRunning;
    public volatile boolean inError; //TODO: adust exit code based on this
    

    @Inject
    public AbstractRunRiverThread(String riverName,  Map<String, Object> rSettings, Client client) {
        super("RunRiver thread");

        //River Settings
        this.riverName = riverName;
        this.settings = rSettings;
        this.client = client;
        //this.logger = Loggers.getLogger(getClass(), settings.globalSettings(), riverName);
        this.logger = Loggers.getLogger(getClass(), "river", riverName);
        this.sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        sdf.setTimeZone(TimeZone.getTimeZone("UTC"));

        //RunRiver Settings
        es_tribe_host = XContentMapValues.nodeStringValue(rSettings.get("es_tribe_host"), "es-tribe");
        es_tribe_cluster = XContentMapValues.nodeStringValue(rSettings.get("es_tribe_cluster"), "es-tribe");
        role = XContentMapValues.nodeStringValue(rSettings.get("role"), "monitor");
        subsystem = XContentMapValues.nodeStringValue(rSettings.get("subsystem"), "cdaq");
        polling_interval = XContentMapValues.nodeIntegerValue(rSettings.get("polling_interval"), 30);
        fetching_interval = XContentMapValues.nodeIntegerValue(rSettings.get("fetching_interval"), 5);
        runNumber = XContentMapValues.nodeStringValue(rSettings.get("runNumber"), "0");
        runIndex_read = XContentMapValues.nodeStringValue(rSettings.get("runIndex_read"), "runindex_cdaq_read");
        runIndex_write = XContentMapValues.nodeStringValue(rSettings.get("runIndex_write"), "runindex_cdaq_write");
        boxinfo_write = XContentMapValues.nodeStringValue(rSettings.get("boxinfo_write"), "boxinfo_cdaq_write");
        statsEnabled = Boolean.valueOf(XContentMapValues.nodeStringValue(rSettings.get("enable_stats"), "false"));
        closeIndices = Boolean.valueOf(XContentMapValues.nodeStringValue(rSettings.get("close_indices"), "true"));
        river_esindex = XContentMapValues.nodeStringValue(rSettings.get("river_esindex"), "river");
        es_central_cluster= XContentMapValues.nodeStringValue(rSettings.get("es_central_cluster"), "es-cdaq");

        interval = polling_interval;
        
        //Thread settings
        isRunning = true;
        inError = true;
    }


    @Override
    public void run() {
        try {
          updateRiverDoc();
          beforeLoop();
        } catch (IOException e) {
          logger.error("beforeLoop IOEception: ", e);
          inError = true;
          return;
        } catch (Exception e) {
          logger.error("beforeLoop Exception: ", e);
          inError = true;
          return;
        }
        //main loop
        while (isRunning) {

            try {
                mainLoop();
            } catch (IOException e) {
               logger.error("Mainloop IOEception: ", e);
               selfDelete();
               inError = true;
               break;
            } catch (Exception e) {
               logger.error("Mainloop Exception: ", e);
               selfDelete();
               inError = true;
               break;
            }   
            

            try {
                Thread.sleep(interval * 1000); // needs milliseconds
            } catch (InterruptedException e) {}
        }
        //afterLoop();
    }

    public void selfDelete(){
        isRunning = false;
        //client.admin().indices().prepareDeleteMapping("_river").setType(riverName).execute();
        //done by service
    }

    public void mainLoop() throws Exception {
        return;
    }
    public void beforeLoop() throws UnknownHostException {
        return;
    }
    public void afterLoop(){
        return;
    }

    public void setRunning(boolean running) {
        isRunning = running;
    }
    
    public String riverName() {
        return riverName;
    }

    //public String nodeName() {
    //    return settings.globalSettings().get("name", "");
    //}

    public JSONObject getJson(String queryName) throws Exception {
        String filename = queryName + ".json" ;
        InputStream is = this.getClass().getResourceAsStream( "/json/" + filename );
        String jsonTxt = IOUtils.toString( is );
        JSONObject json = (JSONObject) JSONSerializer.toJSON( jsonTxt );        
        return json;
    }

    public void updateRiverDoc() throws IOException,InterruptedException,ExecutionException {

        long start_time_millis = System.currentTimeMillis();
        Date start_time = new Date(start_time_millis);
        String sdf_string = sdf.format(start_time); 

        //upda document in river_id in river index
        UpdateRequest updateRequest = new UpdateRequest();
        updateRequest.index(river_esindex);
        updateRequest.type("instance");
        updateRequest.id(riverName);
        updateRequest.doc(
                      jsonBuilder()
                      .startObject()
                      .startObject("node")
                      .field("status","running")
                      .field("ping_timestamp",start_time_millis)
                      .field("ping_time_fmt",sdf_string).endObject());

        client.update(updateRequest).get();
  } 

    public Boolean collectStats(String rivername, String queryname, String index, SearchResponse sResponse) {
        if(!statsEnabled){return true;}
        
        try {
            IndexResponse iResponse = client.prepareIndex("runriver_stats_write", "stats").setRefresh(true)
                .setSource(jsonBuilder()
                    .startObject()
                    .field("rivername", rivername)
                    .field("index", index)
                    .field("query_name", queryname)
                    .field("took", sResponse.getTookInMillis())
                    .field("timed_out", sResponse.isTimedOut())
                    .field("shards_total", sResponse.getTotalShards())
                    .field("shards_successful", sResponse.getSuccessfulShards())
                    .field("shards_failed", sResponse.getFailedShards())
                    .field("hits_total", sResponse.getHits().getTotalHits())
                    .endObject())
                .execute()
                .actionGet();   
            return true;
        } catch (Exception e) {
            logger.error("elasticizeStat exception: ", e);
            return false;
        }
    }

}

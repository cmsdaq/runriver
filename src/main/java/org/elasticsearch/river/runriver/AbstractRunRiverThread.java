package org.elasticsearch.river.runriver;

//JAVA
import java.io.IOException;
import java.io.File;
import java.util.Map;
import java.util.*;

import java.io.InputStream;

//ELASTICSEARCH
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.river.AbstractRiverComponent;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.index.IndexResponse;
import static org.elasticsearch.common.xcontent.XContentFactory.*;

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
    protected final RiverName riverName;
    protected final RiverSettings settings;
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


    //thread
    public int interval;
    public volatile boolean isRunning;
    

    @Inject
    public AbstractRunRiverThread(RiverName riverName, RiverSettings settings, Client client) {
        super("RunRiver thread");

        //River Settings
        this.riverName = riverName;
        this.settings = settings;
        this.client = client;
        this.logger = Loggers.getLogger(getClass(), settings.globalSettings(), riverName);

        //RunRiver Settings
        Map<String, Object> rSettings = settings.settings();
        es_tribe_host = XContentMapValues.nodeStringValue(rSettings.get("es_tribe_host"), "es-tribe");
        es_tribe_cluster = XContentMapValues.nodeStringValue(rSettings.get("es_tribe_cluster"), "es-tribe");
        role = XContentMapValues.nodeStringValue(rSettings.get("role"), "monitor");
        polling_interval = XContentMapValues.nodeIntegerValue(rSettings.get("polling_interval"), 30);
        fetching_interval = XContentMapValues.nodeIntegerValue(rSettings.get("fetching_interval"), 5);
        runNumber = XContentMapValues.nodeStringValue(rSettings.get("runNumber"), "0");
        runIndex_read = XContentMapValues.nodeStringValue(rSettings.get("runIndex_read"), "runindex_cdaq_read");
        runIndex_write = XContentMapValues.nodeStringValue(rSettings.get("runIndex_write"), "runindex_cdaq_write");
        boxinfo_write = XContentMapValues.nodeStringValue(rSettings.get("boxinfo_write"), "boxinfo_cdaq_write");
        statsEnabled = Boolean.valueOf(XContentMapValues.nodeStringValue(rSettings.get("enable_stats"), "false"));

        interval = polling_interval;
        
        //Thread settings
        isRunning = true;
    }


    @Override
    public void run() {
        beforeLoop();
        while (isRunning) {

            try {
                mainLoop();
            } catch (IOException e) {
               logger.error("Mainloop IOEception: ", e);
               selfDelete();
            } catch (Exception e) {
               logger.error("Mainloop Exception: ", e);
               selfDelete();
            }   
            

            try {
                Thread.sleep(interval * 1000); // needs milliseconds
            } catch (InterruptedException e) {}
        }
        afterLoop();
    }

    public void selfDelete(){
        client.admin().indices().prepareDeleteMapping("_river").setType(riverName.name()).execute();
    }

    public void mainLoop() throws Exception {
        return;
    }
    public void beforeLoop(){
        return;
    }
    public void afterLoop(){
        return;
    }

    public void setRunning(boolean running) {
        isRunning = running;
    }
    
    public RiverName riverName() {
        return riverName;
    }

    public String nodeName() {
        return settings.globalSettings().get("name", "");
    }

    public JSONObject getJson(String queryName) throws Exception {
        String filename = queryName + ".json" ;
        InputStream is = this.getClass().getResourceAsStream( "/json/" + filename );
        String jsonTxt = IOUtils.toString( is );
        JSONObject json = (JSONObject) JSONSerializer.toJSON( jsonTxt );        
        return json;
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

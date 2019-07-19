package org.fffriver;

//JAVA
import java.io.IOException;
import java.lang.InterruptedException;
import java.util.concurrent.ExecutionException;
import java.net.UnknownHostException;
//import java.io.File;
import java.util.Map;
import java.util.TimeZone;
import java.util.Date;
import java.text.SimpleDateFormat;

import java.io.InputStream;

//ELASTICSEARCH
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.inject.Inject;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.Loggers;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;

import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;

import org.elasticsearch.common.xcontent.support.XContentMapValues;
import static org.elasticsearch.common.xcontent.XContentFactory.*;
import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy;

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
    //protected final ESLogger logger;
    protected final Logger logger;
    protected final String riverName;//todo:name
    protected final Map<String, Object> settings;
    protected final RestHighLevelClient client;

    //Runrivercomponent
    public String es_local_host;
    public String es_local_cluster;
    public String role;
    public int polling_interval;
    public int fetching_interval;
    public String runNumber;
    public String runindex_read;
    public String runindex_write;
    public String boxinfo_read;
    public Boolean statsEnabled; 
    public Boolean closeIndices; 
    public String subsystem;
    public String river_esindex;
    public String es_central_cluster;
    public Boolean perbu;

    public SimpleDateFormat sdf;

    //thread
    public int interval;
    public volatile boolean isRunning;
    public volatile boolean inError; //TODO: adust exit code based on this
    

    @Inject
    public AbstractRunRiverThread(String riverName,  Map<String, Object> rSettings, RestHighLevelClient client) {
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
        es_local_host = XContentMapValues.nodeStringValue(rSettings.get("es_local_host"), "es-local");
        es_local_cluster = XContentMapValues.nodeStringValue(rSettings.get("es_local_cluster"), "es-local");
        role = XContentMapValues.nodeStringValue(rSettings.get("role"), "monitor");
        subsystem = XContentMapValues.nodeStringValue(rSettings.get("subsystem"), "cdaq");
        polling_interval = XContentMapValues.nodeIntegerValue(rSettings.get("polling_interval"), 30);
        fetching_interval = XContentMapValues.nodeIntegerValue(rSettings.get("fetching_interval"), 5);
        runNumber = XContentMapValues.nodeStringValue(rSettings.get("runNumber"), "0");
        runindex_read = XContentMapValues.nodeStringValue(rSettings.get("runindex_read"), "runindex_cdaq_read");
        runindex_write = XContentMapValues.nodeStringValue(rSettings.get("runindex_write"), "runindex_cdaq_write");
        boxinfo_read = XContentMapValues.nodeStringValue(rSettings.get("boxinfo_read"), "boxinfo_cdaq_read");
        statsEnabled = Boolean.valueOf(XContentMapValues.nodeStringValue(rSettings.get("enable_stats"), "false"));
        closeIndices = Boolean.valueOf(XContentMapValues.nodeStringValue(rSettings.get("close_indices"), "true"));
        river_esindex = XContentMapValues.nodeStringValue(rSettings.get("river_esindex"), "river");
        es_central_cluster= XContentMapValues.nodeStringValue(rSettings.get("es_central_cluster"), "es-cdaq");
        perbu =  Boolean.valueOf(XContentMapValues.nodeStringValue(rSettings.get("streaminfo_per_bu"), "true"));
        //perbu =  Boolean.valueOf(XContentMapValues.nodeStringValue(rSettings.get("streaminfo_per_bu"), "false"));

        interval = polling_interval;
        
        //Thread settings
        isRunning = true;
        inError = false;
    }


    @Override
    public void run() {
        try {
          updateRiverDoc();
          beforeLoop();
        } catch (IOException e) {
          logger.error("beforeLoop IOEception: ", e);
          inError = true;
          System.exit(3);
          return;
        } catch (Exception e) {
          logger.error("beforeLoop Exception: ", e);
          inError = true;
          System.exit(3);
          return;
        }
        //main loop
        while (isRunning) {

            try {
                mainLoop();
            } catch (IOException e) {
               logger.error("Mainloop IOEception: ", e);
               inError = true;
               System.exit(3);
               break;
            } catch (Exception e) {
               logger.error("Mainloop Exception: ", e);
               inError = true;
               System.exit(3);
               break;
            }   
            

            try {
                Thread.sleep(interval * 1000); // needs milliseconds
            } catch (InterruptedException e) {} //should exit in this case
        }

        try {
            afterLoop();
        } catch (Exception e) {
           logger.error("Afterloop Exception: ", e);
           inError = true;
           System.exit(3);
        }   
 
    }

    public void injectMapping() throws UnknownHostException, IOException {
        beforeLoop();
    }

    public void mainLoop() throws Exception, IOException {
        return;
    }
    public void beforeLoop() throws UnknownHostException, IOException {
        return;
    }
    public void afterLoop() throws Exception, IOException {
        return;
    }

    public void setRunning(boolean running) {
        isRunning = running;
    }
    
    public String riverName() {
        return riverName;
    }

    public JSONObject getJson(String queryName) throws Exception {
        String filename = queryName + ".json" ;
        InputStream is = this.getClass().getResourceAsStream( "/json/" + filename );
        String jsonTxt = IOUtils.toString( is );
        JSONObject json = (JSONObject) JSONSerializer.toJSON( jsonTxt );        
        return json;
    }

    public String getJsonAsString(String queryName) throws Exception {
        String filename = queryName + ".json" ;
        InputStream is = this.getClass().getResourceAsStream( "/json/" + filename );
        String jsonTxt = IOUtils.toString( is );
        return jsonTxt;
    }


    public void updateRiverDoc() throws IOException,InterruptedException,ExecutionException {

        long start_time_millis = System.currentTimeMillis();
        Date start_time = new Date(start_time_millis);
        String sdf_string = sdf.format(start_time); 

        //upda document in river_id in river index
        UpdateRequest updateRequest = new UpdateRequest(river_esindex,riverName);
        updateRequest.doc(
                      jsonBuilder()
                      .startObject()
                      .startObject("node")
                      .field("status","running")
                      .field("ping_timestamp",start_time_millis)
                      .field("ping_time_fmt",sdf_string).endObject().endObject());

        client.update(updateRequest,RequestOptions.DEFAULT);
  } 

    public Boolean collectStats(String rivername, String queryname, String index, SearchResponse sResponse) {
        if(!statsEnabled){return true;}
        
        try {
            IndexRequest indexRequest = new IndexRequest("runriver_stats_write")
              .setRefreshPolicy(RefreshPolicy.IMMEDIATE)
              .source(jsonBuilder()
                    .startObject()
                    .field("rivername", rivername)
                    .field("index", index)
                    .field("query_name", queryname)
                    .field("took", sResponse.getTook().getMillis())
                    .field("timed_out", sResponse.isTimedOut())
                    .field("shards_total", sResponse.getTotalShards())
                    .field("shards_successful", sResponse.getSuccessfulShards())
                    .field("shards_failed", sResponse.getFailedShards())
                    .field("hits_total", sResponse.getHits().getTotalHits())
                    .endObject());
            client.index(indexRequest,RequestOptions.DEFAULT);
            return true;
        } catch (Exception e) {
            logger.error("elasticizeStat exception: ", e);
            return false;
        }
    }

}

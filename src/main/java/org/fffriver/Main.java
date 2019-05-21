package org.fffriver;

//JAVA
import java.io.IOException;
import java.util.Map;
import java.util.HashMap;
//import java.net.InetAddress;
//import java.net.InetSocketAddress;
import org.apache.http.HttpHost;

//ELASTICSEARCH
//import org.elasticsearch.client.Client;

import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.RequestOptions;

import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
//import org.elasticsearch.common.logging.ESLogger;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;

public class Main {

  public static void main(String[] argv) {

    //DNS cache timeout (60 seconds)
    java.security.Security.setProperty("networkaddress.cache.ttl" , "60");

    RestHighLevelClient client;
    RunMonitor rm;
    Collector cd;
    String role;

    //duplicated some entries to enable easier lookup from command line tools
    String river_id = argv[0];
    String river_subsys = argv[1];
    String river_eshost = argv[2];
    String river_escluster = argv[3];
    String river_esindex = argv[4];
    int river_runnumber = Integer.parseInt(argv[5]);
    Logger logger = Loggers.getLogger(Main.class, "river",river_id);
    logger.info( "!" + river_id + " " + river_subsys + " " + river_eshost + " " + river_escluster + " " +river_esindex);

    if (river_id.equals("mapping")) role = "mapping";
    else if (river_runnumber == 0) role = "monitor";
    else role = "collector";

    //start REST client
    try {
      //ES 2.X API:

      //Settings settings = Settings.builder().put("cluster.name", river_escluster).build();
      client =  new RestHighLevelClient(
        RestClient.builder(
          new HttpHost("localhost",9200,"http"))
      );
    }
    catch (Exception e) {
      logger.error("RESTClient exception: ", e);
      System.exit(2);
      return;
    }

    if (role.equals("mapping")){
      try {

        Map <String, Object> settings = new HashMap<String, Object>();
        settings.put("role",role);
        settings.put("es_central_cluster",river_escluster);
        settings.put("subsystem",river_subsys);
        settings.put("runindex_write",river_esindex);//reuse arg

        rm = new RunMonitor(river_id,settings,client);
        rm.injectMapping();
        System.exit(0);
      }
      catch (Exception e) {
        logger.error("Mapping river exception: ", e);
        System.exit(10);
      }
    }
    //get document id from river index
    GetResponse response;
    try {
      GetRequest getRequest = new GetRequest(river_esindex,river_id);
      response = client.get(getRequest, RequestOptions.DEFAULT);
    }
    catch (Exception e) {
      logger.error("Main river exception (GET): ", e);
      System.exit(3);
      return;
    }
    //read params and push into settings Map
    Map <String, Object> settings = new HashMap<String, Object>();
    settings.put("role",role);
    settings.put("subsystem",river_subsys);
    settings.put("runNumber",river_runnumber);
    settings.put("es_local_host", response.getSource().get("es_local_host"));
    settings.put("es_local_cluster", response.getSource().get("es_local_cluster"));
    settings.put("polling_interval",response.getSource().get("polling_interval"));
    settings.put("fetching_interval",response.getSource().get("fetching_interval"));
    settings.put("runindex_read",response.getSource().get("runindex_read"));
    settings.put("runindex_write",response.getSource().get("runindex_write"));
    settings.put("boxinfo_read",response.getSource().get("boxinfo_read"));
    settings.put("enable_stats",response.getSource().get("enable_stats"));
    settings.put("close_indices",response.getSource().get("close_indices"));
    settings.put("river_esindex",river_esindex);
    settings.put("es_central_cluster",river_escluster);

    try {
      if (role.equals("monitor")){
        rm = new RunMonitor(river_id,settings,client);
        rm.run();
      } else if (role.equals("collector")) {
        cd = new Collector(river_id,settings,client);
        cd.run();
      }
      client.close();
    }
    catch (Exception e) {
      logger.error("Main river exception: ", e);
      System.exit(3);
      return;
    }

  }
}

package org.fffriver;

//JAVA
import java.io.IOException;
import java.util.Map;
import java.util.HashMap;
import java.net.InetAddress;
import java.net.InetSocketAddress;

//ELASTICSEARCH
import org.elasticsearch.client.Client;
//import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
//import org.elasticsearch.common.logging.ESLogger;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;

public class Main {

  public static void main(String[] argv) {

    Client client;
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
    //ESLogger logger = Loggers.getLogger("Main", "river",river_id);
    Logger logger = Loggers.getLogger("Main", "river",river_id);

    if (river_runnumber == 0) role = "monitor";
    else role = "collector";

    //start transport client
    try {
      //ES 2.X API:

      Settings settings = Settings.builder().put("cluster.name", river_escluster).build();
      client =  new PreBuiltTransportClient(settings).addTransportAddress(new InetSocketTransportAddress(new InetSocketAddress(InetAddress.getByName(river_eshost), 9300)));
    }
    catch (Exception e) {
      logger.error("elasticizeStat exception: ", e);
      System.exit(2);
      return;
    }

    //get document id from river index
    GetResponse response;
    try {
      response = client.prepareGet(river_esindex,"instance",river_id).get();
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
    settings.put("es_tribe_host", response.getSource().get("es_tribe_host"));
    settings.put("es_tribe_cluster", response.getSource().get("es_tribe_cluster"));
    settings.put("polling_interval",response.getSource().get("polling_interval"));
    settings.put("fetching_interval",response.getSource().get("fetching_interval"));
    settings.put("runIndex_read",response.getSource().get("runIndex_read"));
    settings.put("runIndex_write",response.getSource().get("runIndex_write"));
    try {
      settings.put("boxinfo_read",response.getSource().get("boxinfo_read"));
    }
    catch (Exception e) {
      //fallback to old name
      settings.put("boxinfo_read",response.getSource().get("boxinfo_write"));
    }
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

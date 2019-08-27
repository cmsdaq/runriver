package org.fffriver;

//JAVA
import java.io.IOException;
import java.util.Map;
import java.util.HashMap;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;

import net.sf.json.JSONObject;
import net.sf.json.JSONSerializer;
import org.apache.commons.io.IOUtils;

import org.apache.http.HttpHost;

//ELASTICSEARCH
//import org.elasticsearch.client.Client;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;

import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestClientBuilder.HttpClientConfigCallback;
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
  
  protected static final String es_users_file = "/etc/elasticsearch/users";
  protected static final String es_writer_user = "riverwriter";

  protected static final String escdaq_users_file = "/cmsnfses-web/es-web/AUTH/river-users.jsn";
  protected static final String escdaq_user = "f3root";

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

    String es_user = new String();
    String es_writer_pass = new String();

    if (role.equals("mapping")) {
      es_user = escdaq_user;
      try {
        es_writer_pass = parseRootAuth();
      }
      catch (IOException e) {
        logger.error("IOException reading credentials: ", e);
        System.exit(11);
      }
      catch (Exception e) {
        logger.error("Exception reading credentials: ", e);
        System.exit(12);
      }

    }
    else {
      es_user = es_writer_user;
      try {
        es_writer_pass = parseWriterAuth();
      }
      catch (IOException e) {
        logger.error("IOException reading credentials: ", e);
        System.exit(11);
      }
      catch (Exception e) {
        logger.error("Exception reading credentials: ", e);
        System.exit(12);
      }
    }

    final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
        new UsernamePasswordCredentials(es_user, es_writer_pass));

    //start REST client
    try {
      //Settings settings = Settings.builder().put("cluster.name", river_escluster).build();
      RestClientBuilder builder = RestClient.builder(new HttpHost("localhost", 9200, "http"))
         .setHttpClientConfigCallback(new HttpClientConfigCallback() {
           @Override
           public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
             return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
           }
         });

      client =  new RestHighLevelClient( builder);
      /*
        RestClient.builder(new HttpHost("localhost",9200,"http"))
                  .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                      return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                  });
      );*/
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

  private static String parseRootAuth() throws IOException, Exception {
     InputStream is = new FileInputStream(new File(escdaq_users_file));
     JSONObject json = (JSONObject) JSONSerializer.toJSON(IOUtils.toString(is));
     String writer_pass = json.getJSONObject(escdaq_user).getString("pwd");
     if (writer_pass.isEmpty() || escdaq_user.isEmpty())
       throw new Exception("No elasticsearch user or password found");
     return writer_pass;
  }


  private static String parseWriterAuth() throws IOException, Exception {
     String writer_pass = new String();
     String nextLine;
     String prefix = es_writer_user+":";
     BufferedReader br = new BufferedReader(new FileReader(es_users_file));
     while ((nextLine = br.readLine()) != null) {
       if (nextLine.startsWith(es_writer_user+":")) {
         writer_pass = nextLine.substring(prefix.length());
         break;
       }
     }
     if (writer_pass.isEmpty() || es_writer_user.isEmpty())
       throw new Exception("No elasticsearch user or password found");

     return writer_pass;
  }

}

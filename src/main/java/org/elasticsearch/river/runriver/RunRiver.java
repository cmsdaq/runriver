package org.elasticsearch.river.runriver;

//JAVA
import java.io.IOException;
import java.util.Map;

//ELASTICSEARCH
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.river.AbstractRiverComponent;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.common.settings.Settings;



public class RunRiver extends AbstractRiverComponent implements River {

    
    private final Client client;
    private RunMonitor rm;
    private Collector cd;

    public String role;
    
    @SuppressWarnings({"unchecked"})
    @Inject
    public RunRiver(RiverName riverName, RiverSettings settings, Client client) {
        super(riverName, settings);
        this.client = client;

        //RunRiver Settings
        Map<String, Object> rSettings = settings.settings();
        role = XContentMapValues.nodeStringValue(rSettings.get("role"), "monitor");
    }

    @Override
    public void start() {
        if (role.equals("monitor")){
            rm = new RunMonitor(riverName,settings,client);
            rm.start();
        } else if (role.equals("collector")){
            cd = new Collector(riverName,settings,client );
            cd.start();
        }
    }

    @Override
    public void close() {
        if (role.equals("monitor")){
            rm.setRunning(false);
        }else if (role.equals("collector")){
            cd.setRunning(false);
        }
    }   
}
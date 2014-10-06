package org.elasticsearch.plugin.river.runriver;
import org.elasticsearch.river.runriver.RunRiverModule;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.river.RiversModule;


/**
 *
 */
public class RunRiverPlugin extends AbstractPlugin {

    @Inject
    public RunRiverPlugin() {
    }

    @Override
    public String name() {
        return "river-runriver";
    }

    @Override
    public String description() {
        return "runRiver Plugin";
    }

    public void onModule(RiversModule module) {
        module.registerRiver("runriver", RunRiverModule.class);
    }
}

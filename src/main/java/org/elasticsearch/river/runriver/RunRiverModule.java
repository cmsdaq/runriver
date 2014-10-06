package org.elasticsearch.river.runriver;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.river.River;

/**
 *
 */
public class RunRiverModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(River.class).to(RunRiver.class).asEagerSingleton();
    }
}


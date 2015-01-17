package org.infinispan.ext.demo2;

import org.apache.log4j.Logger;
import org.infinispan.filter.KeyFilter;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.metadata.InternalMetadata;
import org.infinispan.persistence.spi.*;

import java.util.concurrent.Executor;

/**
 * Event persistence worker
 *
 * @author Pavlo Pohrebnyi
 */
public class EventPersistenceWorker implements AdvancedLoadWriteStore<String, Event> {

    // Fields
    private final Logger log = Logger.getLogger(EventPersistenceWorker.class);
    private InitializationContext ctx;
    private String cacheName;

    @Override
    public void init(InitializationContext ctx) {
        log.info("Init");
        this.ctx = ctx;
        cacheName = ctx.getCache().getName();
    }

    @Override
    public void start() {
        log.info("Start");
        // TODO Init data base manager
    }

    @Override
    public void stop() {
        log.info("Stop");
        // TODO Release resources
    }

    @Override
    public void write(MarshalledEntry<? extends String, ? extends Event> entry) {
        log.info("Write Event: " + entry.getValue());
        // TODO Save to the database Event + meta data (or not)
    }

    @Override
    public MarshalledEntry<String, Event> load(Object key) {
        log.info("Load Event by key: " + key);
        // TODO load from database event + meta data (or create from Event object)
        Event event = null;
        InternalMetadata metadata = null;
        MarshalledEntry<String, Event> entry = ctx.getMarshalledEntryFactory().newMarshalledEntry(key, event, metadata);
        // TODO Return entry
        return null;
    }

    @Override
    public boolean contains(Object key) {
        log.info("Contains Event by key: " + key);
        // TODO Implement contains data base operation
        return false;
    }

    @Override
    public boolean delete(Object key) {
        log.info("Delete Event by key: " + key);
        // TODO Implement delete data base operation
        return false;
    }

    @Override
    public void process(KeyFilter<? super String> filter, CacheLoaderTask<String, Event> task, Executor executor, boolean fetchValue, boolean fetchMetadata) {
        log.info("Process");
        // TODO Load not precessed events
    }

    @Override
    public void purge(Executor threadPool, PurgeListener<? super String> listener) {
        log.info("Purge");
        // Nothing
    }

    @Override
    public void clear() {
        log.info("Clear");
        // Nothing
    }

    @Override
    public int size() {
        log.info("Size");
        // TODO Number of not precessed events
        return 0;
    }

}

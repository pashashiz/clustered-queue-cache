package org.infinispan.ext.demo2;

import org.apache.log4j.Logger;
import org.infinispan.ext.queue.QueuesManager;
import org.infinispan.filter.KeyFilter;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.persistence.TaskContextImpl;
import org.infinispan.persistence.spi.*;
import org.infinispan.persistence.spi.PersistenceException;

import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.*;

/**
 * Event storage
 *
 * @author Pavlo Pohrebnyi
 */
public class EventStorage implements CacheWriter<String, Event>, AdvancedCacheLoader<String, Event> {

    // Fields
    private final Logger log = Logger.getLogger(EventStorage.class);
    private InitializationContext ctx;
    private String reason;
    private EventDao eventDao;

    @Override
    public void init(InitializationContext ctx) {
        this.ctx = ctx;
        reason = QueuesManager.decodeName(ctx.getCache().getName());
        log.info("Init events storage for reason [" + reason + "]");
    }

    @Override
    public void start() {
        log.info("Start events storage for reason [" + reason + "]");
        eventDao = new EventDao();
    }

    @Override
    public void stop() {
        log.info("Stop events storage for reason [" + reason + "]");
        // Do nothing
    }

    @Override
    public void write(MarshalledEntry<? extends String, ? extends Event> entry) {
        Event event = entry.getValue();
        log.info("Save event [" + event + "] to database for reason [" + reason + "]");
        try {
            eventDao.saveEvent(event);
        } catch (SQLException e) {
            throw new PersistenceException("Error [write event] database operation", e);
        }
    }

    @Override
    public MarshalledEntry<String, Event> load(Object key) {
        log.info("Get event by id [" + key + "] from database for reason [" + reason + "]");
        try {
            int id = Integer.valueOf((String) key);
            Event event = eventDao.getEvent(id);
            if (event != null) {
                @SuppressWarnings("unchecked")
                MarshalledEntry<String, Event> entry =
                        ctx.getMarshalledEntryFactory().newMarshalledEntry(key, event, null);
                return entry;
            }
            else
                return null;
        } catch (SQLException e) {
            throw new PersistenceException("Error [get event] database operation", e);
        }
    }

    @Override
    public boolean contains(Object key) {
        log.info("Contains event by id [" + key + "] in database for reason [" + reason + "]");
        try {
            int id = Integer.valueOf((String) key);
            return eventDao.getEvent(id) != null;
        } catch (SQLException e) {
            throw new PersistenceException("Error [contains event] database operation", e);
        }
    }

    @Override
    public boolean delete(Object key) {
        log.info("Ignore deleting event by id [" + key + "] in database for reason [" + reason + "]");
        return true;
    }

    @Override
    public void process(KeyFilter<? super String> filter, final CacheLoaderTask<String, Event> task,
                        Executor executor, final boolean fetchValue, boolean fetchMetadata) {
        log.debug("Restore all not processed events in storage for reason [" + reason + "]");
        ExecutorCompletionService<Void> ecs = new ExecutorCompletionService<>(executor);
        Future<Void> future = ecs.submit(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                TaskContext taskContext = new TaskContextImpl();
                // Restore all not processed events for current reason
                List<Event> events = eventDao.getNotProcessedEvents(reason);
                for (Event event : events) {
                    if (taskContext.isStopped()) break;
                    @SuppressWarnings("unchecked")
                    MarshalledEntry<String, Event> entry = ctx.getMarshalledEntryFactory().newMarshalledEntry(
                            String.valueOf(event.getId()), fetchValue ? event : null, null);
                    task.processEntry(entry, taskContext);
                }
                return null;
            }
        });
        try {
            future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            String message = "Error process task for loading events from database for reason [" + reason + "]";
            log.error(message, e);
            throw new PersistenceException(message, e);
        }
    }

    @Override
    public int size() {
        log.info("Number of events in database for reason [" + reason + "]");
        try {
            return eventDao.countNotProcessedEvents(reason);
        } catch (SQLException e) {
            throw new PersistenceException("Error [count not processed events] database operation", e);
        }
    }

}
package org.infinispan.ext.demo2;

import org.apache.log4j.Logger;
import org.infinispan.filter.KeyFilter;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.persistence.TaskContextImpl;
import org.infinispan.persistence.spi.*;

import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.*;

/**
 * Event queues storage
 *
 * @author Pavlo Pohrebnyi
 */
public class QueuesStorage implements AdvancedCacheLoader<String, Boolean> {

    // Fields
    private final Logger log = Logger.getLogger(QueuesStorage.class);
    private InitializationContext ctx;
    private EventDao eventDao;

    @Override
    public void init(InitializationContext ctx) {
        log.info("Init queues storage");
        this.ctx = ctx;
    }

    @Override
    public void start() {
        log.debug("Start queues storage");
        eventDao = new EventDao();
    }

    @Override
    public void stop() {
        log.debug("Stop queues storage");
        // Do nothing
    }

    @Override
    public MarshalledEntry<String, Boolean> load(Object key) {
        log.debug("Get queue by key: [" + key + "] from database");
        // Do nothing
        return null;
    }

    @Override
    public boolean contains(Object key) {
        log.debug("Contains queue by key: [" + key + "] in database");
        // Do nothing
        return true;
    }

    @Override
    public void process(KeyFilter<? super String> filter, final CacheLoaderTask<String, Boolean> task,
                        Executor executor, final boolean fetchValue, final boolean fetchMetadata) {
        log.debug("Restore all not processed queues from database");
        ExecutorCompletionService<Void> ecs = new ExecutorCompletionService<>(executor);
        Future<Void> future = ecs.submit(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                TaskContext taskContext = new TaskContextImpl();
                // Restore all not processed event reason queues
                List<String> reasons = eventDao.getNotProcessedReasons();
                for (String reason : reasons) {
                    if (taskContext.isStopped()) break;
                    @SuppressWarnings("unchecked")
                    MarshalledEntry<String, Boolean> entry = ctx.getMarshalledEntryFactory().newMarshalledEntry(
                            reason, fetchValue ? true : null, null);
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
            String message = "Error process task for loading event queues from database";
            log.error(message, e);
            throw new PersistenceException(message, e);
        }
    }

    @Override
    public int size() {
        log.debug("Number of queues in database");
        try {
            return eventDao.countNotProcessedReasons();
        } catch (SQLException e) {
            throw new PersistenceException("Error [count not processed events] database operation", e);
        }
    }

}
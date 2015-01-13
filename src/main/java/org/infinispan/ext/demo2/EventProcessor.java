package org.infinispan.ext.demo2;

import org.apache.log4j.Logger;
import org.infinispan.ext.queue.CacheEntry;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Dummy event processor (singleton)
 *
 * @author Pavlo Pohrebnyi
 */
public enum EventProcessor {

    // Enum singleton
    INSTANCE;

    // Constants
    public final int THREAD_POOL_CAPACITY = 20;
    // Fields
    private final Logger log = Logger.getLogger(EventProcessor.class);
    private final ExecutorService executor;

    // Creating event processor
    private EventProcessor() {
        executor = Executors.newFixedThreadPool(THREAD_POOL_CAPACITY);
    }

    /**
     * Get event processor instance (singleton)
     *
     * @return Event processor
     */
    public static EventProcessor getInstance() {
        return INSTANCE;
    }

    /**
     * Process event asynchronously
     *
     * @param entry Event entry to process
     */
    public void processEventAsync(final CacheEntry<String, Event> entry) {
        executor.submit(new Runnable() {
            @Override
            public void run() {
                Event event = entry.getValue();
                log.debug("Start of Event processing [" + event + "]...");
                try {
                    Thread.sleep(event.getProcessingTime() * 1000);
                } catch (InterruptedException e) {
                    log.error(e);
                }
                event.setProcessed(true);
                entry.update(event);
                log.debug("Event was processed [" + event + "]");
                EventDispatcher.getInstance().removeEvent(event);
            }
        });
    }

}

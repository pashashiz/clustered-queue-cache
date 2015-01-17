package org.infinispan.ext.demo2;

import org.apache.log4j.Logger;
import org.infinispan.ext.queue.CacheEntry;


/**
 * Dummy event processor (singleton)
 *
 * @author Pavlo Pohrebnyi
 */
public enum EventProcessor {

    // Enum singleton
    INSTANCE;

    // Fields
    private final Logger log = Logger.getLogger(EventProcessor.class);

    // Creating event processor
    private EventProcessor() {}

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
    public void processEvent(final CacheEntry<String, Event> entry) {
        Event event = entry.getValue();
        log.debug("Start of Event processing [" + event + "]...");
        try {
            Thread.sleep(event.getProcessingTime());
        } catch (InterruptedException e) {
            log.error(e);
        }
        event.setProcessed(true);
        entry.update(event);
        log.debug("Event was processed [" + event + "]");
        EventDispatcher.getInstance().removeEvent(event);
    }

}

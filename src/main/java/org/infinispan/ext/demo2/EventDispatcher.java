package org.infinispan.ext.demo2;

import org.apache.log4j.Logger;
import org.infinispan.ext.queue.CacheEntry;
import org.infinispan.ext.queue.QueueCache;
import org.infinispan.ext.queue.QueuesManager;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.notifications.cachelistener.event.CacheEntryCreatedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryModifiedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryRemovedEvent;
import org.infinispan.notifications.cachelistener.event.TopologyChangedEvent;
import org.infinispan.notifications.cachemanagerlistener.event.ViewChangedEvent;
import org.infinispan.remoting.transport.Address;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Event dispatcher (singleton)
 *
 * @author Pavlo Pohrebnyi
 */
public enum EventDispatcher {

    INSTANCE;

    // Constants
    public final String CONFIG_FILE = "infinispan.xml";
    // Logger
    private final Logger log = Logger.getLogger(EventDispatcher.class);
    // Fields
    private final String nodeName;
    protected final QueuesManager<String, Event> queuesManager;
    protected final EmbeddedCacheManager cacheManager;

    /**
     * Create event dispatcher
     */
    private EventDispatcher() {
        nodeName = System.getProperty("nodeName");
        cacheManager = createCacheManagerFromXml();
        // Init queues manager with default configuration and base listener
        queuesManager = new QueuesManager<>(cacheManager, QueuesManager.QueueType.PRIORITY, getQueuesListener());
    }

    /**
     * Get event dispatcher (singleton)
     *
     * @return Event dispatcher
     */
    public static EventDispatcher getInstance() {
        return INSTANCE;
    }

    // Read cache manager configuration from file
    private EmbeddedCacheManager createCacheManagerFromXml() {
        log.debug("Starting a cache manager with an XML configuration");
        try {
            return new DefaultCacheManager(CONFIG_FILE);
        } catch (IOException e) {
            throw new IllegalStateException("Infinispan configuration file is not found", e);
        }
    }

    // Get dispatching listeners
    private QueuesManager.Listener<String, Event> getQueuesListener() {
        return new QueuesManager.Listener<String, Event>() {

            @Override
            public void onQueueAdded(CacheEntryCreatedEvent<String, Boolean> event, QueueCache<String, Event> queue) {
                // Listen all queue events
                listenQueue(event.getKey(), queue);
            }

            @Override
            public void onQueueRestored(String name, QueueCache<String, Event> queue) {
                // Listen all queue events
                listenQueue(name, queue);
            }

            @Override
            public void onQueueRemoved(CacheEntryRemovedEvent<String, Boolean> event, QueueCache<String, Event> queue) {
                // Do nothing
            }

            @Override
            public void onTopologyChanged(TopologyChangedEvent event) {
                // Will be improved
                //for (Map.Entry<String, QueueCache<String, Event>> queueCacheEntry : queuesManager.getExistingQueues().entrySet())
                //    tryProcessQueue(queueCacheEntry.getKey());
            }

        };
    }

    // Listen queue events
    private void listenQueue(final String reason, final QueueCache<String, Event> queue) {
        // Init queue entry listeners
        queue.addListener(new QueueCache.Listener<String, Event>() {
            @Override
            public void onEntryAdded(final CacheEntryCreatedEvent<String, Event> event) {
                // Try to process queue (try to take over queue optimistic lock and process)
                tryProcessQueue(reason);
            }

            @Override
            public void onEntryRestored(CacheEntry<String, Event> entry) {
                // Try to process queue (try to take over queue optimistic lock and process)
                tryProcessQueue(reason);
            }

            @Override
            public void onEntryModified(CacheEntryModifiedEvent<String, Event> event) {
                // TODO Implement for synchronous processing
            }


            @Override
            public void onEntryRemoved(CacheEntryRemovedEvent<String, Event> event) {
                // Try to process queue (try to take over queue optimistic lock and process)
                tryProcessQueue(reason);
            }
        });
    }

    // Try to process queue (try to take over queue optimistic lock and process )
    private void tryProcessQueue(final String reason) {
        // Try to process entry - call temporary thread (will be improved later!)
        Thread process = new Thread(new Runnable() {
            @Override
            public void run() {
                imitationWait();
                // Get replicable queue
                QueueCache<String, Event> queue = getQueue(reason);
                // Check last event - if we can process it
                CacheEntry<String, Event> entry = queue.peek();
                // If queue does not empty
                if (entry != null) {
                    // If event does not has an owner - try to get ownership
                    if (entry.getValue().getOwner() == null) {
                        boolean hasOwnership = entry.updateIfConditional(new CacheEntry.Conditional<Event>() {
                            @Override
                            public boolean checkValue(Event value) {
                                // Atomic double check within transaction
                                return value.getOwner() == null;
                            }
                        }, new CacheEntry.Updater<Event>() {
                            @Override
                            public void update(Event value) {
                                // Update ownership
                                value.setOwner(cacheManager.getAddress().toString());
                            }
                        });
                        // If we have ownership - process event!!!
                        if (hasOwnership)
                            EventProcessor.getInstance().processEventAsync(entry);
                    }
                }
            }
        });
        process.setName("process-" + reason);
        process.start();
    }

    // Get queue by reason
    private QueueCache<String, Event> getQueue(String reason) {
        return queuesManager.createQueue(reason);
    }

    /**
     * Post event to processing
     *
     * @param event Event
     * @return {@code true} - event was posted, {@code false} - otherwise
     */
    public boolean postEvent(Event event) {
        log.debug("Event " + event + " was received be Event Dispatcher");
        QueueCache<String, Event> queue = getQueue(event.getReason());
        queue.add(queue.createEntry(String.valueOf(event.getId()), event));
        return true;
    }

    /**
     * Remove processed event
     *
     * @param event Event
     * @return {@code true} - event was removed, {@code false} - otherwise
     */
    public boolean removeEvent(Event event) {
        QueueCache<String, Event> queue = getQueue(event.getReason());
        // Remove processed event
        queue.poll();
        return true;
    }

    // Imitation of real situation
    private void imitationWait() {
        try {
            Thread.sleep((long) (Math.random() * 1000));
        } catch (InterruptedException e) {
            log.error(e);
        }
    }

}

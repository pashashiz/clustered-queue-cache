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
import org.infinispan.remoting.transport.Address;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Event dispatcher (singleton)
 *
 * @author Pavlo Pohrebnyi
 */
public enum EventDispatcher {

    INSTANCE;

    // Constants
    public final String CONFIG_FILE = "infinispan.xml";
    public final int THREAD_POOL_CAPACITY = 20;
    // Logger
    private final Logger log = Logger.getLogger(EventDispatcher.class);
    // Fields
    protected final QueuesManager<String, Event> queuesManager;
    protected final EmbeddedCacheManager cacheManager;
    private final ExecutorService executor;

    /**
     * Create event dispatcher
     */
    private EventDispatcher() {
        // Executor to process events
        // TODO It will be improved by using of bounded thread pool and not blocking queue
        executor = Executors.newCachedThreadPool();
        // Cache manager with base configuration
        cacheManager = createCacheManagerFromXml();
        // Init queues manager with default configuration and base listener
        queuesManager = new QueuesManager<>(cacheManager, QueuesManager.QueueType.PRIORITY,
                getQueuesListener(), getQueueElementListener(),
                "events-queues", "events");
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

    // Get queue listener
    private QueuesManager.Listener<String, Event> getQueuesListener() {
        return new QueuesManager.Listener<String, Event>() {

            @Override
            public void onQueueAdded(CacheEntryCreatedEvent<String, Boolean> event, QueueCache<String, Event> queue) {
                // Do nothing
            }

            @Override
            public void onQueueRestored(String name, QueueCache<String, Event> queue) {
                // Do nothing
            }

            @Override
            public void onQueueRemoved(CacheEntryRemovedEvent<String, Boolean> event, QueueCache<String, Event> queue) {
                // Do nothing
            }

            @Override
            public void onClusterMembersLost(List<Address> addresses) {
                List<String> owners = new ArrayList<>();
                for (Address address : addresses)
                    owners.add(address.toString());
                for (Map.Entry<String, QueueCache<String, Event>> entry : queuesManager.getExistingQueues().entrySet())
                    tryProcessQueueElement(entry.getKey(), owners);
            }

        };
    }

    // Get queue elements listener
    private QueueCache.Listener<String, Event> getQueueElementListener() {
        return new QueueCache.Listener<String, Event>() {
            @Override
            public void onEntryAdded(final CacheEntryCreatedEvent<String, Event> event) {
                // Try to process queue (try to take over queue optimistic lock and process)
                tryProcessQueueElement(event.getValue().getReason());
            }

            @Override
            public void onEntryRestored(CacheEntry<String, Event> entry) {
                // Try to process queue (try to take over queue optimistic lock and process)
                tryProcessQueueElement(entry.getValue().getReason());
            }

            @Override
            public void onEntryModified(CacheEntryModifiedEvent<String, Event> event) {
                // TODO Implement for synchronous processing
            }


            @Override
            public void onEntryRemoved(CacheEntryRemovedEvent<String, Event> event, String underlyingCacheName) {
                // Try to process queue (try to take over queue optimistic lock and process)
                tryProcessQueueElement(QueuesManager.decodeName(underlyingCacheName));
            }
        };
    }

    /**
     * Try to process next element of the queue (try to take over queue optimistic lock and process events).
     * That method can be used only to process new event
     *
     * @param reason Queue reason
     */
    private void tryProcessQueueElement(final String reason) {
        tryProcessQueueElement(reason, null);
    }

    /**
     * Try to process next element of the queue (try to take over queue optimistic lock and process events).
     * That method can be used to process new event as well as reprocess event of other owners
     *
     * @param reason Queue reason
     * @param ownersToReprocess Owners which events we can be reprocessed
     */
    private void tryProcessQueueElement(final String reason, final List<String> ownersToReprocess) {
        // Try to process entry - call temporary thread (will be improved later!)
        executor.submit(new Runnable() {
            @Override
            public void run() {
                imitationWait();
                // Get replicable queue
                QueueCache<String, Event> queue = getQueue(reason);
                // Check if we can process head element of the queue
                CacheEntry<String, Event> entry = queue.peek();
                // If queue does not empty and it was locked successfully by current node - process it
                if (entry != null && tryLock(entry, ownersToReprocess))
                    EventProcessor.getInstance().processEvent(entry);
            }
        });
    }

    /**
     * Try to acquire the optimistic lock of the event.
     * Event could be locked in case it does not has an owner or events of that owner could be reprocessed
     *
     * @param entry Entry to lock
     * @param ownersToReprocess Owners which events we can be reprocessed
     * @return {@code true} - if event was locked successfully, {@code false} - otherwise
     */
    private boolean tryLock(CacheEntry<String, Event> entry, final List<String> ownersToReprocess) {
        String owner = entry.getValue().getOwner();
        // If event does not has an owner or events of that owner could be reprocessed - try to lock it
        return (owner == null || (ownersToReprocess != null && ownersToReprocess.contains(owner)))
                && entry.updateIfConditional(new CacheEntry.Conditional<Event>() {
            @Override
            public boolean checkValue(Event value) {
                // Atomic double check within transaction
                String owner = value.getOwner();
                return owner == null || (ownersToReprocess != null && ownersToReprocess.contains(owner));
            }
        }, new CacheEntry.Updater<Event>() {
            @Override
            public void update(Event value) {
                // Update ownership if atomic double check is true
                value.setOwner(cacheManager.getAddress().toString());
            }
        });
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
        String reason = event.getReason();
        QueueCache<String, Event> queue = getQueue(reason);
        // Remove processed event
        queue.poll();
        // TODO Risky operation
        if (queue.isEmpty())
            queuesManager.removeQueue(reason);
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

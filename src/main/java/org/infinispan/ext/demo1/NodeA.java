package org.infinispan.ext.demo1;

import org.infinispan.ext.queue.CacheEntry;
import org.infinispan.ext.queue.QueueCache;
import org.infinispan.ext.queue.QueuesManager;
import org.infinispan.notifications.cachelistener.event.CacheEntryCreatedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryModifiedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryRemovedEvent;
import org.infinispan.notifications.cachelistener.event.TopologyChangedEvent;
import org.infinispan.notifications.cachemanagerlistener.event.ViewChangedEvent;
import org.infinispan.util.logging.LogFactory;

import java.io.IOException;

/**
 * Node A to test {@link org.infinispan.ext.queue.QueuesManager}
 *
 * @author Pavlo Pohrebnyi
 */
public class NodeA extends Node {

    /**
     * Create node A (and initialize cache manager and queues manager)
     *
     * @param name Node name
     * @throws IOException
     */
    public NodeA(String name) throws IOException {
        super(name);
        log = LogFactory.getLog(NodeA.class);
        queuesManager.addListener(new QueuesManager.Listener<String, String>() {
            @Override
            public void onQueueAdded(final CacheEntryCreatedEvent<String, Boolean> queueEvent, QueueCache<String, String> queue) {
                log.debugf("New queue [%] was added by %s node",
                        queueEvent.getKey(), (queueEvent.isOriginLocal()) ? "current": "remote");
                queue.addListener(new QueueCache.Listener<String, String>() {
                    @Override
                    public void onEntryAdded(CacheEntryCreatedEvent<String, String> event) {
                        log.debugf("New entry [%] was added in the cache %s by %s node",
                                event.getKey(), queueEvent.getKey(), (event.isOriginLocal()) ? "current": "remote");
                    }

                    @Override
                    public void onEntryRestored(CacheEntry<String, String> entry) {
                        // Do nothing
                    }

                    @Override
                    public void onEntryModified(CacheEntryModifiedEvent<String, String> event) {
                        log.debugf("Entry [%] was modified in the cache %s by %s node",
                                event.getKey(), queueEvent.getKey(), (event.isOriginLocal()) ? "current": "remote");
                    }

                    @Override
                    public void onEntryRemoved(CacheEntryRemovedEvent<String, String> event) {
                        log.debugf("Entry [%] was removed in the cache %s by %s node",
                                event.getKey(), queueEvent.getKey(), (event.isOriginLocal()) ? "current": "remote");
                    }

                });
            }

            @Override
            public void onQueueRestored(String name, QueueCache<String, String> queue) {
                // Do nothing
            }

            @Override
            public void onQueueRemoved(CacheEntryRemovedEvent<String, Boolean> event, QueueCache<String, String> queue) {
                log.debugf("Queue [%] was removed by %s node",
                        event.getKey(), (event.isOriginLocal()) ? "current": "remote");
                queuesManager.removeQueue(event.getKey());
            }

            @Override
            public void onTopologyChanged(TopologyChangedEvent event) {

            }

        });
    }

    @Override
    public void run() {
        QueueCache<String, String> queue = queuesManager.createQueue("queue-A");
        CacheEntry<String, String> entry1 = queue.createEntry("entry-1-A", "value-1-A");
        queue.add(entry1);
    }

}

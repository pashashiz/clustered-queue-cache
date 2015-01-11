package org.infinispan.ext.queue;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryRemoved;
import org.infinispan.notifications.cachelistener.event.CacheEntryCreatedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryRemovedEvent;
import org.infinispan.util.concurrent.ConcurrentHashSet;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Manager of {@linkplain QueueCache queues}, which are based on dynamic caches.
 *
 * <p>In its turn that manager based on static {@linkplain Cache Infinispan queuesManagerCache} and inherit queuesManagerCache behavior.
 * <p>In case when it is uses underlying queuesManagerCache witch is replicated - all queues created by that manager
 * will be replicated through the cluster and dynamically created by other nodes
 * <p>In another case when it is uses local queuesManagerCache - all queues will be like local queuesManagerCache
 *
 * <p>If to create {@link QueuesManager} it was used full constructor
 * {@link QueuesManager#QueuesManager(EmbeddedCacheManager, QueueType, String, String)}
 * you specify behaviour of that manager by changing <b>manager named cache</b> properties
 * and behaviour of queues elements by changing <b>queue named cache</b> properties.
 * Otherwise it will be used default cache properties - <b>synchronous replicated cache</b>
 *
 * @param <K> {@link CacheEntry} key of the queue element
 * @param <V> {@link CacheEntry} value of the queue element
 * @author Pavlo Pohrebniy
 */
public class QueuesManager<K, V> {

    /**
     * Queues manager listener. It listen queues changes in the manager (add, remove queue)
     */
    public interface Listener<K, V> {

        /**
         * Listener of event - Queue was added
         *
         * @param event Cache entry event
         * @param queue Added queue
         */
        public void onQueueAdded(CacheEntryCreatedEvent<String, Boolean> event, QueueCache<K, V> queue);

        /**
         * Listener of event - Queue was removed
         *
         * @param event Cache entry event
         * @param queue Removed queue
         */
        public void onQueueRemoved(CacheEntryRemovedEvent<String, Boolean> event, QueueCache<K, V> queue);

    }

    /**
     * Supported types of {@link QueueCache}
     */
    public enum QueueType { PRIORITY }

    // Logger
    private static final Log log = LogFactory.getLog(QueuesManager.class);

    // Constants
    private static final String CACHE_NAME_PREFIX = "qsm";

    // Fields
    private static int number;
    private final EmbeddedCacheManager cacheManager;
    private final Cache<String, Boolean> queuesManagerCache;
    private final QueueType queuesType;
    private final Configuration queuesConf;
    private final ConcurrentHashMap<String, QueueCache<K, V>> queues = new ConcurrentHashMap<>();
    private final ConcurrentHashSet<Listener<K, V>> listeners = new ConcurrentHashSet<>();
    private final Lock integrityMutex = new ReentrantLock();

    /**
     * Create manager of {@linkplain QueueCache queues}, which are based on dynamic caches.
     * It use default caches configuration for
     * static cache of queues wrappers and dynamic caches of elements of queues
     *
     * @param cacheManager Cache manager
     * @param queuesType {@linkplain QueueType Type} of queues, which will be created by current manager
     */
    public QueuesManager(EmbeddedCacheManager cacheManager, QueueType queuesType) {
        this(cacheManager, queuesType, null, null);
    }

    /**
     * Create manager of {@linkplain QueueCache queues}, which are based on dynamic caches.
     *
     * @param cacheManager Cache manager
     * @param queuesType {@linkplain QueueType Type} of queues, which will be created by current manager
     * @param queuesManagerConfName Name of configuration which will be used to create static cache of queues wrappers
     * @param queuesConfName Name of configuration which will be used to create dynamic caches of element of queues
     */
    public QueuesManager(EmbeddedCacheManager cacheManager, QueueType queuesType,
                         String queuesManagerConfName, String queuesConfName) {
        if (cacheManager == null) throw new NullPointerException("Cache manager cannot be null");
        this.queuesType = queuesType;
        this.cacheManager = cacheManager;
        Configuration queuesManagerConf;
        // Read manager cache configuration if it exists
        if (queuesManagerConfName != null)
            queuesManagerConf = new ConfigurationBuilder()
                    .read(cacheManager.getCacheConfiguration(queuesManagerConfName)).build();
        // Use default manager configuration
        else
            queuesManagerConf = new ConfigurationBuilder()
                    .clustering().cacheMode(CacheMode.REPL_SYNC).build();
        // Define queuesManagerCache manager configuration
        String queuesManagerCacheName = CACHE_NAME_PREFIX + (++number);
        cacheManager.defineConfiguration(queuesManagerCacheName, queuesManagerConf);
        // Read queues cache configuration if it exist
        if (queuesConfName != null)
            queuesConf = new ConfigurationBuilder()
                    .read(cacheManager.getCacheConfiguration(queuesConfName)).build();
            // Use default manager configuration
        else
            queuesConf = new ConfigurationBuilder()
                    .clustering().cacheMode(CacheMode.REPL_SYNC).build();
        // Create cache for Queues Manager
        queuesManagerCache = cacheManager.getCache(queuesManagerCacheName);
        // Add listener (cache level)
        queuesManagerCache.addListener(new LocalListener());
        // Restore queues which are existing in the cache
        restoreQueues();
    }

    // If current node was connected to existing cluster, we need to restore all existing queues
    private void restoreQueues() {
        log.debug("Try to restore existing queues in the cluster...");
        try {
            integrityMutex.lock();
            for (Map.Entry<String, Boolean> entry : queuesManagerCache.entrySet())
                createQueueLocal(entry.getKey());
        } finally {
            integrityMutex.unlock();
        }
        if (queues.isEmpty())
            log.debug("No existing queues was found in the cluster");
        else
            log.debugf("%d queues was restored", queues.size());
    }

    /**
     * Create queue (based on dynamic cache).
     * If replicated cache mode is used for static cache of queues wrappers
     * - that queue will be replicated across the cluster.
     *
     * @param name Name of the cache queue
     * @return Created cache queue
     */
    public QueueCache<K, V> createQueue(String name) {
        String queueCacheName = createQueueCacheName(name);
        if (!containsQueue(name)) {
            // Share message [Replicated Queue is created] across cluster
            // After all Nodes get that message, event will be processed in current Thread for current Node
            queuesManagerCache.put(queueCacheName, true);
        }
        return queues.get(queueCacheName);
    }

    // Create queue, but do not put it to the cache (only local storage)
    private QueueCache<K, V> createQueueLocal(String name) {
        // Generate new Queue
        QueueCache<K, V> queue = null;
        log.debugf("New local queue instance [%s] is created in current node", name);
        String queueCacheName = createQueueCacheName(name);
        cacheManager.defineConfiguration(queueCacheName, queuesConf);
        Cache<K, V> queueCache = cacheManager.getCache(queueCacheName);
        // Create queue
        switch (queuesType) {
            case PRIORITY: queue = new PriorityQueueCache<>(queueCache);
        }
        // Thread safe put Replicable Queue to in memory local storage
        queues.putIfAbsent(name, queue);
        return queue;
    }

    /**
     * Get cache queue
     *
     * @param name Queue name
     * @return Cache queue
     */
    public QueueCache<K, V> getQueue(String name) {
        String queueCacheName = createQueueCacheName(name);
        try {
            integrityMutex.lock();
            return (queuesManagerCache.containsKey(queueCacheName)) ? queues.get(queueCacheName) : null;
        } finally {
            integrityMutex.unlock();
        }
    }

    /**
     * Check if manager is contains cache queue
     *
     * @param name Queue name
     * @return {@code true} - queue exists {@code false} - otherwise
     */
    public boolean containsQueue(String name) {
        String queueCacheName = createQueueCacheName(name);
        try {
            integrityMutex.lock();
            return (queuesManagerCache.containsKey(queueCacheName)) && queues.containsKey(queueCacheName);
        } finally {
            integrityMutex.unlock();
        }
    }

    /**
     * Remove cache queue by name
     *
     * @param name Queue name
     */
    public void removeQueue(String name) {
        if (containsQueue(name)) {
            // Share message [Replicated Queue is removed] across cluster
            // After all Nodes get that message, event will be processed in current Thread for current Node
            queuesManagerCache.remove(createQueueCacheName(name));
        }
    }

    protected String createQueueCacheName(String name) {
        return CACHE_NAME_PREFIX + "-" + name;
    }

    protected String parseName(String queueCacheName) {
        return queueCacheName.replaceFirst("^" + CACHE_NAME_PREFIX + "-", "");
    }

    /**
     * Add {@linkplain Listener queue listener}
     *
     * @param listener Queue listener to add
     */
    public void addListener(Listener<K, V> listener) {
        listeners.add(listener);
    }

    /**
     * Remove {@linkplain Listener queue listener}
     *
     * @param listener Queue listener to remove
     */
    public void removeListener(Listener<K, V> listener) {
        listeners.remove(listener);
    }

    /**
     * Remove all {@linkplain Listener queue listeners}
     */
    public void removeAlListeners() {
        listeners.clear();
    }

    /**
     * Local listener. It listens all queues manager changes
     */
    @org.infinispan.notifications.Listener
    private class LocalListener {

        @CacheEntryCreated
        public void onAdded(CacheEntryCreatedEvent<String, Boolean> event) {
            if (event.isPre()) return;
            log.debug("Queue {" + event.getKey() + ": " + event.getValue() + "} was added in queuesManagerCache "
                    + event.getCache());
            QueueCache<K, V> queue = null;
            try {
                integrityMutex.lock();
                if (!queues.containsKey(event.getKey()))
                    queue = createQueueLocal(event.getKey());
            } finally {
                integrityMutex.unlock();
            }
            // Fire on added event
            for (Listener<K, V> listener : listeners)
                listener.onQueueAdded(event, queue);
        }

        @CacheEntryRemoved
        public void onRemoved(CacheEntryRemovedEvent<String, Boolean> event) {
            if (event.isPre()) return;
            log.debug("Queue {" + event.getKey() + ": " + event.getValue() + "} was removed from queuesManagerCache "
                    + event.getCache());
            QueueCache<K, V> queue = null;
            try {
                integrityMutex.lock();
                if (queues.containsKey(event.getKey()))
                    queue = queues.remove(event.getKey());
            } finally {
                integrityMutex.unlock();
            }
            // Fire on remove event
            for (Listener<K, V> listener : listeners)
                listener.onQueueRemoved(event, queue);
        }

    }

}
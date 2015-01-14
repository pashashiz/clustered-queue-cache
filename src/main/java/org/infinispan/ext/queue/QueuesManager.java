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
import org.infinispan.notifications.cachemanagerlistener.annotation.ViewChanged;
import org.infinispan.notifications.cachemanagerlistener.event.ViewChangedEvent;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.lookup.JBossStandaloneJTAManagerLookup;
import org.infinispan.transaction.LockingMode;
import org.infinispan.util.concurrent.ConcurrentHashSet;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Manager of {@linkplain QueueCache queues}, which are based on dynamic caches.
 *
 * <p>In its turn that manager based on static {@linkplain Cache Infinispan queuesManagerCache} and inherit cache behavior.
 * <p>In case when it is uses underlying cache witch is replicated - all queues created by that manager
 * will be replicated through the cluster and dynamically created by other nodes
 * <p>In another case when it is uses local cache - all queues will be like local cache
 *
 * <p>If to create {@link QueuesManager} it was used full constructor
 * {@link QueuesManager#QueuesManager(EmbeddedCacheManager, QueuesManager.QueueType, QueuesManager.Listener, String, String)}
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
         * Listener of event - Queue was restored
         *
         * @param name Queue name
         * @param queue Restored queue
         */
        public void onQueueRestored(String name, QueueCache<K, V> queue);

        /**
         * Listener of event - Queue was removed
         *
         * @param event Cache entry event
         * @param queue Removed queue
         */
        public void onQueueRemoved(CacheEntryRemovedEvent<String, Boolean> event, QueueCache<K, V> queue);

        /**
         * Listener of event - Cluster members were lost
         *
         * @param addresses Lost cluster members addresses
         */
        public void onClusterMembersLost(List<Address> addresses);

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
    private final Lock consistentMutex = new ReentrantLock();

    /**
     * Create manager of {@linkplain QueueCache queues}, which are based on dynamic caches.
     * It use default caches configuration for
     * static cache of queues wrappers and dynamic caches of elements of queues
     *
     * @param cacheManager Cache manager
     * @param queuesType {@linkplain QueueType Type} of queues, which will be created by current manager
     * @param listener Started listener
     */
    public QueuesManager(EmbeddedCacheManager cacheManager, QueueType queuesType, Listener<K, V> listener) {
        this(cacheManager, queuesType, listener, null, null);
    }

    /**
     * Create manager of {@linkplain QueueCache queues}, which are based on dynamic caches.
     *
     * @param cacheManager Cache manager
     * @param queuesType {@linkplain QueueType Type} of queues, which will be created by current manager
     * @param listener Started listener
     * @param queuesManagerConfName Name of configuration which will be used to create static cache of queues wrappers
     * @param queuesConfName Name of configuration which will be used to create dynamic caches of element of queues
     */
    public QueuesManager(EmbeddedCacheManager cacheManager, QueueType queuesType, Listener<K, V> listener,
                         String queuesManagerConfName, String queuesConfName) {
        if (cacheManager == null) throw new NullPointerException("Cache manager cannot be null");
        this.queuesType = queuesType;
        this.cacheManager = cacheManager;
        if (listener != null)
            this.listeners.add(listener);
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
                    .clustering().cacheMode(CacheMode.REPL_SYNC)
                    .transaction().transactionManagerLookup(new JBossStandaloneJTAManagerLookup())
                    .lockingMode(LockingMode.PESSIMISTIC).build();
        // Create cache for Queues Manager
        queuesManagerCache = cacheManager.getCache(queuesManagerCacheName);
        // Add listeners (cache level and manager level)
        LocalListener localListener = new LocalListener();
        queuesManagerCache.addListener(localListener);
        cacheManager.addListener(localListener);
        // Restore queues which are existing in the cache
        restoreQueues();
    }

    // If current node was connected to existing cluster, we need to restore all existing queues
    private void restoreQueues() {
        log.debug("Try to restore existing queues in the cluster...");
        try {
            consistentMutex.lock();
            for (Map.Entry<String, Boolean> entry : queuesManagerCache.entrySet()) {
                QueueCache<K, V> queue = createQueueLocal(entry.getKey());
                // Fire restore event
                for (Listener<K, V> listener : listeners)
                    listener.onQueueRestored(entry.getKey(), queue);
            }
        } finally {
            consistentMutex.unlock();
        }
        if (queues.isEmpty())
            log.debug("No existing queues was found in the cluster");
        else
            log.debugf("%d queues was restored", queues.size());
    }

    /**
     * Encode queue name to as dynamic cache name (to avoid collision)
     *
     * @param name Queue base name
     * @return Queue encoded name
     */
    protected String encodeName(String name) {
        return CACHE_NAME_PREFIX + "-" + name;
    }

    /**
     * Decode dynamic cache name to get based queue name (to avoid collision)
     *
     * @param name Dynamic cache name (encoded queue name)
     * @return Based queue name
     */
    protected String decodeName(String name) {
        return name.replaceFirst("^" + CACHE_NAME_PREFIX + "-", "");
    }

    /**
     * Create queue.
     * If replicated cache mode is used for static cache of queues wrappers
     * - that queue will be replicated across the cluster.
     *
     * @param name Name of the queue
     * @return Created cache queue
     */
    public QueueCache<K, V> createQueue(String name) {
        // TODO Later: Could be improved using transaction
        if (!containsQueue(name)) {
            // Share message [Replicated Queue is created] across cluster
            // After all Nodes get that message, event will be processed in current Thread for current Node
            queuesManagerCache.put(name, true);
        }
        return queues.get(name);
    }

    /**
     * Create queue, but do not put it to the cache (only local storage)
     *
     * @param name Name of the queue
     * @return Created cache queue
     */
    private QueueCache<K, V> createQueueLocal(String name) {
        // Generate new Queue
        QueueCache<K, V> queue = null;
        log.debugf("New local queue instance [%s] is created in current node", name);
        String queueCacheName = encodeName(name);
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
        try {
            consistentMutex.lock();
            return (queuesManagerCache.containsKey(name)) ? queues.get(name) : null;
        } finally {
            consistentMutex.unlock();
        }
    }

    /**
     * Get existing queues
     *
     * @return Existing queues
     */
    public ConcurrentHashMap<String, QueueCache<K, V>> getExistingQueues() {
        return queues;
    }

    /**
     * Check if manager is contains cache queue
     *
     * @param name Queue name
     * @return {@code true} - queue exists {@code false} - otherwise
     */
    public boolean containsQueue(String name) {
        try {
            consistentMutex.lock();
            return (queuesManagerCache.containsKey(name)) && queues.containsKey(name);
        } finally {
            consistentMutex.unlock();
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
            queuesManagerCache.remove(encodeName(name));
        }
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
            String name = event.getKey();
            log.debug("Queue {" + name + ": " + event.getValue() + "} was added in cache "
                    + event.getCache());
            QueueCache<K, V> queue = null;
            try {
                consistentMutex.lock();
                if (!queues.containsKey(name))
                    queue = createQueueLocal(name);
            } finally {
                consistentMutex.unlock();
            }
            // Fire on added event
            for (Listener<K, V> listener : listeners)
                listener.onQueueAdded(event, queue);
        }

        @CacheEntryRemoved
        public void onRemoved(CacheEntryRemovedEvent<String, Boolean> event) {
            if (event.isPre()) return;
            String name = event.getKey();
            log.debug("Queue {" + name + ": " + event.getValue() + "} was removed from cache "
                    + event.getCache());
            QueueCache<K, V> queue = null;
            try {
                consistentMutex.lock();
                // Remove local queue
                if (queues.containsKey(name))
                    queue = queues.remove(name);
                // Remove dynamic cache
                cacheManager.removeCache(decodeName(name));
            } finally {
                consistentMutex.unlock();
            }
            // Fire on remove event
            for (Listener<K, V> listener : listeners)
                listener.onQueueRemoved(event, queue);
        }

        @ViewChanged
        public void onViewChanged(ViewChangedEvent event) {
            List<Address> oldMembers = event.getOldMembers();
            List<Address> currentMembers = event.getNewMembers();
            List<Address> lostMembers = new ArrayList<>();
            for (Address old : oldMembers)
                if (!currentMembers.contains(old))
                    lostMembers.add(old);
            if (!lostMembers.isEmpty())
                log.debugf("It was lost %d cluster node%s %s",
                        lostMembers.size(), lostMembers.size() > 0 ? "s" : "", lostMembers);
            // Fire on lost cluster members were lost event
            for (Listener<K, V> listener : listeners)
                listener.onClusterMembersLost(lostMembers);
        }

    }

}
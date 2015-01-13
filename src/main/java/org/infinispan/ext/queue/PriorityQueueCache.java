package org.infinispan.ext.queue;

import org.infinispan.notifications.cachelistener.annotation.TopologyChanged;
import org.infinispan.notifications.cachelistener.event.TopologyChangedEvent;
import org.infinispan.util.concurrent.ConcurrentHashSet;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.Cache;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryRemoved;
import org.infinispan.notifications.cachelistener.event.CacheEntryCreatedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryModifiedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryRemovedEvent;

import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Unbounded priority {@link QueueCache}.
 * The elements of the priority queue {@link CacheEntry} are ordered according to their
 * {@linkplain Comparable natural ordering}, or by a {@link Comparator} provided at queue construction time,
 * depending on which constructor is used. A priority queue does not permit {@code null} elements.
 * A priority queue relying on natural ordering also does not permit insertion of non-comparable objects
 * (doing so may result in {@code ClassCastException}).
 *
 * <p>That queue based on {@linkplain Cache Infinispan cache} and inherit cache behavior.
 * It means that elements of that queue could be distributed among the cluster (if it is supported by cache).
 * To make all queue replicated you have to use {@link QueuesManager}
 *
 * @param <K> {@link CacheEntry} key
 * @param <V> {@link CacheEntry} value
 * @author Pavlo Pohrebniy
 */
public class PriorityQueueCache<K, V> 
        extends AbstractQueue<CacheEntry<K, V>> implements QueueCache <K, V>{

    /**
     * Cache entry of {@link PriorityQueueCache}.
     * That object based on real cache and reflects some element of that cache.
     * It could has two states:
     * <ul>
     *     <li>binded - it is connected to real cache (all elements added to queue are binded)</li>
     *     <li>unbinded - it is not connected to real cache (if that element does not belongs any queue yet)</li>
     * </ul>
     *
     * @param <K> {@link CacheEntry} key
     * @param <V> {@link CacheEntry} value
     */
    public static class PriorityCacheEntry<K, V> implements CacheEntry<K, V> {

        // Fields
        private K key;
        private V value;
        private Cache<K, V> cache;
        private boolean isCacheBinded;
        private final Lock mutex;

        /**
         * Create priority cache entry.
         * It will unbinded after creating before it will placed into the queue
         *
         * @param key Cache entry key
         * @param value Cache entry value
         */
        public PriorityCacheEntry(K key, V value) {
            mutex = new ReentrantLock();
            this.key = key;
            this.value = value;
        }

        @Override
        public K getKey() {
            return key;
        }

        @Override
        public V getValue() {
            try {
                mutex.lock();
                return (isCacheBinded) ? cache.get(key) : value;
            } finally {
                mutex.unlock();
            }
        }

        @Override
        public void update(V value) {
            try {
                mutex.lock();
                if (isCacheBinded)
                    cache.put(key, value);
                else
                    this.value = value;
            } finally {
                mutex.unlock();
            }
        }

        private void bindCache(Cache<K, V> cache, boolean updateCache) {
            try {
                mutex.lock();
                isCacheBinded = true;
                this.cache = cache;
                if (updateCache)
                    cache.put(key, value);
                value = null;
            } finally {
                mutex.unlock();
            }
        }

        private void unbindCache() {
            try {
                mutex.lock();
                isCacheBinded = false;
                value = cache.get(key);
                cache.remove(key);
                cache = null;
            } finally {
                mutex.unlock();
            }
        }

        @Override
        public boolean isCacheBinded() {
            try {
                mutex.lock();
                return isCacheBinded;
            } finally {
                mutex.unlock();
            }
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) return true;
            if (obj == null) return false;
            if (obj instanceof PriorityCacheEntry) {
                PriorityCacheEntry<K, V> entry = (PriorityCacheEntry<K, V>) obj;
                return (key == null ? entry.key == null : key.equals(entry.key));
            }
            return false;
        }

        @Override
        public int hashCode() {
            return (key == null ? 0 : key.hashCode());
        }

        @Override
        public String toString() {
            return String.format("Cache Entry: {key: %s, value: %s}", getKey(), getValue());
        }

    }

    // Logger
    private static final Log log = LogFactory.getLog(PriorityQueueCache.class);
    // Fields
    private final Cache<K, V> cache;
    private final PriorityQueue<CacheEntry<K, V>> queue;
    private final Lock mutex = new ReentrantLock();
    private final Lock integrityMutex = new ReentrantLock();;
    private final ConcurrentHashSet<Listener<K, V>> listeners = new ConcurrentHashSet<>();

    /**
     * Create unbounded priority {@link QueueCache}.
     * <p>The elements of the priority queue {@link CacheEntry} is based on passed {@link Cache}
     * and are ordered according to their {@linkplain Comparable natural ordering}
     *
     * @param cache {@link Cache} which will be based for the priority queue
     */
    PriorityQueueCache(Cache<K, V> cache) {
        this(cache, null);
    }

    /**
     * Create unbounded priority {@link QueueCache}.
     * <p>The elements of the priority queue {@link CacheEntry} is based on passed {@link Cache}
     * and are ordered according specified comparator
     *
     * @param cache {@link Cache} which will be based for the priority queue
     * @param comparator Comparator to order the elements of the priority queue
     */
    PriorityQueueCache(Cache<K, V> cache, Comparator<? super CacheEntry<K, V>> comparator) {
        queue = (comparator != null) ? new PriorityQueue<>(comparator) : new PriorityQueue<CacheEntry<K, V>>();
        this.cache = cache;
        this.cache.addListener(new LocalListener());
        restoreElements();
    }

    // If current node was connected to existing cluster, we need to restore all existing elements in the queue
    private void restoreElements() {
        try {
            log.debug("Try to restore existing element of queue in the cluster...");
            integrityMutex.lock();
            for (Map.Entry<K, V> entry : cache.entrySet()) {
                PriorityCacheEntry<K, V> cacheEntry = new PriorityCacheEntry<>(entry.getKey(), entry.getValue());
                cacheEntry.bindCache(cache, false);
                queue.add(cacheEntry);
            }
            if (queue.isEmpty())
                log.debug("No existing elements of queues was found in the cluster");
            else
                log.debugf("%d elements of queue was restored", queue.size());
        } finally {
            integrityMutex.unlock();
        }
    }

    @Override
    public CacheEntry<K, V> createEntry(K key, V value) {
        return new PriorityCacheEntry<>(key, value);
    }

    @Override
    public boolean add(CacheEntry<K, V> kvCacheEntry) {
        return offer(kvCacheEntry);
    }

    @Override
    public boolean offer(CacheEntry<K, V> kvCacheEntry) {
        try {
            mutex.lock();
            ((PriorityCacheEntry<K, V>)kvCacheEntry).bindCache(cache, true);
            return true;
        } finally {
            mutex.unlock();
        }
    }

    @Override
    public CacheEntry<K, V> peek() {
        try {
            mutex.lock();
            integrityMutex.lock();
            return queue.peek();
        } finally {
            integrityMutex.unlock();
            mutex.unlock();
        }
    }

    @Override
    public CacheEntry<K, V> element() {
        CacheEntry<K, V> x = peek();
        if (x != null)
            return x;
        else
            throw new NoSuchElementException();
    }

    @Override
    public CacheEntry<K, V> poll() {
        try {
            mutex.lock();
            CacheEntry<K, V> entry = queue.peek();
            ((PriorityCacheEntry<K, V>) entry).unbindCache();
            return entry;
        } finally {
            mutex.unlock();
        }
    }

    @Override
    public CacheEntry<K, V> remove() {
        CacheEntry<K, V> x = poll();
        if (x != null)
            return x;
        else
            throw new NoSuchElementException();
    }

    /**
     * Returns the comparator used to order the elements in this queue, or <tt>null</tt> if this queue uses
     * the {@linkplain Comparable natural ordering} of its elements.
     *
     * @return the comparator used to order the elements in this queue, or <tt>null</tt> if this queue uses the natural
     *         ordering of its elements
     */
    public Comparator<? super CacheEntry<K, V>> comparator() {
        return queue.comparator();
    }

    @Override
    public int size() {
        try {
            mutex.lock();
            integrityMutex.lock();
            return queue.size();
        } finally {
            integrityMutex.unlock();
            mutex.unlock();
        }
    }

    @Override
    public boolean remove(Object o) {
        try {
            mutex.lock();
            integrityMutex.lock();
            return (cache.remove(((PriorityCacheEntry<K, V>) o).key) != null);
        } finally {
            integrityMutex.unlock();
            mutex.unlock();
        }
    }

    @Override
    public boolean contains(Object o) {
        try {
            mutex.lock();
            integrityMutex.lock();
            return queue.contains(o);
        } finally {
            integrityMutex.unlock();
            mutex.unlock();
        }
    }

    public String toString() {
        try {
            mutex.lock();
            integrityMutex.lock();
            return queue.toString();
        } finally {
            integrityMutex.unlock();
            mutex.unlock();
        }
    }

    @Override
    public Object[] toArray() {
        try {
            mutex.lock();
            integrityMutex.lock();
            return queue.toArray();
        } finally {
            integrityMutex.unlock();
            mutex.unlock();
        }
    }

    @Override
    public <T> T[] toArray(T[] a) {
        try {
            mutex.lock();
            integrityMutex.lock();
            return queue.toArray(a);
        } finally {
            integrityMutex.unlock();
            mutex.unlock();
        }
    }

    @Override
    public void clear() {
        try {
            mutex.lock();
            cache.clear();
        } finally {
            mutex.unlock();
        }
    }

    @Override
    public Iterator<CacheEntry<K, V>> iterator() {
        return new Itr(toArray());
    }

    /**
     * Snapshot iterator that works off copy of underlying q array.
     */
    private class Itr implements Iterator<CacheEntry<K, V>> {

        // Array of all elements
        final Object[] array;
        // Index of next element to return;
        int cursor;
        // Index of last element, or -1 if no such
        int lastRet;

        Itr(Object[] array) {
            lastRet = -1;
            this.array = array;
        }

        public boolean hasNext() {
            return cursor < array.length;
        }

        public CacheEntry<K, V> next() {
            if (cursor >= array.length)
                throw new NoSuchElementException();
            lastRet = cursor;
            return (CacheEntry<K, V>)array[cursor++];
        }

        public void remove() {
            if (lastRet < 0)
                throw new IllegalStateException();
            Object x = array[lastRet];
            lastRet = -1;
            // Traverse underlying queue to find == element, not just a .equals element.
            mutex.lock();
            try {
                for (Iterator<CacheEntry<K, V>> it = queue.iterator(); it.hasNext(); ) {
                    CacheEntry<K, V> entry = it.next();
                    if (entry == x) {
                        cache.remove(entry);
                        return;
                    }
                }
            } finally {
                mutex.unlock();
            }
        }

    }

    @Override
    public void addListener(Listener<K, V> listener) {
        listeners.add(listener);
    }

    @Override
    public void removeListener(Listener<K, V> listener) {
        listeners.remove(listener);
    }

    @Override
    public void removeAlListeners() {
        listeners.clear();
    }

    /**
     * Local listener. It listens all queue elements changes
     */
    @org.infinispan.notifications.Listener
    private class LocalListener {

        @CacheEntryCreated
        public void onAdded(CacheEntryCreatedEvent<K, V> event) {
            if (event.isPre()) return;
            log.debugf("Entry {%s: %s} was added in the cache: %s", event.getKey(), event.getValue(), event.getCache());
            try {
                integrityMutex.lock();
                // Replicate adding event in a remote Node
                CacheEntry<K, V> cacheEntry = new PriorityCacheEntry<>(event.getKey(), event.getValue());
                ((PriorityCacheEntry<K, V>)cacheEntry).bindCache(cache, false);
                queue.add(cacheEntry);
            } finally {
                integrityMutex.unlock();
            }
            // Fire on added event
            for (Listener<K, V> listener : listeners)
                listener.onEntryAdded(event);
        }

        @CacheEntryModified
        public void onModified(CacheEntryModifiedEvent<K, V> event) {
            if (event.isPre()) return;
            // Fire on modified event
            for (Listener<K, V> listener : listeners)
                listener.onEntryModified(event);
        }

        @CacheEntryRemoved
        public void onRemoved(CacheEntryRemovedEvent<K, V> event) {
            if (event.isPre()) return;
            log.debugf("Entry {%s: %s} was removed from the cache %s", event.getKey(), event.getValue(), event.getCache());
            integrityMutex.lock();
            // Replicate removing event in a remote Node
            try {
                queue.remove(new PriorityCacheEntry<>(event.getKey(), event.getValue()));
            } finally {
                integrityMutex.unlock();
            }
            // Fire on removed event
            for (Listener<K, V> listener : listeners)
                listener.onEntryRemoved(event);
        }

        @TopologyChanged
        public void onTopologyChanged(TopologyChangedEvent<String, String> event) {
            if (event.isPre()) return;
            // Fire on topology changed event
            for (Listener<K, V> listener : listeners)
                listener.onTopologyChanged(event);
        }

    }

}



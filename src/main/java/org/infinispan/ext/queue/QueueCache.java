package org.infinispan.ext.queue;

import org.infinispan.notifications.cachelistener.event.CacheEntryCreatedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryModifiedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryRemovedEvent;
import org.infinispan.notifications.cachelistener.event.TopologyChangedEvent;

import java.util.Queue;

/**
 * {@link Queue} based on cache.
 * The element of queue is {@link CacheEntry}
 *
 * @param <K> {@link CacheEntry} key
 * @param <V> {@link CacheEntry} value
 * @author Pavlo Pohrebniy
 */
public interface QueueCache<K, V> extends Queue<CacheEntry<K, V>> {

    /**
     * Queue cache listener. It listen entry changes in the underlying queue
     *
     * @param <K> {@link CacheEntry} key
     * @param <V> {@link CacheEntry} value
     */
    public interface Listener<K, V> {

        /**
         * Listener of event - Cache entry was added
         *
         * @param event Cache entry event
         */
        public void onEntryAdded(CacheEntryCreatedEvent<K, V> event);

        /**
         * Listener of event - Cache entry was modified
         *
         * @param event Cache entry event
         */
        public void onEntryModified(CacheEntryModifiedEvent<K, V> event);

        /**
         * Listener of event - Cache entry was removed
         *
         * @param event Cache entry event
         */
        public void onEntryRemoved(CacheEntryRemovedEvent<K, V> event);

        /**
         * Listener of event - Topology was changed
         *
         * @param event Topology changing event
         */
        public void onTopologyChanged(TopologyChangedEvent<String, String> event);

    }

    /**
     * Create cache entry
     *
     * @param key Key
     * @param value Value
     * @return New unbined cache entry
     */
    public CacheEntry<K, V> createEntry(K key, V value);

    /**
     * Add {@linkplain QueueCache.Listener queue listener}
     *
     * @param listener Queue listener to add
     */
    public void addListener(Listener<K, V> listener);

    /**
     * Remove {@linkplain QueueCache.Listener queue listener}
     *
     * @param listener Queue listener to remove
     */
    public void removeListener(Listener<K, V> listener);

    /**
     * Remove all {@linkplain QueueCache.Listener queue listeners}
     */
    public void removeAlListeners();

}

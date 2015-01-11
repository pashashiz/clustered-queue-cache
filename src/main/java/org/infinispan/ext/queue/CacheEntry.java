package org.infinispan.ext.queue;

/**
 * Cache entry.
 * That object based on real cache and reflects some element of that cache.
 * It could has two states:
 * <ul>
 *     <li>binded - it is connected to real cache (all elements added to queue are binded)</li>
 *     <li>unbinded - it is not connected to real cache (if that element does not belongs any queue yet)</li>
 * </ul>
 *
 * @param <K> Entry key
 * @param <V> Entry value
 * @author Pavlo Pohrebnyi
 */
public interface CacheEntry<K,V> {

    /**
     * Get key
     *
     * @return Cache entry key
     */
    public K getKey();

    /**
     * Get value
     *
     * @return Cache entry value (if entry is binded - value from cache, otherwise - local copy)
     */
    public V getValue();

    /**
     * Update cache entry value,
     * (if entry is binded - it will update value from cache, otherwise - local copy)
     *
     * @param value Cache entry value
     */
    public void update(V value);

    /**
     * Check if cache is binded (it is belongs a queue)
     *
     * @return {@code true} - cache is binded, {@code false} otherwise
     */
    public boolean isCacheBinded();

}

package org.infinispan.ext.demo1;

import org.infinispan.ext.queue.QueuesManager;
import org.infinispan.Cache;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;

/**
 * Based node class to test {@link QueuesManager}
 *
 * @author Pavlo Pohrebnyi
 */
public abstract class Node {

    // Constants
    private static final String CONFIG_FILE = "infinispan.xml";
    // Logger
    protected Log log = LogFactory.getLog(Node.class);
    // Fields
    protected final String nodeName;
    protected volatile boolean stop = false;
    protected final QueuesManager<String, String> queuesManager;
    protected final EmbeddedCacheManager cacheManager;

    /**
     * Create node (and initialize cache manager and queues manager)
     *
     * @param name Node name
     * @throws IOException
     */
    public Node(String name) throws IOException {
        nodeName = name;
        cacheManager = createCacheManagerFromXml();
        // Init queues manager with default configuration
        queuesManager = new QueuesManager<>(cacheManager, QueuesManager.QueueType.PRIORITY, null);
    }

    private EmbeddedCacheManager createCacheManagerFromXml() throws IOException {
        log.info("Starting a cache manager with an XML configuration");
        System.setProperty("nodeName", nodeName);
        return new DefaultCacheManager(CONFIG_FILE);
    }

    /**
     * Run node logic
     */
    public abstract void run();

    /**
     * Print cache content
     *
     * @param cache Cache to print
     */
    protected void printCacheContents(Cache<String, String> cache) {
        log.infof("Cache contents on node %s\n", cache.getAdvancedCache().getRpcManager().getAddress());
        ArrayList<Map.Entry<String, String>> entries = new ArrayList<>(cache.entrySet());
        Collections.sort(entries, new Comparator<Map.Entry<String, String>>() {
            @Override
            public int compare(Map.Entry<String, String> o1, Map.Entry<String, String> o2) {
                return o1.getKey().compareTo(o2.getKey());
            }
        });
        for (Map.Entry<String, String> e : entries)
            log.infof("\t%s = %s\n", e.getKey(), e.getValue());
    }

}

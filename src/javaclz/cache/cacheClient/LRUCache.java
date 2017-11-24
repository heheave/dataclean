package javaclz.cache.cacheClient;

import java.util.LinkedHashMap;

/**
 * Created by xiaoke on 17-11-8.
 */
public class LRUCache<K, V> {

    private final LinkedHashMap<K, V> map;

    private final int cacheSize;

    static final int DEFAULT_CACHE_SIZE = 16;

    public LRUCache() {
        this(DEFAULT_CACHE_SIZE);
    }

    public LRUCache(int cacheSize) {
        this.cacheSize = cacheSize;
        int hashTableSize = (int) Math.ceil(cacheSize / 0.75f) + 1;
        map = new LinkedHashMap<K,V>(hashTableSize, 0.75f, true) {
            private static final long serialVersionUID = 1L;
            @Override
            protected boolean removeEldestEntry(java.util.Map.Entry<K,V> eldest) {
                //System.out.println("size=" + size() + " cacheSize=" + LRUCache.this.cacheSize);
                return size() > LRUCache.this.cacheSize;
            }
        };
    }

    public synchronized V put(K key, V value) {
        return map.put(key, value);
    }

    public synchronized V get(K key) {
        return map.get(key);
    }

    public synchronized V remove(K key) {
        return map.remove(key);
    }

    public synchronized int size() {
        return map.size();
    }

    public synchronized void clear() {
        map.clear();
    }

    public int campacity() {
        return cacheSize;
    }
}

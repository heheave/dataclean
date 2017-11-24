package javaclz.cache;

/**
 * Created by xiaoke on 17-11-8.
 */

abstract public class CEntry {

    protected final long createdTime = System.currentTimeMillis();

    public long getCreatedTime() {
        return createdTime;
    }

    abstract public Object getAttach();
}

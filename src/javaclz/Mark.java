package javaclz;

import java.io.Serializable;

/**
 * Created by xiaoke on 17-10-16.
 */
public final class Mark<T> implements Serializable {

    public final String key;

    private final boolean hasDefault;

    private final T defaultV;

    public Mark(String key) {
        this.key = key;
        this.hasDefault = false;
        this.defaultV = null;
    }

    public Mark(String key, T defaultV) {
        this.key = key;
        this.hasDefault = true;
        this.defaultV = defaultV;
    }

    public static <E> Mark<E> makeMark(String key) {
        return new Mark(key);
    }

    public static <E> Mark<E> makeMark(String key, E defaultV) {
        return new Mark(key, defaultV);
    }



    public T dv() {
        if (!hasDefault) {
            throw new UnsupportedOperationException("No default value");
        }
        return defaultV;
    }
}

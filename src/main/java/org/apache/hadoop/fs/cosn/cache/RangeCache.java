package org.apache.hadoop.fs.cosn.cache;

import java.io.Serializable;
import java.util.Map;
import java.util.Objects;

public abstract class RangeCache<K extends Comparable<K>, V> {
    public static final class Entry<K> implements Serializable {
        private static final long serialVersionUID = 5916455377642165613L;

        public enum RangeType {
            CLOSED_OPEN, OPEN_CLOSED, OPEN_OPEN, CLOSED_CLOSED
        }

        public static <K extends Comparable<K>> Entry<K> closed(K lowerEndpoint, K upperEndpoint) {
            return new Entry<>(lowerEndpoint, upperEndpoint, RangeType.CLOSED_CLOSED);
        }

        public static <K extends Comparable<K>> Entry<K> closedOpen(K lowerEndpoint, K upperEndpoint) {
            return new Entry<>(lowerEndpoint, upperEndpoint, RangeType.CLOSED_OPEN);
        }

        public static <K extends Comparable<K>> Entry<K> openClosed(K lowerEndpoint, K upperEndpoint) {
            return new Entry<>(lowerEndpoint, upperEndpoint, RangeType.OPEN_CLOSED);
        }

        public static <K extends Comparable<K>> Entry<K> open(K lowerEndpoint, K upperEndpoint) {
            return new Entry<>(lowerEndpoint, upperEndpoint, RangeType.OPEN_OPEN);
        }

        private final K lowerEndpoint;
        private final K upperEndpoint;
        private final RangeType rangeType;

        private Entry(K lowerEndpoint, K upperEndpoint, RangeType rangeType) {
            this.lowerEndpoint = lowerEndpoint;
            this.upperEndpoint = upperEndpoint;
            this.rangeType = rangeType;
        }

        public K getLowerEndpoint() {
            return lowerEndpoint;
        }

        public K getUpperEndpoint() {
            return upperEndpoint;
        }

        public RangeType getRangeType() {
            return rangeType;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof Entry)) return false;
            Entry<?> entry = (Entry<?>) o;
            return Objects.equals(lowerEndpoint, entry.lowerEndpoint) && Objects.equals(upperEndpoint, entry.upperEndpoint) && rangeType == entry.rangeType;
        }

        @Override
        public int hashCode() {
            return Objects.hash(lowerEndpoint, upperEndpoint, rangeType);
        }
    }

    protected final int capacity;
    protected RemovalListener<Entry<K>, V> removalListener = null;

    public static class RemovalNotification<RANGE_KEY, V> implements Map.Entry<RANGE_KEY, V>, Serializable {
        private static final long serialVersionUID = 2457865473217506034L;

        private final RANGE_KEY rangeKey;
        private final V value;

        RemovalNotification(RANGE_KEY rangeKey, V value) {
            this.rangeKey = rangeKey;
            this.value = value;
        }

        @Override
        public RANGE_KEY getKey() {
            return this.rangeKey;
        }

        @Override
        public V getValue() {
            return this.value;
        }

        @Override
        public V setValue(V value) {
            throw new UnsupportedOperationException();
        }
    }

    public interface RemovalListener<RANGE_KEY, V> {
        void onRemoval(RemovalNotification<RANGE_KEY, V> notification);
    }

    protected RangeCache(int capacity) {
        this.capacity = capacity;
    }

    public void setRemovalListener(RemovalListener<Entry<K>, V> removalListener) {
        this.removalListener = removalListener;
    }

    public abstract void put(Entry<K> entry, V value);

    public abstract V get(K key);

    public abstract Entry<K> getEntry(K key);

    public abstract boolean contains(K key);

    public abstract void remove(K entry );

    public abstract int size();

    public abstract boolean isEmpty();

    public abstract void clear();

    public abstract boolean containsOverlaps(Entry<K> entry);

    protected void notifyRemoval(RemovalNotification<Entry<K>, V> notification) {
        if (removalListener != null) {
            removalListener.onRemoval(notification);
        }
    }
}

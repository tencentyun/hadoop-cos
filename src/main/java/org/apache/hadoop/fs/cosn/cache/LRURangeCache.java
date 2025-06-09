package org.apache.hadoop.fs.cosn.cache;

import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;

class LRURangeCache<K extends Comparable<K>, V> implements RangeCache<K, V> {
    private static final Logger LOG = LoggerFactory.getLogger(LRURangeCache.class);

    private final int capacity;
    private final RangeMap<K, V> rangeMap;
    private final LinkedHashMap<Range<K>, Boolean> lruMap;
    private final RangeCache.RemovalListener<RangeCache.Entry<K>, V> removalListener;

    public LRURangeCache(int capacity) {
        this(capacity, null);
    }

    LRURangeCache(int capacity, RangeCache.RemovalListener<RangeCache.Entry<K>, V> removalListener) {
        this.capacity = capacity;
        this.rangeMap = TreeRangeMap.create();
        this.lruMap = new LinkedHashMap<Range<K>, Boolean>(capacity, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<Range<K>, Boolean> eldest) {
                if (size() > LRURangeCache.this.capacity) {
                    V removedValue = rangeMap.get(eldest.getKey().lowerEndpoint());
                    rangeMap.remove(eldest.getKey());
                    if (removalListener != null) {
                        LOG.debug("Removing key {} from LRURangeCache.", eldest.getKey());
                        RemovalNotification notification = new RemovalNotification<>(
                                covertRangeToEntry(eldest.getKey()), removedValue);
                        removalListener.onRemoval(notification);
                    }
                    return true;
                }
                return false;
            }
        };
        this.removalListener = removalListener;
    }

    // 添加范围和对应的值
    @Override
    public void put(RangeCache.Entry<K> entry, V value) {
        rangeMap.put(Range.closedOpen(entry.getLowerEndpoint(), entry.getUpperEndpoint()), value);
        lruMap.put(Range.closedOpen(entry.getLowerEndpoint(), entry.getUpperEndpoint()), Boolean.TRUE);
    }


    // 获取范围内的值
    @Override
    public V get(K key) {
        V value = rangeMap.get(key);
        if (value != null) {
            // 更新 LRU 顺序
            Range<K> entry = rangeMap.asMapOfRanges().keySet().stream().filter(r -> r.contains(key)).findFirst().orElse(null);
            if (entry != null) {
                lruMap.get(entry);
            }
        }
        return value;
    }

    @Override
    public boolean contains(K key) {
        return rangeMap.get(key) != null;
    }

    @Override
    public void remove(K key) {
        Map.Entry<Range<K>, V> entry = rangeMap.getEntry(key);
        if (entry != null) {
            Range<K> range = entry.getKey();
            V value = entry.getValue();
            rangeMap.remove(range);
            lruMap.remove(range);
            if (removalListener != null) {
                RemovalNotification notification = new RemovalNotification<>(
                        covertRangeToEntry(range), value);
                removalListener.onRemoval(notification);
            }
        }
    }

    // 获取缓存大小
    public int size() {
        return lruMap.size();
    }

    @Override
    public boolean isEmpty() {
        return lruMap.isEmpty();
    }

    @Override
    public void clear() {
        // 逐个清空并等待回调完成
        for (Map.Entry<Range<K>, Boolean> entry : lruMap.entrySet()) {
            Range<K> range = entry.getKey();
            V value = rangeMap.get(range.lowerEndpoint());
            if (value != null && removalListener != null) {
                LOG.debug("Clearing key {} from LRURangeCache.", range);
                RemovalNotification notification = new RemovalNotification<>(
                        this.covertRangeToEntry(range), value);
                removalListener.onRemoval(notification);
            }
        }
    }

    @Override
    public boolean containsOverlaps(RangeCache.Entry<K> entry) {
        Range<K> range = Range.closedOpen(entry.getLowerEndpoint(), entry.getUpperEndpoint());
        return rangeMap.asMapOfRanges().keySet().stream().anyMatch(r -> r.isConnected(range) && !r.intersection(range).isEmpty());
    }

    private Range<K> covertRangeHelper(RangeCache.Entry<K> entry) {
        switch (entry.getRangeType()) {
            case CLOSED_OPEN:
                return Range.closedOpen(entry.getLowerEndpoint(), entry.getUpperEndpoint());
            case OPEN_OPEN:
                return Range.open(entry.getLowerEndpoint(), entry.getUpperEndpoint());
            case OPEN_CLOSED:
                return Range.openClosed(entry.getLowerEndpoint(), entry.getUpperEndpoint());
            case CLOSED_CLOSED:
                return Range.closed(entry.getLowerEndpoint(), entry.getUpperEndpoint());
            default:
                throw new IllegalArgumentException("Unsupported range type: " + entry.getRangeType());
        }
    }

    private RangeCache.Entry<K> covertRangeToEntry(Range<K> range) {
        if (range.lowerBoundType() == BoundType.OPEN && range.upperBoundType() == BoundType.OPEN) {
            return RangeCache.Entry.open(range.lowerEndpoint(), range.upperEndpoint());
        }

        if (range.lowerBoundType() == BoundType.CLOSED && range.upperBoundType() == BoundType.OPEN) {
            return RangeCache.Entry.closedOpen(range.lowerEndpoint(), range.upperEndpoint());
        }

        if (range.lowerBoundType() == BoundType.OPEN && range.upperBoundType() == BoundType.CLOSED) {
            return RangeCache.Entry.openClosed(range.lowerEndpoint(), range.upperEndpoint());
        }

        if (range.lowerBoundType() == BoundType.CLOSED && range.upperBoundType() == BoundType.CLOSED) {
            return RangeCache.Entry.closed(range.lowerEndpoint(), range.upperEndpoint());
        }

        throw new IllegalArgumentException(
                String.format("Unsupported range: %s, lowerBoundedType: %s, uppperBoundedType: %s.",
                        range, range.lowerBoundType(), range.upperBoundType()));
    }
}
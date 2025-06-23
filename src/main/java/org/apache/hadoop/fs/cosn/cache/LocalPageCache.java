package org.apache.hadoop.fs.cosn.cache;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CosNConfigKeys;
import org.apache.hadoop.fs.CosNUtils;
import org.apache.hadoop.fs.cosn.buffer.CosNBufferFactory;
import org.apache.hadoop.fs.cosn.buffer.CosNByteBuffer;
import org.apache.hadoop.fs.cosn.buffer.CosNMappedBufferFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;

public class LocalPageCache implements PageCache {
    private static final Logger LOG = LoggerFactory.getLogger(LocalPageCache.class);

    private static volatile LocalPageCache instance;

    private AtomicInteger referCount = new AtomicInteger(0);
    private AtomicBoolean isInitialized = new AtomicBoolean(false);

    private Map<String, FileCacheEntry> fileCacheMap;
    private int pageNum;
    private int pageSize;
    private String cacheDir;
    private CosNBufferFactory cosNBufferFactory;
    private Constructor<? extends RangeCache> rangeCacheConstructor;

    private final class FileCacheEntry {
        // 头部用来存放每个页面的长度
        private static final int META_HEAD_SIZE = Integer.BYTES;
        // 每个文件缓存项的锁
        private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
        // <rangeInFile, SlotIndex>
        private final RangeCache<Long, Integer> rangeIndex;
        // 0 代表空闲，1 代表占用
        private final byte[] pageIndexes;
        private final CosNByteBuffer pagesBuffer;
        private final AtomicBoolean isValid;

        public FileCacheEntry() throws IOException {
            this.pageIndexes = new byte[pageNum];
            try {
                this.rangeIndex = rangeCacheConstructor.newInstance(pageNum);
            } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
                throw new IOException("Failed to create RangeCache instance", e);
            }
            this.rangeIndex.setRemovalListener(notification -> {
                this.lock.writeLock().lock();
                try {
                    this.pageIndexes[notification.getValue()] = 0;
                } finally {
                    this.lock.writeLock().unlock();
                }
            });
            pagesBuffer = cosNBufferFactory.create((META_HEAD_SIZE + pageSize) * pageNum);
            this.isValid = new AtomicBoolean(true);
        }

        public void put(byte[] pageData, long offsetInFile) throws IOException {
            if (pageData == null || pageData.length == 0) {
                LOG.warn("Page is null or content is null. Skip to put it into cache.");
                return;
            }

            if (pageData.length > pageSize) {
                LOG.warn("Page size is greater than the size of the cache. Skip to put it.");
                return;
            }

            this.lock.writeLock().lock();
            if (!this.isValid.get()) {
                LOG.warn("FileCacheEntry is not valid, cannot put page data.");
                return; // 如果 FileCacheEntry 已经失效，直接返回
            }
            try {
                if (this.rangeIndex.contains(offsetInFile)) {
                    LOG.debug("Skipping page as it is already in use");
                    return;
                }
                // 找一个空闲槽位
                int slotIndex = -1;
                for (int i = 0; i < this.pageIndexes.length; i++) {
                    if (this.pageIndexes[i] == 0) {
                        slotIndex = i;
                        break;
                    }
                }
                if (slotIndex == -1) {
                    LOG.debug("No free slot available in the page cache.");
                    return; // 没有空闲槽位
                }
                // 写入数据
                this.pagesBuffer.position(slotIndex * (META_HEAD_SIZE + pageSize));
                // 写入数据长度到头部
                this.pagesBuffer.putInt(pageData.length);
                // 写入实际数据
                this.pagesBuffer.put(pageData, 0, pageData.length);

                // 更新 page 索引和 range 索引
                this.pageIndexes[slotIndex] = 1;
                this.rangeIndex.put(RangeCache.Entry.closedOpen(offsetInFile,
                    offsetInFile + pageData.length), slotIndex);
            } finally {
                this.lock.writeLock().unlock();
            }
        }

        public byte[] get(long offset) throws IOException {
            this.lock.readLock().lock();
            if (!this.isValid.get()) {
                LOG.warn("FileCacheEntry is not valid, cannot get page data.");
                return null; // 如果 FileCacheEntry 已经失效，直接返回
            }

            try {
                if (!this.rangeIndex.contains(offset)) {
                    LOG.debug("Page not found in cache for offset: {}", offset);
                    return null;
                }

                int slotIndex = this.rangeIndex.get(offset);
                RangeCache.Entry<Long> rangeEntry = this.rangeIndex.getEntry(offset);
                int dataSize = this.pagesBuffer.getInt(slotIndex * (META_HEAD_SIZE + pageSize));
                // 头部放 offsetInFile，后面是实际数据
                ByteBuffer data = ByteBuffer.allocate(Long.BYTES + dataSize);
                data.clear();
                data.putLong(rangeEntry.getLowerEndpoint());
                this.pagesBuffer.position(slotIndex * (META_HEAD_SIZE + pageSize) + META_HEAD_SIZE);
                this.pagesBuffer.get(data.array(), Long.BYTES, dataSize);
                return data.array();
            } finally {
                this.lock.readLock().unlock();
            }
        }

        public void remove(long offset) throws IOException {
            this.lock.writeLock().lock();
            if (!this.isValid.get()) {
                LOG.warn("FileCacheEntry is not valid, cannot remove page data.");
                return; // 如果 FileCacheEntry 已经失效，直接返回
            }
            try {
                if (!this.rangeIndex.contains(offset)) {
                    LOG.warn("Page with offset {} not found in cache.", offset);
                    return; // 如果没有找到对应的页面，直接返回
                }
                int slotIndex = this.rangeIndex.get(offset);
                this.rangeIndex.remove(offset); // 从索引中移除
                this.pageIndexes[slotIndex] = 0; // 标记为未使用
            } finally {
                this.lock.writeLock().unlock();
            }
        }

        public void release() throws IOException {
            this.lock.writeLock().lock();
            if (!this.isValid.get()) {
                LOG.warn("FileCacheEntry is already released, nothing to do.");
                return; // 如果 FileCacheEntry 已经失效，直接返回
            }
            this.isValid.set(false);
            try {
                this.rangeIndex.clear();
                // 清理索引
                Arrays.fill(this.pageIndexes, (byte) 0);
                this.pagesBuffer.close();
            } finally {
                this.lock.writeLock().unlock();
            }
        }

        public boolean contains(long offset) {
            this.lock.readLock().lock();
            if (!this.isValid.get()) {
                LOG.warn("FileCacheEntry is not valid, cannot check contains.");
                return false; // 如果 FileCacheEntry 已经失效，直接返回
            }

            try {
                return this.rangeIndex.contains(offset);
            } finally {
                this.lock.readLock().unlock();
            }
        }

    }

    private LocalPageCache() {
    }

    // 获取单例实例
    public static LocalPageCache getInstance() {
        if (null == instance) {
            synchronized (LocalPageCache.class) {
                if (null == instance) {
                    LOG.info("Created new LocalPageCache instance.");
                    instance = new LocalPageCache();
                }
            }
        }
        return instance;
    }

    public synchronized void initialize(Configuration conf) throws IOException {
        if (isInitialized.get()) {
            LOG.info("Cache is already initialized, incrementing referCount. " +
                    "Current count: {}", referCount.get());
            this.referCount.incrementAndGet();
            return; // 已经初始化，直接返回
        }
        int fileNumber = conf.getInt(CosNConfigKeys.COSN_READ_PAGE_CACHE_FILE_NUM,
                CosNConfigKeys.DEFAULT_READ_PAGE_CACHE_FILE_NUM);
        this.pageNum = conf.getInt(CosNConfigKeys.COSN_READ_FRAGMENT_CACHE_EACH_FILE_FRAGMENT_NUM,
                CosNConfigKeys.DEFAULT_READ_FRAGMENT_CACHE_EACH_FILE_FRAGMENT_NUM);
        this.fileCacheMap = new ConcurrentHashMap<>(fileNumber);
        this.pageSize = conf.getInt(CosNConfigKeys.READ_AHEAD_BLOCK_SIZE_KEY, (int) CosNConfigKeys.DEFAULT_READ_AHEAD_BLOCK_SIZE);
        this.cacheDir = conf.get(CosNConfigKeys.COSN_READ_PAGE_CACHE_DIR, CosNConfigKeys.DEFAULT_READ_PAGE_CACHE_DIR);
        this.cosNBufferFactory = new CosNMappedBufferFactory(new String[]{this.cacheDir}, true);
        this.rangeCacheConstructor = buildRangeCacheConstructor(
                conf.get(CosNConfigKeys.COSN_READ_PAGE_CACHE_RANGE_CACHE_IMPL,
                        CosNConfigKeys.DEFAULT_READ_PAGE_CACHE_RANGE_CACHE_IMPL));

        // 确保缓存目录存在，如果不存在则创建，如果存在，则清空目录下的数据
        CosNUtils.createTempDir(this.cacheDir);
        CosNUtils.clearTempDir(this.cacheDir);

        isInitialized.set(true);
    }

    @Override
    public void put(Page page) throws IOException {
        this.checkInitialize();
        FileCacheEntry fileCacheEntry = this.fileCacheMap.computeIfAbsent(page.getFilePath(),
            new Function<String, FileCacheEntry>() {
            @Override
            public @Nullable FileCacheEntry apply(@Nullable String key) {
                try {
                    return new FileCacheEntry();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        });
        fileCacheEntry.put(page.getContent(), page.getOffsetInFile());
    }

    @Override
    public Page get(String filePath, long startOffsetInFile) throws IOException {
        this.checkInitialize();
        FileCacheEntry fileCacheEntry = this.fileCacheMap.get(filePath);
        if (fileCacheEntry == null) {
            return null;
        }

        byte[] pageData = fileCacheEntry.get(startOffsetInFile);
        if (null == pageData) {
            return null;
        }

        ByteBuffer pageDataBuffer = ByteBuffer.wrap(pageData);
        long offsetInFile = pageDataBuffer.getLong();
        byte[] realData = new byte[pageDataBuffer.remaining()];
        pageDataBuffer.get(realData);
        return new Page(filePath, offsetInFile, realData);
    }

    @Override
    public boolean contains(String filePath, long startOffsetInFile) throws IOException {
        this.checkInitialize();
        if (!this.fileCacheMap.containsKey(filePath) || this.fileCacheMap.get(filePath) == null
                || !this.fileCacheMap.get(filePath).contains(startOffsetInFile)) {
            LOG.debug("File not found in cache for file: {}", filePath);
            return false;
        }
        return true;
    }

    @Override
    public void remove(String filePath, long startOffsetInFile) throws IOException {
        this.checkInitialize();
        FileCacheEntry fileCacheEntry = this.fileCacheMap.get(filePath);
        if (fileCacheEntry == null) {
            LOG.warn("FileCacheEntry for file {} not found, cannot remove page.", filePath);
            return; // 如果没有找到对应的文件缓存，直接返回
        }
        fileCacheEntry.remove(startOffsetInFile);
    }

    @Override
    public void remove(String filePath) throws IOException {
        this.checkInitialize();

        FileCacheEntry fileCacheEntry = this.fileCacheMap.remove(filePath);
        if (fileCacheEntry == null) {
            LOG.warn("FileCacheEntry for file {} not found, cannot remove.", filePath);
            return; // 如果没有找到对应的文件缓存，直接返回
        }

        try {
            fileCacheEntry.release();
        } catch (IOException e) {
            LOG.error("Error releasing FileCacheEntry for file: {}", filePath, e);
        }
    }

    @Override
    public void clear() throws IOException {
        this.checkInitialize();
        Set<String> keys = new HashSet<>(this.fileCacheMap.keySet());
        for (String key : keys) {
            try {
                this.remove(key);
            } catch (IOException e) {
                LOG.error("Error clearing cache for file: {}", key, e);
            }
        }
    }

    @Override
    public void close() {
        if (!this.isInitialized.get()) {
            LOG.warn("LocalPageCache is not initialized, nothing to close.");
            return; // 如果没有初始化，直接返回
        }

        if (this.referCount.decrementAndGet() > 0) {
            LOG.info("Decremented referCount, current count: {}", this.referCount.get());
            return; // 如果引用计数大于0，直接返回
        }

        LOG.info("Closing LocalPageCache, clearing all caches.");
        // 清理所有缓存
        try {
            this.clear();
        } catch (IOException e) {
            LOG.error("Error clearing LocalPageCache during close.", e);
        } finally {
            LOG.info("LocalPageCache closed successfully.");
        }
        this.isInitialized.set(false);
    }

    private void checkInitialize() throws IOException {
        if (!this.isInitialized.get()) {
            throw new IOException("The LocalPageCache has not been initialized.");
        }
    }

    private static Constructor<? extends RangeCache> buildRangeCacheConstructor(String className) throws IOException {
        try {
            Class<?> clazz = Class.forName(className);
            if (RangeCache.class.isAssignableFrom(clazz)) {
                return clazz.asSubclass(RangeCache.class).getConstructor(int.class);
            } else {
                throw new IOException("Class " + className + " is not a subclass of RangeCache.");
            }
        } catch (ClassNotFoundException e) {
            throw new IOException("Failed to find RangeCache implementation: " + className, e);
        } catch (NoSuchMethodException e) {
            throw new IOException("No suitable constructor found for RangeCache implementation: " + className, e);
        }
    }
}
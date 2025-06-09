package org.apache.hadoop.fs.cosn.cache;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CosNConfigKeys;
import org.apache.hadoop.fs.CosNUtils;
import org.apache.hadoop.fs.cosn.common.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class LocalFragmentCache implements FragmentCache {
    private static final Logger LOG = LoggerFactory.getLogger(LocalFragmentCache.class);

    private static volatile LocalFragmentCache instance;

    private AtomicInteger referCount = new AtomicInteger(0);
    private AtomicBoolean isInitialized = new AtomicBoolean(false);

    private Map<String, Pair<ReentrantReadWriteLock, RangeCache<Long, Path>>> fileCacheMap;
    private int fileNumber;
    private int eachFileFragmentNumber;
    private String cacheDir;

    private ExecutorService boundedProcessExecutor;

    private LocalFragmentCache() {
    }

    // 获取单例实例
    public static LocalFragmentCache getInstance() {
        if (null == instance) {
            synchronized (LocalFragmentCache.class) {
                if (null == instance) {
                    LOG.info("Created new LocalFragmentCache instance.");
                    instance = new LocalFragmentCache();
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
        fileCacheMap = new ConcurrentHashMap<>(fileNumber);
        this.fileNumber = conf.getInt(CosNConfigKeys.COSN_READ_FRAGMENT_CACHE_FILE_NUM,
                CosNConfigKeys.DEFAULT_READ_FRAGMENT_CACHE_FILE_NUM);
        this.eachFileFragmentNumber = conf.getInt(CosNConfigKeys.COSN_READ_FRAGMENT_CACHE_EACH_FILE_FRAGMENT_NUM,
                CosNConfigKeys.DEFAULT_READ_FRAGMENT_CACHE_EACH_FILE_FRAGMENT_NUM);
        this.cacheDir = conf.get(CosNConfigKeys.COSN_READ_FRAGMENT_CACHE_DIR, CosNConfigKeys.DEFAULT_READ_FRAGMENT_CACHE_DIR);

        int threadPoolSize = conf.getInt(CosNConfigKeys.COSN_FRAGMENT_CACHE_THREAD_POOL,
                CosNConfigKeys.DEFAULT_FRAGMENT_CACHE_THREAD_POOL);
        int maxThreadPoolSize = conf.getInt(CosNConfigKeys.COSN_READ_FRAGMENT_CACHE_THREAD_POOL_MAX_SIZE,
                CosNConfigKeys.DEFAULT_FRAGMENT_CACHE_THREAD_POOL_MAX_SIZE);
        this.boundedProcessExecutor = new ThreadPoolExecutor(threadPoolSize, maxThreadPoolSize, 60, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(maxThreadPoolSize));

        // 确保缓存目录存在，如果不存在则创建，如果存在，则清空目录下的数据
        CosNUtils.createTempDir(this.cacheDir);
        CosNUtils.clearTempDir(this.cacheDir);

        isInitialized.set(true);
    }

    @Override
    public void put(Fragment fragment) throws IOException {
        this.checkInitialize();

        if (this.fileCacheMap.size() >= this.fileNumber) {
            LOG.info("Cache size limit reached. Current size: {}, limit: {}. Not adding new fragment.",
                    this.fileCacheMap.size(), this.fileNumber);
            return;
        }
        if (null != this.boundedProcessExecutor) {
            this.boundedProcessExecutor.execute(() -> {
                this.putFragment(fragment);
            });
        } else {
            this.putFragment(fragment);
        }
    }

    @Override
    public Fragment get(String filePath, long startOffsetInFile) throws IOException {
        this.checkInitialize();

        String filePathKey = encodeBase64(filePath);
        Pair<ReentrantReadWriteLock, RangeCache<Long, Path>> fileCacheEntryPair = this.fileCacheMap.get(filePathKey);

        if (fileCacheEntryPair == null
                || fileCacheEntryPair.getSecond() == null
                || fileCacheEntryPair.getSecond().isEmpty()) {
            return null;
        }

        fileCacheEntryPair.getFirst().readLock().lock();
        try {
            fileCacheEntryPair = this.fileCacheMap.get(filePathKey);
            if (null == fileCacheEntryPair
                    || fileCacheEntryPair.getSecond() == null
                    || fileCacheEntryPair.getSecond().isEmpty()) {
                LOG.debug("No cache found for file: {}", filePath);
                return null; // 如果没有缓存，直接返回 null
            }

            Path fragmentFilePath = fileCacheEntryPair.getSecond().get(startOffsetInFile);
            if (fragmentFilePath == null) {
                LOG.debug("No cached fragment found for file: {}, offset: {}", filePath, startOffsetInFile);
                return null; // 如果没有找到对应的缓存片段，返回 null
            }

            // 读取文件内容并返回
            byte[] fragmentContent;
            try (BufferedInputStream inputStream = new BufferedInputStream(
                    Files.newInputStream(fragmentFilePath.toFile().toPath()))) {
                fragmentContent = new byte[(int) fragmentFilePath.toFile().length()];
                if (inputStream.read(fragmentContent) != fragmentContent.length) {
                    LOG.warn("Could not read the complete fragment content for file: {}, offset: {}",
                            filePath, startOffsetInFile);
                    return null; // 如果读取的内容不完整，返回 null
                }
            }
            return new Fragment(filePath, Long.parseLong(fragmentFilePath.getFileName().toString()), fragmentContent);
        } catch (IOException e) {
            LOG.error("Error reading fragment from cache for file: {}, offset: {}", filePath, startOffsetInFile, e);
            throw e; // 抛出异常以便上层处理
        } finally {
            fileCacheEntryPair.getFirst().readLock().unlock();
        }
    }

    @Override
    public boolean contains(String filePath, long startOffsetInFile) throws IOException {
        this.checkInitialize();

        String filePathKey = encodeBase64(filePath);
        Pair<ReentrantReadWriteLock, RangeCache<Long, Path>> fileCacheEntryPair = this.fileCacheMap.get(filePathKey);
        if (null == fileCacheEntryPair
                || fileCacheEntryPair.getSecond() == null
                || fileCacheEntryPair.getSecond().isEmpty()) {
            return false;
        }

        fileCacheEntryPair.getFirst().readLock().lock();
        try {
            return fileCacheEntryPair.getSecond().contains(startOffsetInFile);
        } finally {
            fileCacheEntryPair.getFirst().readLock().unlock();
        }
    }

    @Override
    public void remove(String filePath, long startOffsetInFile) throws IOException {
        this.checkInitialize();
        String filePathKey = encodeBase64(filePath);
        Pair<ReentrantReadWriteLock, RangeCache<Long, Path>> fileCacheEntryPair = this.fileCacheMap.get(filePathKey);
        if (fileCacheEntryPair == null
                || fileCacheEntryPair.getSecond() == null
                || fileCacheEntryPair.getSecond().isEmpty()) {
            return;
        }
        fileCacheEntryPair.getFirst().writeLock().lock();
        try {
            fileCacheEntryPair.getSecond().remove(startOffsetInFile);
        } finally {
            fileCacheEntryPair.getFirst().writeLock().unlock();
        }
    }

    @Override
    public void remove(String filePath) throws IOException {
        this.checkInitialize();
        String filePathKey = encodeBase64(filePath);
        Pair<ReentrantReadWriteLock, RangeCache<Long, Path>> fileCacheEntryPair = this.fileCacheMap.remove(filePathKey);
        if (fileCacheEntryPair == null
                || fileCacheEntryPair.getSecond() == null
                || fileCacheEntryPair.getSecond().isEmpty()) {
            return;
        }
        fileCacheEntryPair.getFirst().writeLock().lock();
        try {
            fileCacheEntryPair.getSecond().clear();
        } finally {
            fileCacheEntryPair.getFirst().writeLock().unlock();
        }
    }

    @Override
    public synchronized void clear() throws IOException {
        this.checkInitialize();

        LOG.info("Clearing all caches in LocalFragmentCache.");
        for (Map.Entry<String, Pair<ReentrantReadWriteLock, RangeCache<Long, Path>>> entry : fileCacheMap.entrySet()) {
            entry.getValue().getFirst().writeLock().lock();
            try {
                entry.getValue().getSecond().clear();
            } finally {
                entry.getValue().getFirst().writeLock().unlock();
            }
        }

        fileCacheMap.clear();
        CosNUtils.clearTempDir(this.cacheDir);
    }

    @Override
    public synchronized void close() {
        if (!this.isInitialized.get()) {
            LOG.warn("LocalFragmentCache is not initialized, nothing to close.");
            return; // 如果没有初始化，直接返回
        }

        if (this.referCount.decrementAndGet() > 0) {
            LOG.info("Decremented referCount, current count: {}", this.referCount.get());
            return; // 如果引用计数大于0，直接返回
        }

        LOG.info("Closing LocalFragmentCache, clearing all caches.");
        // 清理所有缓存
        try {
            if (null != this.boundedProcessExecutor) {
                this.boundedProcessExecutor.shutdown();
                if (!this.boundedProcessExecutor.awaitTermination(60, TimeUnit.SECONDS)) {
                    LOG.warn("Thread pool did not terminate in the specified time.");
                }
            }
            this.clear();
        } catch (IOException e) {
            LOG.error("Error clearing LocalFragmentCache during close.", e);
        } catch (InterruptedException e) {
            LOG.error("Interrupted while waiting for LocalFragmentCache to close.", e);
        } finally {
            LOG.info("LocalFragmentCache closed successfully.");
        }
        this.isInitialized.set(false);
    }


    private void checkInitialize() throws IOException {
        if (!this.isInitialized.get()) {
            throw new IOException("The LocalFragmentCache has not been initialized.");
        }
    }

    private void putFragment(Fragment fragment) {
        if (null == fragment) {
            return;
        }

        String filePathKey = encodeBase64(fragment.getFilePath());
        Pair<ReentrantReadWriteLock, RangeCache<Long, Path>> fileCacheEntryPair = fileCacheMap.computeIfAbsent(filePathKey, k -> {
            LOG.info("Creating new lock for file: {}", fragment.getFilePath());
            return new Pair<>(new ReentrantReadWriteLock(), new LRURangeCache<>(eachFileFragmentNumber, notification -> {
                Path fragmentFilePath = notification.getValue();
                String fileKey = fragmentFilePath.getParent().getFileName().toString();
                Pair<ReentrantReadWriteLock, RangeCache<Long, Path>> fileCacheEntryPair1 = fileCacheMap.get(fileKey);
                if (null == fileCacheEntryPair1) {
                    return;
                }
                fileCacheEntryPair1.getFirst().writeLock().lock();
                try {
                    deleteFragmentFile(fileKey, Long.parseLong(fragmentFilePath.getFileName().toString()));
                    if (fileCacheEntryPair1.getSecond() == null || fileCacheEntryPair1.getSecond().isEmpty()) {
                        // 如果 RangeCache 为空，删除对应的缓存和锁
                        fileCacheMap.remove(fileKey);
                        // 同时删除对应子目录
                        deleteFilePath(fileKey);
                    }
                } catch (IOException e) {
                    LOG.error("Failed to delete fragment file: {}", fragmentFilePath.getFileName().toString(), e);
                } finally {
                    fileCacheEntryPair1.getFirst().writeLock().unlock();
                }
            }));
        });
        fileCacheEntryPair.getFirst().writeLock().lock();
        try {
            if (fileCacheEntryPair.getSecond().containsOverlaps(RangeCache.Entry.closedOpen(fragment.getStartOffsetInFile(), fragment.getStartOffsetInFile() + fragment.getContent().length))) {
                // XXX 这里暂时不放入重叠的 Range 片，后续看看怎么优化。
                LOG.debug("Fragment overlaps with existing cache entries for file: {}, offset: {}", fragment.getFilePath(), fragment.getStartOffsetInFile());
                return; // 如果有重叠的缓存，直接返回
            }
            // 写入缓存
            Path fragmentFilePath = getFragmentFilePath(filePathKey, fragment.getStartOffsetInFile());
            // 确保目录存在
            Path parentDir = fragmentFilePath.getParent();
            if (parentDir != null && !parentDir.toFile().exists()) {
                if (!parentDir.toFile().mkdirs()) {
                    throw new IOException("Failed to create directory: " + parentDir);
                }
            }
            // 将内容写入文件
            Files.write(fragmentFilePath, fragment.getContent());
            // 将文件路径存入 RangeCache
            fileCacheEntryPair.getSecond().put(RangeCache.Entry.closedOpen(fragment.getStartOffsetInFile(), fragment.getStartOffsetInFile() + fragment.getContent().length),
                    fragmentFilePath);
        } catch (IOException e) {
            LOG.error("Failed to write fragment to cache for file: {}, offset: {}", fragment.getFilePath(), fragment.getStartOffsetInFile(), e);
        } finally {
            fileCacheEntryPair.getFirst().writeLock().unlock();
        }
    }

    private Path getFragmentFilePath(String filePathKey, long startOffsetInFile) {
        // cacheDir/filePath/startOffsetInFile
        return Paths.get(this.cacheDir, filePathKey, String.valueOf(startOffsetInFile));
    }

    private void deleteFragmentFile(String filePathKey, long startOffsetInFile) throws IOException {
        Path fragmentFilePath = getFragmentFilePath(filePathKey, startOffsetInFile);
        if (Files.exists(fragmentFilePath)) {
            Files.delete(fragmentFilePath);
            LOG.debug("Deleted fragment file: {}", fragmentFilePath);
        } else {
            LOG.debug("Fragment file does not exist: {}", fragmentFilePath);
        }
    }

    private void deleteFilePath(String filePathKey) throws IOException {
        Path fragmentDir = Paths.get(this.cacheDir, filePathKey);
        if (Files.exists(fragmentDir)) {
            Files.delete(fragmentDir);
            LOG.info("Deleted cache directory for file: {}", fragmentDir);
        } else {
            LOG.debug("Cache directory does not exist for file: {}", fragmentDir);
        }
    }

    private static String encodeBase64(String filePath) {
        return Base64.getUrlEncoder().encodeToString(filePath.getBytes());
    }

    private static String decodeBase64(String encodedPath) {
        return new String(Base64.getUrlDecoder().decode(encodedPath));
    }
}
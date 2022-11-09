package org.apache.hadoop.fs.cosn;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CosNConfigKeys;
import org.apache.hadoop.fs.cosn.buffer.CosNRandomAccessMappedBuffer;
import org.apache.hadoop.fs.cosn.buffer.CosNRandomAccessMappedBufferFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Current temporarily used to support the seek write part cache.
 * Support the seek read/write by the RandomAccessFile.
 * It is used by the seek write.
 */
public final class LocalRandomAccessMappedBufferPool {
  private static final Logger LOG = LoggerFactory.getLogger(LocalRandomAccessMappedBufferPool.class);

  // Singleton.
  private static final LocalRandomAccessMappedBufferPool instance = new LocalRandomAccessMappedBufferPool();
  public static LocalRandomAccessMappedBufferPool getInstance() {
    return instance;
  }

  private final AtomicInteger referCount = new AtomicInteger(0);
  private final AtomicBoolean isInitialized = new AtomicBoolean(false);

  private File cacheDir;
  // disk remaining
  private long remainingSpace;
  private long highWaterMarkRemainingSpace;
  private long lowWaterMarkRemainingSpace;
  private CosNRandomAccessMappedBufferFactory mappedBufferFactory;

  public synchronized void initialize(Configuration configuration) throws IOException {
    Preconditions.checkNotNull(configuration, "configuration");
    LOG.info("Initialize the local cache.");
    if (this.isInitialized.get()) {
      LOG.info("The local file cache [{}] has been initialized and referenced once." +
          "current reference count: [{}].", this, this.referCount);
      this.referCount.incrementAndGet();
      return;
    }

    // 获取用户配置的 POSIX extension 特性目录
    String cacheDirPath = configuration.get(
        CosNConfigKeys.COSN_POSIX_EXTENSION_TMP_DIR, CosNConfigKeys.DEFAULT_POSIX_EXTENSION_TMP_DIR);
    this.cacheDir = new File(cacheDirPath);

    // 检查当前目录空间是否足够
    long usableSpace = this.cacheDir.getUsableSpace();
    long quotaSize = configuration.getLong(CosNConfigKeys.COSN_POSIX_EXTENSION_TMP_DIR_QUOTA,
        CosNConfigKeys.DEFAULT_COSN_POSIX_EXTENSION_TMP_DIR_QUOTA);
    Preconditions.checkArgument(quotaSize <= usableSpace,
        String.format("The quotaSize [%d] configured should be less than the usableSpace [%d].", quotaSize, usableSpace));
    this.remainingSpace = quotaSize;

    // 检查高低水位是否配置正确
    float lowWaterMark = configuration.getFloat(CosNConfigKeys.COSN_POSIX_EXTENSION_TMP_DIR_WATERMARK_LOW,
        CosNConfigKeys.DEFAULT_COSN_POSIX_EXTENSION_TMP_DIR_WATERMARK_LOW);
    float highWaterMark = configuration.getFloat(CosNConfigKeys.COSN_POSIX_EXTENSION_TMP_DIR_WATERMARK_HIGH,
        CosNConfigKeys.DEFAULT_COSN_POSIX_EXTENSION_TMP_DIR_WATERMARK_HIGH);
    Preconditions.checkArgument(Math.floor(lowWaterMark * 100) > 0 && Math.floor(lowWaterMark * 100) < 100,
        String.format("The low watermark [%f] should be in (0,1).", lowWaterMark));
    Preconditions.checkArgument(Math.floor(highWaterMark * 100) > 0 && Math.floor(highWaterMark * 100) < 100,
        String.format("The high watermark [%f] should be in (0,1).", highWaterMark));
    Preconditions.checkArgument(Float.compare(lowWaterMark, highWaterMark) < 0,
        String.format("The low watermark [%f] should be less than the high watermark [%f].", lowWaterMark, highWaterMark));
    // 粗略地计算高低水位的容量
    this.highWaterMarkRemainingSpace = (long) (quotaSize * (1 - highWaterMark));
    this.lowWaterMarkRemainingSpace = (long) (quotaSize * (1 - lowWaterMark));

    // 正式构建 MappedFactory 用于后续创建本地缓存文件
    boolean deleteOnExit = configuration.getBoolean(
        CosNConfigKeys.COSN_MAPDISK_DELETEONEXIT_ENABLED, CosNConfigKeys.DEFAULT_COSN_MAPDISK_DELETEONEXIT_ENABLED);
    this.mappedBufferFactory = new CosNRandomAccessMappedBufferFactory(cacheDirPath, deleteOnExit);

    this.referCount.incrementAndGet();
    this.isInitialized.set(true);
  }

  /**
   * create a local cache file specified size.
   * @param fileName the local cache file name.
   * @param size specified size.
   * @return random access file supporting the seekable write and read.
   */
  public synchronized CosNRandomAccessMappedBuffer create(String fileName, int size)
      throws CacheSpaceFullException, IOException {
    Preconditions.checkArgument(size > 0, "The file size should be a positive integer.");
    if (size > this.remainingSpace) {
      throw new CacheSpaceFullException(String.format("The requested size [%d] exceeds the remaining space [%d].",
          size, this.remainingSpace));
    }

    CosNRandomAccessMappedBuffer randomAccessMappedBuffer =
        this.mappedBufferFactory.create(fileName, size);
    randomAccessMappedBuffer.clear();
    this.remainingSpace -= size;
    return randomAccessMappedBuffer;
  }

  public synchronized boolean shouldRelease() {
    // Need to release.
    return this.remainingSpace < this.highWaterMarkRemainingSpace;
  }

  public synchronized void releaseFile(CosNRandomAccessMappedBuffer localFile) {
    int returnSpace = localFile.capacity();
    this.mappedBufferFactory.release(localFile);
    this.remainingSpace += returnSpace;
  }

  public synchronized void close() {
    LOG.info("Close the local file cache instance.");

    if (!this.isInitialized.get()) {
      LOG.warn("The local file cache has been closed. no changes would be exeucte.");
      return;
    }

    if (this.referCount.decrementAndGet() > 0) {
      return;
    }

    // POSIX extension 特性目录的清理由 MappedFactory 的标志决定
    this.isInitialized.set(false);
  }

  /**
   * Throw when the cache is full.
   */
  public static class CacheSpaceFullException extends IOException {
    public CacheSpaceFullException() {
    }
    public CacheSpaceFullException(String message) {
      super(message);
    }
    public CacheSpaceFullException(String message, Throwable cause) {
      super(message, cause);
    }
    public CacheSpaceFullException(Throwable cause) {
      super(cause);
    }
  }
}

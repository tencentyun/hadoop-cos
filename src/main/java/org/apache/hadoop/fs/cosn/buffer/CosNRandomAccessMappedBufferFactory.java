package org.apache.hadoop.fs.cosn.buffer;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * 专用于创建随机写的 MappedBuffer 的工厂方法。
 */
public class CosNRandomAccessMappedBufferFactory implements CosNBufferFactory {
  private static final Logger LOG = LoggerFactory.getLogger(CosNRandomAccessMappedBufferFactory.class);

  private final File cacheDir;
  private final boolean deleteOnExit;

  public CosNRandomAccessMappedBufferFactory(String cacheDir) throws IOException {
    this(cacheDir, false);
  }

  public CosNRandomAccessMappedBufferFactory(String cacheDir, boolean deleteOnExit) throws IOException {
    this.cacheDir = CosNRandomAccessMappedBufferFactory.createDir(cacheDir);
    this.deleteOnExit = deleteOnExit;
  }

  public CosNRandomAccessMappedBuffer create(String randomAccessFileName, int size) throws IOException {
    Preconditions.checkNotNull(randomAccessFileName, "randomAccessFileName");
    Preconditions.checkArgument(size > 0, "The size should be a positive integer.");

    if (null == this.cacheDir) {
      throw new IOException("The cache directory is not initialized. " +
          "No random access file can be created.");
    }

    if (!this.cacheDir.exists()) {
      LOG.warn("The cache directory dose not exist. Try to create it.");
      CosNRandomAccessMappedBufferFactory.createDir(this.cacheDir.getAbsolutePath());
    }

    // 创建指定大小的 RandomAccessFile
    File tmpFile = File.createTempFile(randomAccessFileName, ".cache", this.cacheDir);
    if (this.deleteOnExit) {
      tmpFile.deleteOnExit();
    }
    RandomAccessFile randomAccessFile = new RandomAccessFile(tmpFile, "rw");
    randomAccessFile.setLength(size);
    MappedByteBuffer buf = randomAccessFile.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, size);
    return (null != buf) ? new CosNRandomAccessMappedBuffer(buf, randomAccessFile, tmpFile) : null;
  }

  @Override
  public CosNByteBuffer create(int size) {
    throw new UnsupportedOperationException("the method is not available. " +
        "a filename should be specified.");
  }

  @Override
  public void release(CosNByteBuffer cosNByteBuffer) {
    if (null != cosNByteBuffer) {
      LOG.warn("The Null buffer can not be released. Ignore it.");
      return;
    }

    try {
      cosNByteBuffer.close();
    } catch (IOException e) {
      LOG.error("Failed to release the buffer [{}].", cosNByteBuffer);
    }
  }

  private static File createDir(String cacheDir) throws IOException {
    File cacheDirFile = new File(cacheDir);
    if (!cacheDirFile.exists()) {
      LOG.info("The cache directory [{}] does not exist. Create it first.", cacheDirFile);
      if (cacheDirFile.mkdirs()) {
        if (!cacheDirFile.setWritable(true, false)
            || !cacheDirFile.setReadable(true, false)
            || !cacheDirFile.setExecutable(true, false)) {
          LOG.warn("Set the buffer dir: [{}]'s permission [writable,"
              + "readable, executable] failed.", cacheDirFile);
        }
        LOG.info("Create the cache directory [{}] successfully.", cacheDirFile);
      } else {
        if (!cacheDirFile.exists()) {
          throw new IOException(String.format("Failed to create the cache directory [%s].",
              cacheDirFile));
        }
      }
    } else {
      LOG.info("The cache directory [{}] already exists.", cacheDirFile);
    }
    return cacheDirFile;
  }
}

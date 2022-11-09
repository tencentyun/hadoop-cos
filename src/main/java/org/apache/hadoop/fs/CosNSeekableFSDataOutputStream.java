package org.apache.hadoop.fs;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.chdfs.PosixSeekable;
import org.apache.hadoop.fs.cosn.Abortable;
import org.apache.hadoop.fs.cosn.Constants;
import org.apache.hadoop.fs.cosn.buffer.CosNRandomAccessMappedBuffer;
import org.apache.hadoop.fs.cosn.multipart.upload.MultipartManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;

/**
 * The POSIX seekable writing semantics.
 * Not belong to the Hadoop Compatible FileSystem semantic system.
 */
public class CosNSeekableFSDataOutputStream extends FSDataOutputStream
    implements PosixSeekable, Abortable {
  private static final Logger LOG = LoggerFactory.getLogger(CosNSeekableFSDataOutputStream.class);
  private final SeekableOutputStream seekableOutputStream;

  public CosNSeekableFSDataOutputStream(SeekableOutputStream seekableOutputStream,
                                        FileSystem.Statistics stats) throws IOException {
    super(seekableOutputStream, stats);
    this.seekableOutputStream = seekableOutputStream;
  }

  @Override
  public synchronized int ftruncate(long length) throws IOException {
    try {
      return this.seekableOutputStream.ftruncate(length);
    } catch (IOException ioException) {
      LOG.error("Failed to truncate the outputStream to length [{}].", length);
      return -1;
    }
  }

  @Override
  public void seek(long pos) throws IOException {
    this.seekableOutputStream.seek(pos);
  }

  @Override
  public boolean seekToNewSource(long pos) throws IOException {
    return this.seekableOutputStream.seekToNewSource(pos);
  }

  @Override
  public void abort() {
    this.seekableOutputStream.abort();
  }

  @Override
  public long getPos() {
    return this.seekableOutputStream.getPos();
  }

  public static class SeekableOutputStream extends OutputStream implements PosixSeekable, Abortable {
    private final NativeFileSystemStore nativeStore;
    private final String cosKey;
    private final MultipartManager multipartManager;
    private long pos;
    private boolean dirty;
    private boolean committed;
    private boolean closed;

    SeekableOutputStream(Configuration conf, NativeFileSystemStore nativeStore,
                         String cosKey) throws IOException {
      Preconditions.checkNotNull(conf, "hadoop configuration");
      this.nativeStore = Preconditions.checkNotNull(nativeStore, "nativeStore");
      this.cosKey = Preconditions.checkNotNull(cosKey, "cosKey");

      // 设置 partSize，取出配置的 partSize 和最大最小限制来选出最合适的大小
      long partSize = conf.getLong(
          CosNConfigKeys.COSN_UPLOAD_PART_SIZE_KEY, CosNConfigKeys.DEFAULT_UPLOAD_PART_SIZE);
      if (partSize < Constants.MIN_PART_SIZE) {
        LOG.warn("The minimum size of a single block is limited to " +
            "greater than or equal to {}.", Constants.MIN_PART_SIZE);
      } else if (partSize > Constants.MAX_PART_SIZE) {
        LOG.warn("The maximum size of a single block is limited to " +
            "smaller than or equal to {}.", Constants.MAX_PART_SIZE);
        partSize = Constants.MAX_PART_SIZE;
      }
      this.multipartManager = new MultipartManager(
          this.nativeStore, this.cosKey, partSize);
      this.multipartManager.resumeForWrite();
      // 把 pos 置于末尾
      this.pos = this.multipartManager.getCurrentSize();
      this.dirty = false;
      this.committed = false;
      this.closed = false;
    }

    @Override
    public synchronized void write(int b) throws IOException {
      this.checkOpened();
      byte[] singleBytes = new byte[1];
      singleBytes[0] = (byte) b;
      this.write(singleBytes);
    }

    @Override
    public synchronized void write(byte[] b, int off, int len) throws IOException {
      this.checkOpened();

      if (this.committed) {
        this.multipartManager.resumeForWrite();
        this.committed = false;
      }

      // 根据当前的 pos 计算出要写的块号和块内偏移
      while (len > 0) {
        int partIndex = (int) (this.pos / this.multipartManager.getPartSize());
        int partOffset = (int) (this.pos % this.multipartManager.getPartSize());

        CosNRandomAccessMappedBuffer part = this.multipartManager.getPart(partIndex + 1);
        part.flipWrite();
        part.position(partOffset);
        int writeBytes = Math.min(part.remaining(), len);
        part.put(b, off, writeBytes);
        len -= writeBytes;
        off += writeBytes;
        this.pos += writeBytes;
        this.dirty = true;
      }
    }

    @Override
    public synchronized int ftruncate(long newLen) throws IOException {
      this.checkOpened();
      Preconditions.checkArgument(newLen >= 0 && newLen < this.multipartManager.getMaxFileSizeLimit(),
          String.format("The new length must be a non-negative integer and less than the max file limit [%d].",
              this.multipartManager.getMaxFileSizeLimit()));
      LOG.info("Call the ftruncate({}) on the cos key [{}].", newLen, this.cosKey);
      // 先将文件刷新提交上去
      this.flush();
      // 然后再变换到需要的长度
      this.multipartManager.splitParts(newLen);
      this.dirty = true;
      this.committed = false;
      return 0;
    }

    @Override
    public synchronized void seek(long pos) throws IOException {
      this.checkOpened();
      Preconditions.checkArgument(pos >= 0,
          "The new position must be a non-negative integer.");
      Preconditions.checkArgument(pos < this.multipartManager.getMaxFileSizeLimit(),
          String.format("The seek position [%d] exceeds the maximum file limit [%d].",
              pos, this.multipartManager.getMaxFileSizeLimit()));
      LOG.info("Call the output seek({}) on the cos key [{}].", pos, this.cosKey);
      // seek 是允许超过当前文件长度的
      this.pos = pos;
    }

    @Override
    public synchronized long getPos() {
      return this.pos;
    }

    @Override
    public synchronized boolean seekToNewSource(long l) throws IOException {
      this.checkOpened();
      return false;
    }

    @Override
    public synchronized void abort() {
      if (this.closed) {
        // 已经关闭了，无需额外 abort
        return;
      }

      LOG.info("Aborting the output stream [{}].", this);
      try {
        if (null != this.multipartManager) {
          this.multipartManager.abort();
        }
      } finally {
        this.closed = true;
      }
    }

    @Override
    public synchronized void flush() throws IOException {
      this.checkOpened();
      if (!this.dirty) {
        // 已经刷新过了，不必刷新了
        return;
      }
      this.commit();
      this.dirty = false;
    }

    @Override
    public synchronized void close() throws IOException {
      if (this.closed) {
        return;
      }

      LOG.info("Closing the outputStream [{}].", this);
      try {
        this.flush();
        this.multipartManager.close();
      } finally {
        this.closed = true;
      }
    }

    private void commit() throws IOException {
      if (this.committed) {
        // 已经提交过了
        return;
      }
      // 把当前维持的块都提交可见
      this.multipartManager.commitLocalToRemote();
      this.committed = true;
    }

    private void checkOpened() throws IOException {
      if (this.closed) {
        throw new IOException(FSExceptionMessages.STREAM_IS_CLOSED);
      }
    }
  }
}

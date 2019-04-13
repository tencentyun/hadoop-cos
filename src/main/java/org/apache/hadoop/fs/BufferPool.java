package org.apache.hadoop.fs;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 缓冲池
 * 当内存缓冲区够用时，则使用内存缓冲区
 * 当内存缓冲区不够用时，则使用磁盘缓冲区
 */
public class BufferPool {
    static final Logger LOG = LoggerFactory.getLogger(BufferPool.class);

    private static BufferPool ourInstance = new BufferPool();

    public static BufferPool getInstance() {
        return ourInstance;
    }

    private BlockingQueue<ByteBuffer> bufferPool = null;
    private int singleBufferSize = 0;
    private File diskBufferDir = null;

    private AtomicBoolean isInitialize = new AtomicBoolean(false);

    private BufferPool() {
    }

    private File createDir(String dirPath) throws IOException {
        File dir = new File(dirPath);
        if (null != dir) {
            if (!dir.exists()) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("buffer dir: " + dirPath + " is not exists. creating it.");
                }
                if (dir.mkdirs()) {
                    dir.setWritable(true);
                    dir.setReadable(true);
                    dir.setExecutable(true);
                    String cmd = "chmod 777 " + dir.getAbsolutePath();
                    Runtime.getRuntime().exec(cmd);
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("buffer dir: " + dir.getAbsolutePath() + " is created successfully.");
                    }
                } else {
                    // Once again, check if it has been created successfully.
                    // Prevent problems created by multiple processes at the same time
                    if (!dir.exists()) {
                        throw new IOException("buffer dir:" + dir.getAbsolutePath() + " is created failure");
                    }
                }
            } else {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("buffer dir: " + dirPath + " already exists.");
                }
            }
        } else {
            throw new IOException("creating buffer dir: " + dir.getAbsolutePath() + "failed.");
        }

        return dir;
    }

    public synchronized void initialize(Configuration conf) throws IOException {
        if (this.isInitialize.get()) {
            return;
        }
        this.singleBufferSize = conf.getInt(
                CosNativeFileSystemConfigKeys.COS_BLOCK_SIZE_KEY,
                CosNativeFileSystemConfigKeys.DEFAULT_BLOCK_SIZE);

        int memoryBufferLimit = conf.getInt(
                CosNativeFileSystemConfigKeys.COS_UPLOAD_BUFFER_SIZE_KEY,
                CosNativeFileSystemConfigKeys.DEFAULT_UPLOAD_BUFFER_SIZE);

        this.diskBufferDir = this.createDir(conf.get(
                CosNativeFileSystemConfigKeys.COS_BUFFER_DIR_KEY,
                CosNativeFileSystemConfigKeys.DEFAULT_BUFFER_DIR));

        int bufferPoolSize = memoryBufferLimit / this.singleBufferSize;
        if (LOG.isDebugEnabled()) {
            LOG.debug("buffer pool size: {}", bufferPoolSize);
        }
        if (0 == bufferPoolSize) {
            throw new IOException(
                    String.format("The total size of the buffer[%d] is smaller than a single block [%d]."
                                    + "please consider increase the buffer size or decrease the block size",
                            memoryBufferLimit, this.singleBufferSize));
        }
        this.bufferPool = new LinkedBlockingQueue<ByteBuffer>(bufferPoolSize);
        for (int i = 0; i < bufferPoolSize; i++) {
            this.bufferPool.add(ByteBuffer.allocateDirect(this.singleBufferSize));
        }

        this.isInitialize.set(true);
    }

    void checkInitialize() throws IOException {
        if (!this.isInitialize.get()) {
            throw new IOException("The buffer pool has not been initialized yet");
        }
    }

    public ByteBufferWrapper getBuffer(int bufferSize) throws IOException, InterruptedException {
        this.checkInitialize();
        if (bufferSize > 0 && bufferSize <= this.singleBufferSize) {
            ByteBufferWrapper byteBufferWrapper = this.getByteBuffer();
            if (null == byteBufferWrapper) {
                // 内存缓冲区不够用了，则使用磁盘缓冲区
                byteBufferWrapper = this.getMappedBuffer();
            }
            return byteBufferWrapper;
        } else {
            throw new IOException("Parameter buffer size out of range: 1048576 " + " to "
                    + this.singleBufferSize + " request buffer size: " + String.valueOf(bufferSize));
        }
    }

    ByteBufferWrapper getByteBuffer() throws IOException, InterruptedException {
        this.checkInitialize();
        ByteBuffer buffer = this.bufferPool.poll();
        return buffer == null ? null : new ByteBufferWrapper(buffer);
    }

    ByteBufferWrapper getMappedBuffer() throws IOException, InterruptedException {
        this.checkInitialize();
        File tmpFile = File.createTempFile(
                Constants.BLOCK_TMP_FILE_PREFIX,
                Constants.BLOCK_TMP_FILE_SUFFIX,
                this.diskBufferDir
        );
        tmpFile.deleteOnExit();
        RandomAccessFile raf = new RandomAccessFile(tmpFile, "rw");
        raf.setLength(this.singleBufferSize);
        MappedByteBuffer buf = raf.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, this.singleBufferSize);
        return new ByteBufferWrapper(buf, raf, tmpFile);
    }

    public void returnBuffer(ByteBufferWrapper byteBufferWrapper) throws IOException, InterruptedException {
        if (null == this.bufferPool || null == byteBufferWrapper) {
            return;
        }

        if (byteBufferWrapper.isDiskBuffer()) {
            byteBufferWrapper.close();
        } else {
            ByteBuffer byteBuffer = byteBufferWrapper.getByteBuffer();
            if (null != byteBuffer) {
                byteBuffer.clear();
                this.bufferPool.put(byteBuffer);
            }
        }
    }
}

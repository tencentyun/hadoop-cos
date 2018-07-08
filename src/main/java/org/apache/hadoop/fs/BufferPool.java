package org.apache.hadoop.fs;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class BufferPool {
    static final Logger LOG = LoggerFactory.getLogger(BufferPool.class);

    private static BufferPool ourInstance = new BufferPool();

    public static BufferPool getInstance() {
        return ourInstance;
    }

    private BlockingQueue<ByteBuffer> ByteBufferPool = null;
    private BlockingQueue<ByteBuffer> mappedBufferPool = null;
    private String diskBufferDir = null;

    private int memorySizeLimit = 0;
    private int mappedSizeLimit = 0;
    private AtomicBoolean isInitialize = new AtomicBoolean(false);

    private BufferPool() {
    }

    public synchronized void initialize(Configuration conf) throws IOException {
        if (this.isInitialize.get()) {
            return;
        }
        int blockSize = conf.getInt(
                CosNativeFileSystemConfigKeys.COS_BLOCK_SIZE_KEY,
                CosNativeFileSystemConfigKeys.DEFAULT_BLOCK_SIZE);
        this.memorySizeLimit = conf.getInt(
                CosNativeFileSystemConfigKeys.COS_MEMORY_BUFFER_SIZE_KEY,
                CosNativeFileSystemConfigKeys.DEFAULT_MEMORY_BUFFER_SIZE);

        this.mappedSizeLimit = conf.getInt(
                CosNativeFileSystemConfigKeys.COS_MAPPED_BUFFER_SIZE_KEY,
                CosNativeFileSystemConfigKeys.DEFAULT_MAPPED_BUFFER_SIZE);
        if (this.mappedSizeLimit < 0 || this.mappedSizeLimit == Integer.MAX_VALUE) {
            LOG.warn("The size of the mapped buffer is limited as 1MB to 2GB (not include).");
            this.mappedSizeLimit = CosNativeFileSystemConfigKeys.DEFAULT_MAPPED_BUFFER_SIZE;
        }

        this.diskBufferDir = conf.get(
                CosNativeFileSystemConfigKeys.COS_BUFFER_DIR_KEY,
                CosNativeFileSystemConfigKeys.DEFAULT_BUFFER_DIR);

        int memoryBufferNumber = conf.getInt(
                CosNativeFileSystemConfigKeys.COS_MEMORY_BUFFER_POOL_SIZE_KEY,
                CosNativeFileSystemConfigKeys.DEFAULT_MEMORY_BUFFER_POOL_SIZE
        );
        int mappedBufferNumber = conf.getInt(
                CosNativeFileSystemConfigKeys.COS_MAPPED_BUFFER_POOL_SIZE_KEY,
                CosNativeFileSystemConfigKeys.DEFAULT_MAPPED_BUFFER_POOL_SIZE
        );
        this.ByteBufferPool = new LinkedBlockingQueue<>(memoryBufferNumber);
        for (int i = 0; i < memoryBufferNumber; i++) {
            this.ByteBufferPool.add(ByteBuffer.allocate(this.memorySizeLimit));
        }
        this.mappedBufferPool = new LinkedBlockingQueue<>(mappedBufferNumber);
        for (int i = 0; i < mappedBufferNumber; i++) {
            File tmpFile = File.createTempFile(
                    Constants.BLOCK_TMP_FILE_PREFIX,
                    Constants.BLOCK_TMP_FILE_SUFFIX,
                    new File(this.diskBufferDir)
            );
            tmpFile.deleteOnExit();
            RandomAccessFile raf = new RandomAccessFile(tmpFile, "rw");
            raf.setLength(this.mappedSizeLimit);
            MappedByteBuffer buf = raf.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, this.mappedSizeLimit);
            this.mappedBufferPool.add(buf);
        }
        this.isInitialize.set(true);
        LOG.info(" memory size limit: " + this.memorySizeLimit
                + " mapped size limit: " + this.mappedSizeLimit
                + " disk buffer dir: " + this.diskBufferDir
                + " memory buffer number: " + memoryBufferNumber
                + " mapped buffer number: " + mappedBufferNumber);
    }

    void checkInitialize() throws IOException {
        if (!this.isInitialize.get()) {
            throw new IOException("The buffer pool has not been initialized yet");
        }
    }

    public ByteBuffer getBuffer(int bufferSize) throws IOException, InterruptedException {
        this.checkInitialize();
        if (bufferSize > 0 && bufferSize <= this.memorySizeLimit) {
            return this.getByteBuffer();
        } else if (bufferSize > this.memorySizeLimit && bufferSize <= this.mappedSizeLimit) {
            return this.getMappedBuffer();
        } else {
            throw new IOException("Parameter buffer size out of range " + this.memorySizeLimit + " to "
                    + this.mappedSizeLimit);
        }
    }

    ByteBuffer getByteBuffer() throws IOException, InterruptedException {
        this.checkInitialize();
        return this.ByteBufferPool.poll(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
    }

    ByteBuffer getMappedBuffer() throws IOException, InterruptedException {
        this.checkInitialize();
        return this.mappedBufferPool.poll(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
    }

    public void returnBuffer(ByteBuffer buffer) {
        buffer.clear();
        if (buffer instanceof MappedByteBuffer) {
            if (null == this.mappedBufferPool) {
                return;
            }
            this.mappedBufferPool.add(buffer);
        } else {
            if (null == this.ByteBufferPool) {
                return;
            }
            this.ByteBufferPool.add(buffer);
        }
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        if (null != this.ByteBufferPool) {
            this.ByteBufferPool.clear();
        }
        if (null != this.mappedBufferPool) {
            for (ByteBuffer buffer : this.mappedBufferPool) {
                if (buffer instanceof MappedByteBuffer) {
                    Method getCleanerMethod = null;
                    try {
                        getCleanerMethod = buffer.getClass().getMethod("cleaner", new Class[0]);
                        getCleanerMethod.setAccessible(true);
                        sun.misc.Cleaner cleaner = (sun.misc.Cleaner) getCleanerMethod.invoke(buffer, new Object[0]);
                        cleaner.clean();
                    } catch (NoSuchMethodException e) {
                        LOG.error("closing buffer pool occurs an exception: ", e);
                    } catch (IllegalAccessException e) {
                        LOG.error("closing buffer pool occurs an exception: ", e);
                    } catch (InvocationTargetException e) {
                        LOG.error("closing buffer pool occurs an exception: ", e);
                    }
                }
            }
        }
    }
}

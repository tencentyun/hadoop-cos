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

    enum BufferType {
        MEMORY("Memory"), DISK("Disk");

        private final String str;

        BufferType(String str) {
            this.str = str;
        }

        public String getStr() {
            return str;
        }
    }

    private BlockingQueue<ByteBuffer> bufferPool = null;
    private BufferType type = null;
    private int singleBufferSize = 0;
    private String diskBufferDir = null;

    private AtomicBoolean isInitialize = new AtomicBoolean(false);

    private BufferPool() {
    }

    public synchronized void initialize(Configuration conf) throws IOException {
        if (this.isInitialize.get()) {
            return;
        }
        this.singleBufferSize = conf.getInt(
                CosNativeFileSystemConfigKeys.COS_BLOCK_SIZE_KEY,
                CosNativeFileSystemConfigKeys.DEFAULT_BLOCK_SIZE);

        String strBufferType = conf.get(
                CosNativeFileSystemConfigKeys.COS_UPLOAD_BUFFER_TYPE_KEY,
                CosNativeFileSystemConfigKeys.DEFAULT_UPLOAD_BUFFER_TYPE);
        if (strBufferType.equalsIgnoreCase(BufferType.MEMORY.getStr())) {
            this.type = BufferType.MEMORY;
        }
        if (strBufferType.equalsIgnoreCase(BufferType.DISK.getStr())) {
            this.type = BufferType.DISK;
        }
        if (null == this.type) {
            throw new IOException("The BufferType option specified in the configuration file is invalid. "
                    + "Valid values are: memory or disk");
        }

        int bufferSizeLimit = conf.getInt(
                CosNativeFileSystemConfigKeys.COS_UPLOAD_BUFFER_SIZE_KEY,
                CosNativeFileSystemConfigKeys.DEFAULT_UPLOAD_BUFFER_SIZE);
        this.diskBufferDir = conf.get(
                CosNativeFileSystemConfigKeys.COS_BUFFER_DIR_KEY,
                CosNativeFileSystemConfigKeys.DEFAULT_BUFFER_DIR);

        int bufferPoolSize = bufferSizeLimit / this.singleBufferSize;
        if (0 == bufferPoolSize) {
            throw new IOException(
                    String.format("The total size of the buffer[%d] is smaller than a single block [%d]."
                                    + "please consider increase the buffer size or decrease the block size",
                            bufferSizeLimit, this.singleBufferSize));
        }
        this.bufferPool = new LinkedBlockingQueue<>(bufferPoolSize);
        for (int i = 0; i < bufferPoolSize; i++) {
            if (this.type == BufferType.MEMORY) {
                this.bufferPool.add(ByteBuffer.allocateDirect(this.singleBufferSize));
            } else {
                File tmpFile = File.createTempFile(
                        Constants.BLOCK_TMP_FILE_PREFIX,
                        Constants.BLOCK_TMP_FILE_SUFFIX,
                        new File(this.diskBufferDir)
                );
                tmpFile.deleteOnExit();
                RandomAccessFile raf = new RandomAccessFile(tmpFile, "rw");
                raf.setLength(this.singleBufferSize);
                MappedByteBuffer buf = raf.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, this.singleBufferSize);
                this.bufferPool.add(buf);
            }
        }

        this.isInitialize.set(true);
    }

    void checkInitialize() throws IOException {
        if (!this.isInitialize.get()) {
            throw new IOException("The buffer pool has not been initialized yet");
        }
    }

    public ByteBuffer getBuffer(int bufferSize) throws IOException, InterruptedException {
        this.checkInitialize();
        if (bufferSize > 0 && bufferSize <= this.singleBufferSize) {
            if (this.type == BufferType.MEMORY) {
                return this.getByteBuffer();
            } else {
                return this.getMappedBuffer();
            }
        } else {
            throw new IOException("Parameter buffer size out of range: 1MB " + " to "
                    + this.singleBufferSize);
        }
    }

    ByteBuffer getByteBuffer() throws IOException, InterruptedException {
        this.checkInitialize();
        return this.bufferPool.poll(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
    }

    ByteBuffer getMappedBuffer() throws IOException, InterruptedException {
        this.checkInitialize();
        return this.bufferPool.poll(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
    }

    public void returnBuffer(ByteBuffer buffer) throws InterruptedException {
        buffer.clear();
        if (null == this.bufferPool) {
            return;
        }
        if (buffer instanceof ByteBuffer) {
            this.bufferPool.put(buffer);
        } else {
            this.bufferPool.put(buffer);
        }
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        if (this.type == BufferType.DISK) {
            for (ByteBuffer buffer : this.bufferPool) {
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
        this.bufferPool.clear();
    }
}

package org.apache.hadoop.fs.cosn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.cosn.buffer.*;
import org.apache.hadoop.fs.CosNConfigKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * BufferPool class is used to manage the buffers during program execution.
 * It is provided in a thread-safe singleton mode,and
 * keeps the program's memory and disk consumption at a stable value.
 */
public final class BufferPool {
    private static final Logger LOG =
            LoggerFactory.getLogger(BufferPool.class);

    private static BufferPool ourInstance = new BufferPool();

    /**
     * Use this method to get the instance of BufferPool.
     *
     * @return the instance of BufferPool
     */
    public static BufferPool getInstance() {
        return ourInstance;
    }

    private long partSize = 0;
    private long totalBufferSize = 0;
    private CosNBufferType bufferType;
    private CosNBufferFactory bufferFactory;
    private BlockingQueue<CosNByteBuffer> bufferPool;

    private AtomicInteger referCount = new AtomicInteger(0);
    private AtomicBoolean isInitialize = new AtomicBoolean(false);

    private BufferPool() {
    }

    /**
     * Create buffers correctly by reading the buffer file directory,
     * buffer pool size,and file block size in the configuration.
     *
     * @param conf Provides configurations for the Hadoop runtime
     * @throws IOException Configuration errors,
     *                     insufficient or no access for memory or
     *                     disk space may cause this exception
     */
    public synchronized void initialize(Configuration conf)
            throws IOException {
        LOG.debug("Initialize the buffer pool.");
        if (this.isInitialize.get()) {
            LOG.debug("Buffer pool: [{}] is initialized and referenced once. "
                    + "current reference count: [{}].", this, this.referCount);
            this.referCount.incrementAndGet();
            return;
        }

        this.partSize = conf.getLong(CosNConfigKeys.COSN_UPLOAD_PART_SIZE_KEY,
                CosNConfigKeys.DEFAULT_UPLOAD_PART_SIZE);
        // The part size of CosN can only support up to 2GB.
        if (this.partSize < Constants.MIN_PART_SIZE
                || this.partSize > Constants.MAX_PART_SIZE) {
            String exceptionMsg = String.format(
                    "The block size of CosN is limited to %d to %d. current " +
                            "block size: %d",
                    Constants.MIN_PART_SIZE, Constants.MAX_PART_SIZE,
                    this.partSize);
            throw new IllegalArgumentException(exceptionMsg);
        }

        this.bufferType = CosNBufferType.typeFactory(conf.get(
                CosNConfigKeys.COSN_UPLOAD_BUFFER_TYPE_KEY,
                CosNConfigKeys.DEFAULT_UPLOAD_BUFFER_TYPE));

        if (null == this.bufferType
                || (CosNBufferType.NON_DIRECT_MEMORY != this.bufferType
                && CosNBufferType.DIRECT_MEMORY != this.bufferType
                && CosNBufferType.MAPPED_DISK != this.bufferType)) {
            LOG.warn("The [{}] option is set incorrectly, using the default " +
                            "settings:"
                            + " [{}].",
                    CosNConfigKeys.COSN_UPLOAD_BUFFER_TYPE_KEY,
                    CosNConfigKeys.DEFAULT_UPLOAD_BUFFER_TYPE);
        }

        if (conf.get(CosNConfigKeys.COSN_UPLOAD_BUFFER_SIZE_KEY) == null) {
            this.totalBufferSize = conf.getLong(
                    CosNConfigKeys.COSN_UPLOAD_BUFFER_SIZE_PREV_KEY,
                    CosNConfigKeys.DEFAULT_UPLOAD_BUFFER_SIZE);
        } else {
            this.totalBufferSize = conf.getLong(
                    CosNConfigKeys.COSN_UPLOAD_BUFFER_SIZE_KEY,
                    CosNConfigKeys.DEFAULT_UPLOAD_BUFFER_SIZE);
        }

        if (this.totalBufferSize < 0 && -1 != this.totalBufferSize) {
            String errMsg = String.format("Negative buffer size: %d",
                    this.totalBufferSize);
            throw new IllegalArgumentException(errMsg);
        }

        if (this.totalBufferSize == -1) {
            LOG.info("{} is set to -1, so the 'mapped_disk' buffer will be " +
                    "used by "
                    + "default.", CosNConfigKeys.COSN_UPLOAD_BUFFER_SIZE_KEY);
            this.bufferType = CosNBufferType.MAPPED_DISK;
        }

        LOG.info("The type of the upload buffer pool is [{}]. Buffer size:[{}]",
                this.bufferType, this.totalBufferSize);
        if (this.bufferType == CosNBufferType.NON_DIRECT_MEMORY) {
            this.bufferFactory = new CosNNonDirectBufferFactory();
        } else if (this.bufferType == CosNBufferType.DIRECT_MEMORY) {
            this.bufferFactory = new CosNDirectBufferFactory();
        } else if (this.bufferType == CosNBufferType.MAPPED_DISK) {
            String tmpDir = conf.get(CosNConfigKeys.COSN_TMP_DIR,
                    CosNConfigKeys.DEFAULT_TMP_DIR);
            this.bufferFactory = new CosNMappedBufferFactory(tmpDir);
        } else {
            String exceptionMsg = String.format("The type of the upload " +
                    "buffer is "
                    + "invalid. buffer type: %s", this.bufferType);
            throw new IllegalArgumentException(exceptionMsg);
        }

        // If totalBufferSize is greater than 0, and the buffer type is direct
        // memory
        // or mapped memory, it need to be allocate in advance to reduce the
        // overhead of repeated allocations and releases.
        if (this.totalBufferSize > 0
                && (CosNBufferType.NON_DIRECT_MEMORY == this.bufferType
                || CosNBufferType.DIRECT_MEMORY == this.bufferType
                || CosNBufferType.MAPPED_DISK == this.bufferType)) {
            int bufferNumber = (int) (totalBufferSize / partSize);
            if (bufferNumber == 0) {
                String errMsg = String.format("The buffer size: [%d] is at " +
                                "least "
                                + "greater than or equal to the size of a " +
                                "block: [%d]",
                        this.totalBufferSize, this.partSize);
                throw new IllegalArgumentException(errMsg);
            }

            LOG.info("Initialize the {} buffer pool. size: {}", this.bufferType,
                    bufferNumber);
            this.bufferPool =
                    new LinkedBlockingQueue<>(bufferNumber);
            for (int i = 0; i < bufferNumber; i++) {
                CosNByteBuffer cosNByteBuffer =
                        this.bufferFactory.create((int) this.partSize);
                if (null == cosNByteBuffer) {
                    String exceptionMsg = String.format("create buffer failed" +
                                    ". buffer type: %s, " +
                                    "buffer factory: %s",
                            this.bufferType.getName(),
                            this.bufferFactory.getClass().getName());
                    throw new IOException(exceptionMsg);
                }
                this.bufferPool.add(cosNByteBuffer);
            }
        }

        this.referCount.incrementAndGet();
        this.isInitialize.set(true);
    }

    /**
     * Check if the buffer pool has been initialized.
     *
     * @throws IOException if the buffer pool is not initialized
     */
    private void checkInitialize() throws IOException {
        if (!this.isInitialize.get()) {
            throw new IOException(
                    "The buffer pool has not been initialized yet");
        }

        // If the buffer pool is null, but the buffer size is not -1, this is
        // illegal.
        if (-1 != this.totalBufferSize && null == this.bufferPool) {
            throw new IOException("The buffer pool is null, but the size is " +
                    "not -1"
                    + "(unlimited).");
        }
    }

    /**
     * Obtain a buffer from this buffer pool through the method.
     *
     * @param bufferSize expected buffer size to get
     * @return a buffer that satisfies the totalBufferSize.
     * @throws IOException if the buffer pool not initialized,
     *                     or the totalBufferSize parameter is not within
     *                     the range[1MB to the single buffer size]
     */
    public CosNByteBuffer getBuffer(int bufferSize) throws IOException,
            InterruptedException {
        this.checkInitialize();
        LOG.debug("Get a buffer[size: {}, current buffer size: {}]. Thread " +
                        "[id: {}, " +
                        "name: {}].",
                bufferSize,
                this.totalBufferSize,
                Thread.currentThread().getId(),
                Thread.currentThread().getName());
        if (bufferSize > 0 && bufferSize <= this.partSize) {
            // unlimited
            if (-1 == this.totalBufferSize) {
                return bufferFactory.create(bufferSize);
            }
            // limited
            return this.bufferPool.poll(Long.MAX_VALUE, TimeUnit.SECONDS);
        } else {
            String exceptionMsg = String.format(
                    "Parameter buffer size out of range: 1 to %d",
                    this.partSize
            );
            throw new IOException(exceptionMsg);
        }
    }

    /**
     * return the byte buffer wrapper to the buffer pool.
     *
     * @param buffer the byte buffer wrapper getting from the pool
     * @throws IOException some io error occurs
     */
    public void returnBuffer(CosNByteBuffer buffer)
            throws IOException {
        LOG.debug("Return a buffer. Thread[id: {}, name: {}].",
                Thread.currentThread().getId(),
                Thread.currentThread().getName());
        if (null == buffer) {
            LOG.error("The buffer returned is null. Ignore it.");
            return;
        }

        this.checkInitialize();

        if (-1 == this.totalBufferSize) {
            LOG.debug("No buffer pool is maintained, and release the buffer "
                    + "directly.");
            this.bufferFactory.release(buffer);
        } else {
            LOG.debug("Return the buffer to the buffer pool.");
            buffer.getByteBuffer().clear();
            if (!this.bufferPool.offer(buffer)) {
                LOG.error("Return the buffer to buffer pool failed.");
            }
        }
    }

    /**
     *  close
     */
    public synchronized void close() {
        LOG.info("Close a buffer pool instance.");

        if (!this.isInitialize.get()) {
            // Closed or not initialized, return directly.
            LOG.warn("The buffer pool has been closed. no changes would be " +
                    "execute.");
            return;
        }

        if (this.referCount.decrementAndGet() > 0) {
            return;
        }

        LOG.info("Begin to release the buffers.");
        // First, release the buffers in the buffer queue.
        if (null != this.bufferPool) {
            for (CosNByteBuffer buffer : this.bufferPool) {
                this.bufferFactory.release(buffer);
            }
            this.bufferPool.clear();
        }

        if (this.referCount.get() == 0) {
            this.isInitialize.set(false);
        }
    }
}

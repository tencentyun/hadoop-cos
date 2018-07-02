package org.apache.hadoop.fs.cosnative;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.qcloud.cos.model.PartETag;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;

public class CosFsDataOutputStream extends OutputStream {
    static final Logger LOG = LoggerFactory.getLogger(CosFsDataOutputStream.class);
    private final Configuration conf;
    private final NativeFileSystemStore store;
    private MessageDigest digest;
    private long blockSize;
    private String key;
    private int currentBlockId = 0;
    private final Set<ByteBuffer> blockCacheBuffers = new HashSet<>();
    private ByteBuffer currentBlockBuffer = null;
    private OutputStream currentBlockOutputStream = null;
    private String uploadId = null;
    private final ListeningExecutorService executorService;
    private final List<ListenableFuture<PartETag>> partEtagList = new LinkedList<ListenableFuture<PartETag>>();
    private final BlockingQueue<ByteBuffer> bufferPool;
    private int blockWritten = 0;
    private boolean closed = false;

    public CosFsDataOutputStream(Configuration conf, NativeFileSystemStore store, String key, long blockSize) throws IOException {
        this.conf = conf;
        this.store = store;
        this.key = key;
        this.blockSize = blockSize;
        if (this.blockSize < Constants.MIN_PART_SIZE) {
            LOG.info(String.format("The minimum size of a single block is limited to %d", Constants.MIN_PART_SIZE));
            this.blockSize = Constants.MIN_PART_SIZE;
        }
        if (this.blockSize > Constants.MAX_PART_SIZE) {
            LOG.warn(String.format("The maximum size of a single block is limited to %d", Constants.MAX_PART_SIZE));
            this.blockSize = Constants.MAX_PART_SIZE;
        }
        this.executorService = MoreExecutors.listeningDecorator(
                Executors.newFixedThreadPool(
                        conf.getInt(CosNativeFileSystemConfigKeys.UPLOAD_THREAD_POOL_SIZE_KEY, CosNativeFileSystemConfigKeys.DEFAULT_THREAD_POOL_SIZE)
                )
        );
        this.bufferPool = this.newBufferPool(
                conf.getInt(CosNativeFileSystemConfigKeys.UPLOAD_THREAD_POOL_SIZE_KEY, CosNativeFileSystemConfigKeys.DEFAULT_THREAD_POOL_SIZE) * 2);
        this.currentBlockBuffer = this.getBuffer();
        try {
            this.digest = MessageDigest.getInstance("MD5");
            this.currentBlockOutputStream = new DigestOutputStream(new ByteBufferOutputStream(this.currentBlockBuffer), this.digest);
        } catch (NoSuchAlgorithmException e) {
            this.digest = null;
            this.currentBlockOutputStream = new ByteBufferOutputStream(this.currentBlockBuffer);
        }
    }

    /**
     * Initialize the disk cache pool
     *
     * @param size
     * @return a thread-safe disk cache pool
     */
    public BlockingQueue<ByteBuffer> newBufferPool(int size) throws IOException {
        BlockingQueue<ByteBuffer> bufferPool = new LinkedBlockingQueue<>();
        for (int i = 0; i < size; i++) {
            File tmpFile = File.createTempFile(
                    Constants.BLOCK_TMP_FILE_PREFIX,
                    Constants.BLOCK_TMP_FILE_SUFFIX,
                    new File(
                            this.conf.get(
                                    CosNativeFileSystemConfigKeys.COS_BUFFER_DIR_KEY,
                                    CosNativeFileSystemConfigKeys.DEFAULT_BUFFER_DIR)
                    )
            );
            tmpFile.deleteOnExit();
            RandomAccessFile raf = new RandomAccessFile(tmpFile, "rw");
            raf.setLength(this.blockSize);
            MappedByteBuffer buffer = raf.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, this.blockSize);
            bufferPool.add(buffer);
        }
        return bufferPool;
    }

    public ByteBuffer getBuffer() {
        if (null == this.bufferPool) {
            return null;
        }

        ByteBuffer buf = null;
        try {
            buf = this.bufferPool.poll(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            LOG.error("get a buffer from cache poll interrupted.", e);
            return null;
        }

        return buf;
    }

    public void returnBuffer(ByteBuffer buffer) {
        if (null == this.bufferPool) {
            return;
        }
        buffer.clear();
        this.bufferPool.add(buffer);
    }

    public void closeBufferPool() {
        if (null == this.bufferPool || this.bufferPool.size() == 0) {
            return;
        }

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

    @Override
    public void flush() throws IOException {
        this.currentBlockOutputStream.flush();
    }

    @Override
    public synchronized void close() throws IOException {
        if (this.closed) {
            return;
        }
        this.currentBlockOutputStream.flush();
        this.currentBlockOutputStream.close();
        LOG.info("output stream has been close. begin to upload last block: " + String.valueOf(this.currentBlockId));
        if (!this.blockCacheBuffers.contains(this.currentBlockBuffer)) {
            this.blockCacheBuffers.add(this.currentBlockBuffer);
        }
        // 加到块列表中去
        if (this.blockCacheBuffers.size() == 1) {
            // 单个文件就可以上传完成
            byte[] md5Hash = this.digest == null ? null : this.digest.digest();
            store.storeFile(this.key, new ByteBufferInputStream(this.currentBlockBuffer), md5Hash, this.currentBlockBuffer.remaining());
        } else {
            if (this.blockWritten > 0) {
                // 上传最后一片
                PartETag partETag = store.uploadPart(new ByteBufferInputStream(currentBlockBuffer), key, uploadId, currentBlockId + 1, currentBlockBuffer.remaining());
            }
            this.executorService.shutdown();
            final List<PartETag> partETagList = this.waitForFinishPartUploads();
            store.completeMultipartUpload(this.key, this.uploadId, partETagList);
        }
        LOG.info("OutputStream for key '{}' upload complete", key);
        this.blockWritten = 0;
        this.returnBuffer(currentBlockBuffer);
        this.closeBufferPool();
        this.closed = true;
    }

    private List<PartETag> waitForFinishPartUploads() throws IOException {
        try {
            LOG.info("waiting for finish part uploads ....");
            return Futures.allAsList(this.partEtagList).get();
        } catch (InterruptedException e) {
            LOG.error("Interrupt the part upload", e);
            return null;
        } catch (ExecutionException e) {
            LOG.error("cancelling futures");
            for (ListenableFuture<PartETag> future : this.partEtagList) {
                future.cancel(true);
            }
            (store).abortMultipartUpload(this.key, this.uploadId);
            LOG.error("Multipart upload with id: " + this.uploadId + " to " + this.key, e);
            throw new IOException("Multipart upload with id: " + this.uploadId + " to " + this.key, e);
        }
    }

    private void uploadPart() throws IOException {
        this.currentBlockOutputStream.flush();
        this.currentBlockOutputStream.close();
        this.blockCacheBuffers.add(this.currentBlockBuffer);

        if (this.currentBlockId == 0) {
            uploadId = (store).getUploadId(key);
        }

        ListenableFuture<PartETag> partETagListenableFuture = this.executorService.submit(new Callable<PartETag>() {
            private final ByteBuffer buf = currentBlockBuffer;
            private final String localKey = key;
            private final String localUploadId = uploadId;
            private final int blockId = currentBlockId;

            @Override
            public PartETag call() throws Exception {
                PartETag partETag = (store).uploadPart(new ByteBufferInputStream(this.buf), this.localKey, this.localUploadId, this.blockId + 1, this.buf.remaining());
                returnBuffer(buf);
                return partETag;
            }
        });
        this.partEtagList.add(partETagListenableFuture);
        this.currentBlockBuffer = this.getBuffer();
        this.currentBlockId++;
        if (null != this.digest) {
            this.digest.reset();
            LOG.info("");
            this.currentBlockOutputStream = new DigestOutputStream(new ByteBufferOutputStream(this.currentBlockBuffer), this.digest);
        } else {
            this.currentBlockOutputStream = new ByteBufferOutputStream(this.currentBlockBuffer);
        }
    }

    @Override
    public void write(byte[] b) throws IOException {
        this.write(b, 0, b.length);
    }

    @Override
    public void write(int b) throws IOException {
        byte[] singleBytes = new byte[1];
        singleBytes[0] = (byte) b;
        this.write(singleBytes, 0, 1);
    }

    @Override
    public synchronized void write(byte[] b, int off, int len) throws IOException {
        if (this.closed) {
            throw new IOException("block stream has been closed.");
        }
        this.currentBlockOutputStream.write(b, off, len);
        this.blockWritten += len;
        if (this.blockWritten >= this.blockSize) {
            this.uploadPart();
            this.blockWritten = 0;
        }
    }
}

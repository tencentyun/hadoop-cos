package org.apache.hadoop.fs;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.qcloud.cos.model.PartETag;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.security.DigestInputStream;
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
    private int blockWritten = 0;
    private boolean closed = false;

    public CosFsDataOutputStream(Configuration conf, NativeFileSystemStore store, String key, long blockSize, ExecutorService executorService) throws IOException {
        this.conf = conf;
        this.store = store;
        this.key = key;
        this.blockSize = blockSize;
        if (this.blockSize < Constants.MIN_PART_SIZE) {
            LOG.warn(String.format("The minimum size of a single block is limited to %d.", Constants.MIN_PART_SIZE));
            this.blockSize = Constants.MIN_PART_SIZE;
        }
        if (this.blockSize > Constants.MAX_PART_SIZE) {
            LOG.warn(String.format("The maximum size of a single block is limited to %d.", Constants.MAX_PART_SIZE));
            this.blockSize = Constants.MAX_PART_SIZE;
        }
        this.executorService = MoreExecutors.listeningDecorator(executorService);
        try {
            this.currentBlockBuffer = BufferPool.getInstance().getBuffer((int) this.blockSize);
        } catch (InterruptedException e) {
            throw new IOException("Getting a buffer size: " + String.valueOf(this.blockSize) + " from buffer pool occurs an exception: ", e);
        }
        try {
            this.digest = MessageDigest.getInstance("MD5");
            this.currentBlockOutputStream = new DigestOutputStream(
                    new ByteBufferOutputStream(this.currentBlockBuffer), this.digest);
        } catch (NoSuchAlgorithmException e) {
            this.digest = null;
            this.currentBlockOutputStream = new ByteBufferOutputStream(this.currentBlockBuffer);
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
            PartETag partETag = null;
            if (this.blockWritten > 0) {
                LOG.info("upload last part... blockId: " + this.currentBlockId + " written: " + this.blockWritten);
                partETag = store.uploadPart(
                        new ByteBufferInputStream(currentBlockBuffer), key, uploadId,
                        currentBlockId + 1, currentBlockBuffer.remaining());
            }
            final List<PartETag> partETagList = this.waitForFinishPartUploads();
            if (null != partETag) {
                partETagList.add(partETag);
            }
            store.completeMultipartUpload(this.key, this.uploadId, partETagList);
        }
        try {
            BufferPool.getInstance().returnBuffer(this.currentBlockBuffer);
        } catch (InterruptedException e) {
            LOG.error("Returning the buffer to BufferPool occurs an exception.", e);
        }
        LOG.info("OutputStream for key '{}' upload complete", key);
        this.blockWritten = 0;
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
                PartETag partETag = (store).uploadPart(
                        new ByteBufferInputStream(this.buf), this.localKey, this.localUploadId,
                        this.blockId + 1, this.buf.remaining());
                BufferPool.getInstance().returnBuffer(this.buf);
                return partETag;
            }
        });
        this.partEtagList.add(partETagListenableFuture);
        try {
            this.currentBlockBuffer = BufferPool.getInstance().getBuffer((int) this.blockSize);
        } catch (InterruptedException e) {
            throw new IOException("Getting a buffer size: " + String.valueOf(this.blockSize) + " from buffer pool occurs an exception: ", e);
        }
        this.currentBlockId++;
        if (null != this.digest) {
            this.digest.reset();
            this.currentBlockOutputStream = new DigestOutputStream(
                    new ByteBufferOutputStream(this.currentBlockBuffer), this.digest);
        } else {
            this.currentBlockOutputStream = new ByteBufferOutputStream(this.currentBlockBuffer);
        }
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        if (this.closed) {
            throw new IOException("block stream has been closed.");
        }

        while (len > 0) {
            long writeBytes = 0;
            if (this.blockWritten + len > this.blockSize) {
                writeBytes = this.blockSize - this.blockWritten;
            } else {
                writeBytes = len;
            }

            this.currentBlockOutputStream.write(b, off, (int) writeBytes);
            this.blockWritten += writeBytes;
            if (this.blockWritten >= this.blockSize) {
                this.uploadPart();
                this.blockWritten = 0;
            }
            len -= writeBytes;
            off += writeBytes;
        }
    }

    @Override
    public void write(byte[] b) throws IOException {
        this.write(b, 0, b.length);
    }

    @Override
    public void write(int b) throws IOException {
        if (this.closed) {
            throw new IOException("block stream has been closed.");
        }

        byte[] singleBytes = new byte[1];
        singleBytes[0] = (byte) b;
        this.currentBlockOutputStream.write(singleBytes, 0, 1);
        this.blockWritten += 1;
        if (this.blockWritten >= this.blockSize) {
            this.uploadPart();
            this.blockWritten = 0;
        }
    }
}

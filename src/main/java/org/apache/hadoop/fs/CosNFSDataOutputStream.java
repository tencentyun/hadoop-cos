package org.apache.hadoop.fs;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.qcloud.cos.model.PartETag;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.cosn.*;
import org.apache.hadoop.fs.cosn.buffer.CosNByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

public class CosNFSDataOutputStream extends OutputStream implements Abortable {
    static final Logger LOG =
            LoggerFactory.getLogger(CosNFSDataOutputStream.class);

    private final Configuration conf;
    private final NativeFileSystemStore store;
    private MessageDigest digest;
    private long blockSize;
    private String key;
    private int currentBlockId = 0;
    private CosNByteBuffer currentBlockBuffer;
    private OutputStream currentBlockOutputStream;
    private String uploadId = null;
    private final ListeningExecutorService executorService;
    private final List<ListenableFuture<PartETag>> partEtagList =
            new LinkedList<ListenableFuture<PartETag>>();
    private int blockWritten = 0;
    private WriteConsistencyChecker writeConsistencyChecker = null;
    private boolean closed = false;

    /**
     * Output stream
     *
     * @param conf config
     * @param store native file system
     * @param key cos key
     * @param blockSize block config size
     * @param executorService thread executor
     * @param checksEnabled check flag
     * @throws IOException
     */
    public CosNFSDataOutputStream(
            Configuration conf,
            NativeFileSystemStore store,
            String key, long blockSize,
            ExecutorService executorService, boolean checksEnabled) throws IOException {
        this.conf = conf;
        this.store = store;
        this.key = key;
        this.blockSize = blockSize;

        if (checksEnabled) {
            LOG.info("The consistency checker is enabled.");
            this.writeConsistencyChecker = new WriteConsistencyChecker(this.store, this.key);
        } else {
            LOG.warn("The consistency checker is disabled.");
        }

        if (this.blockSize < Constants.MIN_PART_SIZE) {
            LOG.warn("The minimum size of a single block is limited to " +
                    "greater than or equal to {}.", Constants.MIN_PART_SIZE);
            this.blockSize = Constants.MIN_PART_SIZE;
        }
        if (this.blockSize > Constants.MAX_PART_SIZE) {
            LOG.warn("The maximum size of a single block is limited to " +
                    "smaller than or equal to {}.", Constants.MAX_PART_SIZE);
            this.blockSize = Constants.MAX_PART_SIZE;
        }

        this.executorService =
                MoreExecutors.listeningDecorator(executorService);

        // malloc when trigger the write operations
        this.currentBlockBuffer = null;
        this.currentBlockOutputStream = null;
    }

    @Override
    public void flush() throws IOException {
        // create but not call write before
        if (this.currentBlockOutputStream == null) {
            initCurrentBlock();
        }
        this.currentBlockOutputStream.flush();
    }

    @Override
    public synchronized void close() throws IOException {
        if (this.closed) {
            return;
        }

        if (this.currentBlockOutputStream == null) {
            initCurrentBlock();
        }

        try {
            this.currentBlockOutputStream.flush();
            this.currentBlockOutputStream.close();
            // 加到块列表中去
            if (this.currentBlockId == 0) {
                // 单个文件就可以上传完成
                LOG.info("Single file upload...  key: {}, blockId: {}, blockWritten: {}.", this.key,
                        this.currentBlockId,
                        this.blockWritten);
                byte[] md5Hash = this.digest == null ? null : this.digest.digest();
                int size = this.currentBlockBuffer.getByteBuffer().remaining();
                store.storeFile(this.key,
                        new BufferInputStream(this.currentBlockBuffer),
                        md5Hash,
                        this.currentBlockBuffer.getByteBuffer().remaining());
                if (null != this.writeConsistencyChecker) {
                    this.writeConsistencyChecker.incrementWrittenBytes(size);
                }
                if (null != this.writeConsistencyChecker) {
                    this.writeConsistencyChecker.finish();
                    if (!this.writeConsistencyChecker.getCheckResult().isSucceeded()) {
                        String exceptionMsg = String.format("Failed to upload the key: %s, error message: %s.",
                                this.key,
                                this.writeConsistencyChecker.getCheckResult().getDescription());
                        throw new IOException(exceptionMsg);
                    }
                    LOG.info("Upload the key [{}] successfully. check message: {}.", this.key,
                            this.writeConsistencyChecker.getCheckResult().getDescription());
                } else {
                    LOG.info("OutputStream for key [{}] upload complete. But it is not checked.", key);
                }
            } else {
                PartETag partETag = null;
                if (this.blockWritten > 0) {
                    this.currentBlockId++;
                    LOG.info("Upload the last part. key: {}, blockId: [{}], blockWritten: [{}]",
                            this.key, this.currentBlockId, this.blockWritten);
                    byte[] md5Hash = this.digest == null ? null : this.digest.digest();
                    int size = this.currentBlockBuffer.getByteBuffer().remaining();
                    partETag = store.uploadPart(
                            new BufferInputStream(this.currentBlockBuffer), key,
                            uploadId, currentBlockId,
                            currentBlockBuffer.getByteBuffer().remaining(), md5Hash);
                    if (null != this.writeConsistencyChecker) {
                        this.writeConsistencyChecker.incrementWrittenBytes(size);
                    }
                }
                final List<PartETag> futurePartEtagList = this.waitForFinishPartUploads();
                if (null == futurePartEtagList) {
                    throw new IOException("failed to multipart upload to cos, " +
                            "abort it.");
                }
                List<PartETag> tempPartETagList = new LinkedList<PartETag>(futurePartEtagList);
                if (null != partETag) {
                    tempPartETagList.add(partETag);
                }
                store.completeMultipartUpload(this.key, this.uploadId,
                        tempPartETagList);
                if (null != this.writeConsistencyChecker) {
                    this.writeConsistencyChecker.finish();
                    if (!this.writeConsistencyChecker.getCheckResult().isSucceeded()) {
                        String exceptionMsg = String.format("Failed to upload the key: %s, error message: %s.",
                                this.key,
                                this.writeConsistencyChecker.getCheckResult().getDescription());
                        throw new IOException(exceptionMsg);
                    }
                    LOG.info("Upload the key [{}] successfully. check message: {}.", this.key,
                            this.writeConsistencyChecker.getCheckResult().getDescription());
                } else {
                    LOG.info("OutputStream for key [{}] upload complete. But it is not checked.", key);
                }
            }
        } finally {
            BufferPool.getInstance().returnBuffer(this.currentBlockBuffer);
            this.blockWritten = 0;
            this.closed = true;
            this.writeConsistencyChecker = null;
            this.currentBlockBuffer = null;
            this.currentBlockOutputStream = null;
        }
    }

    @Override
    public void abort() throws IOException {
        LOG.info("abort file upload, key:{}, uploadId:{}", key, uploadId);
        if (this.closed) {
            return;
        }

        try {
            if(currentBlockOutputStream != null) {
                this.currentBlockOutputStream.flush();
                this.currentBlockOutputStream.close();
            }
            if(uploadId != null) {
                this.store.abortMultipartUpload(key, uploadId);
            }
        } finally {
            this.closed = true;
            BufferPool.getInstance().returnBuffer(this.currentBlockBuffer);
            this.blockWritten = 0;
            this.writeConsistencyChecker = null;
            this.currentBlockBuffer = null;
            this.currentBlockOutputStream = null;
        }
    }

    private List<PartETag> waitForFinishPartUploads() throws IOException {
        try {
            LOG.info("Waiting for finish part uploads...");
            return Futures.allAsList(this.partEtagList).get();
        } catch (InterruptedException e) {
            LOG.error("Interrupt the part upload...", e);
            return null;
        } catch (ExecutionException e) {
            LOG.error("Cancelling futures...");
            for (ListenableFuture<PartETag> future : this.partEtagList) {
                future.cancel(true);
            }
            (store).abortMultipartUpload(this.key, this.uploadId);
            String exceptionMsg = String.format("multipart upload with id: %s" +
                    " to %s.", this.uploadId, this.key);
            throw new IOException(exceptionMsg);
        }
    }

    private void uploadPart() throws IOException {
        this.currentBlockOutputStream.flush();
        this.currentBlockOutputStream.close();

        if (this.currentBlockId == 0) {
            uploadId = (store).getUploadId(key);
        }

        this.currentBlockId++;
        LOG.debug("upload part blockId: {}, uploadId: {}.", this.currentBlockId, this.uploadId);
        final byte[] md5Hash = this.digest == null ? null : this.digest.digest();
        ListenableFuture<PartETag> partETagListenableFuture =
                this.executorService.submit(new Callable<PartETag>() {
                    private final CosNByteBuffer buffer = currentBlockBuffer;
                    private final String localKey = key;
                    private final String localUploadId = uploadId;
                    private final int blockId = currentBlockId;
                    private final byte[] blockMD5Hash = md5Hash;

                    @Override
                    public PartETag call() throws Exception {
                        try {
                            PartETag partETag = (store).uploadPart(
                                    new BufferInputStream(this.buffer),
                                    this.localKey,
                                    this.localUploadId,
                                    this.blockId,
                                    this.buffer.getByteBuffer().remaining(), this.blockMD5Hash);
                            return partETag;
                        } finally {
                            BufferPool.getInstance().returnBuffer(this.buffer);
                        }
                    }
                });
        this.partEtagList.add(partETagListenableFuture);
        try {
            this.currentBlockBuffer =
                    BufferPool.getInstance().getBuffer((int) this.blockSize);
        } catch (InterruptedException e) {
            String exceptionMsg = String.format("getting a buffer size: [%d] " +
                            "from the buffer pool occurs an exception.",
                    this.blockSize);
            throw new IOException(exceptionMsg, e);
        }

        if (null != this.digest) {
            this.digest.reset();
            this.currentBlockOutputStream = new DigestOutputStream(
                    new BufferOutputStream(this.currentBlockBuffer),
                    this.digest);
        } else {
            this.currentBlockOutputStream =
                    new BufferOutputStream(this.currentBlockBuffer);
        }
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        if (this.closed) {
            throw new IOException(FSExceptionMessages.STREAM_IS_CLOSED);
        }

        if (this.currentBlockOutputStream == null) {
            initCurrentBlock();
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
                if (null != this.writeConsistencyChecker) {
                    this.writeConsistencyChecker.incrementWrittenBytes(blockWritten);
                }
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
            throw new IOException(FSExceptionMessages.STREAM_IS_CLOSED);
        }

        if (this.currentBlockOutputStream == null) {
            initCurrentBlock();
        }

        byte[] singleBytes = new byte[1];
        singleBytes[0] = (byte) b;
        this.currentBlockOutputStream.write(singleBytes, 0, 1);
        this.blockWritten += 1;
        if (this.blockWritten >= this.blockSize) {
            this.uploadPart();
            if (null != this.writeConsistencyChecker) {
                this.writeConsistencyChecker.incrementWrittenBytes(blockWritten);
            }
            this.blockWritten = 0;
        }
    }

    // init current block buffer and output stream when occur the write operation.
    private void initCurrentBlock() throws IOException {
        // get the buffer
        try {
            this.currentBlockBuffer = BufferPool.getInstance().getBuffer((int) this.blockSize);
        } catch (InterruptedException e) {
            String exceptionMsg = String.format("Getting a buffer size:[%d] " +
                            "from the buffer pool occurs an exception.",
                    this.blockSize);
            throw new IOException(exceptionMsg);
        }
        // init the stream
        try {
            this.digest = MessageDigest.getInstance("MD5");
            this.currentBlockOutputStream = new DigestOutputStream(
                    new BufferOutputStream(this.currentBlockBuffer),
                    this.digest);
        } catch (NoSuchAlgorithmException e) {
            this.digest = null;
            this.currentBlockOutputStream =
                    new BufferOutputStream(this.currentBlockBuffer);
        }
    }
}

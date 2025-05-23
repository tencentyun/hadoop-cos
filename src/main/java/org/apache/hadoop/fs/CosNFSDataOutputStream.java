package org.apache.hadoop.fs;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.qcloud.cos.exception.CosClientException;
import com.qcloud.cos.exception.CosServiceException;
import com.qcloud.cos.model.CompleteMultipartUploadResult;
import com.qcloud.cos.model.PartETag;
import org.apache.commons.io.output.NullOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.cosn.Abortable;
import org.apache.hadoop.fs.cosn.BufferInputStream;
import org.apache.hadoop.fs.cosn.BufferOutputStream;
import org.apache.hadoop.fs.cosn.BufferPool;
import org.apache.hadoop.fs.cosn.ConsistencyChecker;
import org.apache.hadoop.fs.cosn.Constants;
import org.apache.hadoop.fs.cosn.Unit;
import org.apache.hadoop.fs.cosn.buffer.CosNByteBuffer;
import org.apache.hadoop.fs.cosn.multipart.upload.UploadPart;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class CosNFSDataOutputStream extends OutputStream implements Abortable {
    private static final Logger LOG =
            LoggerFactory.getLogger(CosNFSDataOutputStream.class);

    protected final Configuration conf;
    protected final NativeFileSystemStore nativeStore;
    protected final ListeningExecutorService executorService;
    protected String cosKey;
    protected final long partSize;
    protected long fileSize;
    protected MultipartUpload multipartUpload;
    protected int currentPartNumber;
    protected CosNByteBuffer currentPartBuffer;
    protected OutputStream currentPartOutputStream;
    protected long currentPartWriteBytes;
    protected boolean dirty;
    protected boolean committed;
    protected boolean closed;
    protected boolean flushCOSEnabled;
    protected MessageDigest currentPartMessageDigest;
    protected ConsistencyChecker consistencyChecker;
    protected boolean needUploadCurrentPart;
    protected boolean clientEncryptionEnabled;
    public static  final String CSE_MPU_KEY_SUFFIX = "._CSE_";

    /**
     * Output stream
     *
     * @param conf            config
     * @param nativeStore     native file system
     * @param key             cos key
     * @param executorService thread executor
     * @throws IOException
     */
    public CosNFSDataOutputStream(
            Configuration conf,
            NativeFileSystemStore nativeStore,
            String key, ExecutorService executorService) throws IOException {
        this.conf = conf;
        this.nativeStore = nativeStore;
        this.executorService = MoreExecutors.listeningDecorator(executorService);

        this.cosKey = key;
        long partSize = conf.getLong(
                CosNConfigKeys.COSN_UPLOAD_PART_SIZE_KEY, CosNConfigKeys.DEFAULT_UPLOAD_PART_SIZE);
        if (partSize < Constants.MIN_PART_SIZE) {
            LOG.warn("The minimum size of a single block is limited to " +
                    "greater than or equal to {}.", Constants.MIN_PART_SIZE);
            this.partSize = Constants.MIN_PART_SIZE;
        } else if (partSize > Constants.MAX_PART_SIZE) {
            LOG.warn("The maximum size of a single block is limited to " +
                    "smaller than or equal to {}.", Constants.MAX_PART_SIZE);
            this.partSize = Constants.MAX_PART_SIZE;
        } else {
            this.partSize = partSize;
        }

        this.flushCOSEnabled = conf.getBoolean(CosNConfigKeys.COSN_FLUSH_ENABLED,
                CosNConfigKeys.DEFAULT_COSN_FLUSH_ENABLED);
        this.multipartUpload = null;
        this.currentPartNumber = 0;
        // malloc when trigger the write operations
        this.currentPartBuffer = null;
        this.currentPartOutputStream = null;
        this.currentPartWriteBytes = 0;

        this.dirty = true;
        this.committed = false;
        this.closed = false;

        this.fileSize = 0;

        this.needUploadCurrentPart = false;

        this.clientEncryptionEnabled = conf.getBoolean(CosNConfigKeys.COSN_CLIENT_SIDE_ENCRYPTION_ENABLED,
                CosNConfigKeys.DEFAULT_COSN_CLIENT_SIDE_ENCRYPTION_ENABLED);

        //if client encryption is enabled, upload temporary files
        if(clientEncryptionEnabled){
            this.cosKey = key + CSE_MPU_KEY_SUFFIX;
        } else {
            this.cosKey = key;
        }

        if (conf.getBoolean(CosNConfigKeys.COSN_UPLOAD_PART_CHECKSUM_ENABLED_KEY,
                CosNConfigKeys.DEFAULT_UPLOAD_PART_CHECKSUM_ENABLED)) {
            LOG.info("The MPU-UploadPart checksum is enabled, and the message digest algorithm is {}.", "MD5");
            try {
                this.currentPartMessageDigest = MessageDigest.getInstance("MD5");
            } catch (NoSuchAlgorithmException e) {
                LOG.warn("Failed to MD5 digest, the upload will not check.");
                this.currentPartMessageDigest = null;
            }
        } else {
            // close the current part message digest.
            LOG.warn("The MPU-UploadPart checksum is disabled.");
            this.currentPartMessageDigest = null;
        }

        boolean uploadChecksEnabled = conf.getBoolean(CosNConfigKeys.COSN_UPLOAD_CHECKS_ENABLED_KEY,
                CosNConfigKeys.DEFAULT_COSN_UPLOAD_CHECKS_ENABLE);
        if (uploadChecksEnabled) {
            LOG.info("The consistency checker is enabled.");
            this.consistencyChecker = new ConsistencyChecker(this.nativeStore, this.cosKey, this.clientEncryptionEnabled);
        } else {
            LOG.warn("The consistency checker is disabled.");
            this.consistencyChecker = null;
        }
    }

    @Override
    public synchronized void write(byte[] b, int off, int len) throws IOException {
        this.checkOpened();
        this.fileSize += len;
        if (this.currentPartBuffer == null) {
            initNewCurrentPartResource();
        }

        while (len > 0) {
            if(needUploadCurrentPart){
                this.currentPartOutputStream.flush();
                this.currentPartOutputStream.close();
                this.uploadCurrentPart(false);
                this.initNewCurrentPartResource();
            }
            long writeBytes;
            if (this.currentPartWriteBytes + len > this.partSize) {
                writeBytes = this.partSize - this.currentPartWriteBytes;
            } else {
                writeBytes = len;
            }

            this.currentPartOutputStream.write(b, off, (int) writeBytes);
            this.dirty = true;
            this.committed = false;
            this.currentPartWriteBytes += writeBytes;
            if (null != this.consistencyChecker) {
                this.consistencyChecker.writeBytes(b, off, (int) writeBytes);
            }
            if (this.currentPartWriteBytes >= this.partSize) {
                needUploadCurrentPart = true;
            }
            len -= writeBytes;
            off += writeBytes;
        }
    }

    @Override
    public synchronized void write(int b) throws IOException {
        this.checkOpened();

        byte[] singleBytes = new byte[1];
        singleBytes[0] = (byte) b;
        this.write(singleBytes, 0, 1);
    }

    @Override
    public synchronized void flush() throws IOException {
        innerFlush(false);
    }

    private void innerFlush(boolean closeStream) throws IOException {
        this.checkOpened();
        // when close flush data to cos, each time data not upload,
        // at this time if we close stream, it is not dirty but also has data need to upload
        // so the dirty flag only useful when flush cos enabled.
        if (!this.dirty && this.flushCOSEnabled) {
            LOG.debug("The stream is up-to-date, no need to refresh.");
            return;
        }

        // Create but not call write before.
        if (null == this.currentPartBuffer) {
            initNewCurrentPartResource();
        }

        this.doFlush(closeStream);
        // All data has been flushed to the Under Storage.
        this.dirty = false;
    }


    @Override
    public synchronized void close() throws IOException {
        if (this.closed) {
            return;
        }

        LOG.info("Closing the output stream [{}].", this);
        try {
            this.innerFlush(true);
            this.commit();
        } finally {
            this.closed = true;
            this.releaseCurrentPartResource();
            this.resetContext();
        }
    }

    @Override
    public synchronized void doAbort() throws IOException {
        if (this.closed) {
            return;
        }

        LOG.info("Aborting the output stream [{}].", this);
        try {
            if (null != this.multipartUpload) {
                this.multipartUpload.abort();
            }
        } finally {
            this.closed = true;
            this.releaseCurrentPartResource();
            this.resetContext();
        }
    }

    /**
     * commit the current file
     */
    protected void commit() throws IOException {
        if (this.committed) {
            return;
        }

        if (this.currentPartNumber <= 1) {
            // Single file upload
            byte[] digestHash = this.currentPartMessageDigest == null ? null : this.currentPartMessageDigest.digest();
            BufferInputStream currentPartBufferInputStream = new BufferInputStream(this.currentPartBuffer);

            //if client encryption is enabled, remove the suffix of key
            if(clientEncryptionEnabled) {
                this.cosKey = this.cosKey.substring(0, this.cosKey.length() - CSE_MPU_KEY_SUFFIX.length());
                if (null != this.consistencyChecker){
                    this.consistencyChecker.setKey(this.cosKey);
                }
            }
            nativeStore.storeFile(this.cosKey, currentPartBufferInputStream,
                    digestHash, this.currentPartBuffer.remaining());
        } else {
            if (null != this.multipartUpload) {
                this.multipartUpload.complete();
                //if client encryption is enabled, remove the suffix of key
                if(clientEncryptionEnabled) {
                    this.cosKey = this.cosKey.substring(0, this.cosKey.length() - CSE_MPU_KEY_SUFFIX.length());
                    if (null != this.consistencyChecker){
                        this.consistencyChecker.setKey(this.cosKey);
                    }
                }
            }
        }

        this.committed = true;

        if (null != this.consistencyChecker) {
            this.consistencyChecker.finish();
            if (!this.consistencyChecker.getCheckResult().isSucceeded()) {
                String exceptionMsg = String.format("Failed to upload the key: %s, error message: %s.",
                        this.cosKey,
                        this.consistencyChecker.getCheckResult().getDescription());
                throw new IOException(exceptionMsg);
            }
            LOG.info("Upload the key [{}] successfully. check message: {}.", this.cosKey,
                    this.consistencyChecker.getCheckResult().getDescription());
        } else {
            LOG.info("OutputStream for key [{}] upload complete. But it is not checked.", cosKey);
        }
    }

    protected void resetContext() throws IOException {
        if (null != this.multipartUpload) {
            if (!this.multipartUpload.isCompleted() && !this.multipartUpload.isAborted()) {
                this.multipartUpload.abort();
            }
        }

        if (null != this.currentPartOutputStream) {
            this.currentPartOutputStream.close();
        }

        if (null != this.currentPartBuffer) {
            BufferPool.getInstance().returnBuffer(this.currentPartBuffer);
        }

        this.multipartUpload = null;
        this.currentPartNumber = 0;
        this.currentPartBuffer = null;
        this.currentPartOutputStream = null;
        this.currentPartWriteBytes = 0;

        this.dirty = true;
        this.committed = false;

        if (this.currentPartMessageDigest != null) {
            this.currentPartMessageDigest.reset();
        }
        if (this.consistencyChecker != null) {
            this.consistencyChecker.reset();
        }
    }

    protected void checkOpened() throws IOException {
        if (this.closed) {
            throw new IOException(FSExceptionMessages.STREAM_IS_CLOSED);
        }
    }

    /**
     * inner flush operation.
     */
    private void doFlush(boolean closeStream) throws IOException {
        this.currentPartOutputStream.flush();

        // frequent flush may cause qps lower because of need wait upload finish
        if (!this.flushCOSEnabled && !closeStream) {
            return;
        }

        //if client encryption is enabled, and call the Flush(closeStream = false) at the same time, need return
        if(clientEncryptionEnabled && !closeStream) {
            return;
        }

        try {
            if (this.currentPartNumber > 1 && null != this.multipartUpload) {
                if (this.currentPartWriteBytes > 0) {
                    this.uploadCurrentPart(true);
                }
                if(!this.clientEncryptionEnabled) {
                    this.multipartUpload.waitForFinishPartUploads();
                }
            }
        } finally {
            // resume for append write in last part.
            this.resumeCurrentPartMessageDigest();
            this.currentPartBuffer.flipWrite();
        }
    }

    /**
     * Initialize a new currentBlock and a new currentBlockOutputStream.
     */
    protected void initNewCurrentPartResource() throws IOException {
        // get the buffer
        try {
            this.currentPartBuffer = BufferPool.getInstance().getBuffer((int) this.partSize);
            this.currentPartWriteBytes = 0;
            this.currentPartNumber++;
            needUploadCurrentPart = false;
        } catch (InterruptedException e) {
            String exceptionMsg = String.format("Getting a buffer size:[%d] " +
                            "from the buffer pool occurs an exception.",
                    this.partSize);
            throw new IOException(exceptionMsg);
        }
        // init the stream
        if (null != this.currentPartMessageDigest) {
            this.currentPartMessageDigest.reset();
            this.currentPartOutputStream = new DigestOutputStream(
                    new BufferOutputStream(this.currentPartBuffer), this.currentPartMessageDigest);
        } else {
            this.currentPartOutputStream = new BufferOutputStream(this.currentPartBuffer);
        }
    }

    protected void releaseCurrentPartResource() throws IOException {
        if (null != this.currentPartOutputStream) {
            try {
                this.currentPartOutputStream.close();
            } catch (IOException e) {
                LOG.warn("Fail to close current part output stream.", e);
            }
            this.currentPartOutputStream = null;
        }

        if (null != this.currentPartMessageDigest) {
            this.currentPartMessageDigest.reset();
        }

        if (null != this.currentPartBuffer) {
            BufferPool.getInstance().returnBuffer(this.currentPartBuffer);
        }
        this.currentPartBuffer = null;
    }

    private void uploadCurrentPart(boolean isLastPart) throws IOException {
        if (null == this.multipartUpload) {
            this.multipartUpload = new MultipartUpload(this.cosKey);
        }

        byte[] digestHash = this.currentPartMessageDigest == null ? null : this.currentPartMessageDigest.digest();
        UploadPart uploadPart = new UploadPart(this.currentPartNumber, this.currentPartBuffer,
                digestHash, isLastPart);
        if(!clientEncryptionEnabled) {
            this.multipartUpload.uploadPartAsync(uploadPart);
        }
        else {
            this.multipartUpload.uploadPartSync(uploadPart);
        }
    }

    private void resumeCurrentPartMessageDigest() throws IOException {
        if (null != this.currentPartMessageDigest) {
            // recover the digest
            this.currentPartMessageDigest.reset();
            OutputStream tempOutputStream = new DigestOutputStream(
                    new NullOutputStream(), this.currentPartMessageDigest);

            InputStream tempInputStream = new BufferInputStream(this.currentPartBuffer);
            byte[] tempChunk = new byte[(int) (4 * Unit.KB)];
            int readBytes = tempInputStream.read(tempChunk);
            while (readBytes != -1) {
                tempOutputStream.write(tempChunk, 0, readBytes);
                readBytes = tempInputStream.read(tempChunk);
            }
        }
    }

    protected class MultipartUpload {
        protected final String uploadId;
        protected final Map<Integer, ListenableFuture<PartETag>> partETagFutures;
        protected final List<PartETag> partETags;
        protected final AtomicInteger partsSubmitted;
        protected final AtomicInteger partsUploaded;
        protected final AtomicLong bytesSubmitted;
        protected final AtomicLong bytesUploaded;

        protected volatile boolean aborted;
        protected volatile boolean completed;

        protected MultipartUpload(String cosKey) throws IOException {
            this(cosKey, null);
        }

        protected MultipartUpload(String cosKey, String uploadId) throws IOException {
            this(cosKey, uploadId, null,null, 0, 0, 0, 0);
        }

        protected MultipartUpload(String cosKey, String uploadId, Map<Integer, ListenableFuture<PartETag>> partETagFutures,
                                  List<PartETag> partETags, int partsSubmitted, int partsUploaded, long bytesSubmitted, long bytesUploaded) throws IOException {
            if (null == uploadId) {
                uploadId = nativeStore.getUploadId(cosKey);
            }
            this.uploadId = uploadId;
            LOG.debug("Initial multi-part upload for the cos key [{}] with the upload id [{}].",
                    cosKey, uploadId);

            if (null == partETagFutures) {
                this.partETagFutures = new HashMap<>();
            } else {
                this.partETagFutures = partETagFutures;
            }

            if (null == partETags) {
                this.partETags =  new LinkedList<>();
            } else {
                this.partETags = partETags;
            }

            this.partsSubmitted = new AtomicInteger(partsSubmitted);
            this.partsUploaded = new AtomicInteger(partsUploaded);
            this.bytesSubmitted = new AtomicLong(bytesSubmitted);
            this.bytesUploaded = new AtomicLong(bytesUploaded);
            this.aborted = false;
            this.completed = false;
        }

        protected String getUploadId() {
            return uploadId;
        }

        protected int getPartsUploaded() {
            return partsUploaded.get();
        }

        protected int getPartsSubmitted() {
            return partsSubmitted.get();
        }

        protected long getBytesSubmitted() {
            return bytesSubmitted.get();
        }

        protected long getBytesUploaded() {
            return bytesUploaded.get();
        }

        protected boolean isAborted() {
            return aborted;
        }

        protected boolean isCompleted() {
            return completed;
        }

        @Override
        public String toString() {
            return "MultipartUpload{" +
                    "uploadId='" + uploadId + '\'' +
                    ", partETagFutures=" + partETagFutures +
                    ", partETags=" + partETags +
                    ", partsSubmitted=" + partsSubmitted +
                    ", partsUploaded=" + partsUploaded +
                    ", bytesSubmitted=" + bytesSubmitted +
                    ", bytesUploaded=" + bytesUploaded +
                    ", aborted=" + aborted +
                    ", completed=" + completed +
                    '}';
        }

        protected void uploadPartAsync(final UploadPart uploadPart) throws IOException {
            if (this.isCompleted() || this.isAborted()) {
                throw new IOException(String.format("The MPU [%s] has been closed or aborted. " +
                        "Can not execute the upload operation.", this));
            }

            partsSubmitted.incrementAndGet();
            bytesSubmitted.addAndGet(uploadPart.getPartSize());
            ListenableFuture<PartETag> partETagListenableFuture =
                    executorService.submit(new Callable<PartETag>() {
                        private final String localKey = cosKey;
                        private final String localUploadId = uploadId;

                        @Override
                        public PartETag call() throws Exception {
                            try {
                                LOG.debug("Start to upload the part async: {}", uploadPart);
                                PartETag partETag = (nativeStore).uploadPart(
                                        new BufferInputStream(uploadPart.getCosNByteBuffer()),
                                        this.localKey,
                                        this.localUploadId,
                                        uploadPart.getPartNumber(),
                                        uploadPart.getPartSize(), uploadPart.getMd5Hash(), uploadPart.isLast());
                                partsUploaded.incrementAndGet();
                                bytesUploaded.addAndGet(uploadPart.getPartSize());
                                return partETag;
                            } finally {
                                if (!uploadPart.isLast()) {
                                    BufferPool.getInstance().returnBuffer(uploadPart.getCosNByteBuffer());
                                }
                            }
                        }
                    });
            this.partETagFutures.put(uploadPart.getPartNumber(), partETagListenableFuture);
        }

        protected void uploadPartSync(final UploadPart uploadPart) throws IOException {
            if (this.isCompleted() || this.isAborted()) {
                throw new IOException(String.format("The MPU [%s] has been closed or aborted. " +
                        "Can not execute the upload operation.", this));
            }
            partsSubmitted.incrementAndGet();
            bytesSubmitted.addAndGet(uploadPart.getPartSize());
            try {
                LOG.debug("Start to upload the part Sync: {}", uploadPart);
                PartETag partETag = (nativeStore).uploadPart(
                        new BufferInputStream(uploadPart.getCosNByteBuffer()),
                        cosKey,
                        uploadId,
                        uploadPart.getPartNumber(),
                        uploadPart.getPartSize(), uploadPart.getMd5Hash(), uploadPart.isLast());
                partsUploaded.incrementAndGet();
                bytesUploaded.addAndGet(uploadPart.getPartSize());
                partETags.add(partETag);
            }catch (CosServiceException e) {
                LOG.error("Upload part sync upload falied: ", e);
                throw e;
            } catch (CosClientException e) {
                LOG.error("Upload part sync upload falied: ", e);
                throw e;
            } finally {
                if (!uploadPart.isLast()) {
                    BufferPool.getInstance().returnBuffer(uploadPart.getCosNByteBuffer());
                }
            }
        }

        protected List<PartETag> waitForFinishPartUploads() throws IOException {
            try {
                LOG.info("Waiting for finish part uploads...");
                return Futures.allAsList(this.partETagFutures.values()).get();
            } catch (InterruptedException e) {
                LOG.error("Interrupt the part upload...", e);
                return null;
            } catch (ExecutionException e) {
                LOG.error("Cancelling futures...", e);
                for (ListenableFuture<PartETag> future : this.partETagFutures.values()) {
                    future.cancel(true);
                }
                (nativeStore).abortMultipartUpload(cosKey, this.uploadId);
                this.aborted = true;
                String exceptionMsg = String.format("multipart upload with id: %s" +
                        " to %s.", this.uploadId, cosKey);
                throw new IOException(exceptionMsg);
            }
        }

        protected void complete() throws IOException {
            LOG.info("Completing the MPU [{}].", this.getUploadId());
            if (this.isCompleted() || this.isAborted()) {
                throw new IOException(String.format("fail to complete the MPU [%s]. "
                        + "It has been completed or aborted.", this));
            }
            final List<PartETag> PartETagList;
            if(!clientEncryptionEnabled) {
                PartETagList = this.waitForFinishPartUploads();
            }
            else {
                PartETagList = this.partETags;
            }
            if (null == PartETagList) {
                throw new IOException("failed to multipart upload to cos, doAbort it.");
            }

            // notice sometimes complete result may be null
            CompleteMultipartUploadResult completeResult =
                    nativeStore.completeMultipartUpload(cosKey, this.uploadId, new LinkedList<>(PartETagList));
            //开启客户端加密后 修改dataSize元数据
            if(clientEncryptionEnabled) {
                try {
                    nativeStore.ModifyDataSize(cosKey, fileSize);
                } catch (IOException e){
                    LOG.info("Completing the MPU [{}].", this.getUploadId());
                }
            }
            this.completed = true;
            LOG.info("The MPU [{}] has been completed.", this.getUploadId());
        }

        protected void abort() throws IOException {
            LOG.info("Aborting the MPU [{}].", this.getUploadId());
            if (this.isCompleted() || this.isAborted()) {
                throw new IOException(String.format("fail to doAbort the MPU [%s]. "
                        + "It has been completed or aborted.", this.getUploadId()));
            }
            nativeStore.abortMultipartUpload(cosKey, this.uploadId);
            this.aborted = true;
            LOG.info("The MPU [{}] has been aborted.", this.getUploadId());
        }
    }
}

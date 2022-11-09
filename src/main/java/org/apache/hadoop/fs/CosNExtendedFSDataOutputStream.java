package org.apache.hadoop.fs;

import com.google.common.util.concurrent.ListenableFuture;
import com.qcloud.cos.model.PartETag;
import com.qcloud.cos.utils.CRC64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.cosn.Unit;
import org.apache.hadoop.fs.cosn.multipart.upload.UploadPartCopy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

/**
 * Extended supports: append/truncate and visible flush.
 */
public class CosNExtendedFSDataOutputStream extends CosNFSDataOutputStream {
    private static final Logger LOG = LoggerFactory.getLogger(CosNExtendedFSDataOutputStream.class);

    public CosNExtendedFSDataOutputStream(Configuration conf, NativeFileSystemStore nativeStore,
        String cosKey, ExecutorService executorService) throws IOException {
        this(conf, nativeStore, cosKey, executorService, false);
    }

    public CosNExtendedFSDataOutputStream(Configuration conf, NativeFileSystemStore nativeStore,
        String cosKey, ExecutorService executorService, boolean appendFlag) throws IOException {
        super(conf, nativeStore, cosKey, executorService);

        if (appendFlag) {
            this.resumeForWrite();
        }
    }

    @Override
    public synchronized void write(byte[] b, int off, int len) throws IOException {
        if (super.committed) {
            this.resumeForWrite();
        }
        super.write(b, off, len);
    }

    @Override
    public synchronized void write(int b) throws IOException {
        if (super.committed) {
            this.resumeForWrite();
        }
        super.write(b);
    }

    @Override
    public synchronized void flush() throws IOException {
        super.flush();
        // Visible immediately after flushing.
        super.commit();
    }

    private void resumeForWrite() throws IOException {
        FileMetadata fileMetadata = super.nativeStore.retrieveMetadata(super.cosKey);
        if (null == fileMetadata) {
            throw new IOException(String.format("The cos key [%s] is not found.", super.cosKey));
        }
        if (!fileMetadata.isFile()) {
            throw new IOException("The cos key is a directory object. Can not resume the write operation for it.");
        }

        super.resetContext();
        super.initNewCurrentPartResource();

        // resume for write operation.
        try {
            if (fileMetadata.getLength() < super.partSize) {
                // Single file resume
                try (InputStream inputStream = super.nativeStore.retrieve(super.cosKey)) {
                    byte[] chunk = new byte[(int) (4 * Unit.KB)];
                    int readBytes = inputStream.read(chunk);
                    while (readBytes != -1) {
                        super.write(chunk, 0, readBytes);
                        readBytes = inputStream.read(chunk);
                    }
                }
            } else {
                // MultipartManager copy resume
                super.multipartUpload = new MultipartUploadEx(super.cosKey);
                long copyRemaining = fileMetadata.getLength();
                long firstByte = 0;
                long lastByte = firstByte + super.partSize - 1;
                while (copyRemaining >= super.partSize) {
                    UploadPartCopy uploadPartCopy = new UploadPartCopy(super.cosKey, super.cosKey, super.currentPartNumber++,
                            firstByte, lastByte);
                    ((MultipartUploadEx) super.multipartUpload).uploadPartCopyAsync(uploadPartCopy);
                    copyRemaining -= ((lastByte - firstByte) + 1);
                    firstByte = lastByte + 1;
                    lastByte = firstByte + super.partSize - 1;
                }

                // initialize the last part
                if (copyRemaining > 0) {
                    lastByte = firstByte + copyRemaining - 1;
                    try (InputStream inputStream = super.nativeStore.retrieveBlock(super.cosKey, firstByte, lastByte)) {
                        byte[] chunk = new byte[(int) (4 * Unit.KB)];
                        int readBytes = inputStream.read(chunk);
                        while (readBytes != -1) {
                            super.write(chunk, 0, readBytes);
                            readBytes = inputStream.read(chunk);
                        }
                    }
                }
            }
            // initialize the consistency checker.
            BigInteger bigInteger = new BigInteger(fileMetadata.getCrc64ecm());
            this.consistencyChecker = new ConsistencyChecker(super.nativeStore, super.cosKey,
                new CRC64(bigInteger.longValue()), fileMetadata.getLength());
        } catch (Exception e) {
            LOG.error("Failed to resume for writing. Abort it.", e);
            super.abort();
            throw new IOException(e);
        }
    }

    protected class MultipartUploadEx extends MultipartUpload {
        protected MultipartUploadEx(String cosKey) throws IOException {
            this(cosKey, null);
        }

        protected MultipartUploadEx(String cosKey, String uploadId) throws IOException {
            super(cosKey, uploadId);
        }

        protected void uploadPartCopyAsync(final UploadPartCopy uploadPartCopy) throws IOException {
            if (super.isCompleted() || super.isAborted()) {
                throw new IOException(String.format("The MPU [%s] has been closed or aborted. " +
                        "Can not execute the upload part copy operation.", this));
            }

            partsSubmitted.incrementAndGet();
            bytesSubmitted.addAndGet(uploadPartCopy.getLastByte() - uploadPartCopy.getFirstByte() + 1);
            ListenableFuture<PartETag> partETagListenableFuture = executorService.submit(new Callable<PartETag>() {
                @Override
                public PartETag call() throws Exception {
                    LOG.info("Start to copy the part: {}.", uploadPartCopy);
                    PartETag partETag = nativeStore.uploadPartCopy(getUploadId(),
                            uploadPartCopy.getSrcKey(), uploadPartCopy.getDestKey(), uploadPartCopy.getPartNumber(),
                            uploadPartCopy.getFirstByte(), uploadPartCopy.getLastByte());
                    partsUploaded.incrementAndGet();
                    bytesUploaded.addAndGet(uploadPartCopy.getLastByte() - uploadPartCopy.getFirstByte() + 1);
                    return partETag;
                }
            });
            super.partETagFutures.put(uploadPartCopy.getPartNumber(), partETagListenableFuture);
        }
    }
}

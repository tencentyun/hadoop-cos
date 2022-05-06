package org.apache.hadoop.fs;

import com.qcloud.cos.utils.CRC64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.math.BigInteger;

public class ConsistencyChecker {
    private static final Logger LOG = LoggerFactory.getLogger(ConsistencyChecker.class);

    private final NativeFileSystemStore nativeStore;
    private final String key;
    private volatile long writtenBytesLength;

    private CRC64 crc64;
    private volatile boolean finished;
    private CheckResult checkResult;

    public static final class CheckResult {
        private String fsScheme;     // FileSystem scheme
        private String key;
        private long expectedLength;
        private long realLength;

        private long expectedCrc64Value;
        private long realCrc64Value;

        private Exception exception;

        private String description;

        public CheckResult() {
            this("", "", -1, -1, -1, -1, null);
        }

        public CheckResult(String scheme, String cosKey,
                           long expectedLength, long realLength,
                           long expectedCrc64Value, long realCrc64Value, Exception e) {
            this.fsScheme = scheme;
            this.key = cosKey;
            this.expectedLength = expectedLength;
            this.realLength = realLength;
            this.expectedCrc64Value = expectedCrc64Value;
            this.realCrc64Value = realCrc64Value;
            this.exception = e;
            this.description = "";
        }

        public String getFsScheme() {
            return fsScheme;
        }

        public String getKey() {
            return key;
        }

        /**
         * judge the success operation
         * @return whether success
         */
        public boolean isSucceeded() {
            boolean succeeded = true;
            do {
                if (null != this.exception) {
                    this.description = String.format("Failed to check the data due to an exception: %s.",
                            this.exception);
                    succeeded = false;
                    break;
                }

                if (this.expectedLength < 0 || this.realLength < 0) {
                    // The expected length and the real length are invalid.
                    this.description = String.format("Invalid check data. expected length: %d, real length: %d.",
                            this.expectedLength, this.realLength);
                    succeeded = false;
                    break;
                }

                if (this.expectedLength != this.realLength) {
                    this.description = String.format("The expected length is not equal to the the real length. " +
                            "expected length: %d, real length: %d.", this.expectedLength, this.realLength);
                    succeeded = false;
                    break;
                }

                if (this.expectedCrc64Value != this.realCrc64Value) {
                    this.description = String.format("The CRC64 checksum verify failed. " +
                            "expected CRC64 value: %d, real CRC64 value: %d",
                            this.expectedCrc64Value, this.realCrc64Value);
                    succeeded = false;
                    break;
                }
            } while(false);

            if (succeeded) {
                this.description = String.format("File verification succeeded. " +
                        "expected length: %d, real length: %d, expected CRC64 value: %d, real CRC64 value: %d",
                        this.expectedLength, this.realLength, this.expectedCrc64Value, this.realCrc64Value);
            } else {
                this.description = String.format("File verification failure. %s", this.description);
            }

            return succeeded;
        }

        public long getExpectedLength() {
            return expectedLength;
        }

        public long getRealLength() {
            return realLength;
        }

        /**
         * get description string
         * @return description
         */
        public String getDescription() {
            return this.description;
        }

        public void setFsScheme(String fsScheme) {
            this.fsScheme = fsScheme;
        }

        public void setKey(String key) {
            this.key = key;
        }

        public void setExpectedLength(long expectedLength) {
            this.expectedLength = expectedLength;
        }

        public void setRealLength(long realLength) {
            this.realLength = realLength;
        }

        public void setExpectedCrc64Value(long expectedCrc64Value) {
            this.expectedCrc64Value = expectedCrc64Value;
        }

        public void setRealCrc64Value(long realCrc64Value) {
            this.realCrc64Value = realCrc64Value;
        }

        public void setException(Exception exception) {
            this.exception = exception;
        }
    }

    public ConsistencyChecker(final NativeFileSystemStore nativeStore, final String cosKey) throws IOException {
        this(nativeStore, cosKey, null, 0);
    }

    public ConsistencyChecker(final NativeFileSystemStore nativeStore, final String cosKey,
                              CRC64 crc64, long writtenBytesLength) throws IOException {
        if (null == nativeStore || null == cosKey || cosKey.isEmpty()) {
            throw new IOException(String.format(
                    "Native FileSystem store [%s] or key [%s] is illegal.", nativeStore, cosKey));
        }
        this.nativeStore = nativeStore;
        this.key = cosKey;

        if (null != crc64 && writtenBytesLength > 0) {
            this.crc64 = crc64;
            this.writtenBytesLength = writtenBytesLength;
        } else {
            this.crc64 = new CRC64();
            this.writtenBytesLength = 0;
        }

        this.finished = false;
        this.checkResult = new CheckResult("cosn", this.key, -1, -1, -1, -1, null);
    }

    public synchronized void writeBytes(byte[] writeBytes, int offset, int length) {
        if (this.finished) {
            LOG.error("The cos key [{}] has ended statistics.", this.key);
            return;
        }

        // update the crc64 checksum.
        this.crc64.update(writeBytes, offset, length);
        this.writtenBytesLength += length;
    }

    public synchronized void finish() {
        if (this.finished) {
            return;
        }
        this.finished = true;

        FileMetadata fileMetadata;
        try {
            fileMetadata = this.nativeStore.retrieveMetadata(this.key);
            if (null == fileMetadata) {
                throw new FileNotFoundException("The target object is not found. " +
                        "Please terminate your application immediately.");
            }
            LOG.debug("Get the target key [{}]'s length: {} and crc64 checksum: {}.",
                    this.key, fileMetadata.getLength(), fileMetadata.getCrc64ecm());
        } catch (IOException e) {
            LOG.error("Failed to get the target key [{}]'s length and crc64 checksum.", this.key, e);
            this.checkResult.setFsScheme("cosn");
            this.checkResult.setKey(this.key);
            this.checkResult.setExpectedLength(this.writtenBytesLength);
            this.checkResult.setRealLength(-1);
            this.checkResult.setExpectedCrc64Value(this.crc64.getValue());
            this.checkResult.setRealCrc64Value(-1);
            // The result that an exception has occurred is unreliable.
            this.checkResult.setException(e);
            return;
        }

        this.checkResult.setFsScheme("cosn");
        this.checkResult.setKey(this.key);
        this.checkResult.setExpectedLength(this.writtenBytesLength);
        this.checkResult.setRealLength(fileMetadata.getLength());
        this.checkResult.setExpectedCrc64Value(this.crc64.getValue());
        this.checkResult.setRealCrc64Value(new BigInteger(fileMetadata.getCrc64ecm()).longValue());
    }

    public CheckResult getCheckResult() {
        return checkResult;
    }

    public synchronized void reset() {
        this.writtenBytesLength = 0;
        this.crc64.reset();
        this.finished = false;
        this.checkResult = new CheckResult("cosn", this.key, -1, -1, -1, -1, null);
    }
}

package org.apache.hadoop.fs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class WriteConsistencyChecker {
    private static final Logger LOG = LoggerFactory.getLogger(WriteConsistencyChecker.class);

    private NativeFileSystemStore store;
    private String key;
    private volatile long beforeWrittenBytes;
    private long afterWrittenBytes;
    private volatile boolean finished;
    private CheckResult checkResult;

    public static final class CheckResult {
        private String fsScheme;     // FileSystem scheme
        private String key;
        private long expectedLength;
        private long realLength;
        private String description;

        public CheckResult() {
            this("", "", false, -1, -1);
        }

        public CheckResult(String scheme, String cosKey, boolean checkResult, long expectedLength, long realLength) {
            this.fsScheme = scheme;
            this.key = cosKey;
            this.expectedLength = expectedLength;
            this.realLength = realLength;
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
            if (this.expectedLength < 0 && this.realLength < 0) {
                // The expected length and the real length are invalid.
                LOG.debug("Invalid check data. expected length: {}, real length: {}.", this.expectedLength,
                        this.realLength);
                return false;
            }
            if (this.expectedLength >= 0 && this.realLength < 0) {
                // The real length is invalid.
                LOG.debug("Failed to get actual file length. expected length: {}, real length: {}.",
                        this.expectedLength, this.realLength);
                return false;
            }
            if (this.expectedLength < 0 && this.realLength >= 0) {
                // The expected length is invalid.
                LOG.debug("The expected length is invalid. expected length: {}, real length: {}.",
                        this.expectedLength, this.realLength);
                return false;
            }

            LOG.debug("The expected length: {}, the real length: {}.", this.expectedLength, this.realLength);
            return this.expectedLength == this.realLength;
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
            if (!this.description.isEmpty()) {
                return this.description;
            }

            if (this.expectedLength < 0 && this.realLength < 0) {
                this.description = String.format("Invalid check data. expected length: %d, real length: %d",
                        this.expectedLength, this.realLength);
                return this.description;
            }

            if (this.expectedLength >= 0 && this.realLength < 0) {
                this.description = String.format("Failed to get actual file length. expected length: %d, real " +
                        "length: %d", this.expectedLength, this.realLength);
                return this.description;
            }

            if (this.expectedLength < 0 && this.realLength >= 0) {
                this.description = String.format("The expected length is invalid. Forgot to call the write " +
                        "statistics?. expected length: %d, real length: %d", this.expectedLength, this.realLength);
                return this.description;
            }

            // The expected length and real length is valid.
            if (this.expectedLength == this.realLength) {
                this.description = String.format("File verification succeeded. expected length: %d, real length: %d" ,
                        this.expectedLength, this.realLength);
            } else {
                this.description = String.format("File verification failure. expected length: %d, real length: %d",
                        this.expectedLength, this.realLength);
            }
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
    }

    public WriteConsistencyChecker(NativeFileSystemStore store, String cosKey) throws IOException {
        if (null == store || null == cosKey || cosKey.isEmpty()) {
            throw new IOException(String.format(
                    "Native FileSystem store [%s] or key [%s] is illegal.", store, cosKey));
        }

        this.store = store;
        this.key = cosKey;
        this.beforeWrittenBytes = -1;
        this.afterWrittenBytes = -1;
        this.finished = false;
        this.checkResult = new CheckResult("cosn", this.key, false, -1, -1);
    }

    public synchronized void incrementWrittenBytes(long writtenBytes) {
        if (this.finished) {
            LOG.error("The cos key [{}] has ended statistics.", this.key);
            return;
        }

        if (this.beforeWrittenBytes < 0) {
            LOG.debug("Resetting the beforeWrittenBytes as 0.");
            this.beforeWrittenBytes = 0;            // reset the beforeWrittenBytes as zero.
        }

        this.beforeWrittenBytes += writtenBytes;
    }

    public synchronized void finish() {
        if (this.finished) {
            return;
        }
        this.finished = true;
        try {
            this.afterWrittenBytes = this.store.getFileLength(this.key);
            LOG.debug("Get the target key [{}]'s length: {}.", this.key, this.afterWrittenBytes);
        } catch (IOException e) {
            LOG.error("Failed to get the target key [{}]'s length.", this.key, e);
            this.checkResult.setFsScheme("cosn");
            this.checkResult.setKey(this.key);
            this.checkResult.setExpectedLength(this.beforeWrittenBytes);
            this.checkResult.setRealLength(-1);
            return;
        }

        this.checkResult.setFsScheme("cosn");
        this.checkResult.setKey(this.key);
        this.checkResult.setExpectedLength(this.beforeWrittenBytes);
        this.checkResult.setRealLength(this.afterWrittenBytes);
    }

    public boolean isFinished() {
        return finished;
    }

    public CheckResult getCheckResult() {
        return checkResult;
    }
}

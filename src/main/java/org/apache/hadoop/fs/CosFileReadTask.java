package org.apache.hadoop.fs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class CosFileReadTask implements Runnable {
    static final Logger LOG = LoggerFactory.getLogger(CosFileReadTask.class);

    private final String key;
    private final NativeFileSystemStore store;
    private final CosFsInputStream.ReadBuffer readBuffer;

    private RetryPolicy retryPolicy = null;

    public CosFileReadTask(
            Configuration conf,
            String key, NativeFileSystemStore store, CosFsInputStream.ReadBuffer readBuffer) {
        this.key = key;
        this.store = store;
        this.readBuffer = readBuffer;

        RetryPolicy defaultPolicy = RetryPolicies.retryUpToMaximumCountWithFixedSleep(
                conf.getInt(
                        CosNativeFileSystemConfigKeys.COS_MAX_RETRIES_KEY,
                        CosNativeFileSystemConfigKeys.DEFAULT_MAX_RETRIES),
                conf.getLong(
                        CosNativeFileSystemConfigKeys.COS_RETRY_INTERVAL_KEY,
                        CosNativeFileSystemConfigKeys.DEFAULT_RETRY_INTERVAL),
                TimeUnit.SECONDS
        );
        Map<Class<? extends Exception>, RetryPolicy> retryPolicyMap = new HashMap<Class<? extends Exception>, RetryPolicy>();
        retryPolicyMap.put(IOException.class, defaultPolicy);
        retryPolicyMap.put(IndexOutOfBoundsException.class, RetryPolicies.TRY_ONCE_THEN_FAIL);
        retryPolicyMap.put(NullPointerException.class, RetryPolicies.TRY_ONCE_THEN_FAIL);

        this.retryPolicy = RetryPolicies.retryByException(defaultPolicy, retryPolicyMap);
    }

    @Override
    public void run() {
        int retries = 0;
        RetryPolicy.RetryAction retryAction = null;
        try {
            this.readBuffer.lock();
            do {
                try {
                    InputStream inputStream = this.store.retrieveBlock(
                            this.key, this.readBuffer.getStart(), this.readBuffer.getEnd());
                    IOUtils.readFully(
                            inputStream, this.readBuffer.getBuffer(), 0, readBuffer.getBuffer().length);
                    inputStream.close();
                    this.readBuffer.setStatus(CosFsInputStream.ReadBuffer.SUCCESS);
                    break;
                } catch (IOException e) {
                    this.readBuffer.setStatus(CosFsInputStream.ReadBuffer.ERROR);
                    LOG.error("exception: {}", e);
                    LOG.warn("Exception occurs when retrieve the block range start: "
                            + String.valueOf(this.readBuffer.getStart()) + " end: " + this.readBuffer.getEnd());
                    try {
                        retryAction = this.retryPolicy.shouldRetry(e, retries++, 0, true);
                        if (retryAction.action == RetryPolicy.RetryAction.RetryDecision.RETRY) {
                            Thread.sleep(retryAction.delayMillis);
                        }
                    } catch (Exception e1) {
                        String errMsg = String.format("Exception occurs when retry[%s] "
                                        + "to retrieve the block range start: %d, end:%d",
                                this.retryPolicy.toString(),
                                String.valueOf(this.readBuffer.getStart()),
                                String.valueOf(this.readBuffer.getEnd()));
                        LOG.error(errMsg, e1);
                        break;
                    }
                }
            } while (null != retryAction && retryAction.action == RetryPolicy.RetryAction.RetryDecision.RETRY);

            this.readBuffer.signalAll();
        } finally {
            this.readBuffer.unLock();
        }
    }
}

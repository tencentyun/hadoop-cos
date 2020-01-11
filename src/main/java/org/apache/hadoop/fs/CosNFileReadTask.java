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

public class CosNFileReadTask implements Runnable {
    static final Logger LOG = LoggerFactory.getLogger(CosNFileReadTask.class);

    private final String key;
    private final NativeFileSystemStore store;
    private final CosFsInputStream.ReadBuffer readBuffer;

    private RetryPolicy retryPolicy = null;

    public CosNFileReadTask(Configuration conf, String key,
                            NativeFileSystemStore store,
                            CosFsInputStream.ReadBuffer readBuffer) {
        this.key = key;
        this.store = store;
        this.readBuffer = readBuffer;

        RetryPolicy defaultPolicy =
                RetryPolicies.retryUpToMaximumCountWithFixedSleep(
                        conf.getInt(
                                CosNConfigKeys.COSN_MAX_RETRIES_KEY,
                                CosNConfigKeys.DEFAULT_MAX_RETRIES),
                        conf.getLong(
                                CosNConfigKeys.COSN_RETRY_INTERVAL_KEY,
                                CosNConfigKeys.DEFAULT_RETRY_INTERVAL),
                        TimeUnit.SECONDS
                );
        Map<Class<? extends Exception>, RetryPolicy> retryPolicyMap =
                new HashMap<Class<? extends Exception>, RetryPolicy>();
        retryPolicyMap.put(IOException.class, defaultPolicy);
        retryPolicyMap.put(IndexOutOfBoundsException.class,
                RetryPolicies.TRY_ONCE_THEN_FAIL);
        retryPolicyMap.put(NullPointerException.class,
                RetryPolicies.TRY_ONCE_THEN_FAIL);

        this.retryPolicy = RetryPolicies.retryByException(defaultPolicy,
                retryPolicyMap);
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
                            this.key, this.readBuffer.getStart(),
                            this.readBuffer.getEnd());
                    IOUtils.readFully(
                            inputStream, this.readBuffer.getBuffer(), 0,
                            readBuffer.getBuffer().length);
                    int readEof = inputStream.read();
                    if (readEof != -1) {
                        LOG.error("Expect to read the eof, but the return is not -1. key: {}.", this.key);
                    }
                    inputStream.close();
                    this.readBuffer.setStatus(CosFsInputStream.ReadBuffer.SUCCESS);
                    break;
                } catch (IOException e) {
                    this.readBuffer.setStatus(CosFsInputStream.ReadBuffer.ERROR);
                    LOG.warn("Exception occurs when retrieve the block range " +
                            "start: "
                            + String.valueOf(this.readBuffer.getStart()) + " " +
                            "end: " + this.readBuffer.getEnd(), e);
                    try {
                        retryAction = this.retryPolicy.shouldRetry(e,
                                retries++, 0, true);
                        if (null != retryAction && retryAction.action == RetryPolicy.RetryAction.RetryDecision.RETRY) {
                            Thread.sleep(retryAction.delayMillis);
                        }
                    } catch (Exception e1) {
                        String errMsg = String.format("Exception occurs when " +
                                        "retry[%s] "
                                        + "to retrieve the block range start:" +
                                        " %d, end:%d",
                                this.retryPolicy.toString(),
                                this.readBuffer.getStart(),
                                this.readBuffer.getEnd());
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

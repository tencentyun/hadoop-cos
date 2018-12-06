package org.apache.hadoop.fs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class CosCopyFileTask implements Runnable {
    private static Logger LOG = LoggerFactory.getLogger(CosCopyFileTask.class);

    private NativeFileSystemStore store;

    private String srcKey;
    private String dstKey;
    private CosCopyFileContext cosCopyFileContext;

    public CosCopyFileTask(
            NativeFileSystemStore store, String srcKey, String dstKey, CosCopyFileContext cosCopyFileContext) {
        this.store = store;
        this.srcKey = srcKey;
        this.dstKey = dstKey;
        this.cosCopyFileContext = cosCopyFileContext;
    }

    @Override
    public void run() {
        boolean fail = false;
        try {
            this.store.copy(srcKey, dstKey);
        } catch (IOException e) {
            LOG.warn("Exception thrown when copy from {} to {}, exception:{}", this.srcKey, this.dstKey, e);
            fail = true;
        } finally {
            this.cosCopyFileContext.lock();
            if (fail) {
                cosCopyFileContext.setCopySuccess(false);
            }
            cosCopyFileContext.incCopiesFinish();
            cosCopyFileContext.signalAll();
            cosCopyFileContext.unlock();
        }
    }
}

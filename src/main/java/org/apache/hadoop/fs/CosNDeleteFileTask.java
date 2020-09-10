package org.apache.hadoop.fs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class CosNDeleteFileTask implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(CosNCopyFileTask.class);

    private final NativeFileSystemStore store;

    private final String srcKey;
    private final CosNDeleteFileContext cosDeleteFileContext;

    public CosNDeleteFileTask(NativeFileSystemStore store, String srcKey,
                              CosNDeleteFileContext cosDeleteFileContext) {
        this.store = store;
        this.srcKey = srcKey;
        this.cosDeleteFileContext = cosDeleteFileContext;
    }

    @Override
    public void run() {
        boolean fail = false;
        try {
            LOG.debug("Delete the cos key: {}.", srcKey);
            this.store.delete(srcKey);
        } catch (IOException e) {
            LOG.warn("Exception thrown when delete file [{}], exception: ", this.srcKey, e);
            fail = true;
            cosDeleteFileContext.setIOException(e);
        } finally {
            this.cosDeleteFileContext.lock();
            if (fail) {
                cosDeleteFileContext.setDeleteSuccess(false);
            }
            cosDeleteFileContext.incDeletesFinish();
            cosDeleteFileContext.signalAll();
            cosDeleteFileContext.unlock();
        }
    }
}


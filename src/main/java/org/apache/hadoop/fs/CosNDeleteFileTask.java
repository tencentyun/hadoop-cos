package org.apache.hadoop.fs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CosNDeleteFileTask implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(CosNDeleteFileTask.class);

    private final NativeFileSystemStore store;
    private final List<String> deletingKeys;
    private final CosNDeleteFileContext cosDeleteFileContext;

    public CosNDeleteFileTask(NativeFileSystemStore store, String srcKey,
                              CosNDeleteFileContext cosDeleteFileContext) {
        this.store = store;
        this.deletingKeys = new ArrayList<>(1);
        this.deletingKeys.add(srcKey);
        this.cosDeleteFileContext = cosDeleteFileContext;
    }

    public CosNDeleteFileTask(NativeFileSystemStore store, List<String> deletingKeys,
                              CosNDeleteFileContext cosDeleteFileContext) {
        this.store = store;
        this.deletingKeys = deletingKeys;
        this.cosDeleteFileContext = cosDeleteFileContext;
    }

    @Override
    public void run() {
        boolean fail = false;
        int deleteFinishCounter = 0;
        try {
            for (String srcKey : deletingKeys) {
                try {
                    LOG.debug("Delete the cos key: {}.", srcKey);
                    this.store.delete(srcKey);
                    deleteFinishCounter++;
                } catch (IOException e) {
                    LOG.warn("Exception thrown when delete file [{}], exception: ", srcKey, e);
                    fail = true;
                    cosDeleteFileContext.setIOException(e);
                }
            }
        } finally {
            this.cosDeleteFileContext.lock();
            if (fail) {
                cosDeleteFileContext.setDeleteSuccess(false);
            }
            cosDeleteFileContext.incDeletesFinish(deleteFinishCounter);
            cosDeleteFileContext.signalAll();
            cosDeleteFileContext.unlock();
        }
    }
}


package org.apache.hadoop.fs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;

public class CosNFileReadTask implements Runnable {
    static final Logger LOG = LoggerFactory.getLogger(CosNFileReadTask.class);

    private final String key;
    private final NativeFileSystemStore store;
    private final CosNFSInputStream.ReadBuffer readBuffer;

    /**
     * cos file read task
     * @param conf config
     * @param key cos key
     * @param store native file system
     * @param readBuffer read buffer
     */
    public CosNFileReadTask(Configuration conf, String key,
                            NativeFileSystemStore store,
                            CosNFSInputStream.ReadBuffer readBuffer) {
        this.key = key;
        this.store = store;
        this.readBuffer = readBuffer;
    }

    @Override
    public void run() {
        try {
            this.readBuffer.lock();
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
                this.readBuffer.setStatus(CosNFSInputStream.ReadBuffer.SUCCESS);
            } catch (IOException e) {
                this.readBuffer.setStatus(CosNFSInputStream.ReadBuffer.ERROR);
                LOG.error("Exception occurs when retrieve the block range " +
                        "start: "
                        + String.valueOf(this.readBuffer.getStart()) + " " +
                        "end: " + this.readBuffer.getEnd(), e);
            }
            this.readBuffer.signalAll();
        } finally {
            this.readBuffer.unLock();
        }
    }
}

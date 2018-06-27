package org.apache.hadoop.fs.cosnative;

import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;

public class CosFileReadTask implements Runnable {
    static final Logger LOG = LoggerFactory.getLogger(CosFileReadTask.class);

    private final String key;
    private final NativeFileSystemStore store;
    private final CosFsInputStream.ReadBuffer readBuffer;

    public CosFileReadTask(String key, NativeFileSystemStore store, CosFsInputStream.ReadBuffer readBuffer) {
        this.key = key;
        this.store = store;
        this.readBuffer = readBuffer;
    }

    @Override
    public void run() {
        this.readBuffer.setStatus(CosFsInputStream.ReadBuffer.ERROR);
        try {
            try {
                this.readBuffer.lock();
                InputStream inputStream = this.store.retrieveBlock(this.key, this.readBuffer.getStart(), this.readBuffer.getEnd());
                IOUtils.readFully(inputStream, this.readBuffer.getBuffer(), 0, (int) (this.readBuffer.getEnd() - this.readBuffer.getStart() + 1));
                inputStream.close();
                this.readBuffer.setStatus(CosFsInputStream.ReadBuffer.SUCCESS);
                this.readBuffer.signalAll();
            } catch (IOException e) {
                this.readBuffer.setStatus(CosFsInputStream.ReadBuffer.ERROR);
                LOG.error("Exception occurs when retrieve the block range start: " + String.valueOf(this.readBuffer.getStart()) + " end: " + this.readBuffer.getEnd());
            }
            this.readBuffer.signalAll();
        }finally {
            this.readBuffer.unLock();
        }
    }
}

package org.apache.hadoop.fs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.SocketException;
import java.util.concurrent.ThreadLocalRandom;

public class CosNFileReadTask implements Runnable {
    static final Logger LOG = LoggerFactory.getLogger(CosNFileReadTask.class);

    private final Configuration conf;
    private final String key;
    private final NativeFileSystemStore store;
    private final CosNFSInputStream.ReadBuffer readBuffer;
    private final int socketErrMaxRetryTimes;

    /**
     * cos file read task
     * @param conf config
     * @param key cos key
     * @param store native file system
     * @param readBuffer read buffer
     */
    public CosNFileReadTask(Configuration conf, String key,
                            NativeFileSystemStore store,
                            CosNFSInputStream.ReadBuffer readBuffer,
                            int socketErrMaxRetryTimes) {
        this.conf = conf;
        this.key = key;
        this.store = store;
        this.readBuffer = readBuffer;
        this.socketErrMaxRetryTimes = socketErrMaxRetryTimes;
    }

    @Override
    public void run() {
        try {
            this.readBuffer.lock();
            int retryIndex = 1;
            boolean needRetry = false;
            while (true) {
                try {
                    this.retrieveBlock();
                    needRetry = false;
                } catch (SocketException se) {
                    // if we get stream success, but exceptions occurs when read cos input stream
                    String errMsg = String.format("retrieve block sdk socket failed, " +
                                    "retryIndex: [%d / %d], key: %s, range: [%d , %d], exception: %s",
                            retryIndex, this.socketErrMaxRetryTimes, this.key,
                            this.readBuffer.getStart(), this.readBuffer.getEnd(), se.toString());
                    if (retryIndex <= this.socketErrMaxRetryTimes) {
                        LOG.info(errMsg, se);
                        long sleepLeast = retryIndex * 300L;
                        long sleepBound = retryIndex * 500L;
                        try {
                            Thread.sleep(ThreadLocalRandom.current().
                                    nextLong(sleepLeast, sleepBound));
                            ++retryIndex;
                            needRetry = true;
                        } catch (InterruptedException ie) {
                            this.setFailResult(errMsg, new IOException(ie.toString()));
                            break;
                        }
                    } else {
                        this.setFailResult(errMsg, se);
                        break;
                    }
                } catch (IOException e) {
                    String errMsg = String.format("retrieve block sdk socket failed, " +
                                    "retryIndex: [%d / %d], key: %s, range: [%d , %d], exception: %s",
                            retryIndex, this.socketErrMaxRetryTimes, this.key,
                            this.readBuffer.getStart(), this.readBuffer.getEnd(), e.toString());
                    this.setFailResult(errMsg, e);
                    break;
                }

                if (!needRetry) {
                    break;
                }
            } // end of retry
            this.readBuffer.signalAll();
        } finally {
            this.readBuffer.unLock();
        }
    }

    public void setFailResult(String msg, IOException e) {
        this.readBuffer.setStatus(CosNFSInputStream.ReadBuffer.ERROR);
        this.readBuffer.setException(e);
        LOG.error(msg);
    }

    // not thread safe
    public void retrieveBlock() throws IOException {
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
    }
}

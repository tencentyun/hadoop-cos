package org.apache.hadoop.fs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.hadoop.fs.CosNConfigKeys.DEFAULT_READ_BUFFER_ALLOCATE_TIMEOUT_SECONDS;

public class CosNFileReadTask implements Runnable {
    static final Logger LOG = LoggerFactory.getLogger(CosNFileReadTask.class);

    private final Configuration conf;
    private final String key;
    private final NativeFileSystemStore store;
    private final CosNFSInputStream.ReadBuffer readBuffer;
    private final int socketErrMaxRetryTimes;
    private final AtomicBoolean closed;

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
                            int socketErrMaxRetryTimes,
                            AtomicBoolean closed) {
        this.conf = conf;
        this.key = key;
        this.store = store;
        this.readBuffer = readBuffer;
        this.socketErrMaxRetryTimes = socketErrMaxRetryTimes;
        this.closed = closed;
    }

    @Override
    public void run() {
        try {
            this.readBuffer.lock();
            checkStreamClosed();
            try {
                this.readBuffer.allocate(
                    conf.getLong(CosNConfigKeys.COSN_READ_BUFFER_ALLOCATE_TIMEOUT_SECONDS,
                        DEFAULT_READ_BUFFER_ALLOCATE_TIMEOUT_SECONDS), TimeUnit.SECONDS);
            } catch (Exception e) {
                this.setFailResult("allocate read buffer failed.", new IOException(e));
                return;
            }
            int retryIndex = 1;
            boolean needRetry = false;
            while (true) {
                try {
                    this.retrieveBlock();
                    needRetry = false;
                } catch (SocketException | SocketTimeoutException socketException) {
                    // if we get stream success, but exceptions occurs when read cos input stream
                    String errMsg = String.format("retrieve block sdk socket failed, " +
                                    "retryIndex: [%d / %d], key: %s, range: [%d , %d], exception: %s",
                            retryIndex, this.socketErrMaxRetryTimes, this.key,
                            this.readBuffer.getStart(), this.readBuffer.getEnd(), socketException.toString());
                    if (retryIndex <= this.socketErrMaxRetryTimes) {
                        LOG.info(errMsg, socketException);
                        long sleepLeast = retryIndex * 300L;
                        long sleepBound = retryIndex * 500L;
                        try {
                            Thread.sleep(ThreadLocalRandom.current().
                                    nextLong(sleepLeast, sleepBound));
                            ++retryIndex;
                            needRetry = true;
                        } catch (InterruptedException interruptedException) {
                            this.setFailResult(errMsg, new IOException(interruptedException.toString()));
                            break;
                        }
                    } else {
                        this.setFailResult(errMsg, socketException);
                        break;
                    }
                } catch (IOException ioException) {
                    String errMsg = String.format("retrieve block failed, " +
                                    "retryIndex: [%d / %d], key: %s, range: [%d , %d], io exception: %s",
                            retryIndex, this.socketErrMaxRetryTimes, this.key,
                            this.readBuffer.getStart(), this.readBuffer.getEnd(), ioException);
                    this.setFailResult(errMsg, ioException);
                    break;
                }

                if (!needRetry) {
                    break;
                }
            } // end of retry
        } catch (Throwable throwable) {
            this.setFailResult(
                String.format("retrieve block failed, key: %s, range: [%d , %d], exception: %s",
                    this.key, this.readBuffer.getStart(), this.readBuffer.getEnd(), throwable),
                new IOException(throwable));
        } finally {
            this.readBuffer.signalAll();
            this.readBuffer.unLock();
        }
    }

    public void setFailResult(String msg, IOException e) {
        this.readBuffer.setStatus(CosNFSInputStream.ReadBuffer.ERROR);
        this.readBuffer.setException(e);
        if (e.getCause() != null && e.getCause() instanceof CancelledException) {
            // 预期操作，以warn级别导出
            LOG.warn(msg);
        } else {
            LOG.error(msg);
        }
    }

    // not thread safe
    private void retrieveBlock() throws IOException, CancelledException {
        byte[] dataBuf = readBuffer.getBuffer();
        checkStreamClosed();
        Objects.requireNonNull(dataBuf);
        InputStream inputStream = null;
        try {
            inputStream = this.store.retrieveBlock(
                        this.key, this.readBuffer.getStart(), this.readBuffer.getEnd());
            IOUtils.readFully(
                inputStream, dataBuf, 0,
                dataBuf.length);
            int readEof = inputStream.read();
            if (readEof != -1) {
                LOG.error("Expect to read the eof, but the return is not -1. key: {}.", this.key);
            }
            this.readBuffer.setStatus(CosNFSInputStream.ReadBuffer.SUCCESS);
        } finally {
            if (inputStream != null) {
                inputStream.close();
            }
        }
    }

    private void checkStreamClosed() throws CancelledException {
        if (closed.get()) {
            throw new CancelledException("the input stream has been canceled.");
        }
    }


    private static class CancelledException extends Exception {
        public CancelledException(String message) {
            super(message);
        }
    }
}


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
        // 设置线程的context class loader
        // 之前有客户通过spark-sql执行add jar命令, 当spark.eventLog.dir为cos, jar路径也在cos时, 会导致cos读取数据时，
        // http库的日志加载，又会加载cos上的文件，以此形成了逻辑死循环
        // 1 上层调用cos read
        //2 cos插件通过Apache http库读取数据
        //3 http库里面初始化日志对象时要读取日志配置，发现配置是在cos上
        //4 调用cos read

        // 分析后发现，日志库里面获取资源是通过context class loader, 而add jar会改变context class loader，将被add jar也加入classpath路径中
        // 因此这里通过设置context class loader为app class loader。 避免被上层add jar等改变context class loader行为污染
        Thread currentThread = Thread.currentThread();
        LOG.debug("flush task, current classLoader: {}, context ClassLoader: {}",
                this.getClass().getClassLoader(), currentThread.getContextClassLoader());
        currentThread.setContextClassLoader(this.getClass().getClassLoader());

        try {
            this.readBuffer.lock();
            int retryIndex = 1;
            boolean needRetry = false;
            while (true) {
                try {
                    this.retrieveBlock();
                    needRetry = false;
                } catch (IOException ioException) {
                    // if we get stream success, but exceptions occurs when read cos input stream
                    String errMsg = String.format("retrieve block sdk socket failed, " +
                                    "retryIndex: [%d / %d], key: %s, range: [%d , %d], exception: %s",
                            retryIndex, this.socketErrMaxRetryTimes, this.key,
                            this.readBuffer.getStart(), this.readBuffer.getEnd(), ioException.toString());
                    if (retryIndex <= this.socketErrMaxRetryTimes) {
                        LOG.info(errMsg, ioException);
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
                        this.setFailResult(errMsg, ioException);
                        break;
                    }
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

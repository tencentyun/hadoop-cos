package org.apache.hadoop.fs;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.cosn.BufferPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.*;

/*
 * the origin hadoop cos implements
 */
public class CosHadoopFileSystem extends FileSystem {
    static final Logger LOG = LoggerFactory.getLogger(CosHadoopFileSystem.class);
    static final String SCHEME = "cosn";
    static final String PATH_DELIMITER = Path.SEPARATOR;
    static final Charset METADATA_ENCODING = StandardCharsets.UTF_8;
    // The length of name:value pair should be less than or equal to 1024 bytes.
    static final int MAX_XATTR_SIZE = 1024;

    private FileSystem actualImplFS = null;
    private NativeFileSystemStore store;

    private int normalBucketMaxListNum;
    private ExecutorService boundedIOThreadPool;
    private ExecutorService boundedCopyThreadPool;

    // todo: flink or some other case must replace with inner structure.
    public CosHadoopFileSystem() {

    }

    public CosHadoopFileSystem(NativeFileSystemStore store) {
        this.store = store;
    }

    @Override
    public void initialize(URI uri, Configuration conf) throws IOException {
        super.initialize(uri, conf);
        setConf(conf);

        // below is the cos hadoop file system init process
        BufferPool.getInstance().initialize(getConf());

        this.normalBucketMaxListNum = this.getConf().getInt(
                CosNConfigKeys.NORMAL_BUCKET_MAX_LIST_NUM,
                CosNConfigKeys.DEFAULT_NORMAL_BUCKET_MAX_LIST_NUM);

        // initialize the thread pool
        int uploadThreadPoolSize = this.getConf().getInt(
                CosNConfigKeys.UPLOAD_THREAD_POOL_SIZE_KEY,
                CosNConfigKeys.DEFAULT_UPLOAD_THREAD_POOL_SIZE
        );
        int readAheadPoolSize = this.getConf().getInt(
                CosNConfigKeys.READ_AHEAD_QUEUE_SIZE,
                CosNConfigKeys.DEFAULT_READ_AHEAD_QUEUE_SIZE
        );
        Preconditions.checkArgument(uploadThreadPoolSize > 0,
                String.format("The uploadThreadPoolSize[%d] should be positive.", uploadThreadPoolSize));
        Preconditions.checkArgument(readAheadPoolSize > 0,
                String.format("The readAheadQueueSize[%d] should be positive.", readAheadPoolSize));
        // 核心线程数取用户配置的为准，最大线程数结合用户配置和IO密集型任务的最优线程数来看
        int ioCoreTaskSize = uploadThreadPoolSize + readAheadPoolSize;
        int ioMaxTaskSize = Math.max(uploadThreadPoolSize + readAheadPoolSize,
                Runtime.getRuntime().availableProcessors() * 2 + 1);
        if (this.getConf().get(CosNConfigKeys.IO_THREAD_POOL_MAX_SIZE_KEY) != null) {
            int ioThreadPoolMaxSize = this.getConf().getInt(
                    CosNConfigKeys.IO_THREAD_POOL_MAX_SIZE_KEY, CosNConfigKeys.DEFAULT_IO_THREAD_POOL_MAX_SIZE);
            Preconditions.checkArgument(ioThreadPoolMaxSize > 0,
                    String.format("The ioThreadPoolMaxSize[%d] should be positive.", ioThreadPoolMaxSize));
            // 如果设置了 IO 线程池的最大限制，则整个线程池需要被限制住
            ioCoreTaskSize = Math.min(ioCoreTaskSize, ioThreadPoolMaxSize);
            ioMaxTaskSize = ioThreadPoolMaxSize;
        }
        long threadKeepAlive = this.getConf().getLong(
                CosNConfigKeys.THREAD_KEEP_ALIVE_TIME_KEY,
                CosNConfigKeys.DEFAULT_THREAD_KEEP_ALIVE_TIME);
        Preconditions.checkArgument(threadKeepAlive > 0,
                String.format("The threadKeepAlive [%d] should be positive.", threadKeepAlive));
        this.boundedIOThreadPool = new ThreadPoolExecutor(
                ioCoreTaskSize, ioMaxTaskSize,
                threadKeepAlive, TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>(ioCoreTaskSize),
                new ThreadFactoryBuilder().setNameFormat("cos-transfer-shared-%d").setDaemon(true).build(),
                new RejectedExecutionHandler() {
                    @Override
                    public void rejectedExecution(Runnable r,
                                                  ThreadPoolExecutor executor) {
                        if (!executor.isShutdown()) {
                            try {
                                executor.getQueue().put(r);
                            } catch (InterruptedException e) {
                                LOG.error("put a io task into the download " +
                                        "thread pool occurs an exception.", e);
                                throw new RejectedExecutionException(
                                        "Putting the io task failed due to the interruption", e);
                            }
                        } else {
                            LOG.error("The bounded io thread pool has been shutdown.");
                            throw new RejectedExecutionException("The bounded io thread pool has been shutdown");
                        }
                    }
                }
        );

        int copyThreadPoolSize = this.getConf().getInt(
                CosNConfigKeys.COPY_THREAD_POOL_SIZE_KEY,
                CosNConfigKeys.DEFAULT_COPY_THREAD_POOL_SIZE
        );
        Preconditions.checkArgument(copyThreadPoolSize > 0,
                String.format("The copyThreadPoolSize[%d] should be positive.", copyThreadPoolSize));
        this.boundedCopyThreadPool = new ThreadPoolExecutor(
                copyThreadPoolSize, copyThreadPoolSize,
                threadKeepAlive, TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>(copyThreadPoolSize),
                new ThreadFactoryBuilder().setNameFormat("cos-copy-%d").setDaemon(true).build(),
                new RejectedExecutionHandler() {
                    @Override
                    public void rejectedExecution(Runnable r,
                                                  ThreadPoolExecutor executor) {
                        if (!executor.isShutdown()) {
                            try {
                                executor.getQueue().put(r);
                            } catch (InterruptedException e) {
                                LOG.error("put a copy task into the download " +
                                        "thread pool occurs an exception.", e);
                            }
                        }
                    }
                }
        );
    }
}

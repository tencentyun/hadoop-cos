package org.apache.hadoop.fs;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;

public class CosNThreadPool {
	private static final Logger LOG = LoggerFactory.getLogger(CosNThreadPool.class);

	private static volatile CosNThreadPool INSTANCE;

	private final ExecutorService boundedIOThreadPool;
	private final ExecutorService boundedCopyThreadPool;
	private final ExecutorService boundedDeleteThreadPool;
	private static final ConcurrentHashMap<String, Boolean> usedFileSystemMap = new ConcurrentHashMap<>();

	private CosNThreadPool(Configuration conf) {
		// initialize the thread pool
		int uploadThreadPoolSize = conf.getInt(CosNConfigKeys.UPLOAD_THREAD_POOL_SIZE_KEY,
				CosNConfigKeys.DEFAULT_UPLOAD_THREAD_POOL_SIZE);
		int readAheadPoolSize = conf.getInt(CosNConfigKeys.READ_AHEAD_QUEUE_SIZE,
				CosNConfigKeys.DEFAULT_READ_AHEAD_QUEUE_SIZE);

		Preconditions.checkArgument(uploadThreadPoolSize > 0,
				String.format("The uploadThreadPoolSize[%d] should be positive.", uploadThreadPoolSize));
		Preconditions.checkArgument(readAheadPoolSize > 0,
				String.format("The readAheadQueueSize[%d] should be positive.", readAheadPoolSize));
		// 核心线程数取用户配置的为准，最大线程数结合用户配置和IO密集型任务的最优线程数来看
		int ioCoreTaskSize = uploadThreadPoolSize + readAheadPoolSize;
		int ioMaxTaskSize = Math.max(ioCoreTaskSize, CosNConfigKeys.DEFAULT_IO_THREAD_POOL_MAX_SIZE);
		if (conf.get(CosNConfigKeys.IO_THREAD_POOL_MAX_SIZE_KEY) != null) {
			int ioThreadPoolMaxSize = conf.getInt(CosNConfigKeys.IO_THREAD_POOL_MAX_SIZE_KEY,
					CosNConfigKeys.DEFAULT_IO_THREAD_POOL_MAX_SIZE);
			Preconditions.checkArgument(ioThreadPoolMaxSize > 0,
					String.format("The ioThreadPoolMaxSize[%d] should be positive.", ioThreadPoolMaxSize));
			// 如果设置了 IO 线程池的最大限制，则整个线程池需要被限制住
			ioCoreTaskSize = Math.min(ioCoreTaskSize, ioThreadPoolMaxSize);
			ioMaxTaskSize = ioThreadPoolMaxSize;
		}
		long threadKeepAlive = conf.getLong(CosNConfigKeys.THREAD_KEEP_ALIVE_TIME_KEY,
				CosNConfigKeys.DEFAULT_THREAD_KEEP_ALIVE_TIME);
		Preconditions.checkArgument(threadKeepAlive > 0,
				String.format("The threadKeepAlive [%d] should be positive.", threadKeepAlive));
		this.boundedIOThreadPool = new ThreadPoolExecutor(ioCoreTaskSize, ioMaxTaskSize, threadKeepAlive,
				TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(ioCoreTaskSize),
				new ThreadFactoryBuilder().setNameFormat("cos-transfer-shared-%d").setDaemon(true).build(),
				new RejectedExecutionHandler() {
					@Override
					public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
						if (!executor.isShutdown()) {
							try {
								executor.getQueue().put(r);
							} catch (InterruptedException e) {
								LOG.error("Put a io task into the download " + "thread pool occurs an exception.", e);
								throw new RejectedExecutionException("Put the io task failed due to the interruption",
										e);
							}
						} else {
							LOG.error("The bounded io thread pool has been shutdown.");
							throw new RejectedExecutionException("The bounded io thread pool has been shutdown");
						}
					}
				});

		// copy 线程池初始化
		int copyThreadPoolSize = conf.getInt(CosNConfigKeys.COPY_THREAD_POOL_SIZE_KEY,
				CosNConfigKeys.DEFAULT_COPY_THREAD_POOL_SIZE);
		this.boundedCopyThreadPool = new ThreadPoolExecutor(copyThreadPoolSize, copyThreadPoolSize, threadKeepAlive,
				TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(copyThreadPoolSize),
				new ThreadFactoryBuilder().setNameFormat("cos-copy-%d").setDaemon(true).build(),
				new RejectedExecutionHandler() {
					@Override
					public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
						if (!executor.isShutdown()) {
							try {
								executor.getQueue().put(r);
							} catch (InterruptedException e) {
								LOG.error("Put a copy task into the download " + "thread pool occurs an exception.",
										e);
								throw new RejectedExecutionException(
										"Put the copy task failed due to the " + "interruption", e);
							}
						}
					}
				});

		// delete 线程池初始化
		int deleteThreadPoolSize = conf.getInt(CosNConfigKeys.DELETE_THREAD_POOL_SIZE_KEY,
				CosNConfigKeys.DEFAULT_DELETE_THREAD_POOL_SIZE);
		int maxDeleteThreadPoolSize = Math.max(deleteThreadPoolSize,
				CosNConfigKeys.DEFAULT_DELETE_THREAD_POOL_MAX_SIZE);
		if (conf.get(CosNConfigKeys.DELETE_THREAD_POOL_MAX_SIZE_KEY) != null) {
			int deleteThreadPoolMaxSize = conf.getInt(CosNConfigKeys.DELETE_THREAD_POOL_MAX_SIZE_KEY,
					CosNConfigKeys.DEFAULT_DELETE_THREAD_POOL_MAX_SIZE);
			Preconditions.checkArgument(deleteThreadPoolMaxSize > 0,
					String.format("The deleteThreadPoolMaxSize[%d] should be positive.", deleteThreadPoolMaxSize));
			deleteThreadPoolSize = Math.min(deleteThreadPoolSize, deleteThreadPoolMaxSize);
			maxDeleteThreadPoolSize = deleteThreadPoolMaxSize;
		}
		this.boundedDeleteThreadPool = new ThreadPoolExecutor(deleteThreadPoolSize, maxDeleteThreadPoolSize,
				threadKeepAlive, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(deleteThreadPoolSize),
				new ThreadFactoryBuilder().setNameFormat("cos-delete-%d").setDaemon(true).build(),
				new RejectedExecutionHandler() {
					@Override
					public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
						if (!executor.isShutdown()) {
							try {
								executor.getQueue().put(r);
							} catch (InterruptedException e) {
								LOG.error("Put a delete task into the download " + "thread pool occurs an exception.",
										e);
								throw new RejectedExecutionException(
										"Put the delete task failed due to the interruption", e);
							}
						}
					}
				});
	}

	public static CosNThreadPool getInstance(Configuration conf, String uuid) {
		usedFileSystemMap.put(uuid, true);
		if (INSTANCE == null) {
			synchronized (CosNThreadPool.class) {
				if (INSTANCE == null) {
					INSTANCE = new CosNThreadPool(conf);
				}
			}
		}
		return INSTANCE;
	}

	public ExecutorService getBoundedIOThreadPool() {
		return boundedIOThreadPool;
	}

	public ExecutorService getBoundedCopyThreadPool() {
		return boundedCopyThreadPool;
	}

	public ExecutorService getBoundedDeleteThreadPool() {
		return boundedDeleteThreadPool;
	}


	public void maybeCloseThreadPool(String uuid) {
		usedFileSystemMap.remove(uuid);
		if (usedFileSystemMap.isEmpty()) {
			closeThreadPool();
		}
	}

	private void closeThreadPool() {
		try {
			// 先释放掉 IO 线程池以及相关的 IO 资源。
			try {
				this.boundedIOThreadPool.shutdown();
				if (!this.boundedIOThreadPool.awaitTermination(5, TimeUnit.SECONDS)) {
					LOG.warn("boundedIOThreadPool shutdown timeout, force shutdown now.");
					this.boundedIOThreadPool.shutdownNow();
				}
			} catch (InterruptedException e) {
				LOG.error("boundedIOThreadPool shutdown interrupted.", e);
			}
		} finally {
			// copy 和 delete 因为涉及到元数据操作，因此最后再释放
			try {
				this.boundedCopyThreadPool.shutdown();
				if (!this.boundedCopyThreadPool.awaitTermination(5, TimeUnit.SECONDS)) {
					LOG.warn("boundedCopyThreadPool shutdown timeout, force shutdown now.");
					this.boundedCopyThreadPool.shutdownNow();
				}
			} catch (InterruptedException e) {
				LOG.error("boundedCopyThreadPool shutdown interrupted.", e);
			}
			try {
				this.boundedDeleteThreadPool.shutdown();
				if (!this.boundedDeleteThreadPool.awaitTermination(5, TimeUnit.SECONDS)) {
					LOG.warn("boundedDeleteThreadPool shutdown timeout, force shutdown now.");
					this.boundedDeleteThreadPool.shutdownNow();
				}
			} catch (InterruptedException e) {
				LOG.error("boundedDeleteThreadPool shutdown interrupted.", e);
			}
		}
		synchronized (CosNThreadPool.class) {
			INSTANCE = null;
		}
	}
}

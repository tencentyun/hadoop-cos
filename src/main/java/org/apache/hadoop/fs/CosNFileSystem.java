package org.apache.hadoop.fs;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.qcloud.chdfs.permission.RangerAccessType;
import com.qcloud.cos.utils.StringUtils;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.cosn.BufferPool;
import org.apache.hadoop.fs.cosn.CRC32CCheckSum;
import org.apache.hadoop.fs.cosn.CRC64Checksum;
import org.apache.hadoop.fs.cosn.LocalRandomAccessMappedBufferPool;
import org.apache.hadoop.fs.cosn.Unit;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/*
 * the origin hadoop cos implements
 */
public class CosNFileSystem extends FileSystem {
    private static final Logger LOG = LoggerFactory.getLogger(CosNFileSystem.class);

    static final String SCHEME = "cosn";
    static final String PATH_DELIMITER = Path.SEPARATOR;
    static final Charset METADATA_ENCODING = StandardCharsets.UTF_8;
    // The length of name:value pair should be less than or equal to 1024 bytes.
    static final int MAX_XATTR_SIZE = 1024;
    static final int BUCKET_LIST_LIMIT = 999;
    static final int POSIX_BUCKET_LIST_LIMIT = 5000;

    private URI uri;
    private String bucket;
    private boolean isPosixBucket;
    private NativeFileSystemStore nativeStore;
    private boolean isDefaultNativeStore;
    private Path workingDir;
    private String owner = "Unknown";
    private String group = "Unknown";
    private ExecutorService boundedIOThreadPool;
    private ExecutorService boundedCopyThreadPool;

    private RangerCredentialsClient rangerCredentialsClient;

    static final String CSE_ALGORITHM_USER_METADATA = "client-side-encryption-cek-alg";
    private int symbolicLinkSizeThreshold;

    // for fileSystem based on CosN like GooseFS will do folder/file check
    // so in this situation, we need disable this duplicate folder/file check in CosN
    private boolean createOpCheckExistFile = true;

    private boolean directoryFirstEnabled;

    // todo: flink or some other case must replace with inner structure.
    public CosNFileSystem() {
    }

    public CosNFileSystem(NativeFileSystemStore nativeStore) {
        this.nativeStore = nativeStore;
        this.isDefaultNativeStore = false;
    }

    public CosNFileSystem withBucket(String bucket) {
        this.bucket = bucket;
        return this;
    }

    public CosNFileSystem withPosixBucket(boolean isPosixBucket) {
        this.isPosixBucket = isPosixBucket;
        return this;
    }

    public CosNFileSystem withStore(NativeFileSystemStore nativeStore) {
        this.nativeStore = nativeStore;
        this.isDefaultNativeStore = false;
        return this;
    }

    public CosNFileSystem withRangerCredentialsClient(RangerCredentialsClient rc) {
        this.rangerCredentialsClient = rc;
        return this;
    }

    public void disableCreateOpFileExistCheck() {
        this.createOpCheckExistFile = false;
    }

    @Override
    public String getScheme() {
        return CosNFileSystem.SCHEME;
    }

    @Override
    public void initialize(URI uri, Configuration conf) throws IOException {
        super.initialize(uri, conf);
        setConf(conf);

        if (null == this.bucket) {
            this.bucket = uri.getHost();
        }
        if (null == this.nativeStore) {
            this.nativeStore = CosNUtils.createDefaultStore(conf);
            this.isDefaultNativeStore = true;
            this.nativeStore.initialize(uri, conf);
        }

        this.uri = URI.create(uri.getScheme() + "://" + uri.getAuthority());
        this.workingDir = new Path("/user", System.getProperty("user.name"))
                .makeQualified(this.uri, this.getWorkingDirectory());

        this.owner = getOwnerId();
        this.group = getGroupId();
        LOG.debug("uri: {}, bucket: {}, working dir: {}, owner: {}, group: {}.\n" +
                "configuration: {}.", uri, bucket, workingDir, owner, group, conf);

        BufferPool.getInstance().initialize(this.getConf());
        if (this.getConf().getBoolean(CosNConfigKeys.COSN_POSIX_EXTENSION_ENABLED,
            CosNConfigKeys.DEFAULT_COSN_POSIX_EXTENSION_ENABLED)) {
            // 只有在开启 POSIX 扩展语义支持的时候才会初始化
            LocalRandomAccessMappedBufferPool.getInstance().initialize(this.getConf());
        }

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

        this.symbolicLinkSizeThreshold = this.getConf().getInt(
                CosNConfigKeys.COSN_SYMBOLIC_SIZE_THRESHOLD, CosNConfigKeys.DEFAULT_COSN_SYMBOLIC_SIZE_THRESHOLD);
        this.directoryFirstEnabled =  this.getConf().getBoolean(CosNConfigKeys.COSN_FILESTATUS_DIR_FIRST_ENABLED,
            CosNConfigKeys.DEFAULT_FILESTATUS_DIR_FIRST_ENABLED);
    }

    @Override
    public URI getUri() {
        return this.uri;
    }

    @Override
    public Path getHomeDirectory() {
        String homePrefix = this.getConf().get("dfs.user.home.dir.prefix");
        if (null != homePrefix) {
            return makeQualified(new Path(homePrefix + "/" + getOwnerId()));
        }

        return super.getHomeDirectory();
    }

    @Override
    public FSDataOutputStream append(Path f, int bufferSize,
        Progressable progress) throws IOException {
        if (this.isPosixBucket) {
            throw new UnsupportedOperationException(
                "The posix bucket does not support append operation through S3 gateway");
        }

        // 如果配置中开启客户端加密，则不支持
        if (this.getConf().getBoolean(CosNConfigKeys.COSN_CLIENT_SIDE_ENCRYPTION_ENABLED,
                CosNConfigKeys.DEFAULT_COSN_CLIENT_SIDE_ENCRYPTION_ENABLED)) {
            throw new UnsupportedOperationException("Not supported currently with client side encryption enabled");
        }

        // 如果原文件使用了客户端加密，则不支持
        try {
            if(this.getXAttr(f, CSE_ALGORITHM_USER_METADATA) != null) {
                throw new UnsupportedOperationException("Not supported currently because the file is encrypted by client side");
            }
        } catch (IOException e) {
            throw new IOException("Failed to check whether the file is encrypted.", e);
        }

        FileStatus fileStatus = this.getFileStatus(f);
        if (fileStatus.isSymlink()) {
            f = this.getLinkTarget(f);
            fileStatus = this.getFileStatus(f);
        }
        if (fileStatus.isDirectory()) {
            throw new FileAlreadyExistsException(f + " is a directory.");
        }

        LOG.debug("Append the file path: {}.", f);
        Path absolutePath = makeAbsolute(f);
        String cosKey = pathToKey(absolutePath);
        if (this.getConf().getBoolean(CosNConfigKeys.COSN_POSIX_EXTENSION_ENABLED,
            CosNConfigKeys.DEFAULT_COSN_POSIX_EXTENSION_ENABLED)) {
            // 这里使用类的动态加载和创建机制是为了在默认场景下（即不支持随机写的场景），可以不依赖 ofs-sdk-definition 这个 jar 包。
            Class<?> seekableOutputStreamClass;
            Object seekableOutputStream;
            try {
                seekableOutputStreamClass = Class.forName("org.apache.hadoop.fs.CosNSeekableFSDataOutputStream$SeekableOutputStream");
                Constructor<?> constructor = seekableOutputStreamClass.getConstructor(Configuration.class, NativeFileSystemStore.class,
                    String.class, ExecutorService.class, ExecutorService.class);
                seekableOutputStream = constructor.newInstance(this.getConf(), this.nativeStore, cosKey, this.boundedIOThreadPool, this.boundedCopyThreadPool);
            } catch (ClassNotFoundException e) {
                throw new IOException("org.apache.hadoop.fs.CosNSeekableFSDataOutputStream$SeekableOutputStream can not be found. " +
                    "please make sure that ofs-sdk-definition.jar is placed in the classpath.", e);
            } catch (InvocationTargetException e) {
                throw new IOException(e);
            } catch (NoSuchMethodException e) {
                throw new IOException("Failed to find the constructor of the " +
                    "org.apache.hadoop.fs.CosNSeekableFSDataOutputStream$SeekableOutputStream", e);
            } catch (InstantiationException e) {
                throw new IOException("Failed to create the object of the " +
                    "org.apache.hadoop.fs.CosNSeekableFSDataOutputStream$SeekableOutputStream", e);
            } catch (IllegalAccessException e) {
                throw new IOException("Failed to access the constructor of the " +
                    "org.apache.hadoop.fs.CosNSeekableFSDataOutputStream$SeekableOutputStream", e);
            }

            try {
                Class<?> seekableFSDataOutputStreamClass = Class.forName("org.apache.hadoop.fs.CosNSeekableFSDataOutputStream");
                Constructor<?> constructor = seekableFSDataOutputStreamClass.getConstructor(
                    Class.forName("org.apache.hadoop.fs.CosNSeekableFSDataOutputStream$SeekableOutputStream"),
                    FileSystem.Statistics.class);
                return (FSDataOutputStream) constructor.newInstance(seekableOutputStream, statistics);
            } catch (ClassNotFoundException e) {
                throw new IOException("org.apache.hadoop.fs.CosNSeekableFSDataOutputStream can not be found. " +
                    "please make sure that ofs-sdk-definition.jar is placed in the classpath.", e);
            } catch (NoSuchMethodException e) {
                throw new IOException("Failed to find the constructor of the " +
                    "org.apache.hadoop.fs.CosNSeekableFSDataOutputStream", e);
            } catch (InvocationTargetException e) {
                throw new IOException(e);
            } catch (InstantiationException e) {
                throw new IOException("Failed to create the object of the " +
                    "org.apache.hadoop.fs.CosNSeekableFSDataOutputStream", e);
            } catch (IllegalAccessException e) {
                throw new IOException("Failed to access the constructor of the " +
                    "org.apache.hadoop.fs.CosNSeekableFSDataOutputStream", e);
            }
        } else {
            return new FSDataOutputStream(new CosNExtendedFSDataOutputStream(
                this.getConf(), this.nativeStore, cosKey, this.boundedCopyThreadPool, this.boundedCopyThreadPool,true),
                statistics, fileStatus.getLen());
        }
    }

    @Override
    public boolean truncate(Path f, long newLength) throws IOException {
        // 默认可以支持 truncate
        if (this.isPosixBucket) {
            throw new UnsupportedOperationException(
                "The posix bucket does not support the truncate operation through S3 gateway.");
        }

        // 如果配置中开启客户端加密，则不支持
        if (this.getConf().getBoolean(CosNConfigKeys.COSN_CLIENT_SIDE_ENCRYPTION_ENABLED,
                CosNConfigKeys.DEFAULT_COSN_CLIENT_SIDE_ENCRYPTION_ENABLED)) {
            throw new UnsupportedOperationException("Not supported currently with client side encryption enabled");
        }

        // 如果原文件使用了客户端加密，则不支持
        try {
            if(this.getXAttr(f, CSE_ALGORITHM_USER_METADATA) != null) {
                throw new UnsupportedOperationException("Not supported currently because the file is encrypted by client side");
            }
        } catch (IOException e) {
            throw new IOException("Failed to check whether the file is encrypted.", e);
        }

        FileStatus fileStatus = this.getFileStatus(f);
        if (fileStatus.isSymlink()) {
            f = this.getLinkTarget(f);
            fileStatus = this.getFileStatus(f);
        }
        if (fileStatus.isDirectory()) {
            throw new FileNotFoundException(f + " is a directory.");
        }

        if (newLength < 0 || newLength > fileStatus.getLen()) {
            throw new HadoopIllegalArgumentException(
                    String.format("The new length [%d] of the truncate operation must be positive " +
                            "and less than the origin length.", newLength));
        }

        LOG.debug("Truncate the file path: {} to the new length: {}.", f, newLength);
        // Using the single thread to truncate
        Path absolutePath = makeAbsolute(f);
        String cosKey = pathToKey(absolutePath);

        // Use the single thread to truncate.
        try (OutputStream outputStream = new FSDataOutputStream(
                new CosNExtendedFSDataOutputStream(this.getConf(), this.nativeStore, cosKey,
                        this.boundedIOThreadPool, this.boundedCopyThreadPool), statistics)) {
            // If the newLength is equal to 0, just wait for 'try finally' to close.
            if (newLength > 0) {
                try (InputStream inputStream =
                             this.nativeStore.retrieveBlock(cosKey, 0, newLength - 1)) {
                    byte[] chunk = new byte[(int) (4 * Unit.KB)];
                    int readBytes = inputStream.read(chunk);
                    while (readBytes != -1) {
                        outputStream.write(chunk, 0, readBytes);
                        readBytes = inputStream.read(chunk);
                    }
                }
            }
        }

        return true;
    }

    @Override
    public FSDataOutputStream create(Path f, FsPermission permission,
                                     boolean overwrite,
                                     int bufferSize, short replication,
                                     long blockSize, Progressable progress)
            throws IOException {
        if (createOpCheckExistFile) {
            // preconditions
            try {
                FileStatus targetFileStatus = this.getFileStatus(f);
                if (targetFileStatus.isSymlink()) {
                    f = this.getLinkTarget(f);
                    // call the getFileStatus for the latest path again.
                    targetFileStatus = getFileStatus(f);
                }
                if (targetFileStatus.isFile() && !overwrite) {
                    throw new FileAlreadyExistsException("File already exists: " + f);
                }
                if (targetFileStatus.isDirectory()) {
                    throw new FileAlreadyExistsException("Directory already exists: " + f);
                }
            } catch (FileNotFoundException ignore) {
            // NOTE: 这里之前认为可能会出现从 COS 的 SDK 或者 API 上传了一个 / 结尾的有内容对象
            // 那么再在这个文件前缀下面成功创建新的对象而不报错的话，其实是不符合文件系统语义规范的。
            // 同时，也是为了保证一个完整的目录结构，但是确实会带来元数据查询请求的明显放大。
            // 不过这里，因为一般不会出现 / 结尾的内容对象，即使出现也不会覆盖丢失（因为这里相当于它的一个commonPrefix，原始对象还在COS里面）
            // 所以决定去掉这个检查，来改善优化性能。
            // validatePath(f)
            }
        }

        Path absolutePath = makeAbsolute(f);
        String key = pathToKey(absolutePath);
        if (this.getConf().getBoolean(CosNConfigKeys.COSN_FLUSH_ENABLED,
            CosNConfigKeys.DEFAULT_COSN_FLUSH_ENABLED)) {
            // Need to support the synchronous flush.
            return new FSDataOutputStream(
                    new CosNExtendedFSDataOutputStream(this.getConf(), nativeStore, key,
                            this.boundedIOThreadPool, this.boundedCopyThreadPool), statistics);
        } else {
            return new FSDataOutputStream(
                    new CosNFSDataOutputStream(this.getConf(), nativeStore, key,
                            this.boundedIOThreadPool), statistics);
        }
    }

    private boolean rejectRootDirectoryDelete(boolean isEmptyDir, boolean recursive)
            throws PathIOException {
        if (isEmptyDir) {
            return true;
        }
        if (recursive) {
            return false;
        } else {
            throw new PathIOException(this.bucket, "Can not delete root path");
        }
    }

    @Override
    public boolean delete(Path f, boolean recursive) throws IOException {
        FileStatus status;
        try {
            status = getFileStatus(f);
        } catch (FileNotFoundException e) {
            LOG.debug("Delete called for '{}', but the file does not exist and returning the false.", f);
            return false;
        }

        Path absolutePath = makeAbsolute(f);
        String key = pathToKey(absolutePath);
        if (key.compareToIgnoreCase("/") == 0) {
            FileStatus[] fileStatuses = listStatus(f);
            return this.rejectRootDirectoryDelete(fileStatuses.length == 0,
                    recursive);
        }

        if (status.isDirectory()) {
            if (!key.endsWith(PATH_DELIMITER)) {
                key += PATH_DELIMITER;
            }
            if (!recursive && listStatus(f).length > 0) {
                throw new IOException("Can not delete " + f
                        + " as is a not empty directory and recurse option is" +
                        " false");
            }
            // how to tell the result
            if (!isPosixBucket) {
                internalRecursiveDelete(key, BUCKET_LIST_LIMIT);
            } else {
                internalAutoRecursiveDelete(key);
            }
        } else {
            LOG.debug("Delete the cos key [{}].", key);
            this.nativeStore.delete(key);
        }

        if (!isPosixBucket) {
            createParentDirectoryIfNecessary(f);
        }
        return true;
    }

    private void internalRecursiveDelete(String key, int listMaxLength) throws IOException {
        CosNDeleteFileContext deleteFileContext = new CosNDeleteFileContext();
        int deleteToFinishes = 0;

        String priorLastKey = null;
        do {
            CosNPartialListing listing =
                    nativeStore.list(key, listMaxLength, priorLastKey, true);
            for (FileMetadata file : listing.getFiles()) {
                checkPermission(new Path(file.getKey()), RangerAccessType.DELETE);
                this.boundedCopyThreadPool.execute(new CosNDeleteFileTask(
                        this.nativeStore, file.getKey(), deleteFileContext));
                deleteToFinishes++;
                if (!deleteFileContext.isDeleteSuccess()) {
                    break;
                }
            }
            for (FileMetadata commonPrefix : listing.getCommonPrefixes()) {
                checkPermission(new Path(commonPrefix.getKey()), RangerAccessType.DELETE);
                this.boundedCopyThreadPool.execute(new CosNDeleteFileTask(
                        this.nativeStore, commonPrefix.getKey(), deleteFileContext));
                deleteToFinishes++;
                if (!deleteFileContext.isDeleteSuccess()) {
                    break;
                }
            }
            priorLastKey = listing.getPriorLastKey();
        } while (priorLastKey != null);

        deleteFileContext.lock();
        try {
            deleteFileContext.awaitAllFinish(deleteToFinishes);
        } catch (InterruptedException e) {
            LOG.warn("interrupted when wait delete to finish");
        } finally {
            deleteFileContext.unlock();
        }

        // according the flag and exception in thread opr to throw this out
        if (!deleteFileContext.isDeleteSuccess() && deleteFileContext.hasException()) {
            throw deleteFileContext.getIOException();
        }

        try {
            LOG.debug("Delete the cos key [{}].", key);
            nativeStore.delete(key);
        } catch (Exception e) {
            LOG.error("Delete the key failed.");
        }
    }

    // use by posix bucket which support recursive delete dirs by setting flag parameter
    private void internalAutoRecursiveDelete(String key) throws IOException {
        LOG.debug("Delete the cos key auto recursive [{}].", key);
        this.nativeStore.deleteRecursive(key);
    }

    public FileStatus getFileStatus(Path f) throws IOException {

        Path absolutePath = makeAbsolute(f);
        String key = pathToKey(absolutePath);

        if (key.isEmpty() || key.equals(PATH_DELIMITER)) { // root always exists
            return newDirectory(absolutePath);
        }

        CosNResultInfo getObjectMetadataResultInfo = new CosNResultInfo();
        FileMetadata meta = this.nativeStore.retrieveMetadata(key, getObjectMetadataResultInfo);
        if (meta != null) {
            if (meta.isFile()) {
                LOG.debug("Retrieve the cos key [{}] to find that it is a file.", key);
                return getFileStatusHelper(meta, absolutePath, key, getObjectMetadataResultInfo.getRequestID());
            } else {
                LOG.debug("Retrieve the cos key [{}] to find that it is a directory.", key);
                return newDirectory(meta, absolutePath);
            }
        }

        if (isPosixBucket) {
            throw new FileNotFoundException("No such file or directory in posix bucket'" + absolutePath + "'");
        }

        // 对于普通桶，可能存在一些以key为前缀的对象，这种情况下key也可以视为目录。
        FileStatus res = getFileStatusByList(key, absolutePath, getObjectMetadataResultInfo.getRequestID());
        if (res != null) {
            return res;
        }

        throw new FileNotFoundException("No such file or directory '" + absolutePath + "'");
    }

    /**
     * 获取FileStatus
     * 如果directoryFirstEnabled为true且存在同名空文件和目录时，返回目录。
     * 否则返回文件。
     */
    private FileStatus getFileStatusHelper(FileMetadata meta, Path absolutePath, String key,
        String headRequestId) throws IOException {
        if (directoryFirstEnabled && meta.getLength() == 0) {

            if (!key.endsWith(PATH_DELIMITER)) {
                key += PATH_DELIMITER;
            }
            FileMetadata directoryMeta = nativeStore.queryObjectMetadata(key);
            if (directoryMeta != null) {
                return newDirectory(directoryMeta, absolutePath);
            }

            FileStatus res = getFileStatusByList(key, absolutePath, headRequestId);
            if (res != null) {
                return res;
            }
        }
        return newFile(meta, absolutePath);
    }

    /**
     * 使用list接口查询目录
     * @return null 如果目录不存在
     */
    private FileStatus getFileStatusByList(String key, Path absolutePath, String headRequestId) throws IOException {
        if (this.getConf().getBoolean(CosNConfigKeys.COSN_FILESTATUS_LIST_OP_ENABLED,
            CosNConfigKeys.DEFAULT_FILESTATUS_LIST_OP_ENABLED)) {
            if (!key.endsWith(PATH_DELIMITER)) {
                key += PATH_DELIMITER;
            }

            int maxKeys = this.getConf().getInt(
                CosNConfigKeys.FILESTATUS_LIST_MAX_KEYS,
                CosNConfigKeys.DEFAULT_FILESTATUS_LIST_MAX_KEYS);

            LOG.debug("List the cos key [{}] to judge whether it is a directory or not. max keys [{}]", key, maxKeys);
            CosNResultInfo listObjectsResultInfo = new CosNResultInfo();
            CosNPartialListing listing = this.nativeStore.list(key, maxKeys, null,
                    this.getConf().getBoolean(CosNConfigKeys.COSN_FILESTATUS_LIST_RECURSIVE_ENABLED, CosNConfigKeys.DEFAULT_FILESTATUS_LIST_RECURSIVE_ENABLED),
                    listObjectsResultInfo);
            if (listing.getFiles().length > 0 || listing.getCommonPrefixes().length > 0) {
                LOG.debug("List the cos key [{}] to find that it is a directory.", key);
                return newDirectory(absolutePath);
            }

            if (listObjectsResultInfo.isKeySameToPrefix()) {
                LOG.info("List the cos key [{}] same to prefix, head-id:[{}], " +
                        "list-id:[{}], list-type:[{}], thread-id:[{}], thread-name:[{}]", key, headRequestId,
                    listObjectsResultInfo.getRequestID(), listObjectsResultInfo.isKeySameToPrefix(),
                    Thread.currentThread().getId(), Thread.currentThread().getName());
            }
            LOG.debug("Can not find the cos key [{}] on COS.", key);
        }
        return null;
    }

    @Override
    public FileStatus[] listStatus(Path f) throws IOException {
        Path absolutePath = makeAbsolute(f);
        String key = pathToKey(absolutePath);

        int listMaxLength = CosNFileSystem.BUCKET_LIST_LIMIT;
        if (isPosixBucket) {
            listMaxLength = CosNFileSystem.POSIX_BUCKET_LIST_LIMIT;
        }

        if (key.length() > 0) {
            FileStatus fileStatus = this.getFileStatus(f);
            if (fileStatus.isFile() || fileStatus.isSymlink()) {
                return new FileStatus[]{fileStatus};
            }
        }

        if (!key.endsWith(PATH_DELIMITER)) {
            key += PATH_DELIMITER;
        }

        // if introduce the get file status to solve the file not exist exception
        // will expand the list qps, for now only used get file status for posix bucket.
        if (this.isPosixBucket) {
            try {
                this.getFileStatus(f);
            } catch (FileNotFoundException e) {
                throw new FileNotFoundException("No such file or directory:" + f);
            }
        }

        URI pathUri = absolutePath.toUri();
        Set<FileStatus> status = new TreeSet<>();
        String priorLastKey = null;
        do {
            CosNPartialListing listing = this.nativeStore.list(key, listMaxLength,
                    priorLastKey, false);
            for (FileMetadata fileMetadata : listing.getFiles()) {
                Path subPath = keyToPath(fileMetadata.getKey());
                if (fileMetadata.getKey().equals(key)) {
                    // this is just the directory we have been asked to list
                    LOG.debug("This is just the directory we have been asked to list. cos key: {}.",
                            fileMetadata.getKey());
                } else {
                    if (this.supportsSymlinks() && fileMetadata.getLength() < this.symbolicLinkSizeThreshold) {
                        CosNSymlinkMetadata cosNSymlinkMetadata = this.nativeStore.retrieveSymlinkMetadata(
                                fileMetadata.getKey());
                        if (null != cosNSymlinkMetadata) {
                            // 这里 CGI 的 GetSymlink 接口返回的 contentLength 是0，但是 List 接口又返回的是正确的值，
                            // 因此这里还是取 List 接口返回的软连接大小
                            cosNSymlinkMetadata.setLength(fileMetadata.getLength());
                            status.add(newSymlink(cosNSymlinkMetadata, subPath));
                            continue;
                        }
                    }
                    status.add(newFile(fileMetadata, subPath));
                }
            }
            for (FileMetadata commonPrefix : listing.getCommonPrefixes()) {
                Path subPath = keyToPath(commonPrefix.getKey());
                String relativePath =
                        pathUri.relativize(subPath.toUri()).getPath();
                FileStatus directory = newDirectory(commonPrefix, new Path(absolutePath, relativePath));
                if (this.directoryFirstEnabled) {
                    // 如果开启目录优先，需要先调用remove删除可能存在的同名文件，避免目录无法add
                    status.remove(directory);
                }
                status.add(directory);
            }
            priorLastKey = listing.getPriorLastKey();
        } while (priorLastKey != null);

        return status.toArray(new FileStatus[status.size()]);
    }

    private FileStatus newFile(FileMetadata meta, Path path) {
        return new CosNFileStatus(meta.getLength(), false, 1, getDefaultBlockSize(),
                meta.getLastModified(), 0, null, this.owner, this.group,
                path.makeQualified(this.getUri(), this.getWorkingDirectory()), meta.getETag(), meta.getCrc64ecm(),
                meta.getVersionId());
    }

    private FileStatus newDirectory(Path path) {
        return new CosNFileStatus(0, true, 1, 0, 0, 0, null, this.owner, this.group,
                path.makeQualified(this.getUri(), this.getWorkingDirectory()));
    }

    private FileStatus newSymlink(CosNSymlinkMetadata metadata, Path path) {
        FileStatus symlinkStatus = newFile(metadata, path);
        Path targetPath = keyToPath(metadata.getTarget());
        symlinkStatus.setSymlink(makeAbsolute(targetPath).makeQualified(this.getUri(), this.getWorkingDirectory()));
        return symlinkStatus;
    }

    private FileStatus newDirectory(FileMetadata meta, Path path) {
        if (meta == null) {
            return newDirectory(path);
        }
        return new CosNFileStatus(0, true, 1, 0,
                meta.getLastModified(), 0, null, this.owner, this.group,
                path.makeQualified(this.getUri(), this.getWorkingDirectory()), meta.getETag(), meta.getCrc64ecm(),
                meta.getVersionId());
    }

    /**
     * Validate the path from the bottom up.
     *
     * @param path the absolute path to check.
     * @throws IOException an IOException occurred when getting the path's metadata.
     */
    private void validatePath(Path path) throws IOException {
        Path parent = path.getParent();
        do {
            try {
                FileStatus fileStatus = getFileStatus(parent);
                if (fileStatus.isDirectory()) {
                    break;
                } else {
                    throw new FileAlreadyExistsException(String.format(
                            "Can't make directory for path '%s', it is a file" +
                                    ".", parent));
                }
            } catch (FileNotFoundException ignored) {
            }
            parent = parent.getParent();
        } while (parent != null);
    }

    // blew is the target
    @Override
    public boolean mkdirs(Path f, FsPermission permission) throws IOException {
        try {
            FileStatus fileStatus = getFileStatus(f);
            if (fileStatus.isDirectory()) {
                return true;
            } else {
                throw new FileAlreadyExistsException("Path is a file: " + f);
            }
        } catch (FileNotFoundException e) {
            validatePath(f);
            boolean result;
            if (isPosixBucket) {
                result = mkDirAutoRecursively(f, permission);
            } else {
                result = mkDirRecursively(f, permission);
            }
            return result;
        }
    }

    /**
     * Create a directory recursively.
     *
     * @param f          Absolute path to the directory
     * @param permission Directory permissions
     * @return Return true if the creation was successful,  throw a IOException.
     * @throws IOException An IOException occurred when creating a directory object on COS.
     */
    public boolean mkDirRecursively(Path f, FsPermission permission)
            throws IOException {
        LOG.debug("Make the directory recursively. Path: {}, FsPermission: {}.", f, permission);
        Path absolutePath = makeAbsolute(f);
        List<Path> paths = new ArrayList<Path>();
        do {
            paths.add(absolutePath);
            absolutePath = absolutePath.getParent();
        } while (absolutePath != null);

        for (Path path : paths) {
            if (path.isRoot()) {
                break;
            }
            try {
                FileStatus fileStatus = getFileStatus(path);
                if (fileStatus.isFile()) {
                    throw new FileAlreadyExistsException(
                            String.format(
                                    "Can't make directory for path '%s' since" +
                                            " it is a file.", f));
                }
                if (fileStatus.isDirectory()) {
                    if (fileStatus.getModificationTime() > 0) {
                        break;
                    } else {
                        throw new FileNotFoundException("Dir '" + path + "' doesn't exist in COS");
                    }
                }
            } catch (FileNotFoundException e) {
                LOG.debug("Make the directory [{}] on COS.", path);
                String folderPath = pathToKey(makeAbsolute(path));
                if (!folderPath.endsWith(PATH_DELIMITER)) {
                    folderPath += PATH_DELIMITER;
                }
                nativeStore.storeEmptyFile(folderPath);
            }
        }
        return true;
    }

    /**
     * Create a directory recursively auto by posix plan
     * which the put object interface decide the dir which end with the "/"
     *
     * @param f          Absolute path to the directory
     * @param permission Directory permissions
     * @return Return true if the creation was successful,  throw a IOException.
     * @throws IOException An IOException occurred when creating a directory object on COS.
     */
    public boolean mkDirAutoRecursively(Path f, FsPermission permission)
            throws IOException {
        LOG.debug("Make the directory recursively auto. Path: {}, FsPermission: {}.", f, permission);
        // add the end '/' to the path which server auto create middle dir
        String folderPath = pathToKey(makeAbsolute(f));
        if (!folderPath.endsWith(PATH_DELIMITER)) {
            folderPath += PATH_DELIMITER;
        }
        nativeStore.storeEmptyFile(folderPath);
        return true;
    }

    @Override
    public FSDataInputStream open(Path f, int bufferSize) throws IOException {
        FileStatus fileStatus = getFileStatus(f); // will throw if the file doesn't
        if (fileStatus.isSymlink()) {
            f = this.getLinkTarget(f);
            fileStatus = getFileStatus(f);
        }

        // exist
        if (fileStatus.isDirectory()) {
            throw new FileNotFoundException("'" + f + "' is a directory");
        }
        LOG.debug("Opening '" + f + "' for reading");
        Path absolutePath = makeAbsolute(f);
        String key = pathToKey(absolutePath);
        return new FSDataInputStream(new BufferedFSInputStream(
                new CosNFSInputStream(this.getConf(), nativeStore, statistics, key,
                        fileStatus.getLen(), this.boundedIOThreadPool),
                bufferSize));
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        // Renaming the root directory is not allowed
        if (src.isRoot()) {
            LOG.debug("Cannot rename the root directory of a filesystem.");
            return false;
        }

        // check the source path whether exists or not, if not return false.
        FileStatus srcFileStatus;
        try {
            srcFileStatus = this.getFileStatus(src);
        } catch (FileNotFoundException e) {
            LOG.debug("The source path [{}] is not exist.", src);
            return false;
        }

        // Source path and destination path are not allowed to be the same
        if (src.equals(dst)) {
            LOG.debug("The source path and the dest path refer to the same file or " +
                    "directory: {}", dst);
            throw new IOException("the source path and dest path refer to the " +
                    "same file or directory");
        }

        // It is not allowed to rename a parent directory to its subdirectory
        Path dstParentPath;
        dstParentPath = dst.getParent();
        while (null != dstParentPath && !src.equals(dstParentPath)) {
            dstParentPath = dstParentPath.getParent();
        }
        if (null != dstParentPath) {
            LOG.debug("It is not allowed to rename a parent directory:{} to " +
                    "its subdirectory:{}.", src, dst);
            throw new PathIOException(String.format(
                    "It is not allowed to rename a parent directory:%s to its" +
                            " subdirectory:%s",
                    src, dst));
        }

        FileStatus dstFileStatus = null;
        try {
            dstFileStatus = this.getFileStatus(dst);

            // The destination path exists and is a file,
            // and the rename operation is not allowed.
            //
            if (dstFileStatus.isFile()) {
                LOG.debug("File: {} already exists.", dstFileStatus.getPath());
                return false;
            } else {
                // The destination path is an existing directory,
                // and it is checked whether there is a file or directory
                // with the same name as the source path under the
                // destination path
                dst = new Path(dst, src.getName());
                FileStatus[] statuses;
                try {
                    statuses = this.listStatus(dst);
                } catch (FileNotFoundException e) {
                    statuses = null;
                }
                if (null != statuses && statuses.length > 0) {
                    LOG.debug("Cannot rename {} to {}, the destination directory is non-empty.",
                            src, dst);
                    return false;
                }
            }
        } catch (FileNotFoundException e) {
            // destination path not exists
            Path tempDstParentPath = dst.getParent();
            FileStatus dstParentStatus = this.getFileStatus(tempDstParentPath);
            if (!dstParentStatus.isDirectory()) {
                throw new IOException(String.format(
                        "Cannot rename %s to %s, %s is a file", src, dst, dst.getParent()
                ));
            }
            // The default root directory is definitely there.
        }

        if (!isPosixBucket) {
            return internalCopyAndDelete(src, dst, srcFileStatus.isDirectory(),
                srcFileStatus.isSymlink());
        } else {
            return internalRename(src, dst);
        }
    }

    private boolean internalCopyAndDelete(Path srcPath, Path dstPath,
                                          boolean isDir, boolean isSymlink) throws IOException {
        boolean result = false;
        if (isDir) {
            result = this.copyDirectory(srcPath, dstPath);
        } else {
            if (isSymlink) {
                result = this.copySymlink(srcPath, dstPath);
            } else {
                result = this.copyFile(srcPath, dstPath);
            }
        }

        //Considering the renaming operation is a non-atomic operation,
        // it is not allowed to delete the data of the original path to
        // ensure data security if failed.
        return result ? this.delete(srcPath, true) : false;
    }

    private boolean internalRename(Path srcPath, Path dstPath) throws IOException {
        String srcKey = pathToKey(srcPath);
        String dstKey = pathToKey(dstPath);
        this.nativeStore.rename(srcKey, dstKey);
        return true;
    }

    private boolean copyFile(Path srcPath, Path dstPath) throws IOException {
        String srcKey = pathToKey(srcPath);
        String dstKey = pathToKey(dstPath);
        this.nativeStore.copy(srcKey, dstKey);
        return true;
    }

    private boolean copySymlink(Path srcSymlink, Path dstSymlink) throws IOException {
        Path targetFilePath = this.getLinkTarget(srcSymlink);
        this.createSymlink(targetFilePath, dstSymlink, false);
        return true;
    }

    private boolean copyDirectory(Path srcPath, Path dstPath) throws IOException {
        String srcKey = pathToKey(srcPath);
        if (!srcKey.endsWith(PATH_DELIMITER)) {
            srcKey += PATH_DELIMITER;
        }
        String dstKey = pathToKey(dstPath);
        if (!dstKey.endsWith(PATH_DELIMITER)) {
            dstKey += PATH_DELIMITER;
        }

        if (dstKey.startsWith(srcKey)) {
            throw new IOException("can not copy a directory to a subdirectory" +
                    " of self");
        }
        // 这个方法是普通桶调用，普通桶严格区分文件对象和目录对象，这里srcKey是带后缀的，如果使用retrieveMetadata
        // 可能会吞掉目录对象不存在的问题。导致后面的copy srcKey时，报404错误。
        if (this.nativeStore.queryObjectMetadata(srcKey) == null) {
            this.nativeStore.storeEmptyFile(srcKey);
        } else {
            this.nativeStore.copy(srcKey, dstKey);
        }

        CosNCopyFileContext copyFileContext = new CosNCopyFileContext();

        int copiesToFinishes = 0;
        String priorLastKey = null;
        do {
            CosNPartialListing objectList = this.nativeStore.list(srcKey,
                    BUCKET_LIST_LIMIT, priorLastKey, true);
            for (FileMetadata file : objectList.getFiles()) {
                checkPermission(new Path(file.getKey()), RangerAccessType.DELETE);
                this.boundedCopyThreadPool.execute(new CosNCopyFileTask(
                        this.nativeStore,
                        file.getKey(),
                        dstKey.concat(file.getKey().substring(srcKey.length())),
                        copyFileContext));
                copiesToFinishes++;
                if (!copyFileContext.isCopySuccess()) {
                    break;
                }
            }
            priorLastKey = objectList.getPriorLastKey();
        } while (null != priorLastKey);

        copyFileContext.lock();
        try {
            copyFileContext.awaitAllFinish(copiesToFinishes);
        } catch (InterruptedException e) {
            LOG.warn("interrupted when wait copies to finish");
        } finally {
            copyFileContext.unlock();
        }
        return copyFileContext.isCopySuccess();
    }

    private void createParentDirectoryIfNecessary(Path path) throws IOException {
        Path parent = path.getParent();
        if (null != parent && !parent.isRoot()) {
            String parentKey = pathToKey(parent);
            if (!StringUtils.isNullOrEmpty(parentKey) && !exists(parent)) {
                LOG.debug("Create a parent directory [{}] for the path [{}].", parent, path);
                if (!parentKey.endsWith("/")) {
                    parentKey += "/";
                }
                nativeStore.storeEmptyFile(parentKey);
            }
        }
    }

    @Override
    public long getDefaultBlockSize() {
        return getConf().getLong(
                CosNConfigKeys.COSN_BLOCK_SIZE_KEY,
                CosNConfigKeys.DEFAULT_BLOCK_SIZE);
    }

    /**
     * Set the working directory to the given directory.
     */
    @Override
    public void setWorkingDirectory(Path newDir) {
        workingDir = newDir;
    }

    @Override
    public Path getWorkingDirectory() {
        return workingDir;
    }

    @Override
    public FileChecksum getFileChecksum(Path f, long length) throws IOException {
        Preconditions.checkArgument(length >= 0);

        if (this.getFileStatus(f).isSymlink()) {
            f = this.getLinkTarget(f);
        }

        if (this.getConf().getBoolean(CosNConfigKeys.CRC64_CHECKSUM_ENABLED,
                CosNConfigKeys.DEFAULT_CRC64_CHECKSUM_ENABLED)) {
            Path absolutePath = makeAbsolute(f);
            String key = pathToKey(absolutePath);

            FileMetadata fileMetadata = this.nativeStore.retrieveMetadata(key);
            if (null == fileMetadata) {
                throw new FileNotFoundException("File or directory doesn't exist: " + f);
            }
            String crc64ecm = fileMetadata.getCrc64ecm();
            return crc64ecm != null ? new CRC64Checksum(crc64ecm) : super.getFileChecksum(f, length);
        } else if (this.getConf().getBoolean(CosNConfigKeys.CRC32C_CHECKSUM_ENABLED,
                CosNConfigKeys.DEFAULT_CRC32C_CHECKSUM_ENABLED)) {
            Path absolutePath = makeAbsolute(f);
            String key = pathToKey(absolutePath);
            FileMetadata fileMetadata = this.nativeStore.retrieveMetadata(key);
            if (null == fileMetadata) {
                throw new FileNotFoundException("File or directory doesn't exist: " + f);
            }
            String crc32cm = fileMetadata.getCrc32cm();
            return crc32cm != null ? new CRC32CCheckSum(crc32cm) : super.getFileChecksum(f, length);
        } else {
            // disabled
            return super.getFileChecksum(f, length);
        }
    }


    /**
     * Set the value of an attribute for a path
     *
     * @param f     The path on which to set the attribute
     * @param name  The attribute to set
     * @param value The byte value of the attribute to set (encoded in utf-8)
     * @param flag  The mode in which to set the attribute
     * @throws IOException If there was an issue setting the attributing on COS
     */
    @Override
    public void setXAttr(Path f, String name, byte[] value, EnumSet<XAttrSetFlag> flag) throws IOException {
        // First, determine whether the length of the name and value exceeds the limit.
        if (name.getBytes(METADATA_ENCODING).length + value.length > MAX_XATTR_SIZE) {
            throw new HadoopIllegalArgumentException(String.format("The maximum combined size of " +
                            "the name and value of an extended attribute in bytes should be less than or equal to %d",
                    MAX_XATTR_SIZE));
        }

        if (this.getFileStatus(f).isSymlink()) {
            f = this.getLinkTarget(f);
        }

        Path absolutePath = makeAbsolute(f);
        String key = pathToKey(absolutePath);
        FileMetadata fileMetadata = nativeStore.retrieveMetadata(key);
        if (null == fileMetadata) {
            throw new FileNotFoundException("File or directory doesn't exist: " + f);
        }
        boolean xAttrExists = (null != fileMetadata.getUserAttributes()
                && fileMetadata.getUserAttributes().containsKey(name));
        XAttrSetFlag.validate(name, xAttrExists, flag);
        if (fileMetadata.isFile()) {
            nativeStore.storeFileAttribute(key, name, value);
        } else {
            nativeStore.storeDirAttribute(key, name, value);
        }
    }

    /**
     * get the value of an attribute for a path
     *
     * @param f    The path on which to set the attribute
     * @param name The attribute to set
     * @return The byte value of the attribute to set (encoded in utf-8)
     * @throws IOException If there was an issue setting the attribute on COS
     */
    @Override
    public byte[] getXAttr(Path f, String name) throws IOException {
        if (this.getFileStatus(f).isSymlink()) {
            f = this.getLinkTarget(f);
        }

        Path absolutePath = makeAbsolute(f);
        String key = pathToKey(absolutePath);
        FileMetadata fileMetadata = nativeStore.retrieveMetadata(key);
        if (null == fileMetadata) {
            throw new FileNotFoundException("File or directory doesn't exist: " + f);
        }

        if (null != fileMetadata.getUserAttributes()) {
            return fileMetadata.getUserAttributes().get(name);
        }

        return null;
    }

    /**
     * Get all of the xattrs name/value pairs for a cosn file or directory.
     *
     * @param f     Path to get extended attributes
     * @param names XAttr names.
     * @return Map describing the XAttrs of the file or directory
     * @throws IOException If there was an issue gettting the attribute on COS
     */
    @Override
    public Map<String, byte[]> getXAttrs(Path f, List<String> names) throws IOException {
        if (this.getFileStatus(f).isSymlink()) {
            f = this.getLinkTarget(f);
        }

        Path absolutePath = makeAbsolute(f);
        String key = pathToKey(absolutePath);
        FileMetadata fileMetadata = nativeStore.retrieveMetadata(key);
        if (null == fileMetadata) {
            throw new FileNotFoundException("File or directory doesn't exist: " + f);
        }

        Map<String, byte[]> attrs = null;
        if (null != fileMetadata.getUserAttributes()) {
            attrs = new HashMap<>();
            for (String name : names) {
                if (fileMetadata.getUserAttributes().containsKey(name)) {
                    attrs.put(name, fileMetadata.getUserAttributes().get(name));
                }
            }
        }

        return attrs;
    }

    @Override
    public Map<String, byte[]> getXAttrs(Path f) throws IOException {
        if (this.getFileStatus(f).isSymlink()) {
            f = this.getLinkTarget(f);
        }

        Path absolutePath = makeAbsolute(f);
        String key = pathToKey(absolutePath);
        FileMetadata fileMetadata = nativeStore.retrieveMetadata(key);
        if (null == fileMetadata) {
            throw new FileNotFoundException("File or directory doesn't exist: " + f);
        }

        return fileMetadata.getUserAttributes();
    }

    /**
     * Removes an xattr of a cosn file or directory.
     *
     * @param f    Path to remove extended attribute
     * @param name xattr name
     * @throws IOException If there was an issue setting the attribute on COS
     */
    @Override
    public void removeXAttr(Path f, String name) throws IOException {
        if (this.getFileStatus(f).isSymlink()) {
            f = this.getLinkTarget(f);
        }

        Path absolutPath = makeAbsolute(f);
        String key = pathToKey(absolutPath);
        FileMetadata fileMetadata = nativeStore.retrieveMetadata(key);
        if (null == fileMetadata) {
            throw new FileNotFoundException("File or directory doesn't exist: " + f);
        }

        boolean xAttrExists = (null != fileMetadata.getUserAttributes()
                && fileMetadata.getUserAttributes().containsKey(name));
        if (xAttrExists) {
            if (fileMetadata.isFile()) {
                nativeStore.removeFileAttribute(key, name);
            } else {
                nativeStore.removeDirAttribute(key, name);
            }
        }

        // Nothing to do if the specified attribute is not found.
    }

    @Override
    public List<String> listXAttrs(Path f) throws IOException {
        if (this.getFileStatus(f).isSymlink()) {
            f = this.getLinkTarget(f);
        }

        Path absolutePath = makeAbsolute(f);
        String key = pathToKey(absolutePath);
        FileMetadata fileMetadata = nativeStore.retrieveMetadata(key);
        if (null == fileMetadata) {
            throw new FileNotFoundException("File or directory doesn't exist: " + f);
        }

        return new ArrayList<>(fileMetadata.getUserAttributes().keySet());
    }

    @Override
    public void createSymlink(Path target, Path link, boolean createParent)
            throws AccessControlException, FileAlreadyExistsException, FileNotFoundException,
            ParentNotDirectoryException, UnsupportedFileSystemException, IOException {
        if (!this.supportsSymlinks()) {
            super.createSymlink(target, link, createParent);
        }

        try {
          FileStatus parentStatus = this.getFileStatus(link.getParent());
          if (!parentStatus.isDirectory()) {
            throw new ParentNotDirectoryException(
                String.format("The parent path of the symlink [%s] is not a directory.", link));
          }
        } catch (FileNotFoundException parentDirNotFoundException) {
          if (createParent) {
            LOG.debug("The parent directory of the symlink [{}] does not exist, " +
                "creating it.", link.getParent());
            if (!this.mkdirs(link.getParent())) {
              throw new IOException(String.format(
                  "Failed to create the parent directory of the symlink [%s].", link));
            }
          } else {
            throw parentDirNotFoundException;
          }
        }

        try {
            FileStatus fileStatus = this.getFileStatus(link);
            if (null != fileStatus && (fileStatus.isFile() || fileStatus.isSymlink())) {
                throw new FileAlreadyExistsException("File already exists: " + link);
            }
            if (null != fileStatus && fileStatus.isDirectory()) {
                throw new FileAlreadyExistsException("Directory already exists: " + link);
            }
        } catch (FileNotFoundException ignore) {
        }

        Path targetAbsolutePath = makeAbsolute(target);
        Path linkAbsolutePath = makeAbsolute(link);
        String targetKey = pathToKey(targetAbsolutePath);
        String linkKey = pathToKey(linkAbsolutePath);

        this.nativeStore.createSymlink(linkKey, targetKey);
    }

    /**
     * Return a file status object that represents the path. If the path refers to a symlink then the FileStatus of the symlink is returned.
     * The behavior is equivalent to #getFileStatus() if the underlying file system does not support symbolic links.
     *
     * @param f The file or symlink path to be obtained.
     * @return the filestatus for symbolic links or files themselves.
     * @throws AccessControlException
     * @throws FileNotFoundException
     * @throws UnsupportedFileSystemException
     * @throws IOException
     */
    @Override
    public FileStatus getFileLinkStatus(final Path f)
            throws AccessControlException, FileNotFoundException,
            UnsupportedFileSystemException, IOException {
        if (!this.supportsSymlinks()) {
            return super.getFileLinkStatus(f);
        }

        // 这里会得到这个文件的元数据，如果不存在，则软连接也不存在，就直接返回了
        FileStatus fileStatus = this.getFileStatus(f);
        if (fileStatus.isSymlink()) {
            // 如果是软连接则需要取出软连接本身的元数据出来
            Path absolutePath = makeAbsolute(f);
            String symlinkKey = pathToKey(absolutePath);
            CosNSymlinkMetadata cosNSymlinkMetadata = this.nativeStore.retrieveSymlinkMetadata(symlinkKey);
            if (null != cosNSymlinkMetadata) {
                fileStatus = newSymlink(cosNSymlinkMetadata, absolutePath);
            } else {
                throw new FileNotFoundException("Symbolic does not exist: " + f);
            }

            // 这里设置为规范化的 target 路径
            if (fileStatus.isSymlink()) {
                Path targetQual = FSLinkResolver.qualifySymlinkTarget(uri,
                        fileStatus.getPath(), fileStatus.getSymlink());
                fileStatus.setSymlink(targetQual);
            }
        }

        return fileStatus;
    }

    @Override
    public boolean supportsSymlinks() {
      return this.getConf().getBoolean(
          CosNConfigKeys.COSN_SUPPORT_SYMLINK_ENABLED,
          CosNConfigKeys.DEFAULT_COSN_SUPPORT_SYMLINK_ENABLED);
    }

    /**
     * Returns the target of the given symbolic link as it was specified when the link was created.
     * Links in the path leading up to the final path component are resolved transparently.
     *
     * @param f
     * @return
     * @throws IOException
     */
    @Override
    public Path getLinkTarget(final Path f) throws IOException {
        if (!this.supportsSymlinks()) {
            return super.getLinkTarget(f);
        }

        Path absolutePath = makeAbsolute(f);
        return new FileSystemLinkResolver<Path>() {
            @Override
            public Path doCall(Path path) throws IOException, UnresolvedLinkException {
                Path targetPath = CosNFileSystem.this.resolveLink(path);
                try {
                    FileStatus targetFileStatus = getFileStatus(targetPath);
                    if (targetFileStatus.isSymlink()) {
                        throw new UnresolvedLinkException();
                    }
                } catch (FileNotFoundException ignored) {
                }
                return targetPath;
            }

            @Override
            public Path next(FileSystem fileSystem, Path path) throws IOException {
                return fileSystem.getLinkTarget(path);
            }
        }.resolve(this, absolutePath);
    }

    @Override
    protected Path resolveLink(Path f) throws IOException {
        if (!this.supportsSymlinks()) {
            return super.resolveLink(f);
        }

        Path absolutePath = makeAbsolute(f);
        String symlinkKey = pathToKey(absolutePath);
        String targetKey = this.nativeStore.getSymlink(symlinkKey);
        if (null == targetKey) {
            throw new FileNotFoundException("Symbolic does not exist: " + f);
        }

        Path targetPath = keyToPath(targetKey);
        return this.makeAbsolute(targetPath).makeQualified(this.getUri(), this.getWorkingDirectory());
    }

    @Override
    public void close() throws IOException {
        try {
            super.close();
        } finally {
            this.boundedIOThreadPool.shutdown();
            this.boundedCopyThreadPool.shutdown();
            BufferPool.getInstance().close();
            if (null != this.nativeStore && this.isDefaultNativeStore) {
                this.nativeStore.close();
            }
        }
    }

    public NativeFileSystemStore getStore() {
        return this.nativeStore;
    }

    // protected and private methods.
    private String getOwnerId() {
        UserGroupInformation currentUgi;
        try {
            currentUgi = UserGroupInformation.getCurrentUser();
        } catch (IOException e) {
            LOG.warn("get current user failed! use user.name prop", e);
            return System.getProperty("user.name");
        }

        String shortUserName = "";
        if (currentUgi != null) {
            shortUserName = currentUgi.getShortUserName();
        }

        if (shortUserName == null) {
            LOG.warn("get short user name failed! use user.name prop");
            shortUserName = System.getProperty("user.name");
        }
        return shortUserName;
    }

    private String getGroupId() {
        UserGroupInformation currentUgi = null;
        try {
            currentUgi = UserGroupInformation.getCurrentUser();
        } catch (IOException e) {
            LOG.warn("get current user failed! use user.name prop", e);
            return System.getProperty("user.name");
        }
        if (currentUgi != null) {
            String[] groupNames = currentUgi.getGroupNames();
            if (groupNames.length != 0) {
                return groupNames[0];
            } else {
                return getOwnerId();
            }
        }
        return System.getProperty("user.name");
    }

    private String getOwnerInfo(boolean getOwnerId) {
        String ownerInfoId = "";
        try {
            String userName = System.getProperty("user.name");
            String command = "id -u " + userName;
            if (!getOwnerId) {
                command = "id -g " + userName;
            }
            Process child = Runtime.getRuntime().exec(command);
            child.waitFor();

            // Get the input stream and read from it
            InputStream in = child.getInputStream();
            StringBuilder strBuffer = new StringBuilder();
            int c;
            while ((c = in.read()) != -1) {
                strBuffer.append((char) c);
            }
            in.close();
            ownerInfoId = strBuffer.toString();
        } catch (InterruptedException | IOException e) {
            LOG.error("getOwnerInfo occur a exception", e);
        }
        return ownerInfoId;
    }

    private void checkPermission(Path f, RangerAccessType rangerAccessType) throws IOException {
        // hadoop cos file system contains the constructor import native client outside
        // in case of null pointer by this native client not init ranger client.
        if (this.rangerCredentialsClient == null) {
            return;
        }
        this.rangerCredentialsClient.doCheckPermission(f, rangerAccessType, getOwnerId(), getWorkingDirectory());
    }

    private Path makeAbsolute(Path path) {
        if (path.isAbsolute()) {
            return path;
        }
        return new Path(workingDir, path);
    }

    /**
     * 将一个 hadoop 的 path 路径转换为 COS 的 key 路径。
     * 由于每个方法都会调用这个方法，因此这里调用了 checkPath 检查传入路径是否归属于当前的文件系统 URI。
     *
     * @param path hadoop 的 path 路径，原则上应是一个绝对路径。
     * @return 传入的 hadoop 的 path 对应的 COS 的 key 路径。
     */
    public String pathToKey(Path path) {
        super.checkPath(path);
        return CosNUtils.pathToKey(path);
    }

    public Path keyToPath(String key) {
        return CosNUtils.keyToPath(key, PATH_DELIMITER);
    }
}

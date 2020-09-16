package org.apache.hadoop.fs;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.qcloud.cos.utils.StringUtils;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryProxy;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;

/**
 * A {@link FileSystem} for reading and writing files stored on
 * <a href="https://www.qcloud.com/product/cos.html">Tencent Qcloud Cos</a>
 * . Unlike
 * {@link CosFileSystem} this implementation stores files on COS in their
 * native form so they can be read by other cos tools.
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public class CosFileSystem extends FileSystem {
    static final Logger LOG = LoggerFactory.getLogger(CosFileSystem.class);

    static final String SCHEME = "cosn";
    static final String PATH_DELIMITER = Path.SEPARATOR;
    static final int COS_MAX_LISTING_LENGTH = 999;

    static final Charset METADATA_ENCODING = StandardCharsets.UTF_8;
    // The length of name:value pair should be less than or equal to 1024 bytes.
    static final int MAX_XATTR_SIZE = 1024;

    private URI uri;
    String bucket;
    private NativeFileSystemStore store;
    private Path workingDir;
    private String owner = "Unknown";
    private String group = "Unknown";

    private ExecutorService boundedIOThreadPool;
    private ExecutorService boundedCopyThreadPool;

    public CosFileSystem() {
    }

    public CosFileSystem(NativeFileSystemStore store) {
        this.store = store;
    }

    /**
     * Return the protocol scheme for the FileSystem.
     *
     * @return <code>cosn</code>
     */
    @Override
    public String getScheme() {
        return CosFileSystem.SCHEME;
    }

    @Override
    public void initialize(URI uri, Configuration conf) throws IOException {
        super.initialize(uri, conf);
        this.bucket = uri.getHost();
        if (this.store == null) {
            this.store = createDefaultStore(conf);
        }
        this.store.initialize(uri, conf);
        setConf(conf);
        this.uri = URI.create(uri.getScheme() + "://" + uri.getAuthority());
        this.workingDir =
                new Path("/user", System.getProperty("user.name"))
                        .makeQualified(this.uri, this.getWorkingDirectory());
        this.owner = getOwnerId();
        this.group = getGroupId();
        LOG.debug("uri: {}, bucket: {}, working dir: {}, owner: {}, group: {}.\n" +
                        "configuration: {}.",
                uri, bucket, workingDir, owner, group, conf);
        BufferPool.getInstance().initialize(getConf());

        // initialize the thread pool
        int uploadThreadPoolSize = this.getConf().getInt(
                CosNConfigKeys.UPLOAD_THREAD_POOL_SIZE_KEY,
                CosNConfigKeys.DEFAULT_UPLOAD_THREAD_POOL_SIZE
        );
        int readAheadPoolSize = this.getConf().getInt(
                CosNConfigKeys.READ_AHEAD_QUEUE_SIZE,
                CosNConfigKeys.DEFAULT_READ_AHEAD_QUEUE_SIZE
        );
        int ioThreadPoolSize = uploadThreadPoolSize + readAheadPoolSize / 3;
        long threadKeepAlive = this.getConf().getLong(
                CosNConfigKeys.THREAD_KEEP_ALIVE_TIME_KEY,
                CosNConfigKeys.DEFAULT_THREAD_KEEP_ALIVE_TIME
        );
        this.boundedIOThreadPool = new ThreadPoolExecutor(
                ioThreadPoolSize / 2, ioThreadPoolSize,
                threadKeepAlive, TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>(ioThreadPoolSize * 2),
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
                            }
                        }
                    }
                }
        );

        int copyThreadPoolSize = this.getConf().getInt(
                CosNConfigKeys.COPY_THREAD_POOL_SIZE_KEY,
                CosNConfigKeys.DEFAULT_COPY_THREAD_POOL_SIZE
        );
        this.boundedCopyThreadPool = new ThreadPoolExecutor(
                copyThreadPoolSize / 2, copyThreadPoolSize,
                threadKeepAlive, TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>(copyThreadPoolSize * 2),
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

    private static NativeFileSystemStore createDefaultStore(Configuration conf) {
        NativeFileSystemStore store = new CosNativeFileSystemStore();
        RetryPolicy basePolicy =
                RetryPolicies.retryUpToMaximumCountWithFixedSleep(
                        conf.getInt(CosNConfigKeys.COSN_MAX_RETRIES_KEY,
                                CosNConfigKeys.DEFAULT_MAX_RETRIES),
                        conf.getLong(CosNConfigKeys.COSN_RETRY_INTERVAL_KEY,
                                CosNConfigKeys.DEFAULT_RETRY_INTERVAL),
                        TimeUnit.SECONDS);
        Map<Class<? extends Exception>, RetryPolicy> exceptionToPolicyMap =
                new HashMap<Class<? extends Exception>, RetryPolicy>();

        exceptionToPolicyMap.put(IOException.class, basePolicy);
        RetryPolicy methodPolicy =
                RetryPolicies.retryByException(RetryPolicies.TRY_ONCE_THEN_FAIL,
                        exceptionToPolicyMap);
        Map<String, RetryPolicy> methodNameToPolicyMap = new HashMap<String, RetryPolicy>();
        methodNameToPolicyMap.put("storeFile", methodPolicy);
        methodNameToPolicyMap.put("rename", methodPolicy);

        return (NativeFileSystemStore) RetryProxy.create(NativeFileSystemStore.class, store,
                methodNameToPolicyMap);
    }

    private String getOwnerId() {
        return System.getProperty("user.name");
    }

    private String getGroupId() {
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
        } catch (InterruptedException e) {
            LOG.error("getOwnerInfo occur a exception", e);
        } catch (IOException e) {
            LOG.error("getOwnerInfo occur a exception", e);
        }
        return ownerInfoId;
    }

    private static String pathToKey(Path path) {
        if (path.toUri().getScheme() != null && path.toUri().getPath().isEmpty()) {
            // allow uris without trailing slash after bucket to refer to root,
            // like cosn://mybucket
            return "";
        }
        if (!path.isAbsolute()) {
            throw new IllegalArgumentException("Path must be absolute: " + path);
        }
        String ret = path.toUri().getPath();
        if (ret.endsWith("/") && (ret.indexOf("/") != ret.length() - 1)) {
            ret = ret.substring(0, ret.length() - 1);
        }
        return ret;
    }

    private static Path keyToPath(String key) {
        if (!key.startsWith(PATH_DELIMITER)) {
            return new Path("/" + key);
        } else {
            return new Path(key);
        }
    }

    private Path makeAbsolute(Path path) {
        if (path.isAbsolute()) {
            return path;
        }
        return new Path(workingDir, path);
    }

    @Override
    public Path getHomeDirectory() {
        String homePrefix = this.getConf().get("dfs.user.home.dir.prefix");
        if (null != homePrefix) {
            return makeQualified(new Path(homePrefix + "/" + System.getProperty("user.name")));
        }

        return super.getHomeDirectory();
    }

    /**
     * This optional operation is not yet supported.
     */
    @Override
    public FSDataOutputStream append(Path f, int bufferSize,
                                     Progressable progress) {
        throw new UnsupportedOperationException("Not supported currently");
    }

    @Override
    public FSDataOutputStream create(Path f, FsPermission permission,
                                     boolean overwrite,
                                     int bufferSize, short replication,
                                     long blockSize, Progressable progress)
            throws IOException {
        if (exists(f) && !overwrite) {
            throw new FileAlreadyExistsException("File already exists: " + f);
        }

        LOG.debug("Creating a new file [{}] in COS.", f);

        Path absolutePath = makeAbsolute(f);
        String key = pathToKey(absolutePath);
        long uploadPartSize = this.getConf().getLong(
                CosNConfigKeys.COSN_UPLOAD_PART_SIZE_KEY, CosNConfigKeys.DEFAULT_UPLOAD_PART_SIZE);
        boolean uploadChecksEnabled = this.getConf().getBoolean(CosNConfigKeys.COSN_UPLOAD_CHECKS_ENABLE_KEY,
                CosNConfigKeys.DEFAULT_COSN_UPLOAD_CHECKS_ENABLE);
        return new FSDataOutputStream(
                new CosFsDataOutputStream(getConf(), store, key,
                        uploadPartSize,
                        this.boundedIOThreadPool, uploadChecksEnabled), statistics);
    }

    private boolean rejectRootDirectoryDelete(boolean isEmptyDir,
                                              boolean recursive)
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
        LOG.debug("Ready to delete path: {}. recursive: {}.", f, recursive);
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

            CosNDeleteFileContext deleteFileContext = new CosNDeleteFileContext();
            int deleteToFinishes = 0;

            String priorLastKey = null;
            do {
                PartialListing listing =
                        store.list(key, COS_MAX_LISTING_LENGTH, priorLastKey, true);
                for (FileMetadata file : listing.getFiles()) {
                    this.boundedCopyThreadPool.execute(new CosNDeleteFileTask(
                            this.store, file.getKey(), deleteFileContext));
                    deleteToFinishes++;
                    if (!deleteFileContext.isDeleteSuccess()) {
                        break;
                    }
                }
                for (FileMetadata commonPrefix : listing.getCommonPrefixes()) {
                    this.boundedCopyThreadPool.execute(new CosNDeleteFileTask(
                            this.store, commonPrefix.getKey(), deleteFileContext));
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
                store.delete(key);
            } catch (Exception e) {
                LOG.error("Delete the key failed.");
            }

        } else {
            LOG.debug("Delete the cos key [{}].", key);
            store.delete(key);
        }

        createParentDirectoryIfNecessary(f);
        return true;
    }

    @Override
    public FileStatus getFileStatus(Path f) throws IOException {
        LOG.debug("Get file status: {}.", f);
        Path absolutePath = makeAbsolute(f);
        String key = pathToKey(absolutePath);

        if (key.length() == 0) { // root always exists
            return newDirectory(absolutePath);
        }

        FileMetadata meta = store.retrieveMetadata(key);
        if (meta != null) {
            if (meta.isFile()) {
                LOG.debug("Retrieve the cos key [{}] to find that it is a file.", key);
                return newFile(meta, absolutePath);
            } else {
                LOG.debug("Retrieve the cos key [{}] to find that it is a directory.", key);
                return newDirectory(meta, absolutePath);
            }
        }

        if (!key.endsWith(PATH_DELIMITER)) {
            key += PATH_DELIMITER;
        }
        LOG.debug("List the cos key [{}] to judge whether it is a directory or not.", key);
        PartialListing listing = store.list(key, 1);
        if (listing.getFiles().length > 0 || listing.getCommonPrefixes().length > 0) {
            LOG.debug("List the cos key [{}] to find that it is a directory.", key);
            return newDirectory(absolutePath);
        }

        LOG.debug("Can not find the cos key [{}] on COS.", key);

        throw new FileNotFoundException("No such file or directory '" + absolutePath + "'");
    }

    @Override
    public URI getUri() {
        return uri;
    }

    /**
     * <p>
     * If <code>f</code> is a file, this method will make a single call to`
     * COS. If <code>f</code> is
     * a directory, this method will make a maximum of ( <i>n</i> / 199) + 2
     * calls to cos, where
     * <i>n</i> is the total number of files and directories contained
     * directly in <code>f</code>.
     * </p>
     */
    @Override
    public FileStatus[] listStatus(Path f) throws IOException {
        LOG.debug("list status:" + f);

        Path absolutePath = makeAbsolute(f);
        String key = pathToKey(absolutePath);

        if (key.length() > 0) {
            FileMetadata meta = store.retrieveMetadata(key);
            if (meta != null && meta.isFile()) {
                return new FileStatus[]{newFile(meta, absolutePath)};
            }
        }

        if (!key.endsWith(PATH_DELIMITER)) {
            key += PATH_DELIMITER;
        }

        URI pathUri = absolutePath.toUri();
        Set<FileStatus> status = new TreeSet<FileStatus>();
        String priorLastKey = null;
        do {
            PartialListing listing = store.list(key, COS_MAX_LISTING_LENGTH,
                    priorLastKey, false);
            for (FileMetadata fileMetadata : listing.getFiles()) {
                Path subPath = keyToPath(fileMetadata.getKey());
                if (fileMetadata.getKey().equals(key)) {
                    // this is just the directory we have been asked to list
                    LOG.debug("This is just the directory we have been asked to list. cos key: {}.",
                            fileMetadata.getKey());
                } else {
                    status.add(newFile(fileMetadata, subPath));
                }
            }
            for (FileMetadata commonPrefix : listing.getCommonPrefixes()) {
                Path subpath = keyToPath(commonPrefix.getKey());
                String relativePath =
                        pathUri.relativize(subpath.toUri()).getPath();
                status.add(newDirectory(commonPrefix, new Path(absolutePath,
                        relativePath)));
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

    @Override
    public boolean mkdirs(Path f, FsPermission permission)
            throws IOException {
        LOG.debug("mkdirs path: {}.", f);
        try {
            FileStatus fileStatus = getFileStatus(f);
            if (fileStatus.isDirectory()) {
                return true;
            } else {
                throw new FileAlreadyExistsException("Path is a file: " + f);
            }
        } catch (FileNotFoundException e) {
            validatePath(f);
            return mkDirRecursively(f, permission);
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
                store.storeEmptyFile(folderPath);
            }
        }
        return true;
    }

    @Override
    public FSDataInputStream open(Path f, int bufferSize) throws IOException {
        FileStatus fileStatus = getFileStatus(f); // will throw if the file doesn't
        // exist
        if (fileStatus.isDirectory()) {
            throw new FileNotFoundException("'" + f + "' is a directory");
        }
        LOG.info("Opening '" + f + "' for reading");
        Path absolutePath = makeAbsolute(f);
        String key = pathToKey(absolutePath);
        return new FSDataInputStream(new BufferedFSInputStream(
                new CosFsInputStream(this.getConf(), store, statistics, key,
                        fileStatus.getLen(), this.boundedIOThreadPool),
                bufferSize));
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        LOG.debug("Rename the source path [{}] to the dest path [{}].", src, dst);

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
            throw new IOException(String.format(
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
//                throw new FileAlreadyExistsException(String.format(
//                        "File:%s already exists", dstFileStatus.getPath()));
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
                    LOG.debug("Cannot rename {} to {}, file already exists.",
                            src, dst);
//                    throw new FileAlreadyExistsException(
//                            String.format(
//                                    "File: %s already exists", dst
//                            )
//                    );
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

        boolean result = false;
        if (srcFileStatus.isDirectory()) {
            result = this.copyDirectory(src, dst);
        } else {
            result = this.copyFile(src, dst);
        }

        if (!result) {
            //Since rename is a non-atomic operation, after copy fails,
            // it is not allowed to delete the data of the original path to
            // ensure data security.
            return false;
        } else {
            return this.delete(src, true);
        }
    }

    private boolean copyFile(Path srcPath, Path dstPath) throws IOException {
        String srcKey = pathToKey(srcPath);
        String dstKey = pathToKey(dstPath);
        this.store.copy(srcKey, dstKey);
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

        if (this.store.retrieveMetadata(srcKey) == null) {
            this.store.storeEmptyFile(srcKey);
        } else {
            this.store.copy(srcKey, dstKey);
        }

        CosNCopyFileContext copyFileContext = new CosNCopyFileContext();

        int copiesToFinishes = 0;
        String priorLastKey = null;
        do {
            PartialListing objectList = this.store.list(srcKey,
                    COS_MAX_LISTING_LENGTH, priorLastKey, true);
            for (FileMetadata file : objectList.getFiles()) {
                this.boundedCopyThreadPool.execute(new CosNCopyFileTask(
                        this.store,
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
                store.storeEmptyFile(parentKey);
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
    public String getCanonicalServiceName() {
        // Does not support Token
        return null;
    }

    @Override
    public FileChecksum getFileChecksum(Path f, long length) throws IOException {
        Preconditions.checkArgument(length >= 0);
        LOG.debug("Call the checksum for the path: {}.", f);

        if (this.getConf().getBoolean(CosNConfigKeys.CRC64_CHECKSUM_ENABLED,
                CosNConfigKeys.DEFAULT_CRC64_CHECKSUM_ENABLED)) {
            Path absolutePath = makeAbsolute(f);
            String key = pathToKey(absolutePath);
            FileMetadata fileMetadata = this.store.retrieveMetadata(key);
            String crc64ecm = fileMetadata.getCrc64ecm();
            return crc64ecm != null ? new CRC64Checksum(crc64ecm) : super.getFileChecksum(f, length);
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
        LOG.debug("set XAttr: {}.", f);

        // First, determine whether the length of the name and value exceeds the limit.
        if (name.getBytes(METADATA_ENCODING).length + value.length > MAX_XATTR_SIZE) {
            throw new HadoopIllegalArgumentException(String.format("The maximum combined size of " +
                    "the name and value of an extended attribute in bytes should be less than or equal to %d",
                    MAX_XATTR_SIZE));
        }

        Path absolutePath = makeAbsolute(f);

        String key = pathToKey(absolutePath);
        FileMetadata fileMetadata = store.retrieveMetadata(key);
        if (null == fileMetadata) {
            throw new FileNotFoundException("File or directory doesn't exist: " + f);
        }
        boolean xAttrExists = (null != fileMetadata.getUserAttributes()
                && fileMetadata.getUserAttributes().containsKey(name));
        XAttrSetFlag.validate(name, xAttrExists, flag);
        if (fileMetadata.isFile()) {
            store.storeFileAttribute(key, name, value);
        } else {
            store.storeDirAttribute(key, name, value);
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
        LOG.debug("get XAttr: {}.", f);

        Path absolutePath = makeAbsolute(f);

        String key = pathToKey(absolutePath);
        FileMetadata fileMetadata = store.retrieveMetadata(key);
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
        LOG.debug("get XAttrs: {}.", f);

        Path absolutePath = makeAbsolute(f);

        String key = pathToKey(absolutePath);
        FileMetadata fileMetadata = store.retrieveMetadata(key);
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

    /**
     * Removes an xattr of a cosn file or directory.
     *
     * @param f    Path to remove extended attribute
     * @param name xattr name
     * @throws IOException If there was an issue setting the attribute on COS
     */
    @Override
    public void removeXAttr(Path f, String name) throws IOException {
        LOG.debug("remove XAttr: {}.", f);

        Path absolutPath = makeAbsolute(f);

        String key = pathToKey(absolutPath);
        FileMetadata fileMetadata = store.retrieveMetadata(key);
        if (null == fileMetadata) {
            throw new FileNotFoundException("File or directory doesn't exist: " + f);
        }

        boolean xAttrExists = (null != fileMetadata.getUserAttributes()
                && fileMetadata.getUserAttributes().containsKey(name));
        if (xAttrExists) {
            if (fileMetadata.isFile()) {
                store.removeFileAttribute(key, name);
            } else {
                store.removeDirAttribute(key, name);
            }
        }

        // Nothing to do if the specified attribute is not found.
    }

    @Override
    public Map<String, byte[]> getXAttrs(Path f) throws IOException {
        LOG.debug("get XAttrs: {}.", f);

        Path absolutePath = makeAbsolute(f);

        String key = pathToKey(absolutePath);
        FileMetadata fileMetadata = store.retrieveMetadata(key);
        if (null == fileMetadata) {
            throw new FileNotFoundException("File or directory doesn't exist: " + f);
        }

        return fileMetadata.getUserAttributes();
    }

    @Override
    public List<String> listXAttrs(Path f) throws IOException {
        LOG.debug("list XAttrs: {}.", f);

        Path absolutePath = makeAbsolute(f);

        String key = pathToKey(absolutePath);
        FileMetadata fileMetadata = store.retrieveMetadata(key);
        if (null == fileMetadata) {
            throw new FileNotFoundException("File or directory doesn't exist: " + f);
        }

        return new ArrayList<>(fileMetadata.getUserAttributes().keySet());
    }

    @Override
    public void close() throws IOException {
        try {
            this.store.close();
            this.boundedIOThreadPool.shutdown();
            this.boundedCopyThreadPool.shutdown();
            BufferPool.getInstance().close();
        } finally {
            super.close();
        }
    }

    public NativeFileSystemStore getStore() {
        return this.store;
    }
}

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.fs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.*;
import java.util.concurrent.*;

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

/**
 * A {@link FileSystem} for reading and writing files stored on
 * <a href="https://www.qcloud.com/product/cos.html">Tencent Qcloud Cos</a>. Unlike
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
        this.workingDir = new Path("/user", System.getProperty("user.name")).makeQualified(this.uri,
                this.getWorkingDirectory());
        this.owner = getOwnerId();
        this.group = getGroupId();
        if (LOG.isDebugEnabled()) {
            LOG.debug("owner:" + owner + ", group:" + group);
        }
        BufferPool.getInstance().initialize(getConf());

        // initialize the thread pool
        int uploadThreadPoolSize = this.getConf().getInt(
                CosNativeFileSystemConfigKeys.UPLOAD_THREAD_POOL_SIZE_KEY,
                CosNativeFileSystemConfigKeys.DEFAULT_UPLOAD_THREAD_POOL_SIZE
        );
        int readAheadPoolSize = this.getConf().getInt(
                CosNativeFileSystemConfigKeys.READ_AHEAD_QUEUE_SIZE,
                CosNativeFileSystemConfigKeys.DEFAULT_READ_AHEAD_QUEUE_SIZE
        );
        int ioThreadPoolSize = uploadThreadPoolSize + readAheadPoolSize / 3;
        long threadKeepAlive = this.getConf().getLong(
                CosNativeFileSystemConfigKeys.THREAD_KEEP_ALIVE_TIME_KEY,
                CosNativeFileSystemConfigKeys.DEFAULT_THREAD_KEEP_ALIVE_TIME
        );
        this.boundedIOThreadPool = new ThreadPoolExecutor(
                ioThreadPoolSize / 2, ioThreadPoolSize,
                threadKeepAlive, TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>(ioThreadPoolSize * 2),
                new RejectedExecutionHandler() {
                    @Override
                    public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                        if (!executor.isShutdown()) {
                            try {
                                executor.getQueue().put(r);
                            } catch (InterruptedException e) {
                                LOG.error("put a io task into the download thread pool occurs an exception.", e);
                            }
                        }
                    }
                }
        );

        int copyThreadPoolSize = this.getConf().getInt(
                CosNativeFileSystemConfigKeys.COPY_THREAD_POOL_SIZE_KEY,
                CosNativeFileSystemConfigKeys.DEFAULT_COPY_THREAD_POOL_SIZE
        );
        this.boundedCopyThreadPool = new ThreadPoolExecutor(
                copyThreadPoolSize / 2, copyThreadPoolSize,
                threadKeepAlive, TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>(copyThreadPoolSize * 2),
                new RejectedExecutionHandler() {
                    @Override
                    public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                        if (!executor.isShutdown()) {
                            try {
                                executor.getQueue().put(r);
                            } catch (InterruptedException e) {
                                LOG.error("put a copy task into the download thread pool occurs an exception.", e);
                            }
                        }
                    }
                }
        );
    }

    private static NativeFileSystemStore createDefaultStore(Configuration conf) {
        NativeFileSystemStore store = new CosNativeFileSystemStore();
        RetryPolicy basePolicy = RetryPolicies.retryUpToMaximumCountWithFixedSleep(
                conf.getInt(CosNativeFileSystemConfigKeys.COS_MAX_RETRIES_KEY,
                        CosNativeFileSystemConfigKeys.DEFAULT_MAX_RETRIES),
                conf.getLong(CosNativeFileSystemConfigKeys.COS_RETRY_INTERVAL_KEY,
                        CosNativeFileSystemConfigKeys.DEFAULT_RETRY_INTERVAL),
                TimeUnit.SECONDS);
        Map<Class<? extends Exception>, RetryPolicy> exceptionToPolicyMap =
                new HashMap<Class<? extends Exception>, RetryPolicy>();

        exceptionToPolicyMap.put(IOException.class, basePolicy);
        RetryPolicy methodPolicy = RetryPolicies.retryByException(RetryPolicies.TRY_ONCE_THEN_FAIL,
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
            StringBuffer strBuffer = new StringBuffer();
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

    /**
     * This optional operation is not yet supported.
     */
    @Override
    public FSDataOutputStream append(Path f, int bufferSize, Progressable progress)
            throws IOException {
        throw new IOException("Not supported");
    }

    @Override
    public FSDataOutputStream create(Path f, FsPermission permission, boolean overwrite,
                                     int bufferSize, short replication, long blockSize, Progressable progress)
            throws IOException {

        if (exists(f) && !overwrite) {
            throw new FileAlreadyExistsException("File already exists: " + f);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Creating new file '" + f + "' in COS");
        }
        Path absolutePath = makeAbsolute(f);
        String key = pathToKey(absolutePath);
        return new FSDataOutputStream(
                new CosFsDataOutputStream(getConf(), store, key, blockSize, this.boundedIOThreadPool),
                statistics);
    }

    private boolean rejectRootDirectoryDelete(boolean isEmptyDir, boolean recursive) throws PathIOException {
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
        LOG.debug("ready to delete path:" + f + ", recursive:" + recursive);
        FileStatus status;
        try {
            status = getFileStatus(f);
        } catch (FileNotFoundException e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Delete called for '" + f
                        + "' but file does not exist, so returning false");
            }
            return false;
        }
        Path absolutePath = makeAbsolute(f);
        String key = pathToKey(absolutePath);
        if (key.compareToIgnoreCase("/") == 0) {
            FileStatus[] fileStatuses = listStatus(f);
            return this.rejectRootDirectoryDelete(fileStatuses.length == 0, recursive);
        }

        if (status.isDirectory()) {
            if (!key.endsWith(PATH_DELIMITER)) {
                key += PATH_DELIMITER;
            }
            if (!recursive && listStatus(f).length > 0) {
                throw new IOException("Can not delete " + f
                        + " as is a not empty directory and recurse option is false");
            }

            createParent(f);

            String priorLastKey = null;
            do {
                PartialListing listing =
                        store.list(key, COS_MAX_LISTING_LENGTH, priorLastKey, true);
                for (FileMetadata file : listing.getFiles()) {
                    store.delete(file.getKey());
                }
                for (FileMetadata commonprefix : listing.getCommonPrefixes()) {
                    store.delete(commonprefix.getKey());
                }
                priorLastKey = listing.getPriorLastKey();
            } while (priorLastKey != null);
            try {
                store.delete(key);
            } catch (Exception e) {
            }

        } else {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Deleting file '" + f + "'");
            }
            createParent(f);
            store.delete(key);
        }
        return true;
    }

    @Override
    public FileStatus getFileStatus(Path f) throws IOException {
        LOG.debug("getFileStatus: " + f);
        Path absolutePath = makeAbsolute(f);
        String key = pathToKey(absolutePath);

        if (key.length() == 0) { // root always exists
            return newDirectory(absolutePath);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("getFileStatus retrieving metadata for key '" + key + "'");
        }
        FileMetadata meta = store.retrieveMetadata(key);
        if (meta != null) {
            if (meta.isFile()) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("getFileStatus returning 'file' for key '" + key + "'");
                }
                return newFile(meta, absolutePath);
            } else {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("getFileStatus returning 'dir' for key '" + key + "'");
                }
                return newDirectory(meta, absolutePath);
            }
        }

        if (!key.endsWith(PATH_DELIMITER)) {
            key += PATH_DELIMITER;
        }
        LOG.debug("getFileStatus listing key '" + key + "'");
        PartialListing listing = store.list(key, 1);
        if (listing.getFiles().length > 0 || listing.getCommonPrefixes().length > 0) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("getFileStatus returning 'directory' for key '" + key
                        + "' as it has contents");
            }
            return newDirectory(absolutePath);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("getFileStatus could not find key '" + key + "'");
        }

        throw new FileNotFoundException("No such file or directory '" + absolutePath + "'");
    }

    @Override
    public URI getUri() {
        return uri;
    }

    /**
     * <p>
     * If <code>f</code> is a file, this method will make a single call to COS. If <code>f</code> is
     * a directory, this method will make a maximum of ( <i>n</i> / 199) + 2 calls to cos, where
     * <i>n</i> is the total number of files and directories contained directly in <code>f</code>.
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
            PartialListing listing = store.list(key, COS_MAX_LISTING_LENGTH, priorLastKey, false);
            for (FileMetadata fileMetadata : listing.getFiles()) {
                LOG.debug("fileMeta:" + fileMetadata.toString());
                Path subpath = keyToPath(fileMetadata.getKey());

                if (fileMetadata.getKey().equals(key)) {
                    // this is just the directory we have been asked to list
                } else {
                    status.add(newFile(fileMetadata, subpath));
                }
            }
            for (FileMetadata commonPrefix : listing.getCommonPrefixes()) {
                Path subpath = keyToPath(commonPrefix.getKey());
                LOG.debug("commprefix, subpath" + subpath);
                String relativePath = pathUri.relativize(subpath.toUri()).getPath();
                LOG.debug("commprefix, relativePath:" + relativePath);
                LOG.debug("full path:" + new Path(absolutePath, relativePath));
                status.add(newDirectory(commonPrefix, new Path(absolutePath, relativePath)));
            }
            priorLastKey = listing.getPriorLastKey();
        } while (priorLastKey != null);

        return status.toArray(new FileStatus[status.size()]);
    }

    private FileStatus newFile(FileMetadata meta, Path path) {
        return new FileStatus(meta.getLength(), false, 1, getDefaultBlockSize(),
                meta.getLastModified(), 0, null, this.owner, this.group,
                path.makeQualified(this.getUri(), this.getWorkingDirectory()));
    }

    private FileStatus newDirectory(Path path) {
        return new FileStatus(0, true, 1, 0, 0, 0, null, this.owner, this.group,
                path.makeQualified(this.getUri(), this.getWorkingDirectory()));
    }

    private FileStatus newDirectory(FileMetadata meta, Path path) {
        if (meta == null) {
            return newDirectory(path);
        }
        FileStatus status = new FileStatus(0, true, 1, 0, meta.getLastModified(), 0, null, this.owner, this.group,
                path.makeQualified(this.getUri(), this.getWorkingDirectory()));
        LOG.debug("status: " + status.toString());
        return status;
    }

    /**
     * Validate the path from the bottom up.
     *
     * @param path
     * @throws IOException
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
                            "Can't make directory for path '%s', it is a file.", parent));
                }
            } catch (FileNotFoundException e) {
            }
            parent = parent.getParent();
        } while (parent != null);
    }

    @Override
    public boolean mkdirs(Path f, FsPermission permission)
            throws IOException {
        try {
            FileStatus fileStatus = getFileStatus(f);
            if (fileStatus.isDirectory()) {
                return true;
            } else {
                throw new FileAlreadyExistsException("Path is a file: " + f);
            }
        } catch (FileNotFoundException e) {
            validatePath(f);
        }

        return mkDirRecursively(f, permission);
    }

    /**
     * Recursively create a directory.
     *
     * @param f          Absolute path to the directory
     * @param permission Directory permissions
     * @return Return true if the creation was successful,  throw a IOException.
     * @throws IOException
     */
    public boolean mkDirRecursively(Path f, FsPermission permission)
            throws IOException {
        System.out.println(f);
        Path absolutePath = makeAbsolute(f);
        List<Path> paths = new ArrayList<Path>();
        do {
            paths.add(absolutePath);
            absolutePath = absolutePath.getParent();
        } while (absolutePath != null);

        for (Path path : paths) {
            if (path.equals(CosFileSystem.PATH_DELIMITER)) {
                break;
            }
            try {
                FileStatus fileStatus = getFileStatus(path);
                if (fileStatus.isFile()) {
                    throw new FileAlreadyExistsException(
                            String.format(
                                    "Can't make directory for path '%s' since it is a file.", f));
                }
                if (fileStatus.isDirectory()) {
                    break;
                }
            } catch (FileNotFoundException e) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Making dir '" + f + "' in COS");
                }

                String folderPath = pathToKey(makeAbsolute(f));
                if (!folderPath.endsWith(PATH_DELIMITER)) {
                    folderPath += PATH_DELIMITER;
                }
                store.storeEmptyFile(folderPath);
            }
        }
        return true;
    }

    private boolean mkdir(Path f) throws IOException {
        LOG.debug("mkdir " + f);
        try {
            FileStatus fileStatus = getFileStatus(f);
            if (fileStatus.isFile()) {
                throw new FileAlreadyExistsException(
                        String.format(
                                "Can't make directory for path '%s' since it is a file.", f));
            }
        } catch (FileNotFoundException e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Making dir '" + f + "' in COS");
            }

            String folderPath = pathToKey(makeAbsolute(f));
            if (!folderPath.endsWith(PATH_DELIMITER)) {
                folderPath += PATH_DELIMITER;
            }
            store.storeEmptyFile(folderPath);
        }
        return true;
    }

    @Override
    public FSDataInputStream open(Path f, int bufferSize) throws IOException {
        FileStatus fs = getFileStatus(f); // will throw if the file doesn't
        // exist
        if (fs.isDirectory()) {
            throw new FileNotFoundException("'" + f + "' is a directory");
        }
        LOG.info("Opening '" + f + "' for reading");
        Path absolutePath = makeAbsolute(f);
        String key = pathToKey(absolutePath);
        long fileSize = store.getFileLength(key);
        return new FSDataInputStream(new BufferedFSInputStream(
                new CosFsInputStream(this.getConf(), store, statistics, key, fileSize, this.boundedIOThreadPool),
                bufferSize));
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        LOG.debug("input rename: src:" + src + " , dst:" + dst);

        // Renaming the root directory is not allowed
        if (src.isRoot()) {
            LOG.debug("Cannot rename the root directory of a filesystem.");
            return false;
        }

        // check the source path whether exists or not
        FileStatus srcFileStatus = this.getFileStatus(src);

        // Source path and destination path are not allowed to be the same
        if (src.equals(dst)) {
            LOG.debug("source path and dest path refer to the same file or directory: {}", dst);
            throw new IOException("source path and dest path refer to the same file or directory");
        }

        // It is not allowed to rename a parent directory to its subdirectory
        Path dstParentPath;
        for (dstParentPath = dst.getParent();
             null != dstParentPath && !src.equals(dstParentPath);
             dstParentPath = dstParentPath.getParent()) {
        }

        if (null != dstParentPath) {
            LOG.debug("It is not allowed to rename a parent directory:{} to its subdirectory:{}.",
                    src, dst);
            throw new IOException(String.format(
                    "It is not allowed to rename a parent directory:%s to its subdirectory:%s",
                    src, dst));
        }

        FileStatus dstFileStatus = null;
        try {
            dstFileStatus = this.getFileStatus(dst);

            // The destination path exists and is a file,
            // and the rename operation is not allowed.
            if (dstFileStatus.isFile()) {
                throw new FileAlreadyExistsException(String.format(
                        "File:%s already exists", dstFileStatus.getPath()));
            } else {
                // The destination path is an existing directory,
                // and it is checked whether there is a file or directory
                // with the same name as the source path under the destination path
                dst = new Path(dst, src.getName());
                FileStatus[] statuses;
                try {
                    statuses = this.listStatus(dst);
                } catch (FileNotFoundException e) {
                    statuses = null;
                }
                if (null != statuses && statuses.length > 0) {
                    LOG.debug("Cannot rename {} to {}, file already exists.", src, dst);
                    throw new FileAlreadyExistsException(
                            String.format(
                                    "File: %s already exists", dst
                            )
                    );
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
            // it is not allowed to delete the data of the original path to ensure data security.
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
            throw new IOException("can not copy a directory to a subdirectory of self");
        }

        this.store.storeEmptyFile(dstKey);
        CosCopyFileContext copyFileContext = new CosCopyFileContext();

        int copiesToFinishes = 0;
        String priorLastKey = null;
        do {
            PartialListing objectList = this.store.list(srcKey, COS_MAX_LISTING_LENGTH, priorLastKey, true);
            for (FileMetadata file : objectList.getFiles()) {
                this.boundedCopyThreadPool.execute(new CosCopyFileTask(
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

    private void createParent(Path path) throws IOException {
        Path parent = path.getParent();
        if (parent != null) {
            String parentKey = pathToKey(parent);
            LOG.debug("createParent parentKey:" + parentKey);
            if (!parentKey.equals(PATH_DELIMITER)) {
                String key = pathToKey(makeAbsolute(parent));
                if (key.length() > 0) {
                    try {
                        store.storeEmptyFile(key + PATH_DELIMITER);
                    } catch (Exception e) {
                        LOG.debug("storeEmptyFile exception: " + e.toString());
                    }
                }
            }
        }
    }

    @Override
    public long getDefaultBlockSize() {
        return getConf().getLong(
                CosNativeFileSystemConfigKeys.COS_BLOCK_SIZE_KEY,
                CosNativeFileSystemConfigKeys.DEFAULT_BLOCK_SIZE);
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
    public void close() throws IOException {
        try {
            this.store.close();
            this.boundedIOThreadPool.shutdown();
            this.boundedCopyThreadPool.shutdown();
        } finally {
            super.close();
        }
    }
}

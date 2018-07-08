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
    private NativeFileSystemStore store;
    private Path workingDir;
    private String owner = "Unknown";
    private String group = "Unknown";

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
        LOG.debug("owner:" + owner + ", group:" + group);
        BufferPool.getInstance().initialize(getConf());
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
        } catch (IOException | InterruptedException e) {
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
                new CosFsDataOutputStream(getConf(), store, key, blockSize),
                statistics);
    }

    @Override
    public boolean delete(Path f, boolean recurse) throws IOException {
        LOG.debug("ready to delete path:" + f + ", recurse:" + recurse);
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
        if (status.isDirectory()) {
            if (!key.endsWith(PATH_DELIMITER)) {
                key += PATH_DELIMITER;
            }
            if (!recurse && listStatus(f).length > 0) {
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
            /*
            if (meta == null) {
                throw new FileNotFoundException("File " + f + " does not exist.");
            }
            if (meta.isFile()) {
                return new FileStatus[] {newFile(meta, absolutePath)};
            }
            */
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

    @Override
    public boolean mkdirs(Path f, FsPermission permission) throws IOException {
        LOG.debug("mkdirs " + f);
        Path absolutePath = makeAbsolute(f);
        List<Path> paths = new ArrayList<Path>();
        do {
            paths.add(0, absolutePath);
            absolutePath = absolutePath.getParent();
        } while (absolutePath != null);

        boolean result = true;
        for (Path path : paths) {
            result &= mkdir(path);
        }
        return result;
    }

    private boolean mkdir(Path f) throws IOException {
        LOG.debug("mkdir " + f);
        try {
            FileStatus fileStatus = getFileStatus(f);
            if (fileStatus.isFile()) {
                throw new FileAlreadyExistsException(
                        String.format("Can't make directory for path '%s' since it is a file.", f));

            }
        } catch (FileNotFoundException e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Making dir '" + f + "' in COS");
            }
            // 创建目录
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
                new CosFsInputStream(this.getConf(), store, statistics, key, fileSize),
                bufferSize));
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        LOG.debug("input rename: src:" + src + " , dst:" + dst);

        String srcKey = pathToKey(makeAbsolute(src));

        if (srcKey.length() == 0) {
            // Cannot rename root of file system
            return false;
        }

        final String debugPreamble = "Renaming '" + src + "' to '" + dst + "' - ";
        LOG.debug(debugPreamble);

        // Figure out the final destination
        String dstKey;
        try {
            boolean dstIsFile = getFileStatus(dst).isFile();
            if (dstIsFile) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(debugPreamble + "returning false as dst is an already existing file");
                }
                return false;
            } else {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(debugPreamble + "using dst as output directory");
                }
                dstKey = pathToKey(makeAbsolute(new Path(dst, src.getName())));
            }
        } catch (FileNotFoundException e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug(debugPreamble + "using dst as output destination");
            }
            dstKey = pathToKey(makeAbsolute(dst));
            try {
                if (getFileStatus(dst.getParent()).isFile()) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(debugPreamble
                                + "returning false as dst parent exists and is a file");
                    }
                    return false;
                }
            } catch (FileNotFoundException ex) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(debugPreamble + "returning false as dst parent does not exist");
                }
                return false;
            }
        }

        boolean srcIsFile;
        try {
            srcIsFile = getFileStatus(src).isFile();
        } catch (FileNotFoundException e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug(debugPreamble + "returning false as src does not exist");
            }
            return false;
        }
        if (srcIsFile) {
            if (LOG.isDebugEnabled()) {
                LOG.debug(debugPreamble + "src is file, so doing copy then delete in COS");
            }
            store.copy(srcKey, dstKey);
            store.delete(srcKey);
        } else {
            if (LOG.isDebugEnabled()) {
                LOG.debug(debugPreamble + "src is directory, so copying contents");
            }

            if (!srcKey.endsWith(PATH_DELIMITER)) {
                srcKey += PATH_DELIMITER;
            }
            if (!dstKey.endsWith(PATH_DELIMITER)) {
                dstKey += PATH_DELIMITER;
            }
            store.storeEmptyFile(dstKey);
            List<String> keysToDelete = new ArrayList<String>();
            String priorLastKey = null;
            do {
                PartialListing listing =
                        store.list(srcKey, COS_MAX_LISTING_LENGTH, priorLastKey, true);
                for (FileMetadata file : listing.getFiles()) {

                    keysToDelete.add(file.getKey());
                    store.copy(file.getKey(), dstKey + file.getKey().substring(srcKey.length()));
                }
                for (FileMetadata commonPrefix : listing.getCommonPrefixes()) {
                    keysToDelete.add(commonPrefix.getKey());
                    try {
                        store.storeEmptyFile(dstKey + commonPrefix.getKey().substring(srcKey.length()));
                    } catch (Exception e) {
                        LOG.debug(e.toString());
                    }
                }
                priorLastKey = listing.getPriorLastKey();
            } while (priorLastKey != null);

            if (LOG.isDebugEnabled()) {
                LOG.debug(debugPreamble + "all files in src copied, now removing src files");
            }
            for (String key : keysToDelete) {
                store.delete(key);
            }
            try {
                store.delete(srcKey);
            } catch (Exception e) {
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug(debugPreamble + "done");
            }
        }

        return true;
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
        return getConf().getLong(CosNativeFileSystemConfigKeys.COS_BLOCK_SIZE_KEY, CosNativeFileSystemConfigKeys.DEFAULT_BLOCK_SIZE);
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
        super.close();

    }
}

package org.apache.hadoop.fs;

import com.google.common.base.Preconditions;
import com.qcloud.chdfs.fs.CHDFSHadoopFileSystemAdapter;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.cosn.Constants;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.qcloud.chdfs.permission.RangerAccessType;
import org.apache.hadoop.fs.auth.RangerCredentialsProvider;
import org.apache.hadoop.fs.cosn.ranger.client.RangerQcloudObjectStorageClient;
import org.apache.hadoop.fs.cosn.ranger.security.authorization.AccessType;
import org.apache.hadoop.fs.cosn.ranger.security.authorization.PermissionRequest;
import org.apache.hadoop.fs.cosn.ranger.security.authorization.PermissionResponse;
import org.apache.hadoop.fs.cosn.ranger.security.authorization.ServiceType;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * A {@link FileSystem} for reading and writing files stored on
 * <a href="https://www.qcloud.com/product/cos.html">Tencent Qcloud Cos</a>
 * . Unlike
 * {@link CosFileSystem} this implementation stores files on COS in their
 * native form so they can be read by other cos tools.
 * <p>
 * for new version cos file system is the shell which using the head bucket
 * to decide whether normal bucket or posix bucket, if it is posix bucket
 * direct use the network shell implements.
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public class CosFileSystem extends FileSystem {
    static final Logger LOG = LoggerFactory.getLogger(CosFileSystem.class);

    static final String SCHEME = "cosn";

    // Related to COS Object Storage
    private NativeFileSystemStore nativeStore;
    private boolean isPosixFSStore;
    private boolean isDefaultNativeStore;
    private FileSystem actualImplFS = null;

    private URI uri;
    private String bucket;
    private Path workingDir;
    // Authorization related.
    private UserGroupInformation userGroupInformation;
    private boolean enableRangerPluginPermissionCheck = false;
    public static RangerQcloudObjectStorageClient rangerQcloudObjectStorageStorageClient = null;


    public CosFileSystem() {
    }

    public CosFileSystem(NativeFileSystemStore nativeStore) {
        this.nativeStore = nativeStore;
        this.isDefaultNativeStore = false;
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
        setConf(conf);

        // initialize the things authorization related.
        UserGroupInformation.setConfiguration(conf);
        this.userGroupInformation = UserGroupInformation.getCurrentUser();
        this.initRangerClientImpl(conf);

        String bucket = uri.getHost();
        this.bucket = bucket;
        this.uri = URI.create(uri.getScheme() + "://" + uri.getAuthority());
        this.workingDir = new Path("/user", System.getProperty("user.name"))
                .makeQualified(this.uri, this.getWorkingDirectory());

        if (null == this.nativeStore) {
            this.nativeStore = CosNUtils.createDefaultStore(conf);
            this.nativeStore.initialize(uri, conf);
            this.isDefaultNativeStore = true;
        }

        this.isPosixFSStore = this.nativeStore.headBucket(bucket).isMergeBucket();
        LOG.info("The cos bucket {} bucket.", isPosixFSStore ? "is the posix" : "is the normal");
        if (isPosixFSStore) {
            String posixBucketFSImpl = this.getConf().get(CosNConfigKeys.COSN_POSIX_BUCKET_FS_IMPL);
            if (null == posixBucketFSImpl) {
                posixBucketFSImpl = CosNConfigKeys.DEFAULT_COSN_POSIX_BUCKET_FS_IMPL;
                // Provide to the outer layer to judge the implementation class of the Posix bucket.
                this.getConf().set(CosNConfigKeys.COSN_POSIX_BUCKET_FS_IMPL,
                        CosNConfigKeys.DEFAULT_COSN_POSIX_BUCKET_FS_IMPL);
            }

            LOG.info("The posix bucket [{}] use the class [{}] as the filesystem implementation.",
                    bucket, posixBucketFSImpl);
            // if ofs impl.
            // network version start from the 2.7.
            // sdk version start from the 1.0.4.
            this.actualImplFS = getActualFileSystemByClassName(posixBucketFSImpl);
            if (this.actualImplFS instanceof CHDFSHadoopFileSystemAdapter) {
                // before the init, must transfer the config and disable the range in ofs
                this.transferOfsConfig();
                this.nativeStore.close();
                this.nativeStore = null;
            } else if (this.actualImplFS instanceof CosNFileSystem) {
                ((CosNFileSystem) this.actualImplFS).withStore(this.nativeStore);
                ((CosNFileSystem) this.actualImplFS).withBucket(bucket);
                ((CosNFileSystem) this.actualImplFS).withPosixBucket(isPosixFSStore);
            } else {
                // Another class
                throw new IOException(
                        String.format("The posix bucket does not currently support the implementation [%s].",
                                posixBucketFSImpl));
            }
        } else { // normal cos hadoop file system implements
            this.actualImplFS = getActualFileSystemByClassName("org.apache.hadoop.fs.CosNFileSystem");
            ((CosNFileSystem) this.actualImplFS).withStore(this.nativeStore);
            ((CosNFileSystem) this.actualImplFS).withBucket(bucket);
            ((CosNFileSystem) this.actualImplFS).withPosixBucket(this.isPosixFSStore);
        }


        this.actualImplFS.initialize(uri, conf);
    }

    // load class to get relate file system
    private static FileSystem getActualFileSystemByClassName(String className)
            throws IOException {
        try {
            Class<?> actualClass = Class.forName(className);
            return (FileSystem) actualClass.newInstance();
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            String errMsg = String.format("load class failed, className: %s", className);
            LOG.error(errMsg, e);
            throw new IOException(errMsg, e);
        }
    }

    @Override
    public Path getHomeDirectory() {
        return this.actualImplFS.getHomeDirectory();
    }

    /**
     * This optional operation is not yet supported for current cos process.
     * but it can be used by posix bucket.
     */
    @Override
    public FSDataOutputStream append(Path f, int bufferSize,
                                     Progressable progress) throws IOException {
        LOG.debug("append file [{}] in COS.", f);
        checkPermission(f, RangerAccessType.WRITE);
        return this.actualImplFS.append(f, bufferSize, progress);
    }

    @Override
    public boolean truncate(Path f, long newLength) throws IOException {
        LOG.debug("truncate file [{}] in COS.", f);
        checkPermission(f, RangerAccessType.WRITE);
        return this.actualImplFS.truncate(f, newLength);
    }

    @Override
    public FSDataOutputStream create(Path f, FsPermission permission,
                                     boolean overwrite,
                                     int bufferSize, short replication,
                                     long blockSize, Progressable progress)
            throws IOException {
        LOG.debug("Creating a new file [{}] in COS.", f);
        checkPermission(f, RangerAccessType.WRITE);
        return this.actualImplFS.create(f, permission, overwrite, bufferSize,
                replication, blockSize, progress);
    }


    @Override
    public boolean delete(Path f, boolean recursive) throws IOException {
        LOG.debug("Ready to delete path: {}. recursive: {}.", f, recursive);
        checkPermission(f, RangerAccessType.DELETE);
        return this.actualImplFS.delete(f, recursive);
    }

    @Override
    public FileStatus getFileStatus(Path f) throws IOException {
        LOG.debug("Get file status: {}.", f);
        checkPermission(f, RangerAccessType.READ);
        return this.actualImplFS.getFileStatus(f);
    }

    @Override
    public URI getUri() {
        return this.actualImplFS.getUri();
    }

    /**
     * <p>
     * If <code>f</code> is a file, this method will make a single call to`
     * COS. If <code>f</code> is
     * a directory, this method will make a maximum of ( <i>n</i> / 199) + 2
     * calls to cos, where
     * <i>n</i> is the total number of files and directories contained
     * directly in <code>f</code>.
     * </p>a
     */
    @Override
    public FileStatus[] listStatus(Path f) throws FileNotFoundException, IOException {
        LOG.debug("list status:" + f);
        checkPermission(f, RangerAccessType.LIST);
        return this.actualImplFS.listStatus(f);
    }

    @Override
    public boolean mkdirs(Path f, FsPermission permission)
            throws IOException {
        LOG.debug("mkdirs path: {}.", f);
        checkPermission(f, RangerAccessType.WRITE);
        return this.actualImplFS.mkdirs(f, permission);
    }

    @Override
    public FSDataInputStream open(Path f, int bufferSize) throws IOException {
        LOG.debug("Open file [{}] to read, buffer [{}]", f, bufferSize);
        checkPermission(f, RangerAccessType.READ);
        return this.actualImplFS.open(f, bufferSize);
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        LOG.debug("Rename the source path [{}] to the dest path [{}].", src, dst);
        checkPermission(src, RangerAccessType.DELETE);
        checkPermission(dst, RangerAccessType.WRITE);
        return this.actualImplFS.rename(src, dst);
    }

    @Override
    public long getDefaultBlockSize() {
        return this.actualImplFS.getDefaultBlockSize();
    }

    /**
     * Set the working directory to the given directory.
     */
    @Override
    public void setWorkingDirectory(Path newDir) {
        this.workingDir = newDir;
        this.actualImplFS.setWorkingDirectory(newDir);
    }

    @Override
    public Path getWorkingDirectory() {
        return this.workingDir;
    }

    @Override
    public FileChecksum getFileChecksum(Path f, long length) throws IOException {
        LOG.debug("call the checksum for the path: {}.", f);
        checkPermission(f, RangerAccessType.READ);
        Preconditions.checkArgument(length >= 0);
        return this.actualImplFS.getFileChecksum(f, length);
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
        checkPermission(f, RangerAccessType.WRITE);
        this.actualImplFS.setXAttr(f, name, value, flag);
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
        checkPermission(f, RangerAccessType.READ);
        return this.actualImplFS.getXAttr(f, name);
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
        checkPermission(f, RangerAccessType.READ);
        return this.actualImplFS.getXAttrs(f, names);
    }

    @Override
    public Map<String, byte[]> getXAttrs(Path f) throws IOException {
        LOG.debug("get XAttrs: {}.", f);
        checkPermission(f, RangerAccessType.READ);
        return this.actualImplFS.getXAttrs(f);
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
        checkPermission(f, RangerAccessType.WRITE);
        this.actualImplFS.removeXAttr(f, name);
    }

    @Override
    public List<String> listXAttrs(Path f) throws IOException {
        LOG.debug("list XAttrs: {}.", f);
        checkPermission(f, RangerAccessType.READ);
        return this.actualImplFS.listXAttrs(f);
    }

    @Override
    public Token<?> getDelegationToken(String renewer) throws IOException {
        LOG.info("getDelegationToken, renewer: {}, stack: {}",
                renewer, Arrays.toString(Thread.currentThread().getStackTrace()).replace(',', '\n'));
        if (rangerQcloudObjectStorageStorageClient != null) {
            return rangerQcloudObjectStorageStorageClient.getDelegationToken(renewer);
        }
        return super.getDelegationToken(renewer);
    }

    public NativeFileSystemStore getStore() {
        return this.nativeStore;
    }

    // exclude the ofs original config, filter the ofs config with COSN_CONFIG_TRANSFER_PREFIX
    private void transferOfsConfig() {
        // 1. list to get transfer prefix ofs config
        Map<String, String> tmpConf = new HashMap<>();
        for (Map.Entry<String, String> entry : this.getConf()) {
            if (entry.getKey().startsWith(Constants.COSN_OFS_CONFIG_PREFIX)) {
                this.getConf().unset(entry.getKey());
            }
            if (entry.getKey().startsWith(Constants.COSN_CONFIG_TRANSFER_PREFIX)) {
                int pos = Constants.COSN_CONFIG_TRANSFER_PREFIX.length();
                String subConfigKey = entry.getKey().substring(pos);
                tmpConf.put(subConfigKey, entry.getValue());
            }
        }

        // 2. trim the prefix and overwrite the config
        for (Map.Entry<String, String> entry : tmpConf.entrySet()) {
            LOG.info("Transfer the ofs config. key: {}, value: {}.", entry.getKey(), entry.getValue());
            this.getConf().set(entry.getKey(), entry.getValue());
        }
    }

    // CHDFS Support Only
    public void releaseFileLock(Path f) throws IOException {
        LOG.debug("Release the file lock: {}.", f);
        if (this.actualImplFS instanceof CHDFSHadoopFileSystemAdapter) {
            ((CHDFSHadoopFileSystemAdapter) this.actualImplFS).releaseFileLock(f);
        } else {
            throw new UnsupportedOperationException("Not supported currently");
        }
    }

    @Override
    public String getCanonicalServiceName() {
        if (rangerQcloudObjectStorageStorageClient != null) {
            return rangerQcloudObjectStorageStorageClient.getCanonicalServiceName();
        }
        return null;
    }

    private void initRangerClientImpl(Configuration conf) throws IOException {
        Class<?>[] cosClasses = CosNUtils.loadCosProviderClasses(
                conf,
                CosNConfigKeys.COSN_CREDENTIALS_PROVIDER);

        if (cosClasses.length == 0) {
            this.enableRangerPluginPermissionCheck = false;
            return;
        }

        for (Class<?> credClass : cosClasses) {
            if (credClass.getName().contains(RangerCredentialsProvider.class.getName())) {
                this.enableRangerPluginPermissionCheck = true;
                break;
            }
        }

        if (!this.enableRangerPluginPermissionCheck) {
            return;
        }

        Class<?> rangerClientImplClass = conf.getClass(CosNConfigKeys.COSN_RANGER_PLUGIN_CLIENT_IMPL, null);
        if (rangerClientImplClass == null) {
            try {
                rangerClientImplClass = conf.getClassByName(CosNConfigKeys.DEFAULT_COSN_RANGER_PLUGIN_CLIENT_IMPL);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        }

        if (rangerQcloudObjectStorageStorageClient == null) {
            synchronized (CosFileSystem.class) {
                if (rangerQcloudObjectStorageStorageClient == null) {
                    try {
                        RangerQcloudObjectStorageClient tmpClient =
                                (RangerQcloudObjectStorageClient) rangerClientImplClass.newInstance();
                        tmpClient.init(conf);
                        rangerQcloudObjectStorageStorageClient = tmpClient;
                    } catch (Exception e) {
                        LOG.error(String.format("init %s failed", CosNConfigKeys.COSN_RANGER_PLUGIN_CLIENT_IMPL), e);
                        throw new IOException(String.format("init %s failed",
                                CosNConfigKeys.COSN_RANGER_PLUGIN_CLIENT_IMPL), e);
                    }
                }
            }
        }

    }

    private void checkPermission(Path f, RangerAccessType rangerAccessType) throws IOException {
        if (!this.enableRangerPluginPermissionCheck) {
            return;
        }

        AccessType accessType = null;
        switch (rangerAccessType) {
            case LIST:
                accessType = AccessType.LIST;
                break;
            case WRITE:
                accessType = AccessType.WRITE;
                break;
            case READ:
                accessType = AccessType.READ;
                break;
            case DELETE:
                accessType = AccessType.DELETE;
                break;
            default:
                throw new IOException(String.format("unknown access type %s", rangerAccessType.toString()));
        }

        Path absolutePath = makeAbsolute(f);
        String allowKey = CosNFileSystem.pathToKey(absolutePath);
        if (allowKey.startsWith("/")) {
            allowKey = allowKey.substring(1);
        }

        PermissionRequest permissionReq = new PermissionRequest(ServiceType.COS, accessType,
                CosNUtils.getBucketNameWithoutAppid(this.bucket, this.getConf().get(CosNConfigKeys.COSN_APPID_KEY)),
                allowKey, "", "");
        boolean allowed = false;
        PermissionResponse permission = rangerQcloudObjectStorageStorageClient.checkPermission(permissionReq);
        if (permission != null) {
            allowed = permission.isAllowed();
        }
        if (!allowed) {
            throw new IOException(String.format("Permission denied, [key: %s], [user: %s], [operation: %s]",
                    allowKey, this.userGroupInformation.getShortUserName(), rangerAccessType.name()));
        }
    }

    private Path makeAbsolute(Path path) {
        if (path.isAbsolute()) {
            return path;
        }
        return new Path(workingDir, path);
    }


    @Override
    public void close() throws IOException {
        LOG.info("begin to close cos file system");
        this.actualImplFS.close();
        if (null != this.nativeStore && this.isDefaultNativeStore) {
            this.nativeStore.close();
        }
    }
}

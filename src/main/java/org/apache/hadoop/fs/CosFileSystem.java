package org.apache.hadoop.fs;

import com.google.common.base.Preconditions;
import com.qcloud.cos.model.HeadBucketResult;
import com.qcloud.chdfs.permission.RangerAccessType;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.auth.RangerCredentialsProvider;
import org.apache.hadoop.fs.cosn.ranger.client.RangerQcloudObjectStorageClient;
import org.apache.hadoop.fs.cosn.ranger.security.authorization.AccessType;
import org.apache.hadoop.fs.cosn.ranger.security.authorization.PermissionRequest;
import org.apache.hadoop.fs.cosn.ranger.security.authorization.PermissionResponse;
import org.apache.hadoop.fs.cosn.ranger.security.authorization.ServiceType;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryProxy;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
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
 *
 * todo: for new version cos file system is the shell which using the head bucket
 * to decide whether normal bucket or merge bucket, if it is merge bucket
 * direct use the network shell implements.
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public class CosFileSystem extends FileSystem {
    static final Logger LOG = LoggerFactory.getLogger(CosFileSystem.class);

    static final String SCHEME = "cosn";
    static final String PATH_DELIMITER = Path.SEPARATOR;

    static final Charset METADATA_ENCODING = StandardCharsets.UTF_8;
    // The length of name:value pair should be less than or equal to 1024 bytes.
    static final int MAX_XATTR_SIZE = 1024;

    private URI uri;
    private String bucket;
    private boolean isMergeBucket;
    private boolean checkMergeBucket;
    private Path workingDir;
    private String owner = "Unknown";
    private String group = "Unknown";
    private NativeFileSystemStore store;

    private UserGroupInformation currentUser;
    private boolean enableRangerPluginPermissionCheck = false;
    public static RangerQcloudObjectStorageClient rangerQcloudObjectStorageStorageClient = null;

    private String formatBucket;
    private FileSystem actualImplFS = null;


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
        setConf(conf);

        UserGroupInformation.setConfiguration(conf);
        this.currentUser = UserGroupInformation.getCurrentUser();

        // use the topper cos ranger. ignore the ofs ranger
        initRangerClientImpl(conf);

        this.bucket = uri.getHost();
        this.formatBucket = CosNUtils.formatBucket(uri.getHost(), conf);

        if (this.store == null) {
            this.store = createDefaultStore(conf);
        }

        // this store no need to init twice.
        this.store.initialize(uri, conf);
        this.uri = URI.create(uri.getScheme() + "://" + uri.getAuthority());
        this.workingDir =
                new Path("/user", System.getProperty("user.name"))
                        .makeQualified(this.uri, this.getWorkingDirectory());
        this.owner = getOwnerId();
        this.group = getGroupId();
        LOG.debug("uri: {}, bucket: {}, working dir: {}, owner: {}, group: {}.\n" +
                        "configuration: {}.",
                uri, bucket, workingDir, owner, group, conf);

        this.isMergeBucket = false;
        // for now all need to head bucket to check whether is the merge bucket
        this.checkMergeBucket = true;
        // head the bucket to judge whether the merge bucket
        HeadBucketResult headBucketResult = this.store.headBucket(this.bucket);

        LOG.info("cos file system bucket is merged {}", this.isMergeBucket);
        if (headBucketResult.isMergeBucket()) { // ofs implements
            this.actualImplFS = getActualFileSystemByClassName("com.qcloud.chdfs.fs.CHDFSHadoopFileSystemAdapter");
            this.isMergeBucket = true;
            this.store.setMergeBucket(true);
        } else { // normal cos hadoop file system implements
            this.actualImplFS = getActualFileSystemByClassName("org.apache.hadoop.fs.CosHadoopFileSystem");
            ((CosHadoopFileSystem) this.actualImplFS).setNativeFileSystemStore(this.store, this.bucket, this.uri,
                    this.owner, this.group);
            this.actualImplFS.setWorkingDirectory(this.workingDir);
        }

        if (this.actualImplFS == null) {
            // should never reach here
            throw new IOException("impl file system is null");
        }
        this.actualImplFS.initialize(uri, conf);
    }

    // load class to get relate file system
    private static FileSystem getActualFileSystemByClassName(String className) throws IOException {
        try {
            Class<?> actualClass = Class.forName(className);
            return (FileSystem)actualClass.newInstance();
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            String errMsg = String.format("load class failed, className: %s", className);
            LOG.error(errMsg, e);
            throw new IOException(errMsg, e);
        }
    }

    public static NativeFileSystemStore createDefaultStore(Configuration conf) {
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

        return (NativeFileSystemStore) RetryProxy.create(NativeFileSystemStore.class, store,
                methodNameToPolicyMap);
    }

    private String getOwnerId() {
        UserGroupInformation currentUgi = null;
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
        } catch (InterruptedException e) {
            LOG.error("getOwnerInfo occur a exception", e);
        } catch (IOException e) {
            LOG.error("getOwnerInfo occur a exception", e);
        }
        return ownerInfoId;
    }


    @Override
    public Path getHomeDirectory() {
        String homePrefix = this.getConf().get("dfs.user.home.dir.prefix");
        if (null != homePrefix) {
            return makeQualified(new Path(homePrefix + "/" + getOwnerId()));
        }

        return super.getHomeDirectory();
    }

    /**
     * This optional operation is not yet supported.
     */
    @Override
    public FSDataOutputStream append(Path f, int bufferSize,
                                     Progressable progress) throws IOException {
        checkPermission(f, RangerAccessType.WRITE);
        return this.actualImplFS.append(f, bufferSize, progress);
    }

    @Override
    public FSDataOutputStream create(Path f, FsPermission permission,
                                     boolean overwrite,
                                     int bufferSize, short replication,
                                     long blockSize, Progressable progress)
            throws IOException {
        checkPermission(f, RangerAccessType.WRITE);
        return this.actualImplFS.create(f, permission, overwrite, bufferSize,
                replication, blockSize, progress);
    }


    @Override
    public boolean delete(Path f, boolean recursive) throws IOException {
        checkPermission(f, RangerAccessType.DELETE);
        return this.actualImplFS.delete(f, recursive);
    }

    @Override
    public FileStatus getFileStatus(Path f) throws IOException {
        return this.actualImplFS.getFileStatus(f);
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
    public FileStatus[] listStatus(Path f) throws FileNotFoundException, IOException {
        checkPermission(f, RangerAccessType.LIST);
        return this.actualImplFS.listStatus(f);
    }


    @Override
    public boolean mkdirs(Path f, FsPermission permission)
            throws IOException {
        checkPermission(f, RangerAccessType.WRITE);
        return this.actualImplFS.mkdirs(f, permission);
    }

    @Override
    public FSDataInputStream open(Path f, int bufferSize) throws IOException {
        checkPermission(f, RangerAccessType.READ);
        return this.actualImplFS.open(f, bufferSize);
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
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
        workingDir = newDir;
    }

    @Override
    public Path getWorkingDirectory() {
        return workingDir;
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

    @Override
    public String getCanonicalServiceName() {
        if (rangerQcloudObjectStorageStorageClient != null) {
            return rangerQcloudObjectStorageStorageClient.getCanonicalServiceName();
        }
        return null;
    }

    @Override
    public FileChecksum getFileChecksum(Path f, long length) throws IOException {
        Preconditions.checkArgument(length >= 0);

        // The order of each file, must support both crc at same time, how to tell the difference crc request?
        checkPermission(f, RangerAccessType.READ);
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
        checkPermission(f, RangerAccessType.READ);
        return this.actualImplFS.getXAttrs(f, names);
    }

    @Override
    public Map<String, byte[]> getXAttrs(Path f) throws IOException {
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
        checkPermission(f, RangerAccessType.WRITE);
        this.actualImplFS.removeXAttr(f, name);
    }

    @Override
    public List<String> listXAttrs(Path f) throws IOException {
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
        String allowKey = CosHadoopFileSystem.pathToKey(absolutePath);
        if (allowKey.startsWith("/")) {
            allowKey = allowKey.substring(1);
        }


        PermissionRequest permissionReq = new PermissionRequest(ServiceType.COS, accessType,
                this.formatBucket, allowKey, "", "");
        boolean allowed = false;
        PermissionResponse permission = rangerQcloudObjectStorageStorageClient.checkPermission(permissionReq);
        if (permission != null) {
            allowed = permission.isAllowed();
        }
        if (!allowed) {
            throw new IOException(String.format("Permission denied, [key: %s], [user: %s], [operation: %s]",
                    allowKey, currentUser.getShortUserName(), rangerAccessType.name()));
        }
    }

    @Override
    public void close() throws IOException {
        this.actualImplFS.close();
    }

    public NativeFileSystemStore getStore() {
        return this.store;
    }

    private Path makeAbsolute(Path path) {
        if (path.isAbsolute()) {
            return path;
        }
        return new Path(workingDir, path);
    }
}

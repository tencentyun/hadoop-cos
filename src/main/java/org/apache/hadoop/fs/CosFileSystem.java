package org.apache.hadoop.fs;

import com.google.common.base.Preconditions;
import com.qcloud.chdfs.fs.CHDFSHadoopFileSystemAdapter;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.cosn.Constants;
import org.apache.hadoop.fs.cosn.OperationCancellingStatusProvider;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.qcloud.chdfs.permission.RangerAccessType;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.hadoop.fs.CosNUtils.propagateBucketOptions;


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
    private volatile boolean initialized = false;
    private boolean isPosixUseOFSRanger;
    private boolean isPosixImpl = false;
    private FileSystem actualImplFS = null;

    private URI uri;
    private Path workingDir;
    // Authorization related.

    private RangerCredentialsClient rangerCredentialsClient;

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
    public void initialize(URI uri, Configuration originalConf) throws IOException {
        String bucket = uri.getHost();
        Configuration conf = propagateBucketOptions(originalConf, bucket);

        super.initialize(uri, conf);
        setConf(conf);
        // initialize the things authorization related.
        UserGroupInformation.setConfiguration(conf);

        this.uri = URI.create(uri.getScheme() + "://" + uri.getAuthority());
        this.workingDir = new Path("/user", System.getProperty("user.name"))
                .makeQualified(this.uri, this.getWorkingDirectory());

        if (null == this.nativeStore) {
            this.nativeStore = CosNUtils.createDefaultStore(conf);
            // init ranger client inside the native store
            this.nativeStore.initialize(uri, conf);
            this.isDefaultNativeStore = true;
        }
        this.rangerCredentialsClient = this.nativeStore.getRangerCredentialsClient();
        this.isPosixUseOFSRanger = this.getConf().
                getBoolean(CosNConfigKeys.COSN_POSIX_BUCKET_USE_OFS_RANGER_ENABLED,
                        CosNConfigKeys.DEFAULT_COSN_POSIX_BUCKET_USE_OFS_RANGER_ENABLED);

        // required checkCustomAuth if ranger is enabled and custom authentication is enabled
        checkCustomAuth(conf);

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

            LOG.info("The posix bucket [{}] use the class [{}] as the filesystem implementation, " +
                    "use each ranger [{}]", bucket, posixBucketFSImpl, this.isPosixUseOFSRanger);
            // if ofs impl.
            // network version start from the 2.7.
            // sdk version start from the 1.0.4.
            this.actualImplFS = getActualFileSystemByClassName(posixBucketFSImpl);

            // judge normal impl first, skip the class nodef error when only use normal bucket
            // outside can use native store to tell whether is posix bucket, not need head bucket twice. can be used by flink cos.
            this.nativeStore.setPosixBucket(true);
            if (this.actualImplFS instanceof CosNFileSystem) {
                ((CosNFileSystem) this.actualImplFS).withStore(this.nativeStore).withBucket(bucket)
                        .withPosixBucket(isPosixFSStore).withRangerCredentialsClient(rangerCredentialsClient);
            } else if (this.actualImplFS instanceof CHDFSHadoopFileSystemAdapter) {
                this.isPosixImpl = true;
                // judge whether ranger client contains policy url or other config need to pass to ofs
                this.passThroughRangerConfig();
                // before the init, must transfer the config and disable the range in ofs
                this.transferOfsConfig();
                // not close native store here, emr use.
            } else {
                // Another class
                throw new IOException(
                        String.format("The posix bucket does not currently support the implementation [%s].",
                                posixBucketFSImpl));
            }
        } else { // normal cos hadoop file system implements
            this.actualImplFS = getActualFileSystemByClassName(conf.get(CosNConfigKeys.COSN_BUCKET_FS_IMPL,
                    CosNConfigKeys.DEFAULT_COSN_BUCKET_FS_IMPL));
            this.nativeStore.setPosixBucket(false);
            ((CosNFileSystem) this.actualImplFS).withStore(this.nativeStore).withBucket(bucket)
                    .withPosixBucket(this.isPosixFSStore).withRangerCredentialsClient(rangerCredentialsClient);
        }


        this.actualImplFS.initialize(uri, conf);
        // init status
        this.initialized = true;
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
        checkInitialized();
        checkPermission(f, RangerAccessType.WRITE);
        return this.actualImplFS.append(f, bufferSize, progress);
    }

    @Override
    public boolean truncate(Path f, long newLength) throws IOException {
        LOG.debug("truncate file [{}] in COS.", f);
        checkInitialized();
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
        checkInitialized();
        checkPermission(f, RangerAccessType.WRITE);
        return this.actualImplFS.create(f, permission, overwrite, bufferSize,
                replication, blockSize, progress);
    }


    @Override
    public boolean delete(Path f, boolean recursive) throws IOException {
        LOG.debug("Ready to delete path: {}. recursive: {}.", f, recursive);
        checkInitialized();
        checkPermission(f, RangerAccessType.DELETE);
        return this.actualImplFS.delete(f, recursive);
    }

    @Override
    public FileStatus getFileStatus(Path f) throws IOException {
        return getFileStatus(f, false);
    }

    /**
     * @param mustExist, 如果为true，代表默认LIST-1必然非空，用于优化默认getFileStatus性能。
     */
    public FileStatus getFileStatus(Path f, boolean mustExist) throws IOException {
        LOG.debug("Get file status: {}, mustExist: {}.", f, mustExist);
        checkInitialized();
        // keep same not change ranger permission here
        if (this.actualImplFS instanceof  CosNFileSystem) {
            return ((CosNFileSystem) this.actualImplFS).getFileStatus(f, mustExist);
        }
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
        checkInitialized();
        checkPermission(f, RangerAccessType.LIST);
        return this.actualImplFS.listStatus(f);
    }

    @Override
    public boolean mkdirs(Path f, FsPermission permission)
            throws IOException {
        LOG.debug("mkdirs path: {}.", f);
        checkInitialized();
        checkPermission(f, RangerAccessType.WRITE);
        return this.actualImplFS.mkdirs(f, permission);
    }

    @Override
    public FSDataInputStream open(Path f, int bufferSize) throws IOException {
        LOG.debug("Open file [{}] to read, buffer [{}]", f, bufferSize);
        checkInitialized();
        checkPermission(f, RangerAccessType.READ);
        return this.actualImplFS.open(f, bufferSize);
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        LOG.debug("Rename the source path [{}] to the dest path [{}].", src, dst);
        checkInitialized();
        renameCheckPermission(src);
        checkPermission(dst, RangerAccessType.WRITE);
        return this.actualImplFS.rename(src, dst);
    }

    private void renameCheckPermission(Path src) throws IOException {
        if (useOFSRanger()) {
            return;
        }
        if (this.isPosixImpl) {
            checkPermission(src, RangerAccessType.WRITE);
        } else {
            checkPermission(src, RangerAccessType.DELETE);
        }
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
        checkInitialized();
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
        checkInitialized();
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
        checkInitialized();
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
        checkInitialized();
        checkPermission(f, RangerAccessType.READ);
        return this.actualImplFS.getXAttrs(f, names);
    }

    @Override
    public Map<String, byte[]> getXAttrs(Path f) throws IOException {
        LOG.debug("get XAttrs: {}.", f);
        checkInitialized();
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
        checkInitialized();
        checkPermission(f, RangerAccessType.WRITE);
        this.actualImplFS.removeXAttr(f, name);
    }

    @Override
    public List<String> listXAttrs(Path f) throws IOException {
        LOG.debug("list XAttrs: {}.", f);
        checkInitialized();
        checkPermission(f, RangerAccessType.READ);
        return this.actualImplFS.listXAttrs(f);
    }

    @Override
    public Token<?> getDelegationToken(String renewer) throws IOException {
        LOG.info("getDelegationToken, renewer: {}, stack: {}",
                renewer, Arrays.toString(Thread.currentThread().getStackTrace()).replace(',', '\n'));
        if (useOFSRanger()) {
            return this.actualImplFS.getDelegationToken(renewer);
        }
        Token<?> token = this.rangerCredentialsClient.doGetDelegationToken(renewer);
        if (token != null)
            return token;
        return super.getDelegationToken(renewer);
    }

    @Override
    public void createSymlink(Path target, Path link, boolean createParent)
            throws AccessControlException, FileAlreadyExistsException, FileNotFoundException,
            ParentNotDirectoryException, UnsupportedFileSystemException, IOException {
        LOG.debug("Create a symlink [{}] for the path [{}]. createParent: {}.", link, target, createParent);
        checkPermission(link, RangerAccessType.WRITE);
        this.actualImplFS.createSymlink(target, link, createParent);
    }

    @Override
    public FileStatus getFileLinkStatus(Path f)
            throws AccessControlException, FileNotFoundException,
            UnsupportedFileSystemException, IOException {
        LOG.debug("Get the link [{}]'s status.", f);
        checkPermission(f, RangerAccessType.READ);
        return this.actualImplFS.getFileLinkStatus(f);
    }

    @Override
    public boolean supportsSymlinks() {
        return this.actualImplFS.supportsSymlinks();
    }

    @Override
    public Path getLinkTarget(Path f) throws IOException {
        LOG.debug("Get the target path the link [{}] refer.", f);
        checkPermission(f, RangerAccessType.READ);
        return this.actualImplFS.getLinkTarget(f);
    }

    // some other implements of ofs, for now only support
    // for posix bucket. contain the getContentSummary, setOwner,setPermission,setTimes
    @Override
    public ContentSummary getContentSummary(Path f) throws IOException {
        LOG.debug("get content summary: {}.", f);
        checkInitialized();
        return this.actualImplFS.getContentSummary(f);
    }

    @Override
    public void setOwner(Path p, String userName, String groupName) throws IOException {
        LOG.debug("set owner, path: {}, userName: {}, groupName: {}", p, userName, groupName);
        checkInitialized();
        checkPermission(p, RangerAccessType.WRITE);
        this.actualImplFS.setOwner(p, userName, groupName);
    }

    @Override
    public void setPermission(Path p, FsPermission permission) throws IOException {
        LOG.debug("set permission, path :{}", p);
        checkInitialized();
        checkPermission(p, RangerAccessType.WRITE);
        this.actualImplFS.setPermission(p, permission);
    }

    @Override
    public void setTimes(Path p, long mtime, long atime) throws IOException {
        LOG.debug("set times, path :{}, mtime: {}, atime: {}", p, mtime, atime);
        checkInitialized();
        checkPermission(p, RangerAccessType.WRITE);
        this.actualImplFS.setTimes(p, mtime, atime);
    }

    @Override
    public void setAcl(Path path, List<AclEntry> aclSpec) throws IOException {
        LOG.debug("set acl, path: {}, aclSpec: {}", path, aclSpec);
        checkInitialized();
        checkPermission(path, RangerAccessType.WRITE);
        this.actualImplFS.setAcl(path, aclSpec);
    }

    @Override
    public AclStatus getAclStatus(Path path) throws IOException {
        LOG.debug("getAclStatus, path: {}", path);
        checkInitialized();
        checkPermission(path, RangerAccessType.READ);
        return this.actualImplFS.getAclStatus(path);
    }

    @Override
    public void modifyAclEntries(Path path, List<AclEntry> aclSpec) throws IOException {
        LOG.debug("modifyAclEntries, path: {}, aclSpec: {}", path, aclSpec);
        checkInitialized();
        checkPermission(path, RangerAccessType.WRITE);
        this.actualImplFS.modifyAclEntries(path, aclSpec);
    }

    @Override
    public void removeAclEntries(Path path, List<AclEntry> aclSpec) throws IOException {
        LOG.debug("removeAclEntries, path: {}, aclSpec: {}", path, aclSpec);
        checkInitialized();
        checkPermission(path, RangerAccessType.WRITE);
        this.actualImplFS.removeAclEntries(path, aclSpec);
    }

    @Override
    public void removeDefaultAcl(Path path) throws IOException {
        LOG.debug("removeDefaultAcl, path: {}", path);
        checkInitialized();
        checkPermission(path, RangerAccessType.WRITE);
        this.actualImplFS.removeDefaultAcl(path);
    }

    @Override
    public void removeAcl(Path path) throws IOException {
        LOG.debug("removeAcl, path: {}", path);
        checkInitialized();
        checkPermission(path, RangerAccessType.WRITE);
        this.actualImplFS.removeAcl(path);
    }

    public NativeFileSystemStore getStore() {
        return this.nativeStore;
    }

    // pass ofs ranger client config to ofs
    private void passThroughRangerConfig() {
        // ofs ranger init get ranger policy auto
        String ofsRangerKey = Constants.COSN_CONFIG_TRANSFER_PREFIX.
                concat(Constants.COSN_POSIX_BUCKCET_OFS_RANGER_FLAG);
        String ofsRangerValue = this.getConf().get(ofsRangerKey);
        if (null == ofsRangerValue || ofsRangerValue.isEmpty()) {
            if (useOFSRanger()) {
                // set ofs ranger open
                this.getConf().setBoolean(ofsRangerKey, true);
                return;
            } else {
                // set false, avoid sdk change the default value
                this.getConf().setBoolean(ofsRangerKey, false);
            }
        } else {
            LOG.info("ofs transfer config already exist, transfer key {}, value {}",
                    ofsRangerKey, ofsRangerValue);
        }

        if (!this.rangerCredentialsClient.isEnableRangerPluginPermissionCheck()) {
            LOG.info("not enable ranger plugin permission check");
            return;
        }

        if (this.rangerCredentialsClient.getRangerPolicyUrl() != null) {
            String policyUrlKey = Constants.COSN_CONFIG_TRANSFER_PREFIX.
                    concat(Constants.COSN_POSIX_BUCKET_RANGER_POLICY_URL);
            String policyUrlValue = this.getConf().get(policyUrlKey);
            if (policyUrlValue == null || policyUrlValue.isEmpty()) {
                this.getConf().set(policyUrlKey, this.rangerCredentialsClient.getRangerPolicyUrl());
            } else {
                LOG.info("ofs transfer config already exist, transfer key {}, value {}",
                        policyUrlKey, policyUrlValue);
            }
        }

        if (this.rangerCredentialsClient.getAuthJarMd5() != null) {
            String authJarMd5Key = Constants.COSN_CONFIG_TRANSFER_PREFIX.
                    concat(Constants.COSN_POSIX_BUCKET_RANGER_AUTH_JAR_MD5);
            String authJarMd5Value = this.getConf().get(authJarMd5Key);
            if (authJarMd5Value == null || authJarMd5Value.isEmpty()) {
                this.getConf().set(authJarMd5Key, this.rangerCredentialsClient.getAuthJarMd5());
            } else {
                LOG.info("ofs transfer config already exist, transfer key {}, value {}",
                        authJarMd5Key, authJarMd5Value);
            }
        }
    }

    private HashMap<String, String> getPOSIXBucketConfigMap() {
        HashMap<String, String> configMap = new HashMap<>();
        configMap.put(CosNConfigKeys.COSN_APPID_KEY, Constants.COSN_POSIX_BUCKET_APPID_CONFIG);
        configMap.put(CosNConfigKeys.COSN_REGION_KEY, Constants.COSN_POSIX_BUCKET_REGION_CONFIG);
        configMap.put(CosNConfigKeys.COSN_REGION_PREV_KEY, Constants.COSN_POSIX_BUCKET_REGION_CONFIG);
        configMap.put(CosNConfigKeys.COSN_SERVER_SIDE_ENCRYPTION_ALGORITHM, Constants.COSN_POSIX_BUCKET_SSE_MODE);
        configMap.put(CosNConfigKeys.COSN_SERVER_SIDE_ENCRYPTION_CONTEXT, Constants.COSN_POSIX_BUCKET_SSE_KMS_CONTEXT);
        return configMap;
    }

    // exclude the ofs original config, filter the ofs config with COSN_CONFIG_TRANSFER_PREFIX
    private void transferOfsConfig() {
        // cosn config -> ofs config -> trsf ofs config
        HashMap<String, String> configMap = getPOSIXBucketConfigMap();
        for (String org : configMap.keySet()) {
            String content = this.getConf().get(org);
            if (null != content && !content.isEmpty()) {
                String transferKey = Constants.COSN_CONFIG_TRANSFER_PREFIX.
                        concat(configMap.get(org));
                // if ofs transfer appid set we ignore it, then we can use appid to version control
                String transferContent = this.getConf().get(transferKey);
                if (null != transferContent && !transferContent.isEmpty()) {
                    LOG.info("ofs transfer config already exist, transfer key {}, value {}",
                            transferKey, transferContent);
                    continue;
                }
                this.getConf().set(transferKey, content);
            }
        }

        // according to different sse mode to set relate key
        String sseAlgortithm = this.getConf().
                get(CosNConfigKeys.COSN_SERVER_SIDE_ENCRYPTION_ALGORITHM, "");
        if (null != sseAlgortithm && !sseAlgortithm.isEmpty()) {
            String sseKey = this.getConf().get(CosNConfigKeys.COSN_SERVER_SIDE_ENCRYPTION_KEY);
            if (null != sseKey && !sseKey.isEmpty()) {
                // judge the algorithm to choose which key config to transfer
                if (sseAlgortithm.equals(Constants.COSN_SSE_MODE_KMS)) {
                    String transferKey = Constants.COSN_CONFIG_TRANSFER_PREFIX.
                            concat(Constants.COSN_POSIX_BUCKET_SSE_KMS_KEYID);
                    this.getConf().set(transferKey, sseKey);
                } else if (sseAlgortithm.equals(Constants.COSN_SSE_MODE_C)) {
                    String transferKey = Constants.COSN_CONFIG_TRANSFER_PREFIX.
                            concat(Constants.COSN_POSIX_BUCKET_SSE_C_KEY);
                    this.getConf().set(transferKey, sseKey);
                }
            }
        }

        // 1. list to get transfer prefix ofs config
        Map<String, String> tmpConf = new HashMap<>();
        for (Map.Entry<String, String> entry : this.getConf()) {
            if (entry.getKey().startsWith(Constants.COSN_OFS_CONFIG_PREFIX)) {
                // here not unset origin ofs config, because if concurrent init file system
                // may remove other file system's config when use same configuration
                // which may cause the init ofs file system failed.
                // change the other way to init, if transfer config exist use it,
                // if not use original ofs config.
                // this.getConf().unset(entry.getKey());
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
        checkInitialized();
        if (this.actualImplFS instanceof CHDFSHadoopFileSystemAdapter) {
            ((CHDFSHadoopFileSystemAdapter) this.actualImplFS).releaseFileLock(f);
        } else {
            throw new UnsupportedOperationException("Not supported currently");
        }
    }

    // CHDFS Support Only
    public void enableSSECos() throws IOException {
        LOG.debug("enable SSE-COS");
        checkInitialized();
        if (this.actualImplFS instanceof CHDFSHadoopFileSystemAdapter) {
            ((CHDFSHadoopFileSystemAdapter) this.actualImplFS).enableSSECos();
        } else {
            throw new UnsupportedOperationException("Not supported currently");
        }
    }

    // CHDFS Support Only
    public void disableSSE() throws IOException {
        LOG.debug("disable SSE");
        checkInitialized();
        if (this.actualImplFS instanceof CHDFSHadoopFileSystemAdapter) {
            ((CHDFSHadoopFileSystemAdapter) this.actualImplFS).disableSSE();
        } else {
            throw new UnsupportedOperationException("Not supported currently");
        }
    }

    // CosNFileSystem Support Only ignore exist file and folder check
    public void disableCreateOpFileExistCheck() throws IOException {
        LOG.debug("create op file exist check");
        checkInitialized();
        if (this.actualImplFS instanceof CosNFileSystem) {
            ((CosNFileSystem) this.actualImplFS).disableCreateOpFileExistCheck();
        } else {
            throw new UnsupportedOperationException("Not supported currently");
        }
    }

    @Override
    public String getCanonicalServiceName() {
        if (useOFSRanger()) {
            return this.actualImplFS.getCanonicalServiceName();
        }
        return this.rangerCredentialsClient.doGetCanonicalServiceName();
    }

    private void checkPermission(Path f, RangerAccessType rangerAccessType) throws IOException {
        if (useOFSRanger()) {
            return;
        }
        this.rangerCredentialsClient.doCheckPermission(f, rangerAccessType, getOwnerId(), getWorkingDirectory());
    }

    private boolean useOFSRanger() {
        if (this.isPosixImpl && this.isPosixUseOFSRanger) {
            return true;
        }
        return false;
    }

    /**
     * @param conf
     * @throws IOException
     */
    private void checkCustomAuth(Configuration conf) throws IOException {
        // todo: need get token first
        this.rangerCredentialsClient.doCheckCustomAuth(conf);
    }

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

    private void checkInitialized() throws IOException {
        if (!this.initialized) {
            throw new IOException("The FileSystem has been closed or not initialized.");
        }
    }

    @Override
    public void close() throws IOException {
        LOG.info("begin to close cos file system");
        this.initialized = false;
        try {
            super.close();
        } finally {
            this.actualImplFS.close();
            if (null != this.nativeStore && this.isDefaultNativeStore) {
                // close range client later, inner native store
                this.nativeStore.close();
            }
        }
    }

    public void setOperationCancellingStatusProvider(OperationCancellingStatusProvider operationCancellingStatusProvider) {
        if (this.actualImplFS instanceof CosNFileSystem) {
            ((CosNFileSystem) this.actualImplFS).setOperationCancellingStatusProvider(operationCancellingStatusProvider);
        } else {
            throw new UnsupportedOperationException(
                    String.format("Not supported currently for the filesystem '%s'.",
                            this.actualImplFS.getClass().getName()));
        }
    }

    public void removeOperationCancelingStatusProvider() {
        if (this.actualImplFS instanceof CosNFileSystem) {
            ((CosNFileSystem) this.actualImplFS).removeOperationCancelingStatusProvider();
        } else {
            throw new UnsupportedOperationException(
                    String.format("Not supported currently for the filesystem '%s'.",
                            this.actualImplFS.getClass().getName()));
        }
    }
}

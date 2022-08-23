package org.apache.hadoop.fs;

import com.qcloud.chdfs.permission.RangerAccessType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.auth.RangerCredentialsProvider;
import org.apache.hadoop.fs.cosn.ranger.client.RangerQcloudObjectStorageClient;
import org.apache.hadoop.fs.cosn.ranger.security.authorization.*;
import org.apache.hadoop.fs.cosn.ranger.security.sts.GetSTSResponse;
import org.apache.hadoop.security.token.Token;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;

import static org.apache.hadoop.fs.cosn.Constants.CUSTOM_AUTHENTICATION;

public class RangerCredentialsClient {
    private static final Logger log = LoggerFactory.getLogger(RangerCredentialsClient.class);

    private Configuration conf;

    private String bucket;

    public static RangerQcloudObjectStorageClient rangerQcloudObjectStorageStorageClient = null;

    private boolean enableRangerPluginPermissionCheck = false;

    private String rangerPolicyUrl;

    private String authJarMd5;

    public RangerCredentialsClient() {
    }

    public void doInitialize(Configuration conf, String bucket) throws IOException {
        this.conf = conf;
        this.bucket = bucket;
        initRangerClientImpl(conf);
    }

    public RangerCredentialsClient withBucket(String bucket) {
        this.bucket = bucket;
        return this;
    }

    public void doCheckPermission(Path f, RangerAccessType rangerAccessType, String ownerId, Path workingDirectory) throws IOException {
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

        Path absolutePath = makeAbsolute(f, workingDirectory);
        String allowKey = CosNFileSystem.pathToKey(absolutePath);
        if (allowKey.startsWith("/")) {
            allowKey = allowKey.substring(1);
        }

        PermissionRequest permissionReq = new PermissionRequest(ServiceType.COS, accessType,
                CosNUtils.getBucketNameWithoutAppid(this.bucket, this.conf.get(CosNConfigKeys.COSN_APPID_KEY)),
                allowKey, "", "");
        boolean allowed = false;
        String checkPermissionActualUserName = ownerId;
        PermissionResponse permission = rangerQcloudObjectStorageStorageClient.checkPermission(permissionReq);
        if (permission != null) {
            allowed = permission.isAllowed();
            if (permission.getRealUserName() != null && !permission.getRealUserName().isEmpty()) {
                checkPermissionActualUserName = permission.getRealUserName();
            }
        }
        if (!allowed) {
            throw new IOException(String.format("Permission denied, [key: %s], [user: %s], [operation: %s]",
                    allowKey, checkPermissionActualUserName, rangerAccessType.name()));
        }
    }

    public void doCheckCustomAuth(Configuration conf) throws IOException {
        if (!this.enableRangerPluginPermissionCheck) {
            return;
        }
        String bucketRegion = conf.get(CosNConfigKeys.COSN_REGION_KEY);
        if (bucketRegion == null || bucketRegion.isEmpty()) {
            bucketRegion = conf.get(CosNConfigKeys.COSN_REGION_PREV_KEY);
        }

        GetSTSResponse stsResp = RangerCredentialsClient.rangerQcloudObjectStorageStorageClient.getSTS(bucketRegion,
                bucket);
        if (!stsResp.isCheckAuthPass()) {
            throw new IOException(String.format("Permission denied, [operation: %s], please check user and " +
                    "password", CUSTOM_AUTHENTICATION));
        }
    }

    public Token<?> doGetDelegationToken(String renewer) throws IOException {
        log.info("getDelegationToken, renewer: {}, stack: {}",
                renewer, Arrays.toString(Thread.currentThread().getStackTrace()).replace(',', '\n'));
        if (RangerCredentialsClient.rangerQcloudObjectStorageStorageClient != null) {
            return RangerCredentialsClient.rangerQcloudObjectStorageStorageClient.getDelegationToken(renewer);
        }
        return null;
    }

    public String doGetCanonicalServiceName() {
        if (RangerCredentialsClient.rangerQcloudObjectStorageStorageClient != null) {
            return RangerCredentialsClient.rangerQcloudObjectStorageStorageClient.getCanonicalServiceName();
        }
        return null;
    }

    private Path makeAbsolute(Path path, Path workingDirectory) {
        if (path.isAbsolute()) {
            return path;
        }
        return new Path(workingDirectory, path);
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

        if (RangerCredentialsClient.rangerQcloudObjectStorageStorageClient == null) {
            synchronized (RangerCredentialsClient.class) {
                if (RangerCredentialsClient.rangerQcloudObjectStorageStorageClient == null) {
                    try {
                        RangerQcloudObjectStorageClient tmpClient =
                                (RangerQcloudObjectStorageClient) rangerClientImplClass.newInstance();
                        tmpClient.init(conf);
                        RangerCredentialsClient.rangerQcloudObjectStorageStorageClient = tmpClient;

                        // set ranger policy url and other auth info.
                        // when use posix mode to query bucket. server side also auth the policy url.
                        // so need to pass these configurations to ofs java sdk which carried on when mount fs.
                        RangerAuthPolicyResponse rangerAuthPolicyResp =
                                rangerQcloudObjectStorageStorageClient.getRangerAuthPolicy();
                        if (rangerAuthPolicyResp != null) {
                            if (rangerAuthPolicyResp.getRangerPolicyUrl() != null) {
                                this.rangerPolicyUrl = rangerAuthPolicyResp.getRangerPolicyUrl();
                            }
                            if (rangerAuthPolicyResp.getAuthJarMd5() != null) {
                                this.authJarMd5 = rangerAuthPolicyResp.getAuthJarMd5();
                            }
                        }
                    } catch (Exception e) {
                        log.error(String.format("init %s failed", CosNConfigKeys.COSN_RANGER_PLUGIN_CLIENT_IMPL), e);
                        throw new IOException(String.format("init %s failed",
                                CosNConfigKeys.COSN_RANGER_PLUGIN_CLIENT_IMPL), e);
                    }
                }
            }
        }
    } // end of init ranger impl

    // must call after init
    public String getRangerPolicyUrl() {
        return this.rangerPolicyUrl;
    }

    public String getAuthJarMd5() {
        return this.authJarMd5;
    }

    public boolean isEnableRangerPluginPermissionCheck() {
        return this.enableRangerPluginPermissionCheck;
    }

    public void close() {
        if (RangerCredentialsClient.rangerQcloudObjectStorageStorageClient != null) {
            RangerCredentialsClient.rangerQcloudObjectStorageStorageClient.close();
            RangerCredentialsClient.rangerQcloudObjectStorageStorageClient = null;
        }
    }
}

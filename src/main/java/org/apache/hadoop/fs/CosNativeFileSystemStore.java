package org.apache.hadoop.fs;

import com.qcloud.cos.COSClient;
import com.qcloud.cos.COSEncryptionClient;
import com.qcloud.cos.ClientConfig;
import com.qcloud.cos.endpoint.EndpointResolver;
import com.qcloud.cos.endpoint.SuffixEndpointBuilder;
import com.qcloud.cos.exception.CosClientException;
import com.qcloud.cos.exception.CosServiceException;
import com.qcloud.cos.exception.ResponseNotCompleteException;
import com.qcloud.cos.http.HandlerAfterProcess;
import com.qcloud.cos.http.HttpProtocol;
import com.qcloud.cos.internal.SkipMd5CheckStrategy;
import com.qcloud.cos.internal.crypto.CryptoConfiguration;
import com.qcloud.cos.internal.crypto.CryptoMode;
import com.qcloud.cos.internal.crypto.CryptoStorageMode;
import com.qcloud.cos.internal.crypto.EncryptionMaterials;
import com.qcloud.cos.internal.crypto.StaticEncryptionMaterialsProvider;
import com.qcloud.cos.model.AbortMultipartUploadRequest;
import com.qcloud.cos.model.COSObject;
import com.qcloud.cos.model.COSObjectSummary;
import com.qcloud.cos.model.CompleteMultipartUploadRequest;
import com.qcloud.cos.model.CompleteMultipartUploadResult;
import com.qcloud.cos.model.CopyObjectRequest;
import com.qcloud.cos.model.CopyPartRequest;
import com.qcloud.cos.model.CopyPartResult;
import com.qcloud.cos.model.DeleteObjectRequest;
import com.qcloud.cos.model.GetObjectMetadataRequest;
import com.qcloud.cos.model.GetObjectRequest;
import com.qcloud.cos.model.GetSymlinkRequest;
import com.qcloud.cos.model.GetSymlinkResult;
import com.qcloud.cos.model.HeadBucketRequest;
import com.qcloud.cos.model.HeadBucketResult;
import com.qcloud.cos.model.InitiateMultipartUploadRequest;
import com.qcloud.cos.model.InitiateMultipartUploadResult;
import com.qcloud.cos.model.ListObjectsRequest;
import com.qcloud.cos.model.ObjectListing;
import com.qcloud.cos.model.ObjectMetadata;
import com.qcloud.cos.model.PartETag;
import com.qcloud.cos.model.PutObjectRequest;
import com.qcloud.cos.model.PutObjectResult;
import com.qcloud.cos.model.PutSymlinkRequest;
import com.qcloud.cos.model.PutSymlinkResult;
import com.qcloud.cos.model.RenameRequest;
import com.qcloud.cos.model.SSEAlgorithm;
import com.qcloud.cos.model.SSECOSKeyManagementParams;
import com.qcloud.cos.model.SSECustomerKey;
import com.qcloud.cos.model.StorageClass;
import com.qcloud.cos.model.UploadPartRequest;
import com.qcloud.cos.model.UploadPartResult;
import com.qcloud.cos.model.ListPartsRequest;
import com.qcloud.cos.model.PartSummary;
import com.qcloud.cos.model.PartListing;
import com.qcloud.cos.region.Region;
import com.qcloud.cos.utils.Base64;
import com.qcloud.cos.utils.IOUtils;
import com.qcloud.cos.utils.Jackson;
import com.qcloud.cos.utils.StringUtils;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.auth.COSCredentialProviderList;
import org.apache.hadoop.fs.cosn.Constants;
import org.apache.hadoop.fs.cosn.CustomerDomainEndpointResolver;
import org.apache.hadoop.fs.cosn.ResettableFileInputStream;
import org.apache.hadoop.fs.cosn.CosNPartListing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.AccessDeniedException;
import java.security.KeyPair;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.LinkedList;
import java.util.concurrent.ThreadLocalRandom;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class CosNativeFileSystemStore implements NativeFileSystemStore {
    public static final Logger LOG =
            LoggerFactory.getLogger(CosNativeFileSystemStore.class);

    private static final String XATTR_PREFIX = "cosn-xattr-";
    private static  final String CLIENT_SIDE_ENCRYPTION_PREFIX = "client-side-encryption";
    private static  final String COS_TAG_LEN_PREFIX = "x-cos-tag-len";
    private static  final String CSE_SINGLE_UPLOAD_FILE_SIZE = "client-side-encryption-unencrypted-content-length";
    public static  final String CSE_MPU_FILE_SIZE = "client-side-encryption-data-size";
    public static  final String CSE_MPU_KEY_SUFFIX = "._CSE_";

    private COSClient cosClient;
    private String bucketName;
    // default is normal bucket.
    private boolean isPosixBucket = false;

    private StorageClass storageClass;
    private int maxRetryTimes;
    private int trafficLimit;
    private boolean crc32cEnabled;
    private boolean completeMPUCheckEnabled;
    private long partSize;
    private boolean clientEncryptionEnabled;
    private CosNEncryptionSecrets encryptionSecrets;
    private RangerCredentialsClient rangerCredentialsClient = null;
    private boolean useL5Id = false;
    private int l5UpdateMaxRetryTimes;
    private EndpointResolver tencentL5EndpointResolver;

    private boolean usePolaris = false;
    private EndpointResolver tencentPolarisEndpointResolver;
    private boolean isAZConsistencyEnabled = false;

    private void innerInitCOSClient(URI uri, Configuration conf) throws IOException {
        COSCredentialProviderList cosCredentialProviderList =
                CosNUtils.createCosCredentialsProviderSet(uri, conf, this.rangerCredentialsClient);

        ClientConfig config;

        String customerDomain = conf.get(CosNConfigKeys.CUSTOMER_DOMAIN);

        if (null == customerDomain) {
            String region = conf.get(CosNConfigKeys.COSN_REGION_KEY);
            if (null == region) {
                region = conf.get(CosNConfigKeys.COSN_REGION_PREV_KEY);
            }

            String endpointSuffix =
                    conf.get(CosNConfigKeys.COSN_ENDPOINT_SUFFIX_KEY);
            if (null == endpointSuffix) {
                endpointSuffix =
                        conf.get(CosNConfigKeys.COSN_ENDPOINT_SUFFIX_PREV_KEY);
            }

            if (null == region && null == endpointSuffix) {
                String exceptionMsg = String.format("config '%s' and '%s' specify at least one.",
                        CosNConfigKeys.COSN_REGION_KEY,
                        CosNConfigKeys.COSN_ENDPOINT_SUFFIX_KEY);
                throw new IOException(exceptionMsg);
            }

            if (null != endpointSuffix) {
                config = new ClientConfig(new Region(""));
                config.setEndPointSuffix(endpointSuffix);
            } else {
                config = new ClientConfig(new Region(region));
            }

            // 以上 region 和 endpoint 配置设置和使用是有问题的, 新版本用以下配置纠正, 后期慢慢退化上面的 endpoint suffix 配置
            String customEndpointSuffix = conf.get(CosNConfigKeys.COSN_CUSTOM_ENDPOINT_SUFFIX);
            if (customEndpointSuffix != null && !customEndpointSuffix.isEmpty()) {
                if (null == region) {
                    String exceptionMsg = String.format("missing config '%s' or '%s'.",
                            CosNConfigKeys.COSN_REGION_KEY,
                            CosNConfigKeys.COSN_REGION_PREV_KEY);
                    throw new IOException(exceptionMsg);
                }
                config = new ClientConfig(new Region(region));
                SuffixEndpointBuilder suffixEndPointBuilder = new SuffixEndpointBuilder(customEndpointSuffix);
                config.setEndpointBuilder(suffixEndPointBuilder);
                boolean distinguishHost = conf.getBoolean(CosNConfigKeys.COSN_DISTINGUISH_HOST_FLAG,
                        CosNConfigKeys.DEFAULT_COSN_DISTINGUISH_HOST_FLAG);
                LOG.info("{}: {}", CosNConfigKeys.COSN_DISTINGUISH_HOST_FLAG, distinguishHost);
                if (distinguishHost) {
                    // 域名未备案时不设置 header host
                    config.setIsDistinguishHost(true);
                }
            }

            this.useL5Id = conf.getBoolean(
                    CosNConfigKeys.COSN_USE_L5_ENABLE,
                    CosNConfigKeys.DEFAULT_COSN_USE_L5_ENABLE);
            this.l5UpdateMaxRetryTimes = conf.getInt(CosNConfigKeys.COSN_L5_UPDATE_MAX_RETRIES_KEY,
                    CosNConfigKeys.DEFAULT_COSN_L5_UPDATE_MAX_RETRIES);
            if (useL5Id) {
                String l5Id = conf.get(CosNConfigKeys.COSN_L5_KEY);
                if (null != l5Id) {
                    int l5modId;
                    int l5cmdId;
                    if (l5Id.contains(":")) {
                        l5modId = Integer.parseInt(l5Id.split(":")[0]);
                        l5cmdId = Integer.parseInt(l5Id.split(":")[1]);
                    } else {
                        l5modId = Integer.parseInt(l5Id.split(",")[0]);
                        l5cmdId = Integer.parseInt(l5Id.split(",")[1]);
                    }

                    Class<?> l5EndpointResolverClass;
                    try {
                        l5EndpointResolverClass = Class.forName(conf.get(CosNConfigKeys.COSN_L5_RESOLVER_CLASS,
                                CosNConfigKeys.DEFAULT_COSN_L5_RESOLVER_CLASS));
                        Constructor<?> constructor = l5EndpointResolverClass.getConstructor(int.class, int.class, int.class);
                        this.tencentL5EndpointResolver = (EndpointResolver) constructor.newInstance(l5modId, l5cmdId, this.l5UpdateMaxRetryTimes);
                    } catch (ClassNotFoundException | NoSuchMethodException | InvocationTargetException |
                             InstantiationException | IllegalAccessException e) {
                        throw new IOException("The current version does not support L5 resolver.", e);
                    }
                    config.setEndpointResolver(tencentL5EndpointResolver);
                    // used by cos java sdk to handle
                    config.turnOnRefreshEndpointAddrSwitch();
                    if (this.tencentL5EndpointResolver instanceof HandlerAfterProcess) {
                        config.setHandlerAfterProcess((HandlerAfterProcess) tencentL5EndpointResolver);
                    }
                }
            }

            // 使用北极星初始化
            this.usePolaris = conf.getBoolean(
                    CosNConfigKeys.COSN_USE_POLARIS_ENABLED,
                    CosNConfigKeys.DEFAULT_COSN_USE_POLARIS_ENABLED);
            if (this.usePolaris) {
                String namespace = conf.get(CosNConfigKeys.COSN_POLARIS_NAMESPACE);
                String service = conf.get(CosNConfigKeys.COSN_POLARIS_SERVICE);
                try {
                    Class<?> polarisEndpointResolverClass = Class.forName(conf.get(CosNConfigKeys.COSN_POLARIS_RESOLVER_CLASS,
                            CosNConfigKeys.DEFAULT_COSN_POLARIS_RESOLVER_CLASS));
                    Constructor<?> constructor = polarisEndpointResolverClass.getConstructor(String.class, String.class, int.class);
                    this.tencentPolarisEndpointResolver = (EndpointResolver) constructor.newInstance(namespace, service, this.l5UpdateMaxRetryTimes);
                } catch (ClassNotFoundException | NoSuchMethodException | InvocationTargetException |
                         InstantiationException | IllegalAccessException e) {
                    throw new IOException("The current version does not support Polaris resolver.", e);
                }

                config.setEndpointResolver(this.tencentPolarisEndpointResolver);
                config.turnOnRefreshEndpointAddrSwitch();
                if (this.tencentPolarisEndpointResolver instanceof HandlerAfterProcess) {
                    config.setHandlerAfterProcess((HandlerAfterProcess) this.tencentPolarisEndpointResolver);
                }
            }

            // 使用北极星 sidecar 初始化
            boolean usePolarisSidecar = conf.getBoolean(CosNConfigKeys.COSN_USE_POLARIS_SIDECAR_ENABLED,
                    CosNConfigKeys.DEFAULT_COSN_USE_POLARIS_SIDECAR_ENABLED);
            if( usePolarisSidecar ){
                String namespace = conf.get(CosNConfigKeys.COSN_POLARIS_NAMESPACE);
                String service = conf.get(CosNConfigKeys.COSN_POLARIS_SERVICE);
                String address = conf.get(CosNConfigKeys.COSN_POLARIS_SIDECAR_ADDRESS);
                Class<?> polarisSideCarClientClass;
                Object polarisSideCarClient;
                try {
                    polarisSideCarClientClass = Class.forName(conf.get(CosNConfigKeys.COSN_POLARIS_SIDECAR_CLIENT_IMPL,
                            CosNConfigKeys.DEFAULT_COSN_POLARIS_SIDECAR_CLIENT_IMPL));
                    Constructor<?> constructor = polarisSideCarClientClass.getConstructor(String.class);
                    polarisSideCarClient = constructor.newInstance(address);
                } catch (ClassNotFoundException | NoSuchMethodException | InvocationTargetException |
                         InstantiationException | IllegalAccessException e) {
                    throw new IOException("The current version does not support Polaris sidecar resolver.", e);
                }
                try {
                    Class<?> polarisEndpointResolverClass = Class.forName(conf.get(CosNConfigKeys.COSN_POLARIS_SIDECAR_RESOLVER_CLASS,
                            CosNConfigKeys.DEFAULT_COSN_POLARIS_SIDECAR_RESOLVER_CLASS));
                    Constructor<?> constructor = polarisEndpointResolverClass.getConstructor(polarisSideCarClientClass, String.class, String.class, int.class);
                    this.tencentPolarisEndpointResolver = (EndpointResolver) constructor.newInstance(polarisSideCarClient, namespace, service, this.l5UpdateMaxRetryTimes);
                } catch (ClassNotFoundException | NoSuchMethodException | InvocationTargetException |
                         InstantiationException | IllegalAccessException e) {
                    throw new IOException("The current version does not support Polaris sidecar resolver.", e);
                }
                config.setEndpointResolver(this.tencentPolarisEndpointResolver);
                config.turnOnRefreshEndpointAddrSwitch();
                if (this.tencentL5EndpointResolver instanceof HandlerAfterProcess) {
                    config.setHandlerAfterProcess((HandlerAfterProcess) this.tencentPolarisEndpointResolver);
                }
            }
        } else {
            config = new ClientConfig(new Region(""));
            LOG.info("Use Customer Domain is {}", customerDomain);
            CustomerDomainEndpointResolver customerDomainEndpointResolver
                    = new CustomerDomainEndpointResolver();
            customerDomainEndpointResolver.setEndpoint(customerDomain);
            config.setEndpointBuilder(customerDomainEndpointResolver);
        }
        boolean useHttps = conf.getBoolean(
                CosNConfigKeys.COSN_USE_HTTPS_KEY,
                CosNConfigKeys.DEFAULT_USE_HTTPS);
        if (useHttps) {
            config.setHttpProtocol(HttpProtocol.https);
        } else {
            // 这里 config 的默认值改过了，默认值变成了https了。
            config.setHttpProtocol(HttpProtocol.http);
        }

        int socketTimeoutSec = conf.getInt(
                CosNConfigKeys.COSN_CLIENT_SOCKET_TIMEOUTSEC,
                CosNConfigKeys.DEFAULT_CLIENT_SOCKET_TIMEOUTSEC);
        config.setSocketTimeout(socketTimeoutSec * 1000);
        config.setUseConnectionMonitor(true); // Idle线程单例化

        this.crc32cEnabled = conf.getBoolean(CosNConfigKeys.CRC32C_CHECKSUM_ENABLED,
                CosNConfigKeys.DEFAULT_CRC32C_CHECKSUM_ENABLED);
        this.completeMPUCheckEnabled = conf.getBoolean(CosNConfigKeys.COSN_COMPLETE_MPU_CHECK,
                CosNConfigKeys.DEFAULT_COSN_COMPLETE_MPU_CHECK_ENABLE);
        this.clientEncryptionEnabled = conf.getBoolean(CosNConfigKeys.COSN_CLIENT_SIDE_ENCRYPTION_ENABLED,
                CosNConfigKeys.DEFAULT_COSN_CLIENT_SIDE_ENCRYPTION_ENABLED);

        // Proxy settings
        String httpProxyIp = conf.getTrimmed(CosNConfigKeys.HTTP_PROXY_IP);
        int httpProxyPort = conf.getInt(CosNConfigKeys.HTTP_PROXY_PORT, CosNConfigKeys.DEFAULT_HTTP_PROXY_PORT);
        if (null != httpProxyIp && !httpProxyIp.isEmpty()) {
            config.setHttpProxyIp(httpProxyIp);
            if (httpProxyPort >= 0) {
                config.setHttpProxyPort(httpProxyPort);
            } else {
                if (useHttps) {
                    LOG.warn("Proxy IP set without port. Using HTTPS default port 443");
                    config.setHttpProxyPort(443);
                } else {
                    LOG.warn("Proxy IP set without port, Using HTTP default port 80");
                    config.setHttpProxyPort(80);
                }
            }
            // setting the proxy username and password
            String proxyUserName = conf.get(CosNConfigKeys.HTTP_PROXY_USERNAME);
            String proxyPassword = conf.get(CosNConfigKeys.HTTP_PROXY_PASSWORD);
            if ((null == proxyUserName || proxyUserName.isEmpty())
                    != (null == proxyPassword || proxyPassword.isEmpty())) {
                String exceptionMessage = String.format("Proxy error: '%s' or '%s' set without the other.",
                        CosNConfigKeys.HTTP_PROXY_USERNAME, CosNConfigKeys.HTTP_PROXY_PASSWORD);
                throw new IllegalArgumentException(exceptionMessage);
            }
            config.setProxyUsername(proxyUserName);
            config.setProxyPassword(proxyPassword);
        }

        String versionNum = getPluginVersionInfo();
        String versionInfo = versionNum.equals("unknown") ? CosNConfigKeys.DEFAULT_USER_AGENT : "cos-hadoop-plugin-v" + versionNum;
        String userAgent = conf.get(CosNConfigKeys.USER_AGENT, versionInfo);
        String emrVersion = conf.get(CosNConfigKeys.TENCENT_EMR_VERSION_KEY);
        if (null != emrVersion && !emrVersion.isEmpty()) {
            userAgent = String.format("%s on %s", userAgent, emrVersion);
        }
        config.setUserAgent(userAgent);
        config.setThrow412Directly(true);

        this.maxRetryTimes = conf.getInt(
                CosNConfigKeys.COSN_MAX_RETRIES_KEY,
                CosNConfigKeys.DEFAULT_MAX_RETRIES);

        this.isAZConsistencyEnabled = conf.getBoolean(CosNConfigKeys.COSN_AZ_ACCELERATOR_CONSISTENCY_ENABLED,
                CosNConfigKeys.DEFAULT_COSN_AZ_ACCELERATOR_CONSISTENCY_ENABLED);

        //用于客户端加密
        this.partSize = conf.getLong(
                CosNConfigKeys.COSN_UPLOAD_PART_SIZE_KEY, CosNConfigKeys.DEFAULT_UPLOAD_PART_SIZE);
        if (partSize < Constants.MIN_PART_SIZE) {
            LOG.warn("The minimum size of a single block is limited to " +
                    "greater than or equal to {}.", Constants.MIN_PART_SIZE);
            this.partSize = Constants.MIN_PART_SIZE;
        } else if (partSize > Constants.MAX_PART_SIZE) {
            LOG.warn("The maximum size of a single block is limited to " +
                    "smaller than or equal to {}.", Constants.MAX_PART_SIZE);
            this.partSize = Constants.MAX_PART_SIZE;
        }

        // 设置COSClient的最大重试次数
        int clientMaxRetryTimes = conf.getInt(
                CosNConfigKeys.CLIENT_MAX_RETRIES_KEY,
                CosNConfigKeys.DEFAULT_CLIENT_MAX_RETRIES);
        config.setMaxErrorRetry(clientMaxRetryTimes);
        LOG.info("hadoop cos retry times: {}, cos client retry times: {}",
                this.maxRetryTimes, clientMaxRetryTimes);

        if (conf.getBoolean(CosNConfigKeys.COSN_USE_SHORT_CONNECTION,
                CosNConfigKeys.DEFAULT_COSN_USE_SHORT_CONNECTION)) {
            config.setShortConnection();
        }

        if (conf.getBoolean(CosNConfigKeys.COSN_CLIENT_USE_REQUEST_TIMEOUT,
                CosNConfigKeys.DEFAULT_COSN_CLIENT_USE_REQUEST_TIMEOUT)) {
            config.setRequestTimeOutEnable(true);
            config.setRequestTimeout(
                    conf.getInt(
                            CosNConfigKeys.COSN_CLIENT_REQUEST_TIMEOUT,
                            CosNConfigKeys.DEFAULT_COSN_CLIENT_REQUEST_TIMEOUT));
            config.setTimeoutClientThreadSize(
                    conf.getInt(
                            CosNConfigKeys.COSN_CLIENT_REQUEST_TIMEOUT_THREAD_SIZE,
                            CosNConfigKeys.DEFAULT_COSN_CLIENT_REQUEST_TIMEOUT_THREAD_SIZE));
        }

        config.setMaxConnectionsCount(
                conf.getInt(
                        CosNConfigKeys.MAX_CONNECTION_NUM,
                        CosNConfigKeys.DEFAULT_MAX_CONNECTION_NUM));
        config.setConnectionRequestTimeout(
                conf.getInt(CosNConfigKeys.COSN_CONNECTION_REQUEST_TIMEOUT,
                        CosNConfigKeys.DEFAULT_COSN_CONNECTION_REQUEST_TIMEOUT));
        config.setConnectionTimeout(
                conf.getInt(CosNConfigKeys.COSN_CONNECTION_TIMEOUT,
                        CosNConfigKeys.DEFAULT_COSN_CONNECTION_TIMEOUT));
        config.setIdleConnectionAlive(
                conf.getInt(CosNConfigKeys.COSN_IDLE_CONNECTION_ALIVE,
                        CosNConfigKeys.DEFAULT_COSN_IDLE_CONNECTION_ALIVE));
        config.setPrintShutdownStackTrace(
                conf.getBoolean(CosNConfigKeys.COSN_CLIENT_SHUTDOWN_STACK_TRACE_LOG,
                        CosNConfigKeys.DEFAULT_COSN_CLIENT_SHUTDOWN_STACK_TRACE_LOG));

        // 设置是否进行服务器端加密
        String serverSideEncryptionAlgorithm = conf.get(CosNConfigKeys.COSN_SERVER_SIDE_ENCRYPTION_ALGORITHM, "");
        CosNEncryptionMethods cosSSE = CosNEncryptionMethods.getMethod(
                serverSideEncryptionAlgorithm);
        String sseKey = conf.get(
                CosNConfigKeys.COSN_SERVER_SIDE_ENCRYPTION_KEY, "");
        String sseContext = conf.get(
                CosNConfigKeys.COSN_SERVER_SIDE_ENCRYPTION_CONTEXT, "");
        checkEncryptionMethod(config, cosSSE, sseKey);
        this.encryptionSecrets = new CosNEncryptionSecrets(cosSSE, sseKey, sseContext);

        // Set the traffic limit
        this.trafficLimit = conf.getInt(CosNConfigKeys.TRAFFIC_LIMIT, CosNConfigKeys.DEFAULT_TRAFFIC_LIMIT);
        if (this.trafficLimit >= 0
                && (this.trafficLimit < 100 * 1024 * 8 || this.trafficLimit > 100 * 1024 * 1024 * 8)) {
            String exceptionMessage = String.format("The '%s' needs to be between %d and %d. but %d.",
                    CosNConfigKeys.TRAFFIC_LIMIT, 100 * 1024 * 8, 100 * 1024 * 1024 * 8, this.trafficLimit);
            throw new IllegalArgumentException(exceptionMessage);
        }

        // 设置是否进行客户端加密
        if (clientEncryptionEnabled) {
            LOG.info("client side encryption enabled");
            // 为防止请求头部被篡改导致的数据无法解密，强烈建议只使用 https 协议发起请求
            config.setHttpProtocol(HttpProtocol.https);
            KeyPair asymKeyPair = null;

            try {
                // 加载保存在文件中的秘钥, 如果不存在，请先使用buildAndSaveAsymKeyPair生成秘钥
                asymKeyPair = CosNUtils.loadAsymKeyPair(conf.get(CosNConfigKeys.COSN_CLIENT_SIDE_ENCRYPTION_PUBLIC_KEY_PATH),
                        conf.get(CosNConfigKeys.COSN_CLIENT_SIDE_ENCRYPTION_PRIVATE_KEY_PATH));
            } catch (Exception e) {
                throw new CosClientException(e);
            }
            // 初始化 KMS 加密材料
            EncryptionMaterials encryptionMaterials = new EncryptionMaterials(asymKeyPair);
            // 使用AES/GCM模式，并将加密信息存储在文件元信息中.
            CryptoConfiguration cryptoConf = new CryptoConfiguration(CryptoMode.AuthenticatedEncryption)
                    .withStorageMode(CryptoStorageMode.ObjectMetadata);
            // 生成加密客户端EncryptionClient, COSEncryptionClient是COSClient的子类, 所有COSClient支持的接口他都支持。
            // EncryptionClient覆盖了COSClient上传下载逻辑，操作内部会执行加密操作，其他操作执行逻辑和COSClient一致
            this.cosClient =
                    new COSEncryptionClient(cosCredentialProviderList,
                            new StaticEncryptionMaterialsProvider(encryptionMaterials), config,
                            cryptoConf);
            return;
        }
        this.cosClient = new COSClient(cosCredentialProviderList, config);
    }

    private void initCOSClient(URI uri, Configuration conf) throws IOException {
        ClassLoader originalClassLoader = Thread.currentThread().getContextClassLoader();
        LOG.debug("init cos client: {}, context ClassLoader: {}",
                this.getClass().getClassLoader(), originalClassLoader);
        try {
            Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());
            innerInitCOSClient(uri, conf);
        } finally {
            Thread.currentThread().setContextClassLoader(originalClassLoader);
        }
    }

    @Override
    public void initialize(URI uri, Configuration conf) throws IOException {
        String bucket = uri.getHost();
        RangerCredentialsClient rangerCredentialsClient = new RangerCredentialsClient();
        rangerCredentialsClient.doInitialize(conf, bucket);
        this.initialize(uri, conf, rangerCredentialsClient);
    }

    @Override
    public void initialize(URI uri, Configuration conf, RangerCredentialsClient rangerClient) throws IOException {
        // Close previously unreleased resources first
        this.preClose();

        // Begin to initialize.
        try {
            if (null == rangerClient) {
                // avoid NPE
                throw new IOException("native store ranger client param is null");
            }
            this.rangerCredentialsClient = rangerClient;
            initCOSClient(uri, conf);
            if (null == this.bucketName) {
                this.bucketName = uri.getHost();
            }
            String storageClass = conf.get(CosNConfigKeys.COSN_STORAGE_CLASS_KEY);
            if (null != storageClass && !storageClass.isEmpty()) {
                try {
                    this.storageClass = StorageClass.fromValue(storageClass);
                } catch (IllegalArgumentException e) {
                    // 使用 StringBuilder 来构造字符串
                    StringBuilder supportedClasses = new StringBuilder();
                    for (StorageClass sc : StorageClass.values()) {
                        if (supportedClasses.length() > 0) {
                            supportedClasses.append(", ");
                        }
                        supportedClasses.append(sc.name());
                    }
                    String exceptionMessage = String.format("The specified storage class [%s] is invalid. " +
                                    "The supported storage classes are: %s.", storageClass, supportedClasses);

                    throw new IllegalArgumentException(exceptionMessage);
                }
            }
            if (null != this.storageClass && StorageClass.Archive == this.storageClass) {
                LOG.warn("The storage class of the CosN FileSystem is set to {}. Some file operations may not be " +
                        "supported.", this.storageClass);
            }
        } catch (Exception e) {
            LOG.error("Failed to initialize the COS native filesystem store.", e);
            handleException(e, "");
        }
    }

    @Override
    public HeadBucketResult headBucket(String bucketName) throws IOException {
        HeadBucketRequest headBucketRequest = new HeadBucketRequest(bucketName);
        try {
            HeadBucketResult result = (HeadBucketResult) callCOSClientWithRetry(headBucketRequest);
            return result;
        } catch (Exception e) {
            String errMsg = String.format("head bucket [%s] occurs an exception: %s.",
                    bucketName, e);
            LOG.error(errMsg, e);
            handleException(new Exception(errMsg), bucketName);
        }
        return null; // never will get here
    }

    private PutObjectResult storeFileWithRetry(String key, InputStream inputStream,
                                    byte[] md5Hash, long length)
            throws IOException {
        try {
            ObjectMetadata objectMetadata = new ObjectMetadata();
            if (null != md5Hash) {
                objectMetadata.setContentMD5(Base64.encodeAsString(md5Hash));
            }
            objectMetadata.setContentLength(length);
            if (crc32cEnabled) {
                objectMetadata.setHeader(Constants.CRC32C_REQ_HEADER, Constants.CRC32C_REQ_HEADER_VAL);
            }

            PutObjectRequest putObjectRequest =
                    new PutObjectRequest(bucketName, key, inputStream, objectMetadata);
            if (null != this.storageClass) {
                putObjectRequest.setStorageClass(this.storageClass);
            }
            if (this.trafficLimit >= 0) {
                putObjectRequest.setTrafficLimit(this.trafficLimit);
            }
            this.setEncryptionMetadata(putObjectRequest, objectMetadata);

            PutObjectResult putObjectResult =
                    (PutObjectResult) callCOSClientWithRetry(putObjectRequest);
            LOG.debug("Store the file successfully. cos key: {}, ETag: {}.", key, putObjectResult.getETag());
            return putObjectResult;
        } catch (CosServiceException cse) {
            // 避免并发上传的问题
            int statusCode = cse.getStatusCode();
            if (statusCode == 409) {
                // Check一下这个文件是否已经存在
                FileMetadata fileMetadata = this.queryObjectMetadata(key);
                if (null == fileMetadata) {
                    // 如果文件不存在，则需要抛出异常
                    handleException(cse, key);
                }
                LOG.warn("Upload the cos key [{}] concurrently", key);
            } else {
                // 其他错误都要抛出来
                handleException(cse, key);
            }
        } catch (Exception e) {
            String errMsg =
                    String.format("Store the file failed, cos key: %s, exception: %s.", key, e);
            handleException(new Exception(errMsg), key);
        }
        return null;
    }

    @Override
    public PutObjectResult storeFile(String key, File file, byte[] md5Hash) throws IOException {
        if (null != md5Hash) {
            LOG.debug("Store the file, local path: {}, length: {}, md5hash: {}.",
                    file.getCanonicalPath(), file.length(),
                    Hex.encodeHexString(md5Hash));
        }
        return storeFileWithRetry(key,
                new ResettableFileInputStream(file), md5Hash, file.length());
    }

    @Override
    public PutObjectResult storeFile(String key, InputStream inputStream,
                          byte[] md5Hash, long contentLength) throws IOException {
        if (null != md5Hash) {
            LOG.debug("Store the file to the cos key: {}, input stream md5 hash: {}, content length: {}.", key,
                    Hex.encodeHexString(md5Hash),
                    contentLength);
        }
        return storeFileWithRetry(key, inputStream, md5Hash, contentLength);
    }

    // for cos, storeEmptyFile means create a directory
    @Override
    public void storeEmptyFile(String key) throws IOException {
        LOG.debug("Store an empty file to the key: {}.", key);

        ObjectMetadata objectMetadata = new ObjectMetadata();
        objectMetadata.setContentLength(0);
        if (crc32cEnabled) {
            objectMetadata.setHeader(Constants.CRC32C_REQ_HEADER, Constants.CRC32C_REQ_HEADER_VAL);
        }

        InputStream input = new ByteArrayInputStream(new byte[0]);
        PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName,
                key, input, objectMetadata);
        if (null != this.storageClass) {
            putObjectRequest.setStorageClass(this.storageClass);
        }
        // 这个头部非常重要，防止覆盖到同名的对象
        putObjectRequest.putCustomRequestHeader("x-cos-forbid-overwrite", "true");

        try {
            PutObjectResult putObjectResult =
                    (PutObjectResult) callCOSClientWithRetry(putObjectRequest);
            LOG.debug("Store the empty file successfully, cos key: {}, ETag: {}.", key, putObjectResult.getETag());
        } catch (CosServiceException cse) {
            int statusCode = cse.getStatusCode();
            if (statusCode == 409) {
                // 并发上传文件导致，再check一遍文件是否存在
                FileMetadata fileMetadata = this.queryObjectMetadata(key);
                if (null == fileMetadata) {
                    // 文件还是不存在，必须要抛出异常
                    handleException(cse, key);
                }
                LOG.warn("Upload the file [{}] concurrently.", key);
            } else {
                // 其他错误必须抛出
                handleException(cse, key);
            }
        } catch (Exception e) {
            String errMsg =
                    String.format("Store the empty file failed, cos key: %s, " +
                            "exception: %s", key, e);
            handleException(new Exception(errMsg), key);
        }
    }

    public PartETag uploadPart(File file, String key, String uploadId,
                               int partNum, byte[] md5Hash, Boolean isLastPart) throws IOException {
        InputStream inputStream = new ResettableFileInputStream(file);
        return uploadPart(inputStream, key, uploadId, partNum, file.length(), md5Hash, isLastPart);
    }

    @Override
    public PartETag uploadPart(InputStream inputStream, String key, String uploadId,
                               int partNum, long partSize, byte[] md5Hash,
                               Boolean isLastPart) throws IOException {
        LOG.debug("Upload the part to the cos key [{}]. upload id: {}, part number: {}, part size: {}",
                key, uploadId, partNum, partSize);
        ObjectMetadata objectMetadata = new ObjectMetadata();
        if (crc32cEnabled) {
            objectMetadata.setHeader(Constants.CRC32C_REQ_HEADER, Constants.CRC32C_REQ_HEADER_VAL);
        }

        UploadPartRequest uploadPartRequest = new UploadPartRequest();
        uploadPartRequest.setBucketName(this.bucketName);
        uploadPartRequest.setUploadId(uploadId);
        uploadPartRequest.setInputStream(inputStream);
        uploadPartRequest.setPartNumber(partNum);
        uploadPartRequest.setPartSize(partSize);
        uploadPartRequest.setObjectMetadata(objectMetadata);
        if(isLastPart == null && clientEncryptionEnabled) {
            throw new IOException("when client encryption is enabled, isLastPart can't be null");
        }
        if(isLastPart != null) {
            uploadPartRequest.setLastPart(isLastPart);
        }
        if (null != md5Hash && !this.clientEncryptionEnabled) {
            uploadPartRequest.setMd5Digest(Base64.encodeAsString(md5Hash));
        }
        uploadPartRequest.setKey(key);
        if (this.trafficLimit >= 0) {
            uploadPartRequest.setTrafficLimit(this.trafficLimit);
        }
        this.setEncryptionMetadata(uploadPartRequest, objectMetadata);

        try {
            UploadPartResult uploadPartResult =
                    (UploadPartResult) callCOSClientWithRetry(uploadPartRequest);
            return uploadPartResult.getPartETag();
        } catch (CosServiceException cse) {
            String errMsg = String.format("The current thread:%d, "
                            + "cos key: %s, upload id: %s, part num: %d, exception: %s",
                    Thread.currentThread().getId(), key, uploadId, partNum, cse);
            int statusCode = cse.getStatusCode();
            if (409 == statusCode) {
                // conflict upload same upload part, because of nginx syn retry,
                // sometimes 31s reconnect to cgi, but client side default 30s timeout,
                // which may cause the retry request and previous request arrive at cgi at same time.
                // so we for now use list parts to double-check this part whether exist.
                CosNPartListing partListing = listParts(key, uploadId);
                PartETag partETag = isPartExist(partListing, partNum, partSize);
                if (null == partETag) {
                    handleException(new Exception(errMsg), key);
                }
                LOG.warn("Upload the file [{}] uploadId [{}], part [{}] concurrently.", key, uploadId, partNum);
                return partETag;
            } else {
                handleException(new Exception(errMsg), key);
            }
        } catch (Exception e) {
            String errMsg = String.format("The current thread:%d, "
                            + "cos key: %s, upload id: %s, part num: %d, exception: %s",
                    Thread.currentThread().getId(), key, uploadId, partNum, e);
            handleException(new Exception(errMsg), key);
        }
        return null;
    }

    public PartETag uploadPart(File file, String key, String uploadId,
                               int partNum, byte[] md5Hash) throws IOException {
        InputStream inputStream = new ResettableFileInputStream(file);
        return uploadPart(inputStream, key, uploadId, partNum, file.length(), md5Hash);
    }

    public PartETag uploadPart(
            InputStream inputStream,
            String key, String uploadId, int partNum, long partSize, byte[] md5Hash) throws IOException {
        return uploadPart(inputStream, key, uploadId, partNum, partSize, md5Hash, null);
    }

    @Override
    public PartETag uploadPartCopy(String uploadId, String srcKey, String destKey,
                                   int partNum, long firstByte, long lastByte) throws IOException {
        LOG.debug("Execute a part copy from the source key [{}] to the dest key [{}]. " +
                        "upload id: {}, part number: {}, firstByte: {}, lastByte: {}.",
                srcKey, destKey, uploadId, partNum, firstByte, lastByte);
        try {
            CopyPartRequest copyPartRequest = new CopyPartRequest();
            copyPartRequest.setSourceBucketName(this.bucketName);
            copyPartRequest.setDestinationBucketName(this.bucketName);
            copyPartRequest.setSourceEndpointBuilder(this.cosClient.getClientConfig().getEndpointBuilder());
            copyPartRequest.setUploadId(uploadId);
            copyPartRequest.setSourceKey(srcKey);
            copyPartRequest.setDestinationKey(destKey);
            copyPartRequest.setPartNumber(partNum);
            copyPartRequest.setFirstByte(firstByte);
            copyPartRequest.setLastByte(lastByte);
            CopyPartResult copyPartResult = (CopyPartResult) this.callCOSClientWithRetry(copyPartRequest);
            return copyPartResult.getPartETag();
        } catch (Exception e) {
            String exceptionMessage = String.format(
                    "Copy the object part [%d-%d] from the srcKey[%s] to the destKey[%s] failed. " +
                            "upload id: %s, part number: %d, exception: %s.",
                    firstByte, lastByte, srcKey, destKey, uploadId, partNum, e);
            handleException(new Exception(exceptionMessage), srcKey);
        }

        return null;
    }

    public void abortMultipartUpload(String key, String uploadId) throws IOException {
        LOG.info("Ready to doAbort the multipart upload. cos key: {}, upload id: {}.", key, uploadId);

        try {
            AbortMultipartUploadRequest abortMultipartUploadRequest =
                    new AbortMultipartUploadRequest(
                            bucketName, key, uploadId);
            this.callCOSClientWithRetry(abortMultipartUploadRequest);
        } catch (Exception e) {
            String errMsg = String.format("Aborting the multipart upload failed. cos key: %s, upload id: %s. " +
                    "exception: %s.", key, uploadId, e);
            handleException(new Exception(errMsg), key);
        }
    }

    /**
     * get cos upload Id
     *
     * @param key cos key
     * @return uploadId
     * @throws IOException when fail to get the MultipartManager Upload ID.
     */
    public String getUploadId(String key) throws IOException {
        if (null == key || key.length() == 0) {
            return "";
        }

        ObjectMetadata objectMetadata = new ObjectMetadata();
        if (crc32cEnabled) {
            objectMetadata.setHeader(Constants.CRC32C_REQ_HEADER, Constants.CRC32C_REQ_HEADER_VAL);
        }

        InitiateMultipartUploadRequest initiateMultipartUploadRequest =
                new InitiateMultipartUploadRequest(bucketName, key);
        if (null != this.storageClass) {
            initiateMultipartUploadRequest.setStorageClass(this.storageClass);
        }
        initiateMultipartUploadRequest.setObjectMetadata(objectMetadata);
        this.setEncryptionMetadata(initiateMultipartUploadRequest, objectMetadata);
        //客户端加密先用partsize代替datasize，后面再做替换
        if(clientEncryptionEnabled){
            initiateMultipartUploadRequest.setDataSizePartSize(this.partSize, this.partSize);
        }
        try {
            InitiateMultipartUploadResult initiateMultipartUploadResult =
                    (InitiateMultipartUploadResult) this.callCOSClientWithRetry(initiateMultipartUploadRequest);
            return initiateMultipartUploadResult.getUploadId();
        } catch (Exception e) {
            String errMsg =
                    String.format("Get the upload id failed, cos key: %s, " + "exception: %s", key, e);
            handleException(new Exception(errMsg), key);
        }

        return null;
    }

    /**
     * complete cos mpu, sometimes return null complete mpu result
     *
     * @param key          cos key
     * @param uploadId     upload id
     * @param partETagList each part etag list
     * @return result
     * @throws IOException when fail to complete the multipart upload.
     */
    public CompleteMultipartUploadResult completeMultipartUpload(
            String key, String uploadId, List<PartETag> partETagList) throws IOException {
        Collections.sort(partETagList, new Comparator<PartETag>() {
            @Override
            public int compare(PartETag o1, PartETag o2) {
                return o1.getPartNumber() - o2.getPartNumber();
            }
        });
        try {
            ObjectMetadata objectMetadata = new ObjectMetadata();
            if (crc32cEnabled) {
                objectMetadata.setHeader(Constants.CRC32C_REQ_HEADER, Constants.CRC32C_REQ_HEADER_VAL);
            }
            CompleteMultipartUploadRequest completeMultipartUploadRequest =
                    new CompleteMultipartUploadRequest(bucketName, key, uploadId,
                            partETagList);
            completeMultipartUploadRequest.setObjectMetadata(objectMetadata);
            return (CompleteMultipartUploadResult) this.callCOSClientWithRetry(completeMultipartUploadRequest);
        } catch (CosServiceException cse) {
            // when first calling with 503 access time out, next retry will 409.
            // Usually, 404 occurs on the network packet loss.
            int statusCode = cse.getStatusCode();
            if (statusCode == 409 || statusCode == 404) {
                // check file whether exist
                FileMetadata fileMetadata = this.queryObjectMetadata(key);
                if (null == fileMetadata) {
                    // if file not exist through exception
                    handleException(cse, key);
                }
                LOG.warn("Upload the cos key [{}] complete mpu concurrently", key);
            } else {
                // other exception
                String errMsg = String.format("Complete the multipart upload failed. " +
                        "cos service exception, cos key: %s, upload id: %s, " +
                        "exception: %s", key, uploadId, cse.toString());
                handleException(new Exception(errMsg), key);
            }
        } catch (Exception e) {
            String errMsg = String.format("Complete the multipart upload failed. " +
                    "cos key: %s, upload id: %s, " +
                    "exception: %s", key, uploadId, e.toString());
            handleException(new Exception(errMsg), key);
        }
        return null;
    }

    public FileMetadata queryObjectMetadata(String key) throws IOException {
        return queryObjectMetadata(key, null);
    }

    public FileMetadata queryObjectMetadata(String key,
                                            CosNResultInfo info) throws IOException {
        LOG.debug("Query Object metadata. cos key: {}.", key);
        GetObjectMetadataRequest getObjectMetadataRequest =
                new GetObjectMetadataRequest(bucketName, key);

        this.setEncryptionMetadata(getObjectMetadataRequest, new ObjectMetadata());
        try {
            ObjectMetadata objectMetadata =
                    (ObjectMetadata) callCOSClientWithRetry(getObjectMetadataRequest);
            long mtime = 0;
            long fileSize = 0;
            if (objectMetadata.getLastModified() != null) {
                mtime = objectMetadata.getLastModified().getTime();
            }
            fileSize = objectMetadata.getContentLength();
            if(clientEncryptionEnabled) {
                if (objectMetadata.getUserMetadata().containsKey(CSE_SINGLE_UPLOAD_FILE_SIZE)) {
                    fileSize = Long.parseLong(
                            objectMetadata.getUserMetadata().get(CSE_SINGLE_UPLOAD_FILE_SIZE));
                } else if (objectMetadata.getUserMetadata().containsKey(CSE_MPU_FILE_SIZE)) {
                    fileSize = Long.parseLong(
                            objectMetadata.getUserMetadata().get(CSE_MPU_FILE_SIZE));
                }
            }
            String ETag = objectMetadata.getETag();
            String crc64ecm = objectMetadata.getCrc64Ecma();
            String crc32cm = (String) objectMetadata.getRawMetadataValue(Constants.CRC32C_RESP_HEADER);
            String versionId = objectMetadata.getVersionId();
            Map<String, byte[]> userMetadata = null;
            if (objectMetadata.getUserMetadata() != null) {
                userMetadata = new HashMap<>();
                for (Map.Entry<String, String> userMetadataEntry : objectMetadata.getUserMetadata().entrySet()) {
                    if (userMetadataEntry.getKey().startsWith(ensureValidAttributeName(XATTR_PREFIX))) {
                        String xAttrJsonStr = new String(Base64.decode(userMetadataEntry.getValue()),
                                StandardCharsets.UTF_8);
                        CosNXAttr cosNXAttr;
                        try {
                            cosNXAttr = Jackson.fromJsonString(xAttrJsonStr, CosNXAttr.class);
                        } catch (CosClientException e) {
                            LOG.warn("Parse the xAttr failed. name: {}, XAttJsonStr: {}.",
                                    userMetadataEntry.getKey(), xAttrJsonStr);
                            continue;               // Skip
                        }

                        if (null != cosNXAttr) {
                            userMetadata.put(cosNXAttr.getName(),
                                    cosNXAttr.getValue().getBytes(CosNFileSystem.METADATA_ENCODING));
                        }
                    }
                    //metadata of client side encryption
                    if(userMetadataEntry.getKey().startsWith(CLIENT_SIDE_ENCRYPTION_PREFIX)
                            || userMetadataEntry.getKey().startsWith(COS_TAG_LEN_PREFIX)) {
                        userMetadata.put(userMetadataEntry.getKey(), userMetadataEntry.getValue().getBytes(CosNFileSystem.METADATA_ENCODING));
                    }
                }
            }
            boolean isFile = true;
            if (isPosixBucket) {
                if (objectMetadata.isFileModeDir() || key.equals(CosNFileSystem.PATH_DELIMITER)) {
                    isFile = false;
                }
            } else {
                isFile = !key.endsWith(CosNFileSystem.PATH_DELIMITER);
            }
            FileMetadata fileMetadata =
                    new FileMetadata(key, fileSize, mtime, isFile,
                            ETag, crc64ecm, crc32cm, versionId,
                            objectMetadata.getStorageClass(), userMetadata);
            // record the last request result info
            if (info != null) {
                info.setRequestID(objectMetadata.getRequestId());
            }
            LOG.debug("Retrieve the file metadata. cos key: {}, ETag:{}, length:{}, crc64ecm: {}.", key,
                    objectMetadata.getETag(), objectMetadata.getContentLength(), objectMetadata.getCrc64Ecma());
            return fileMetadata;
        } catch (CosServiceException e) {
            if (info != null) {
                info.setRequestID(e.getRequestId());
            }
            if (e.getStatusCode() != 404) {
                String errorMsg =
                        String.format("Retrieve the file metadata file failure. " +
                                "cos key: %s, exception: %s", key, e);
                handleException(new Exception(errorMsg), key);
            }
        }
        return null;
    }

    @Override
    public FileMetadata retrieveMetadata(String key) throws IOException {
        return retrieveMetadata(key, null);
    }

    // this method only used in getFileStatus to get the head request result info
    @Override
    public FileMetadata retrieveMetadata(String key,
                                         CosNResultInfo info) throws IOException {
        if (key.endsWith(CosNFileSystem.PATH_DELIMITER)) {
            key = key.substring(0, key.length() - 1);
        }

        if (!key.isEmpty()) {
            FileMetadata fileMetadata = queryObjectMetadata(key, info);
            if (fileMetadata != null) {
                return fileMetadata;
            }
        }
        // judge if the key is directory
        key = key + CosNFileSystem.PATH_DELIMITER;
        return queryObjectMetadata(key, info);
    }

    @Override
    public CosNSymlinkMetadata retrieveSymlinkMetadata(String symlink) throws IOException {
        return this.retrieveSymlinkMetadata(symlink, null);
    }

    @Override
    public CosNSymlinkMetadata retrieveSymlinkMetadata(String symlink, CosNResultInfo info) throws IOException {
        LOG.debug("Get the symlink [{}]'s metadata.", symlink);
        try {
            GetSymlinkRequest getSymlinkRequest = new GetSymlinkRequest(
                this.bucketName, symlink, null);
            GetSymlinkResult getSymlinkResult = (GetSymlinkResult) callCOSClientWithRetry(getSymlinkRequest);
            if (null != info) {
                info.setRequestID(getSymlinkResult.getRequestId());
            }
            return new CosNSymlinkMetadata(symlink, getSymlinkResult.getContentLength(),
                getSymlinkResult.getLastModified(), false,
                getSymlinkResult.getETag(), null, null,
                getSymlinkResult.getVersionId(), StorageClass.Standard.toString(), getSymlinkResult.getTarget());
        } catch (CosServiceException cosServiceException) {
            if (null != info) {
                info.setRequestID(cosServiceException.getRequestId());
            }
            if (cosServiceException.getStatusCode() == 400 && cosServiceException.getErrorCode()
                .compareToIgnoreCase("NotSymlink") == 0) {
                LOG.debug("The key [{}] is not a symlink.", symlink);
                return null;
            }
            if (cosServiceException.getStatusCode() != 404) {
                String errMsg = String.format("Retrieve the symlink metadata failed. symlink: %s, exception: %s.",
                    symlink, cosServiceException);
                handleException(new Exception(errMsg), symlink);
            }
        }
        return null;
    }

    @Override
    public byte[] retrieveAttribute(String key, String attribute) throws IOException {
        LOG.debug("Get the extended attribute. cos key: {}, attribute: {}.", key, attribute);

        FileMetadata fileMetadata = retrieveMetadata(key);
        if (null != fileMetadata) {
            if (null != fileMetadata.getUserAttributes()) {
                return fileMetadata.getUserAttributes().get(attribute);
            }
        }

        return null;
    }

    @Override
    public void storeDirAttribute(String key, String attribute, byte[] value) throws IOException {
        LOG.debug("Store a attribute to the specified directory. cos key: {}, attribute: {}, value: {}.",
                key, attribute, new String(value, CosNFileSystem.METADATA_ENCODING));
        if (!key.endsWith(CosNFileSystem.PATH_DELIMITER)) {
            key = key + CosNFileSystem.PATH_DELIMITER;
        }
        storeAttribute(key, attribute, value, false);
    }

    @Override
    public void storeFileAttribute(String key, String attribute, byte[] value) throws IOException {
        LOG.debug("Store a attribute to the specified file. cos key: {}, attribute: {}, value: {}.",
                key, attribute, new String(value, CosNFileSystem.METADATA_ENCODING));
        storeAttribute(key, attribute, value, false);
    }

    @Override
    public void removeDirAttribute(String key, String attribute) throws IOException {
        LOG.debug("Remove the attribute from the specified directory. cos key: {}, attribute: {}.",
                key, attribute);
        if (!key.endsWith(CosNFileSystem.PATH_DELIMITER)) {
            key = key + CosNFileSystem.PATH_DELIMITER;
        }
        storeAttribute(key, attribute, null, true);
    }

    @Override
    public void removeFileAttribute(String key, String attribute) throws IOException {
        LOG.debug("Remove the attribute from the specified file. cos key: {}, attribute: {}.",
                key, attribute);
        storeAttribute(key, attribute, null, true);
    }

    private void storeAttribute(String key, String attribute, byte[] value, boolean deleted) throws IOException {
        if (deleted) {
            LOG.debug("Delete the extended attribute. cos key: {}, attribute: {}.", key, attribute);
        }

        if (null != value && !deleted) {
            LOG.debug("Store the extended attribute. cos key: {}, attribute: {}, value: {}.",
                    key, attribute, new String(value, StandardCharsets.UTF_8));
        }

        if (null == value && !deleted) {
            throw new IOException("The attribute value to be set can not be null.");
        }

        GetObjectMetadataRequest getObjectMetadataRequest = new GetObjectMetadataRequest(bucketName, key);
        this.setEncryptionMetadata(getObjectMetadataRequest, new ObjectMetadata());
        ObjectMetadata objectMetadata = null;
        try {
            objectMetadata = (ObjectMetadata) callCOSClientWithRetry(getObjectMetadataRequest);
        } catch (CosServiceException e) {
            if (e.getStatusCode() != 404) {
                String errorMessage = String.format("Retrieve the file metadata failed. " +
                        "cos key: %s, exception: %s.", key, e);
                handleException(new Exception(errorMessage), key);
            }
        }

        if (null != objectMetadata) {
            Map<String, String> userMetadata = objectMetadata.getUserMetadata();
            if (deleted) {
                if (null != userMetadata) {
                    userMetadata.remove(ensureValidAttributeName(attribute));
                } else {
                    return;
                }
            } else {
                if (null == userMetadata) {
                    userMetadata = new HashMap<>();
                }
                CosNXAttr cosNXAttr = new CosNXAttr();
                cosNXAttr.setName(attribute);
                cosNXAttr.setValue(new String(value, CosNFileSystem.METADATA_ENCODING));
                String xAttrJsonStr = Jackson.toJsonString(cosNXAttr);
                userMetadata.put(ensureValidAttributeName(XATTR_PREFIX + attribute),
                        Base64.encodeAsString(xAttrJsonStr.getBytes(StandardCharsets.UTF_8)));
            }
            objectMetadata.setUserMetadata(userMetadata);

            // 构造原地copy请求来设置用户自定义属性
            if (crc32cEnabled) {
                objectMetadata.setHeader(Constants.CRC32C_REQ_HEADER, Constants.CRC32C_REQ_HEADER_VAL);
            }

            CopyObjectRequest copyObjectRequest = new CopyObjectRequest(bucketName, key, bucketName, key);
            if (null != objectMetadata.getStorageClass()) {
                copyObjectRequest.setStorageClass(objectMetadata.getStorageClass());
            }
            copyObjectRequest.setNewObjectMetadata(objectMetadata);
            copyObjectRequest.setRedirectLocation("Replaced");
            this.setEncryptionMetadata(copyObjectRequest, objectMetadata);
            copyObjectRequest.setSourceEndpointBuilder(
                    this.cosClient.getClientConfig().getEndpointBuilder());

            try {
                callCOSClientWithRetry(copyObjectRequest);
            } catch (Exception e) {
                String errMsg = String.format("Failed to modify the user-defined attributes. " +
                        "cos key: %s, attribute: %s, exception: %s.", key, attribute, e);
                handleException(new Exception(errMsg), key);
            }
        }
    }

    /**
     * retrieve cos key
     *
     * @param key The key is the object name that is being retrieved from the
     *            cos bucket.
     * @return This method returns null if the key is not found.
     * @throws IOException Retrieve the specified cos key failed.
     */
    @Override
    public InputStream retrieve(String key) throws IOException {
        return retrieve(key, null);
    }

    @Override
    public InputStream retrieve(String key, FileMetadata fileMetadata) throws IOException {
        LOG.debug("Retrieve the key: {}, file metadata: {}.", key, fileMetadata);
        GetObjectRequest getObjectRequest =
                new GetObjectRequest(this.bucketName, key);
        if (this.trafficLimit >= 0) {
            getObjectRequest.setTrafficLimit(this.trafficLimit);
        }
        if (fileMetadata != null && fileMetadata.getETag() != null) {
            getObjectRequest.setMatchingETagConstraints(Collections.singletonList(fileMetadata.getETag()));
        }
        this.setEncryptionMetadata(getObjectRequest, new ObjectMetadata());

        boolean retryNoCache;
        do {
            retryNoCache = false;
            try {
                COSObject cosObject =
                        (COSObject) this.callCOSClientWithRetry(getObjectRequest);
                return cosObject.getObjectContent();
            } catch (CosServiceException e) {
                if (e.getStatusCode() == 412 &&  fileMetadata != null && fileMetadata.getETag() != null) {
                    // ETag mismatch，继续重试，尝试使用no-cache重试。
                    LOG.warn("ETag mismatch, retry to retrieve the key: {}, object etag: {}",
                            key, fileMetadata.getETag());
                    getObjectRequest.putCustomRequestHeader("Cache-Control", "no-cache");
                    getObjectRequest.setMatchingETagConstraints(new ArrayList<String>());
                    retryNoCache = true;
                    continue;
                }
                if (e.getErrorCode().compareToIgnoreCase("InvalidObjectState") == 0) {
                    // 可能是归档数据，应该抛出新的异常
                    throw new AccessDeniedException(key, null, e.getMessage());
                }
                String errMsg =
                        String.format("Retrieving the key %s occurs an exception: %s.",
                                key, e);
                handleException(new Exception(errMsg), key);

            } catch (CosClientException e) {
                String errMsg =
                        String.format("Retrieving key %s occurs an exception: %s.", key, e);
                handleException(new Exception(errMsg), key);
            }
        } while (retryNoCache);

        return null; // never will get here
    }

    /**
     * @param key The key is the object name that is being retrieved from the
     *            cos bucket.
     * @return This method returns null if the key is not found.
     * @throws IOException Retrieve the specified cos key with a specified byte range start failed.
     */

    @Override
    public InputStream retrieve(String key, long byteRangeStart) throws IOException {
        FileMetadata fileMetadata = this.retrieveMetadata(key);
        return retrieveBlock(key, fileMetadata, byteRangeStart, fileMetadata.getLength());
    }

    @Override
    public InputStream retrieveBlock(String key, long byteRangeStart,
                                     long byteRangeEnd) throws IOException {
        return retrieveBlock(key, this.isAZConsistencyEnabled ? this.retrieveMetadata(key) : null, byteRangeStart, byteRangeEnd);
    }

    @Override
    public InputStream retrieveBlock(String key, FileMetadata fileMetadata, long byteRangeStart, long byteRangeEnd) throws IOException {
        LOG.debug("Retrieve the cos key: {}, file metadata: {}, byte range start: {}, byte range end: {}.",
                key, fileMetadata, byteRangeStart,byteRangeEnd);
        GetObjectRequest request = new GetObjectRequest(this.bucketName, key);
        request.setRange(byteRangeStart, byteRangeEnd);
        if (this.trafficLimit >= 0) {
            request.setTrafficLimit(this.trafficLimit);
        }
        if (fileMetadata != null && fileMetadata.getETag() != null) {
            request.setMatchingETagConstraints(Collections.singletonList(fileMetadata.getETag()));
        }
        this.setEncryptionMetadata(request, new ObjectMetadata());

        boolean retryNoCache;
        do {
            retryNoCache = false;
            try {
                COSObject cosObject =
                        (COSObject) this.callCOSClientWithRetry(request);
                return cosObject.getObjectContent();
            } catch (CosServiceException e) {
                if (e.getStatusCode() == 412 &&  fileMetadata != null && fileMetadata.getETag() != null) {
                    // ETag mismatch，继续重试，尝试使用no-cache重试。
                    LOG.warn("ETag mismatch, retry to retrieve the key: {}, object etag: {}, byte range start: {}, byte range end: {}.",
                            key, fileMetadata.getETag(), byteRangeStart, byteRangeEnd);
                    request.putCustomRequestHeader("Cache-Control", "no-cache");
                    request.setMatchingETagConstraints(new ArrayList<String>());
                    retryNoCache = true;
                    continue;
                }
                if (e.getErrorCode().compareToIgnoreCase("InvalidObjectState") == 0) {
                    // 可能是归档数据，应该抛出新的异常
                    throw new AccessDeniedException(key, null, e.getMessage());
                }
                String errMsg =
                        String.format("Retrieving the key %s with the byteRangeStart [%d] " +
                                        "occurs an exception: %s.",
                                key, byteRangeStart, e);
                handleException(new Exception(errMsg), key);

            } catch (CosClientException e) {
                String errMsg =
                        String.format("Retrieving key %s with the byteRangeStart [%d] and the byteRangeEnd [%d] " +
                                "occurs an exception: %s.", key, byteRangeStart, byteRangeEnd, e);
                handleException(new Exception(errMsg), key);
            }
        } while (retryNoCache);

        return null;
    }

    public ObjectMetadata getClientSideEncryptionHeader(FileMetadata sourceFileMetadata) {
        Map<String, String> userClientEncryptionMetadata = new HashMap<>();
        for (Map.Entry<String, byte[]> userMetadataEntry : sourceFileMetadata.getUserAttributes().entrySet()) {
            if((userMetadataEntry.getKey().startsWith(CLIENT_SIDE_ENCRYPTION_PREFIX))
                    || userMetadataEntry.getKey().startsWith(COS_TAG_LEN_PREFIX)){
                userClientEncryptionMetadata.put(userMetadataEntry.getKey(), new String(userMetadataEntry.getValue()));
            }
        }
        ObjectMetadata newObjectMetadata = new ObjectMetadata();
        if(! userClientEncryptionMetadata.isEmpty()) {
            newObjectMetadata.setUserMetadata(userClientEncryptionMetadata);
        }
        return  newObjectMetadata;
    }

    @Override
    public boolean retrieveBlock(String key, long byteRangeStart,
                                 long blockSize,
                                 String localBlockPath) throws IOException {
        long fileSize = getFileLength(key);
        long byteRangeEnd = 0;
        try {
            GetObjectRequest request = new GetObjectRequest(this.bucketName,
                    key);
            if (this.trafficLimit >= 0) {
                request.setTrafficLimit(this.trafficLimit);
            }
            this.setEncryptionMetadata(request, new ObjectMetadata());
            if (fileSize > 0) {
                byteRangeEnd = Math.min(fileSize - 1,
                        byteRangeStart + blockSize - 1);
                request.setRange(byteRangeStart, byteRangeEnd);
            }
            cosClient.getObject(request, new File(localBlockPath));
            return true;
        } catch (Exception e) {
            String errMsg =
                    String.format("Retrieving block key [%s] with range [%d - %d] " +
                                    "occurs an exception: %s",
                            key, byteRangeStart, byteRangeEnd, e.getMessage());
            handleException(new Exception(errMsg), key);
            return false; // never will get here
        }
    }


    @Override
    public CosNPartialListing list(String prefix, int maxListingLength) throws IOException {
        return list(prefix, maxListingLength, null);
    }

    @Override
    public CosNPartialListing list(String prefix, int maxListingLength, CosNResultInfo info) throws IOException {
        return list(prefix, maxListingLength, null, false, info);
    }

    @Override
    public CosNPartialListing list(String prefix, int maxListingLength,
                                   String priorLastKey,
                                   boolean recurse) throws IOException {
        return list(prefix, maxListingLength, priorLastKey, recurse, null);
    }

    @Override
    public CosNPartialListing list(String prefix, int maxListingLength,
                                   String priorLastKey,
                                   boolean recurse, CosNResultInfo info) throws IOException {
        return list(prefix, recurse ? null : CosNFileSystem.PATH_DELIMITER, maxListingLength, priorLastKey, info);
    }

    /**
     * list objects
     *
     * @param prefix           prefix
     * @param delimiter        delimiter
     * @param maxListingLength max no. of entries
     * @param priorLastKey     last key in any previous search
     * @return a list of matches
     * @throws IOException on any reported failure
     */

    private CosNPartialListing list(String prefix, String delimiter,
                                    int maxListingLength,
                                    String priorLastKey, CosNResultInfo info) throws IOException {
        LOG.debug("List the cos key prefix: {}, max listing length: {}, delimiter: {}, prior last key: {}.",
                prefix,
                delimiter, maxListingLength, priorLastKey);
        if (!prefix.startsWith(CosNFileSystem.PATH_DELIMITER)) {
            prefix += CosNFileSystem.PATH_DELIMITER;
        }
        ListObjectsRequest listObjectsRequest = new ListObjectsRequest();
        listObjectsRequest.setBucketName(bucketName);
        listObjectsRequest.setPrefix(prefix);
        listObjectsRequest.setDelimiter(delimiter);
        listObjectsRequest.setMarker(priorLastKey);
        listObjectsRequest.setMaxKeys(maxListingLength);
        ObjectListing objectListing = null;
        try {
            objectListing =
                    (ObjectListing) callCOSClientWithRetry(listObjectsRequest);
        } catch (Exception e) {
            String errMsg = String.format("prefix: %s, delimiter: %s, maxListingLength: %d, "
                            + "priorLastKey: %s. list occur a exception: %s",
                    prefix, (delimiter == null) ? "" : delimiter, maxListingLength, priorLastKey, e);
            LOG.error(errMsg);
            handleException(new Exception(errMsg), prefix);
        }
        if (null == objectListing) {
            String errMessage = String.format("List objects failed for the prefix: %s, delimiter: %s, "
                            + "maxListingLength:%d, priorLastKey: %s.", prefix, (delimiter == null) ? "" : delimiter,
                    maxListingLength, priorLastKey);
            handleException(new Exception(errMessage), prefix);
        }

        ArrayList<FileMetadata> fileMetadataArray =
                new ArrayList<>();
        ArrayList<FileMetadata> commonPrefixArray =
                new ArrayList<>();
        List<COSObjectSummary> summaries = objectListing.getObjectSummaries();
        boolean isKeySamePrefix = false;
        for (COSObjectSummary cosObjectSummary : summaries) {
            String filePath = cosObjectSummary.getKey();
            if (!filePath.startsWith(CosNFileSystem.PATH_DELIMITER)) {
                filePath = CosNFileSystem.PATH_DELIMITER + filePath;
            }
            if (filePath.equals(prefix)) {
                isKeySamePrefix = true;
                continue;
            }
            long mtime = 0;
            if (cosObjectSummary.getLastModified() != null) {
                mtime = cosObjectSummary.getLastModified().getTime();
            }
            long fileLen = cosObjectSummary.getSize();
            String fileEtag = cosObjectSummary.getETag();
            if (cosObjectSummary.getKey().endsWith(CosNFileSystem.PATH_DELIMITER) && cosObjectSummary.getSize() == 0) {
                fileMetadataArray.add(new FileMetadata(filePath, fileLen, mtime, false,
                        fileEtag, null, null, null,
                        cosObjectSummary.getStorageClass()));
            } else {
                fileMetadataArray.add(new FileMetadata(filePath, fileLen, mtime,
                        true, fileEtag, null, null, null,
                        cosObjectSummary.getStorageClass()));
            }
        }
        List<String> commonPrefixes = objectListing.getCommonPrefixes();
        for (String commonPrefix : commonPrefixes) {
            if (!commonPrefix.startsWith(CosNFileSystem.PATH_DELIMITER)) {
                commonPrefix = CosNFileSystem.PATH_DELIMITER + commonPrefix;
            }
            commonPrefixArray.add(new FileMetadata(commonPrefix, 0, 0, false));
        }

        FileMetadata[] fileMetadata = new FileMetadata[fileMetadataArray.size()];
        for (int i = 0; i < fileMetadataArray.size(); ++i) {
            fileMetadata[i] = fileMetadataArray.get(i);
        }
        FileMetadata[] commonPrefixMetaData =
                new FileMetadata[commonPrefixArray.size()];
        for (int i = 0; i < commonPrefixArray.size(); ++i) {
            commonPrefixMetaData[i] = commonPrefixArray.get(i);
        }

        // 如果truncated为false, 则表明已经遍历完
        if (!objectListing.isTruncated()) {
            CosNPartialListing ret = new CosNPartialListing(null, fileMetadata, commonPrefixMetaData);
            if (info != null) {
                info.setRequestID(objectListing.getRequestId());
                info.setKeySameToPrefix(isKeySamePrefix);
            }
            return ret;
        } else {
            CosNPartialListing ret = new CosNPartialListing(objectListing.getNextMarker(),
                    fileMetadata, commonPrefixMetaData);
            if (info != null) {
                info.setRequestID(objectListing.getRequestId());
                info.setKeySameToPrefix(isKeySamePrefix);
            }
            return ret;
        }
    }

    @Override
    public void delete(String key) throws IOException {
        LOG.debug("Delete the cos key: {} from bucket: {}.", key, this.bucketName);
        try {
            DeleteObjectRequest deleteObjectRequest =
                    new DeleteObjectRequest(bucketName, key);
            callCOSClientWithRetry(deleteObjectRequest);
        } catch (Exception e) {
            String errMsg = String.format("Deleting the cos key [%s] occurs an exception: " +
                    "%s", key, e);
            handleException(new Exception(errMsg), key);
        }
    }

    /**
     * delete recursive only used on posix bucket to delete dir recursive
     *
     * @param key cos key
     * @throws IOException e
     */
    @Override
    public void deleteRecursive(String key) throws IOException {
        LOG.debug("Delete the cos key recursive: {} from bucket: {}.", key, this.bucketName);
        try {
            DeleteObjectRequest deleteObjectRequest =
                    new DeleteObjectRequest(bucketName, key);
            deleteObjectRequest.setRecursive(true);
            callCOSClientWithRetry(deleteObjectRequest);
        } catch (Exception e) {
            String errMsg = String.format("Deleting the cos key recursive [%s] occurs an exception: " +
                    "%s", key, e);
            handleException(new Exception(errMsg), key);
        }
    }

    @Override
    public void copy(String srcKey, String dstKey) throws IOException {
        copy(srcKey, null , dstKey);
    }

    @Override
    public void copy(String srcKey, FileMetadata srcFileMetadata, String dstKey) throws IOException {
        try {
            if (srcFileMetadata == null || srcFileMetadata.getUserAttributes() == null) {
                srcFileMetadata = this.retrieveMetadata(srcKey);
            }
            ObjectMetadata objectMetadata = getClientSideEncryptionHeader(srcFileMetadata);
            if (crc32cEnabled) {
                objectMetadata.setHeader(Constants.CRC32C_REQ_HEADER, Constants.CRC32C_REQ_HEADER_VAL);
            }

            CopyObjectRequest copyObjectRequest =
                    new CopyObjectRequest(bucketName, srcKey, bucketName, dstKey);
            // 如果 sourceFileMetadata 为 null，则有可能这个文件是个软链接，但是也兼容 copy
            if (null != srcFileMetadata.getStorageClass()) {
                copyObjectRequest.setStorageClass(srcFileMetadata.getStorageClass());
            }
            copyObjectRequest.setNewObjectMetadata(objectMetadata);
            this.setEncryptionMetadata(copyObjectRequest, objectMetadata);
            copyObjectRequest.setSourceEndpointBuilder(this.cosClient.getClientConfig().getEndpointBuilder());
            callCOSClientWithRetry(copyObjectRequest);
        } catch (Exception e) {
            String errMsg = String.format(
                    "Copy the object failed, src cos key: %s, dst cos key: %s, " +
                            "exception: %s", srcKey, dstKey, e);
            handleException(new Exception(errMsg), srcKey);
        }
    }

    /**
     * rename operation
     *
     * @param srcKey src cos key
     * @param dstKey dst cos key
     * @throws IOException when fail to rename the resource key to the dest key.
     */
    @Override
    public void rename(String srcKey, String dstKey) throws IOException {
        if (!isPosixBucket) {
            objectRename(srcKey, dstKey);
        } else {
            posixFileRename(srcKey, dstKey);
        }
    }

    @Override
    public void createSymlink(String symLink, String targetKey) throws IOException {
        LOG.debug("Create a symlink [{}] for the target object key [{}].", symLink, targetKey);
        if (targetKey.startsWith(CosNFileSystem.PATH_DELIMITER) && !targetKey.equals(CosNFileSystem.PATH_DELIMITER)) {
            targetKey = targetKey.substring(1);
        }
        try {
            PutSymlinkRequest putSymlinkRequest = new PutSymlinkRequest(this.bucketName, symLink, targetKey);
            PutSymlinkResult putSymlinkResult = (PutSymlinkResult) callCOSClientWithRetry(putSymlinkRequest);
        } catch (Exception e) {
            String errMsg = String.format(
                "Create the symlink failed. symlink: %s, target cos key: %s, exception: %s.",
                symLink, targetKey, e);
            handleException(new Exception(errMsg), symLink);
        }
    }

    @Override
    public String getSymlink(String symlink) throws IOException {
        LOG.debug("Get the symlink [{}].", symlink);
        try {
            GetSymlinkRequest getSymlinkRequest = new GetSymlinkRequest(
                this.bucketName, symlink, null);
            GetSymlinkResult getSymlinkResult = (GetSymlinkResult) callCOSClientWithRetry(getSymlinkRequest);
            return getSymlinkResult.getTarget();
        } catch (CosServiceException cosServiceException) {
            if (cosServiceException.getStatusCode() == 400 &&
                cosServiceException.getErrorCode().compareToIgnoreCase("NotSymlink") == 0) {
                LOG.debug("The key [{}] is not a symlink.", symlink);
                return null;
            }

            if (cosServiceException.getStatusCode() != 404) {
                String errMsg = String.format(
                    "Get the symlink failed. symlink: %s, exception: %s.", symlink, cosServiceException);
                handleException(new Exception(errMsg), symlink);
            }
        }
        return null;
    }

    public void objectRename(String srcKey, String dstKey) throws IOException {
        LOG.debug("Rename normal bucket key, the source cos key [{}] to the dest cos key [{}].", srcKey, dstKey);
        try {
            FileMetadata sourceFileMetadata = this.retrieveMetadata(srcKey);
            ObjectMetadata objectMetadata = getClientSideEncryptionHeader(sourceFileMetadata);
            if (crc32cEnabled) {
                objectMetadata.setHeader(Constants.CRC32C_REQ_HEADER, Constants.CRC32C_REQ_HEADER_VAL);
            }
            CopyObjectRequest copyObjectRequest =
                    new CopyObjectRequest(bucketName, srcKey, bucketName, dstKey);
            // get the storage class of the source file
            if (null != sourceFileMetadata.getStorageClass()) {
                copyObjectRequest.setStorageClass(sourceFileMetadata.getStorageClass());
            }
            copyObjectRequest.setNewObjectMetadata(objectMetadata);
            this.setEncryptionMetadata(copyObjectRequest, objectMetadata);
            copyObjectRequest.setSourceEndpointBuilder(this.cosClient.getClientConfig().getEndpointBuilder());
            callCOSClientWithRetry(copyObjectRequest);
            DeleteObjectRequest deleteObjectRequest =
                    new DeleteObjectRequest(bucketName, srcKey);
            callCOSClientWithRetry(deleteObjectRequest);
        } catch (Exception e) {
            String errMsg = String.format(
                    "Rename object failed, normal bucket, source cos key: %s, dest cos key: %s, " +
                            "exception: %s", srcKey, dstKey, e);
            handleException(new Exception(errMsg), srcKey);
        }
    }

    public void posixFileRename(String srcKey, String dstKey) throws IOException {
        LOG.debug("Rename posix bucket key, the source cos key [{}] to the dest cos key [{}].", srcKey, dstKey);
        try {
            RenameRequest renameRequest = new RenameRequest(bucketName, srcKey, dstKey);
            callCOSClientWithRetry(renameRequest);
        } catch (Exception e) {
            String errMsg = String.format(
                    "Rename object failed, posix bucket, source cos key: %s, dest cos key: %s, " +
                            "exception: %s", srcKey, dstKey, e);
            handleException(new Exception(errMsg), srcKey);
        }
    }

    @Override
    public void purge(String prefix) throws IOException {
        throw new IOException("purge not supported");
    }

    @Override
    public void dump() throws IOException {
        throw new IOException("dump not supported");
    }

    @Override
    public void close() {
        if (null != this.cosClient) {
            this.cosClient.shutdown();
        }

        if (null != this.rangerCredentialsClient) {
            this.rangerCredentialsClient.close();
        }

        this.rangerCredentialsClient = null;
        this.cosClient = null;
    }

    private void preClose() {
        if (null != this.cosClient) {
            this.cosClient.shutdown();
        }

        this.cosClient = null;
    }

    @Override
    public void setPosixBucket(boolean isPosixBucket) {
        this.isPosixBucket = isPosixBucket;
    }

    // process Exception and print detail
    private void handleException(Exception e, String key) throws IOException {
        String cosPath = "cosn://" + bucketName + key;
        String exceptInfo = String.format("%s : %s", cosPath, e.toString());
        throw new IOException(exceptInfo);
    }

    @Override
    public long getFileLength(String key) throws IOException {
        GetObjectMetadataRequest getObjectMetadataRequest =
                new GetObjectMetadataRequest(bucketName, key);
        this.setEncryptionMetadata(getObjectMetadataRequest, new ObjectMetadata());
        try {
            ObjectMetadata objectMetadata =
                    (ObjectMetadata) callCOSClientWithRetry(getObjectMetadataRequest);
            return objectMetadata.getContentLength();
        } catch (Exception e) {
            String errMsg = String.format("Getting the file length occurs an exception, " +
                            "cos key: %s, exception: %s",
                    key, e.getMessage());
            handleException(new Exception(errMsg), key);
            return 0; // never will get here
        }
    }

    @Override
    public CosNPartListing listParts(String key, String uploadId) throws IOException {
        LOG.debug("List parts key: {}, uploadId: {}", key, uploadId);
        ListPartsRequest listPartsRequest = new ListPartsRequest(bucketName, key, uploadId);
        PartListing partListing = null;
        List<PartSummary> partSummaries = new LinkedList<>();
        do {
           try {
               partListing = (PartListing) callCOSClientWithRetry(listPartsRequest);
           } catch (Exception e) {
               String errMsg = String.format("list parts occurs an exception, " +
                               "cos key: %s, upload id: %s, exception: %s",
                       key, uploadId, e.getMessage());
               LOG.error(errMsg);
               handleException(new Exception(errMsg), key);
               return null;
           }
           partSummaries.addAll(partListing.getParts());
           listPartsRequest.setPartNumberMarker(partListing.getNextPartNumberMarker());
        } while (partListing.isTruncated());
        return new CosNPartListing(partSummaries);
    }

    @Override
    public boolean isPosixBucket() {
        return this.isPosixBucket;
    }

    private PartETag isPartExist(CosNPartListing partListing, int partNum, long partSize) {
        PartETag ret = null;
        if (null == partListing) {
            return null;
        }
        for (PartSummary partSummary : partListing.getPartSummaries()){
            // for now only check number and size.
            if (partSummary.getPartNumber() == partNum && partSummary.getSize() == partSize) {
                ret = new PartETag(partSummary.getPartNumber(), partSummary.getETag());
                break;
            }
        }
        return ret;
    }

    private <X> void callCOSClientWithSSEKMS(X request, SSECOSKeyManagementParams managementParams) {
        try {
            if (request instanceof PutObjectRequest) {
                ((PutObjectRequest) request).setSSECOSKeyManagementParams(managementParams);
            } else if (request instanceof CopyObjectRequest) {
                ((CopyObjectRequest) request).setSSECOSKeyManagementParams(managementParams);
            } else if (request instanceof InitiateMultipartUploadRequest) {
                ((InitiateMultipartUploadRequest) request).setSSECOSKeyManagementParams(managementParams);
            }
        } catch (Exception e) {
            String errMsg =
                    String.format("callCOSClientWithSSEKMS failed:" +
                            " %s", e);
            LOG.error(errMsg);
        }
    }

    private <X> void callCOSClientWithSSECOS(X request, ObjectMetadata objectMetadata) {
        try {
            objectMetadata.setServerSideEncryption(SSEAlgorithm.AES256.getAlgorithm());
            if (request instanceof PutObjectRequest) {
                ((PutObjectRequest) request).setMetadata(objectMetadata);
            } else if (request instanceof UploadPartRequest) {
                ((UploadPartRequest) request).setObjectMetadata(objectMetadata);
            } else if (request instanceof CopyObjectRequest) {
                ((CopyObjectRequest) request).setNewObjectMetadata(objectMetadata);
            } else if (request instanceof InitiateMultipartUploadRequest) {
                ((InitiateMultipartUploadRequest) request).setObjectMetadata(objectMetadata);
            }
        } catch (Exception e) {
            String errMsg = String.format("callCOSClientWithSSECOS failed: %s", e);
            LOG.error(errMsg);
        }
    }

    private <X> void callCOSClientWithSSEC(X request, SSECustomerKey sseKey) {
        try {
            if (request instanceof PutObjectRequest) {
                ((PutObjectRequest) request).setSSECustomerKey(sseKey);
            } else if (request instanceof UploadPartRequest) {
                ((UploadPartRequest) request).setSSECustomerKey(sseKey);
            } else if (request instanceof GetObjectMetadataRequest) {
                ((GetObjectMetadataRequest) request).setSSECustomerKey(sseKey);
            } else if (request instanceof CopyObjectRequest) {
                ((CopyObjectRequest) request).setDestinationSSECustomerKey(sseKey);
                ((CopyObjectRequest) request).setSourceSSECustomerKey(sseKey);
            } else if (request instanceof GetObjectRequest) {
                ((GetObjectRequest) request).setSSECustomerKey(sseKey);
            } else if (request instanceof InitiateMultipartUploadRequest) {
                ((InitiateMultipartUploadRequest) request).setSSECustomerKey(sseKey);
            }
        } catch (Exception e) {
            String errMsg = String.format("callCOSClientWithSSEC failed: %s", e);
            LOG.error(errMsg);
        }
    }

    private <X> void setEncryptionMetadata(X request, ObjectMetadata objectMetadata) {
        switch (encryptionSecrets.getEncryptionMethod()) {
            case SSE_C:
                callCOSClientWithSSEC(request, new SSECustomerKey(encryptionSecrets.getEncryptionKey()));
                break;
            case SSE_COS:
                callCOSClientWithSSECOS(request, objectMetadata);
                break;
            case SSE_KMS:
                String key = encryptionSecrets.getEncryptionKey();
                String context = Base64.encodeAsString(encryptionSecrets.getEncryptionContext().getBytes());
                SSECOSKeyManagementParams ssecosKeyManagementParams = new SSECOSKeyManagementParams(key, context);
                callCOSClientWithSSEKMS(request, ssecosKeyManagementParams);
                break;
            case NONE:
            default:
                break;
        }
    }

    private void checkEncryptionMethod(ClientConfig config,
                                       CosNEncryptionMethods cosSSE, String sseKey) throws IOException {
        int sseKeyLen = StringUtils.isNullOrEmpty(sseKey) ? 0 : sseKey.length();

        String description = "Encryption key:";
        if (sseKey == null) {
            description += "null ";
        } else {
            switch (sseKeyLen) {
                case 0:
                    description += " empty";
                    break;
                case 1:
                    description += " of length 1";
                    break;
                default:
                    description = description + " of length " + sseKeyLen + " ending with "
                            + sseKey.charAt(sseKeyLen - 1);
            }
        }

        switch (cosSSE) {
            case SSE_C:
                LOG.debug("Using SSE_C with {}", description);
                config.setHttpProtocol(HttpProtocol.https);
                if (sseKeyLen == 0) {
                    throw new IOException("missing encryption key for SSE_C ");
                } else if (!sseKey.matches(CosNConfigKeys.BASE64_PATTERN)) {
                    throw new IOException("encryption key need to Base64 encoding for SSE_C ");
                }
                break;
            case SSE_COS:
                if (sseKeyLen != 0) {
                    LOG.debug("Using SSE_COS");
                    throw new IOException("SSE_COS to encryption with key error: "
                            + " (" + description + ")");
                }
                break;
            case SSE_KMS:
                // sseKeyLen can be empty which auto used by cos server
                System.setProperty(SkipMd5CheckStrategy.DISABLE_PUT_OBJECT_MD5_VALIDATION_PROPERTY, "true");
                config.setHttpProtocol(HttpProtocol.https);
            case NONE:
            default:
                LOG.debug("Data is unencrypted");
                break;
        }
    }

    // posix bucket mkdir if the middle part exist will return the 500 error,
    // and the rename if the dst exist will return the 500 status too,
    // which make the related 5** retry useless. mo improve the resp info to filter.
    private <X> Object innerCallWithRetry(X request) throws CosServiceException, IOException {
        String sdkMethod = "";
        int retryIndex = 1;
        int l5ErrorCodeRetryIndex = 1;
        while (true) {
            try {
                if (request instanceof PutObjectRequest) {
                    sdkMethod = "putObject";
                    if (((PutObjectRequest) request).getInputStream().markSupported()) {
                        ((PutObjectRequest) request).getInputStream()
                                .mark((int) ((PutObjectRequest) request).getMetadata().getContentLength());
                    }
                    return this.cosClient.putObject((PutObjectRequest) request);
                } else if (request instanceof UploadPartRequest) {
                    sdkMethod = "uploadPart";
                    if (((UploadPartRequest) request).getInputStream().markSupported()) {
                        ((UploadPartRequest) request).getInputStream()
                                .mark((int) ((UploadPartRequest) request).getPartSize());
                    }
                    return this.cosClient.uploadPart((UploadPartRequest) request);
                } else if (request instanceof CopyPartRequest) {
                    sdkMethod = "copyPartRequest";
                    return this.cosClient.copyPart((CopyPartRequest) request);
                } else if (request instanceof HeadBucketRequest) { // use for checking bucket type
                    sdkMethod = "headBucket";
                    return this.cosClient.headBucket((HeadBucketRequest) request);
                } else if (request instanceof RenameRequest) {
                    sdkMethod = "rename";
                    this.cosClient.rename((RenameRequest) request);
                    return new Object();
                } else if (request instanceof GetObjectMetadataRequest) {
                    sdkMethod = "queryObjectMeta";
                    return this.cosClient.getObjectMetadata((GetObjectMetadataRequest) request);
                } else if (request instanceof DeleteObjectRequest) {
                    sdkMethod = "deleteObject";
                    this.cosClient.deleteObject((DeleteObjectRequest) request);
                    return new Object();
                } else if (request instanceof CopyObjectRequest) {
                    sdkMethod = "copyFile";
                    return this.cosClient.copyObject((CopyObjectRequest) request);
                } else if (request instanceof GetObjectRequest) {
                    sdkMethod = "getObject";
                    return this.cosClient.getObject((GetObjectRequest) request);
                } else if (request instanceof ListObjectsRequest) {
                    sdkMethod = "listObjects";
                    return this.cosClient.listObjects((ListObjectsRequest) request);
                } else if (request instanceof InitiateMultipartUploadRequest) {
                    sdkMethod = "initiateMultipartUpload";
                    return this.cosClient.initiateMultipartUpload((InitiateMultipartUploadRequest) request);
                } else if (request instanceof CompleteMultipartUploadRequest) {
                    sdkMethod = "completeMultipartUpload";
                    return this.cosClient.completeMultipartUpload((CompleteMultipartUploadRequest) request);
                } else if (request instanceof AbortMultipartUploadRequest) {
                    sdkMethod = "abortMultipartUpload";
                    this.cosClient.abortMultipartUpload((AbortMultipartUploadRequest) request);
                    return new Object();
                } else if (request instanceof PutSymlinkRequest) {
                    sdkMethod = "putSymlink";
                    return this.cosClient.putSymlink((PutSymlinkRequest) request);
                } else if (request instanceof GetSymlinkRequest) {
                    sdkMethod = "getSymlink";
                    return this.cosClient.getSymlink((GetSymlinkRequest) request);
                } else if (request instanceof ListPartsRequest) {
                    sdkMethod = "listParts";
                    return this.cosClient.listParts((ListPartsRequest) request);
                } else {
                    throw new IOException("no such method");
                }
            } catch (ResponseNotCompleteException nce) {
                if (this.completeMPUCheckEnabled && request instanceof CompleteMultipartUploadRequest) {
                    String key = ((CompleteMultipartUploadRequest) request).getKey();
                    FileMetadata fileMetadata = this.queryObjectMetadata(key);
                    if (null == fileMetadata) {
                        // if file not exist must throw the exception.
                        handleException(nce, key);
                    }
                    LOG.warn("Complete mpu resp not complete key [{}]", key);
                    // todo: some other double check after cgi unified the ctime of mpu.
                    return new CompleteMultipartUploadResult();
                } else {
                    throw new IOException(nce);
                }
            } catch (CosServiceException cse) {
                String errMsg = String.format("all cos sdk failed, retryIndex: [%d / %d], "
                        + "call method: %s, exception: %s", retryIndex, this.maxRetryTimes, sdkMethod, cse);
                int statusCode = cse.getStatusCode();
                String errorCode = cse.getErrorCode();
                LOG.debug("fail to retry statusCode {}, errorCode {}", statusCode, errorCode);
                // 对5xx错误进行重试
                if (request instanceof CopyObjectRequest && hasErrorCode(statusCode, errorCode)) {
                    if (retryIndex <= this.maxRetryTimes) {
                        LOG.info(errMsg, cse);
                        ++retryIndex;
                    } else {
                        LOG.error(errMsg, cse);
                        throw new IOException(errMsg);
                    }
                } else if (request instanceof CompleteMultipartUploadRequest && hasErrorCode(statusCode, errorCode)) {
                    // complete mpu error code might be in body when status code is 200
                    // double check to head object only works in big data job case which key is not same.
                    String key = ((CompleteMultipartUploadRequest) request).getKey();
                    FileMetadata fileMetadata = this.queryObjectMetadata(key);
                    if (null != fileMetadata) {
                        // if file exist direct return.
                        LOG.info("complete mpu error in body, error code {}, but key {} already exist, length {}",
                                errorCode, key, fileMetadata.getLength());
                        return new CompleteMultipartUploadResult();
                    }
                    // here same like the copy request not setting the interval sleep for now
                    if (retryIndex <= this.maxRetryTimes) {
                        LOG.info(errMsg, cse);
                        ++retryIndex;
                    } else {
                        LOG.error(errMsg, cse);
                        throw new IOException(errMsg);
                    }
                } else if (statusCode / 100 == 5) {
                    if (retryIndex <= this.maxRetryTimes) {
                        if (statusCode == 503) {
                            if (useL5Id) {
                                if (l5ErrorCodeRetryIndex >= this.l5UpdateMaxRetryTimes) {
                                    // L5上报，进行重试
                                    if (this.tencentL5EndpointResolver instanceof HandlerAfterProcess) {
                                        ((HandlerAfterProcess) this.tencentL5EndpointResolver).handle(-1, 0);
                                    }
                                    l5ErrorCodeRetryIndex = 1;
                                } else {
                                    l5ErrorCodeRetryIndex = l5ErrorCodeRetryIndex + 1;
                                }
                            }

                            if (this.usePolaris) {
                                if (l5ErrorCodeRetryIndex >= this.l5UpdateMaxRetryTimes) {
                                    // Polaris上报，进行重试
                                    if (this.tencentPolarisEndpointResolver instanceof HandlerAfterProcess) {
                                        ((HandlerAfterProcess) this.tencentPolarisEndpointResolver).handle(-1, 0);
                                    }
                                    l5ErrorCodeRetryIndex = 1;
                                } else {
                                    l5ErrorCodeRetryIndex = l5ErrorCodeRetryIndex + 1;
                                }
                            }
                        }

                        LOG.info(errMsg, cse);
                        long sleepLeast = retryIndex * 300L;
                        long sleepBound = retryIndex * 500L;
                        try {
                            if (request instanceof PutObjectRequest) {
                                LOG.info("Try to reset the put object request input stream.");
                                if (((PutObjectRequest) request).getInputStream().markSupported()) {
                                    ((PutObjectRequest) request).getInputStream().reset();
                                } else {
                                    LOG.error("The put object request input stream can not be reset, so it can not be" +
                                            " retried.");
                                    throw cse;
                                }
                            }

                            if (request instanceof UploadPartRequest) {
                                LOG.info("Try to reset the upload part request input stream.");
                                if (((UploadPartRequest) request).getInputStream().markSupported()) {
                                    ((UploadPartRequest) request).getInputStream().reset();
                                } else {
                                    LOG.error("The upload part request input stream can not be reset, so it can not " +
                                            "be retried" +
                                            ".");
                                    throw cse;
                                }
                            }

                            // mpu might occur 503 access time out but already completed,
                            // if direct retry may occur 403 not found the upload id.
                            if (request instanceof CompleteMultipartUploadRequest && isServiceError(statusCode)) {
                                String key = ((CompleteMultipartUploadRequest) request).getKey();
                                FileMetadata fileMetadata = this.queryObjectMetadata(key);
                                if (null != fileMetadata) {
                                    // if file exist direct return.
                                    LOG.info("complete mpu error might access time out, " +
                                                    "but key {} already exist, length {}",
                                            key, fileMetadata.getLength());
                                    return new CompleteMultipartUploadResult();
                                }
                            }
                            LOG.info("Ready to retry [{}], wait time: [{} - {}] ms", retryIndex, sleepLeast, sleepBound);
                            Thread.sleep(
                                    ThreadLocalRandom.current().nextLong(sleepLeast, sleepBound));
                            ++retryIndex;
                        } catch (InterruptedException e) {
                            throw new IOException(e.toString());
                        }
                    } else {
                        LOG.error(errMsg, cse);
                        throw new IOException(errMsg);
                    }
                } else {
                    throw cse;
                }
            } catch (IllegalStateException e) {
                String message = e.getMessage();
                if (message.contains("Connection pool shut down")) {
                    throw new IOException("call cos happen error which http connection pool has shutdown,"
                            + "please check whether the file system is closed or the program has an OOM, , exception:"
                            + " {}", e);
                } else {
                    throw new IOException(e);
                }
            } catch (Exception e) {
                throw new IOException(e);
            }
        }
    }

    private <X> Object callCOSClientWithRetry(X request) throws CosServiceException, IOException {
        // 设置线程的context class loader
        // 之前有客户通过spark-sql执行add jar命令, 当spark.eventLog.dir为cos, jar路径也在cos时, 会导致cos读取数据时，
        // http库的日志加载，又会加载cos上的文件，以此形成了逻辑死循环
        // 1 上层调用cos read
        //2 cos插件通过Apache http库读取数据
        //3 http库里面初始化日志对象时要读取日志配置，发现配置是在cos上
        //4 调用cos read

        // 分析后发现，日志库里面获取资源是通过context class loader, 而add jar会改变context class loader，将被add jar也加入classpath路径中
        // 因此这里通过设置context class loader为app class loader。 避免被上层add jar等改变context class loader行为污染
        // it seems like a temporary solution, why we need to care dependence class loader way?
        ClassLoader originalClassLoader = Thread.currentThread().getContextClassLoader();
        LOG.debug("flush task, current classLoader: {}, context ClassLoader: {}",
                this.getClass().getClassLoader(), originalClassLoader);
        try {
            Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());
            return innerCallWithRetry(request);
        } finally {
            Thread.currentThread().setContextClassLoader(originalClassLoader);
        }
    }

    private static String ensureValidAttributeName(String attributeName) {
        return attributeName.replace('.', '-').toLowerCase();
    }

    private boolean hasErrorCode(int statusCode, String errCode) {
        return statusCode / 100 == 2 && errCode != null && !errCode.isEmpty();
    }

    private boolean isServiceError(int statusCode) {
        return statusCode / 100 == 5;
    }

    public COSClient getCOSClient() {
        return this.cosClient;
    }

    public RangerCredentialsClient getRangerCredentialsClient() {
        return this.rangerCredentialsClient;
    }

    private String getPluginVersionInfo() {
        Properties versionProperties = new Properties();
        InputStream inputStream = null;
        String versionStr = "unknown";
        final String versionFile = "hadoopCosPluginVersionInfo.properties";
        try {
            inputStream = this.getClass().getClassLoader().getResourceAsStream(versionFile);
            if (inputStream != null) {
                versionProperties.load(inputStream);
                versionStr = versionProperties.getProperty("plugin_version");
            } else {
                LOG.error("load versionInfo properties failed, propName: {} ", versionFile);
            }
        } catch (IOException e) {
            LOG.error("load versionInfo properties exception, propName: {} ", versionFile);
        } finally {
            IOUtils.closeQuietly(inputStream, LOG);
        }
        return versionStr;
    }

    //for client side encryption，copy tmp file to expected file，set real file size in user metadata.
    public void ModifyDataSize(String key, long fileSize) throws IOException {
        try {
            GetObjectMetadataRequest getObjectMetadataRequest = new GetObjectMetadataRequest(bucketName, key);
            ObjectMetadata objectMetadata = (ObjectMetadata) callCOSClientWithRetry(getObjectMetadataRequest);

            Map<String, String> objectMetadata_tmp= objectMetadata.getUserMetadata();
            objectMetadata_tmp.put(CSE_MPU_FILE_SIZE, Long.toString(fileSize));
            objectMetadata.setUserMetadata(objectMetadata_tmp);

            CopyObjectRequest copyObjectRequest =
                    new CopyObjectRequest(bucketName, key, bucketName, key.substring(0, key.length() - CSE_MPU_KEY_SUFFIX.length()));
            if (null != objectMetadata.getStorageClass()) {
                copyObjectRequest.setStorageClass(objectMetadata.getStorageClass());
            }
            copyObjectRequest.setNewObjectMetadata(objectMetadata);
            this.setEncryptionMetadata(copyObjectRequest, objectMetadata);
            copyObjectRequest.setSourceEndpointBuilder(this.cosClient.getClientConfig().getEndpointBuilder());
            callCOSClientWithRetry(copyObjectRequest);
        } catch (Exception e) {
            String errMsg = String.format(
                    "Modify the object datasize failed, cos key: %s, " +
                            "exception: %s", key, e);
            handleException(new Exception(errMsg), key);
        } finally {
            try {
                delete(key);
            } catch (IOException e) {
                String errMsg = String.format(
                        "delete CSE temporary file failed, cos key: %s, " +
                                "exception: %s", key, e);
                handleException(new Exception(errMsg), key);
            }
        }
    }
}

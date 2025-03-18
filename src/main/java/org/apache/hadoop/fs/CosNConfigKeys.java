package org.apache.hadoop.fs;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.cosn.Constants;
import org.apache.hadoop.fs.cosn.Unit;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * This class contains constants for configuration keys used in the cos file system.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class CosNConfigKeys extends CommonConfigurationKeys {
    public static final String USER_AGENT = "fs.cosn.user.agent";

    private static String version;

    static {
        String path = "META-INF/maven/com.qcloud.cos/hadoop-cos/pom.properties";
        Properties properties = new Properties();
        try (InputStream in = CosNConfigKeys.class.getClassLoader().getResourceAsStream(path)) {
            if (in == null) {
                version = "unknown";
            } else {
                properties.load(in);
                version = properties.getProperty("version");
            }
        } catch (IOException e) {
            version = "unknown";
        }
    }

    public static final String DEFAULT_USER_AGENT = "cos-hadoop-plugin-v" + version;

    public static final String TENCENT_EMR_VERSION_KEY = "fs.emr.version";

    public static final String COSN_STORAGE_CLASS_KEY = "fs.cosn.storage.class";

    public static final String COSN_CREDENTIALS_PROVIDER = "fs.cosn.credentials.provider";
    public static final String COSN_APPID_KEY = "fs.cosn.userinfo.appid";
    public static final String COSN_UIN_KEY = "fs.cosn.userinfo.uin";
    public static final String COSN_REQUEST_ID = "fs.cosn.request.id";
    public static final String COS_REMOTE_CREDENTIAL_PROVIDER_URL = "fs.cosn.remote-credential-provider.url";
    public static final String COS_REMOTE_CREDENTIAL_PROVIDER_PATH = "fs.cosn.remote-credential-provider.path";
    public static final String COS_CUSTOM_CREDENTIAL_PROVIDER_URL = "fs.cosn.custom-credential-provider.url";
    public static final String COSN_USERINFO_SECRET_ID_KEY = "fs.cosn.userinfo.secretId";
    public static final String COSN_USERINFO_SECRET_KEY_KEY = "fs.cosn.userinfo.secretKey";
    public static final String COSN_USERINFO_SESSION_TOKEN = "fs.cosn.userinfo.sessionToken";
    public static final String COSN_REGION_KEY = "fs.cosn.bucket.region";
    public static final String COSN_REGION_PREV_KEY = "fs.cosn.userinfo.region";
    public static final String COSN_ENDPOINT_SUFFIX_KEY = "fs.cosn.bucket.endpoint_suffix";
    public static final String COSN_ENDPOINT_SUFFIX_PREV_KEY = "fs.cosn.userinfo.endpoint_suffix";

    public static final String COSN_CUSTOM_ENDPOINT_SUFFIX = "fs.cosn.custom.endpoint_suffix";

    public static final String COSN_DISTINGUISH_HOST_FLAG = "fs.cosn.distinguish.host.flag";

    public static final boolean DEFAULT_COSN_DISTINGUISH_HOST_FLAG = false;

    public static final String COSN_USE_HTTPS_KEY = "fs.cosn.useHttps";
    public static final boolean DEFAULT_USE_HTTPS = true;   // 现在 COS 强制使用 https 作为访问协议

    // 这个腾讯内部 L5 负载均衡系统的配置，主要限于内部项目依赖使用，现在改用北极星了。
    // 这些内部扩展（L5、北极星和北极星 SideCar）需要依赖 tencent-internal-extension 组件，这个只有腾讯内部仓库才有。坐标如下：
    // <dependency>
    //    <groupId>com.qcloud.cos</groupId>
    //    <artifactId>tencent-internal-extension</artifactId>
    //    <version>${version}</version>
    //</dependency>
    @Deprecated
    public static final String COSN_L5_KEY = "fs.cosn.bucket.l5";
    public static final boolean DEFAULT_COSN_USE_L5_ENABLE = false;
    public static final String COSN_USE_L5_ENABLE = "fs.cosn.use.l5.enable";
    // 默认这个不要更需要更改。只有运行时依赖的组件确实发生了变动再更改
    public static final String COSN_L5_RESOLVER_CLASS = "fs.cosn.l5.resolver.class";
    public static final String DEFAULT_COSN_L5_RESOLVER_CLASS = "com.qcloud.cos.internal.endpoint.resolver.TencentCloudL5EndpointResolver";

    public static final String COSN_USE_POLARIS_ENABLED = "fs.cosn.polaris.enabled";
    public static final boolean DEFAULT_COSN_USE_POLARIS_ENABLED = false;
    public static final String COSN_POLARIS_RESOLVER_CLASS = "fs.cosn.polaris.resolver.class";
    public static final String DEFAULT_COSN_POLARIS_RESOLVER_CLASS = "com.qcloud.cos.internal.endpoint.resolver.TencentPolarisEndpointResolver";
    public static final String COSN_POLARIS_NAMESPACE = "fs.cosn.polaris.namespace";
    public static final String COSN_POLARIS_SERVICE = "fs.cosn.polaris.service";
    public static final String COSN_L5_UPDATE_MAX_RETRIES_KEY = "fs.cosn.l5.update.maxRetries";
    public static final int DEFAULT_COSN_L5_UPDATE_MAX_RETRIES = 5;

    // 如果进程不能内嵌运行北极星，使用sidecar方式运行
    public static final String COSN_USE_POLARIS_SIDECAR_ENABLED = "fs.cosn.polaris.sidecar.enabled";
    public static final String COSN_POLARIS_SIDECAR_CLIENT_IMPL = "fs.cosn.polaris.sidecar.client.impl";
    public static final String DEFAULT_COSN_POLARIS_SIDECAR_CLIENT_IMPL = "com.qcloud.cos.internal.endpoint.resolver.TencentPolarisSidecarClient";
    public static final String COSN_POLARIS_SIDECAR_RESOLVER_CLASS = "fs.cosn.polaris.sidecar.resolver.class";
    public static final String DEFAULT_COSN_POLARIS_SIDECAR_RESOLVER_CLASS = "com.qcloud.cos.internal.endpoint.resolver.TencentPolarisSidecarEndpointResolver";
    public static final boolean DEFAULT_COSN_USE_POLARIS_SIDECAR_ENABLED = false;
    public static final String COSN_POLARIS_SIDECAR_ADDRESS = "fs.cosn.polaris.sidecar.address";

    public static final String COSN_TMP_DIR = "fs.cosn.tmp.dir";
    public static final String DEFAULT_TMP_DIR = "/tmp/hadoop_cos";

    // upload checks, turn on default.
    public static final String COSN_UPLOAD_CHECKS_ENABLED_KEY = "fs.cosn.upload.checks.enabled";
    public static final boolean DEFAULT_COSN_UPLOAD_CHECKS_ENABLE = true;

    public static final String COSN_UPLOAD_PART_SIZE_KEY = "fs.cosn.upload.part.size";
    public static final long DEFAULT_UPLOAD_PART_SIZE = 8 * Unit.MB;

    public static final String COSN_UPLOAD_BUFFER_TYPE_KEY = "fs.cosn.upload.buffer";
    public static final String DEFAULT_UPLOAD_BUFFER_TYPE = "mapped_disk";

    public static final String COSN_UPLOAD_PART_CHECKSUM_ENABLED_KEY = "fs.cosn.upload.part.checksum.enabled";
    public static final boolean DEFAULT_UPLOAD_PART_CHECKSUM_ENABLED = true;

    public static final String COSN_UPLOAD_BUFFER_SIZE_KEY = "fs.cosn.upload.buffer.size";
    public static final String COSN_UPLOAD_BUFFER_SIZE_PREV_KEY = "fs.cosn.buffer.size";
    public static final int DEFAULT_UPLOAD_BUFFER_SIZE = -1;

    public static final String COSN_POSIX_EXTENSION_ENABLED = "fs.cosn.posix_extension.enabled";
    public static final boolean DEFAULT_COSN_POSIX_EXTENSION_ENABLED = false;
    // Auxiliary space to support the POSIX seekable writing semantics.
    public static final String COSN_POSIX_EXTENSION_TMP_DIR = "fs.cosn.posix_extension.tmp.dir";

    public static final String COSN_POSIX_EXTENSION_TMP_DIR_WATERMARK_HIGH =
        "fs.cosn.posix_extension.tmp.dir.watermark.high";
    public static final float DEFAULT_COSN_POSIX_EXTENSION_TMP_DIR_WATERMARK_HIGH = (float) 0.85;
    public static final String COSN_POSIX_EXTENSION_TMP_DIR_WATERMARK_LOW =
        "fs.cosn.posix_extension.tmp.dir.watermark.low";
    public static final float DEFAULT_COSN_POSIX_EXTENSION_TMP_DIR_WATERMARK_LOW = (float) 0.5;

    public static final String COSN_POSIX_EXTENSION_TMP_DIR_QUOTA = "fs.cosn.posix_extension.tmp.dir.quota";
    public static final long DEFAULT_COSN_POSIX_EXTENSION_TMP_DIR_QUOTA = 2 * Unit.GB;

    public static final String COSN_BLOCK_SIZE_KEY = "fs.cosn.block.size";
    public static final long DEFAULT_BLOCK_SIZE = 128 * Unit.MB;
    public static final String COSN_MAX_RETRIES_KEY = "fs.cosn.maxRetries";
    public static final int DEFAULT_MAX_RETRIES = 200;
    public static final String COSN_RETRY_INTERVAL_KEY = "fs.cosn.retry.interval.seconds";
    public static final long DEFAULT_RETRY_INTERVAL = 3;
    public static final String CLIENT_MAX_RETRIES_KEY = "fs.cosn.client.maxRetries";
    public static final int DEFAULT_CLIENT_MAX_RETRIES = 5;
    public static final String CLIENT_SOCKET_ERROR_MAX_RETRIES = "fs.cosn.socket.error.maxRetries";
    public static final int DEFAULT_CLIENT_SOCKET_ERROR_MAX_RETRIES = 5;

    public static final String UPLOAD_THREAD_POOL_SIZE_KEY = "fs.cosn.upload_thread_pool";
    public static final int DEFAULT_UPLOAD_THREAD_POOL_SIZE = 10;

    public static final String IO_THREAD_POOL_MAX_SIZE_KEY = "fs.cosn.io_thread_pool.maxSize";
    public static final int DEFAULT_IO_THREAD_POOL_MAX_SIZE = 2 * Runtime.getRuntime().availableProcessors();

    public static final String COPY_THREAD_POOL_SIZE_KEY = "fs.cosn.copy_thread_pool";
    public static final int DEFAULT_COPY_THREAD_POOL_SIZE = 10;

    public static final String DELETE_THREAD_POOL_SIZE_KEY = "fs.cosn.delete_thread_pool";
    public static final int DEFAULT_DELETE_THREAD_POOL_SIZE = 10;

    public static final String DELETE_THREAD_POOL_MAX_SIZE_KEY = "fs.cosn.delete_thread_pool.maxSize";
    public static final int DEFAULT_DELETE_THREAD_POOL_MAX_SIZE = 2 * Runtime.getRuntime().availableProcessors();

    public static final String THREAD_KEEP_ALIVE_TIME_KEY = "fs.cosn.threads.keep_alive_time";
    public static final long DEFAULT_THREAD_KEEP_ALIVE_TIME = 60L;

    public static final String READ_AHEAD_BLOCK_SIZE_KEY = "fs.cosn.read.ahead.block.size";
    public static final long DEFAULT_READ_AHEAD_BLOCK_SIZE = 1 * Unit.MB;
    public static final String READ_AHEAD_QUEUE_SIZE = "fs.cosn.read.ahead.queue.size";
    public static final int DEFAULT_READ_AHEAD_QUEUE_SIZE = 6;
    // used to control getFileStatus list to judge dir whether exist.
    public static final String FILESTATUS_LIST_MAX_KEYS = "fs.cosn.filestatus.list_max_keys";
    public static final int DEFAULT_FILESTATUS_LIST_MAX_KEYS = 1;

    // used for double check complete mpu in case of return cos client exception but status is 200 ok.
    public static final String COSN_COMPLETE_MPU_CHECK = "fs.cosn.complete.mpu.check";
    public static final boolean DEFAULT_COSN_COMPLETE_MPU_CHECK_ENABLE = true;
    public static final String MAX_CONNECTION_NUM = "fs.cosn.max.connection.num";
    public static final int DEFAULT_MAX_CONNECTION_NUM = 1024;

    public static final String COSN_USE_SHORT_CONNECTION = "fs.cosn.use.short.connection";

    public static final boolean DEFAULT_COSN_USE_SHORT_CONNECTION = false;

    public static final String COSN_CONNECTION_REQUEST_TIMEOUT = "fs.cosn.connection.request.timeout";
    public static final int DEFAULT_COSN_CONNECTION_REQUEST_TIMEOUT = -1;

    public static final String COSN_CONNECTION_TIMEOUT = "fs.cosn.connection.timeout";

    public static final int DEFAULT_COSN_CONNECTION_TIMEOUT = 10000;

    public static final String COSN_IDLE_CONNECTION_ALIVE = "fs.cosn.idle.connection.alive";

    public static final int DEFAULT_COSN_IDLE_CONNECTION_ALIVE = 60000;

    // request time out enable then request timeout and client thread size config work
    public static final String COSN_CLIENT_USE_REQUEST_TIMEOUT = "fs.cosn.client.use.request.timeout";
    public static final boolean DEFAULT_COSN_CLIENT_USE_REQUEST_TIMEOUT = false;
    public static final String COSN_CLIENT_REQUEST_TIMEOUT = "fs.cosn.client.request.timeout";
    public static final int DEFAULT_COSN_CLIENT_REQUEST_TIMEOUT = 60 * 1000; //1min but client default 5min
    public static final String COSN_CLIENT_REQUEST_TIMEOUT_THREAD_SIZE = "fs.cosn.client.request.timeout.thread.size";
    public static final int DEFAULT_COSN_CLIENT_REQUEST_TIMEOUT_THREAD_SIZE = 10; // client default cpu*5

    public static final String CUSTOMER_DOMAIN = "fs.cosn.customer.domain";
    public static final String COSN_SERVER_SIDE_ENCRYPTION_ALGORITHM = "fs.cosn.server-side-encryption.algorithm";
    public static final String COSN_SERVER_SIDE_ENCRYPTION_KEY = "fs.cosn.server-side-encryption.key";
    public static final String COSN_SERVER_SIDE_ENCRYPTION_CONTEXT = "fs.cosn.server-side-encryption.context";

    public static final String BASE64_PATTERN = "^([A-Za-z0-9+/]{4})*([A-Za-z0-9+/]{4}|[A-Za-z0-9+/]{3}=|[A-Za-z0-9" +
            "+/]{2}==)$";

    public static final String COSN_CLIENT_SIDE_ENCRYPTION_ENABLED = "fs.cosn.client-side-encryption.enabled";
    public static final boolean DEFAULT_COSN_CLIENT_SIDE_ENCRYPTION_ENABLED = false;
    public static final String COSN_CLIENT_SIDE_ENCRYPTION_PUBLIC_KEY_PATH = "fs.cosn.client-side-encryption.public.key.path";
    public static final String COSN_CLIENT_SIDE_ENCRYPTION_PRIVATE_KEY_PATH = "fs.cosn.client-side-encryption.private.key.path";

    // traffic limit
    public static final String TRAFFIC_LIMIT = "fs.cosn.traffic.limit";
    // The default（-1）is unlimited. Considering the late expansion, the initial value is set -1.
    public static final int DEFAULT_TRAFFIC_LIMIT = -1;

    // checksum
    // crc64
    public static final String CRC64_CHECKSUM_ENABLED = "fs.cosn.crc64.checksum.enabled";
    public static final boolean DEFAULT_CRC64_CHECKSUM_ENABLED = false;
    // crc32c
    public static final String CRC32C_CHECKSUM_ENABLED = "fs.cosn.crc32c.checksum.enabled";
    public static final boolean DEFAULT_CRC32C_CHECKSUM_ENABLED = false;

    public static final String HTTP_PROXY_IP = "fs.cosn.http.proxy.ip";
    public static final String HTTP_PROXY_PORT = "fs.cosn.http.proxy.port";
    public static final int DEFAULT_HTTP_PROXY_PORT = -1;
    public static final String HTTP_PROXY_USERNAME = "fs.cosn.http.proxy.username";
    public static final String HTTP_PROXY_PASSWORD = "fs.cosn.http.proxy.password";
    public static final String COSN_RANGER_TEMP_TOKEN_REFRESH_INTERVAL = "fs.cosn.ranger.temp.token.refresh.interval";
    public static final int DEFAULT_COSN_RANGER_TEMP_TOKEN_REFRESH_INTERVAL = 20;

    public static final String COSN_RANGER_PLUGIN_CLIENT_IMPL = "fs.cosn.ranger.plugin.client.impl";
    public static final String DEFAULT_COSN_RANGER_PLUGIN_CLIENT_IMPL  =
            "org.apache.hadoop.fs.cosn.ranger.client.RangerQcloudObjectStorageClientImpl";
    public static final String COSN_CLIENT_SOCKET_TIMEOUTSEC = "fs.cosn.client.socket.timeoutsec";
    public static final int DEFAULT_CLIENT_SOCKET_TIMEOUTSEC = 30;

    // default disable emr v2 instance url.
    public static final String COSN_EMRV2_INSTANCE_PROVIDER_ENABLED = "fs.cosn.emrv2.instance.provider.enabled";
    public static final boolean DEFAULT_COSN_EMRV2_INSTANCE_PROVIDER_ENABLED = false;
    public static final String COSN_POSIX_BUCKET_FS_IMPL = "fs.cosn.posix_bucket.fs.impl";
    public static final String DEFAULT_COSN_POSIX_BUCKET_FS_IMPL = Constants.COSN_POSIX_BUCKET_FS_CHDFS_IMPL;

    public static final String COSN_SYMBOLIC_SIZE_THRESHOLD = "fs.cosn.symbolic_link.sizeThreshold";
    public static final int DEFAULT_COSN_SYMBOLIC_SIZE_THRESHOLD = 4096;

    public static final String COSN_FLUSH_ENABLED = "fs.cosn.flush.enabled";
    public static final boolean DEFAULT_COSN_FLUSH_ENABLED = false;
    public static final String COSN_MAPDISK_DELETEONEXIT_ENABLED = "fs.cosn.map_disk.delete_on_exit.enabled";
    public static final boolean DEFAULT_COSN_MAPDISK_DELETEONEXIT_ENABLED = true;

    // range control, whether meta engine need query own ranger. can be used when transfer from ofs to cos ranger
    public static final String COSN_POSIX_BUCKET_USE_OFS_RANGER_ENABLED = "fs.cosn.posix.bucket.use_ofs_ranger.enabled";
    public static final boolean DEFAULT_COSN_POSIX_BUCKET_USE_OFS_RANGER_ENABLED = false;

    // POSIX bucket does not support the SYMLINK interface by default. Only used for some internal projects, such as GooseFS / GooseFS Lite, etc.
    @Deprecated
    public static final String COSN_SUPPORT_SYMLINK_ENABLED = "fs.cosn.support_symlink.enabled";
    public static final boolean DEFAULT_COSN_SUPPORT_SYMLINK_ENABLED = false;

    // 这个仅限于 GooseFS / GooseFS Lite 某些确定不会有同名文件和目录的场景才能关闭。
    @Deprecated
    public static final String COSN_FILESTATUS_LIST_OP_ENABLED = "fs.cosn.filestatus.list.op.enabled";
    public static final boolean DEFAULT_FILESTATUS_LIST_OP_ENABLED = true;

    public static final String COSN_FILESTATUS_LIST_RECURSIVE_ENABLED = "fs.cosn.filestatus.list.recursive.enabled";
    public static final boolean DEFAULT_FILESTATUS_LIST_RECURSIVE_ENABLED = false;

    // 存在同名文件，且该文件长度为0时，目录优先（默认关）
    public static final String COSN_FILESTATUS_DIR_FIRST_ENABLED = "fs.cosn.filestatus.dir.first.enabled";
    public static final boolean DEFAULT_FILESTATUS_DIR_FIRST_ENABLED = false;

    // create 新文件的时候，检查文件的存在性
    public static final String COSN_CREATE_FILE_EXIST_OP_ENABLED = "fs.cosn.create.check.file.exist.op.enabled";
    public static final boolean DEFAULT_COSN_CREATE_FILE_EXIST_OP_ENABLED = true;

    public static final String COSN_READ_BUFFER_POOL_CAPACITY = "fs.cosn.read.buffer.pool.capacity";
    public static final long DEFAULT_READ_BUFFER_POOL_CAPACITY = -1;

    public static final String COSN_READ_BUFFER_ALLOCATE_TIMEOUT_SECONDS = "fs.cosn.read.buffer.allocate.timeout.seconds";
    public static final long DEFAULT_READ_BUFFER_ALLOCATE_TIMEOUT_SECONDS = 5;

    // 这里问了一下 cos-java-sdk 的维护同事，在新版本的 SDK 里面增加了一个配置，可以控制是否打印关闭时的堆栈信息。
    // 这里主要是为了给客户排查他们是否主动关闭 Client。
    public static final String COSN_CLIENT_SHUTDOWN_STACK_TRACE_LOG = "fs.cosn.client.shutdown.stack.trace.log";
    public static final boolean DEFAULT_COSN_CLIENT_SHUTDOWN_STACK_TRACE_LOG = false;

    // 配合的 COS AZ 加速器相关的选项
    // 是否强制开启 AZ 加速器的强一致访问
    public static final String COSN_AZ_ACCELERATOR_CONSISTENCY_ENABLED = "fs.cosn.az.accelerator.consistency.enabled";
    public static final boolean DEFAULT_COSN_AZ_ACCELERATOR_CONSISTENCY_ENABLED = false;
}

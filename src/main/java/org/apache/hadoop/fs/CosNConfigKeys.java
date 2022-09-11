package org.apache.hadoop.fs;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.cosn.Constants;
import org.apache.hadoop.fs.cosn.Unit;

/**
 * This class contains constants for configuration keys used in the cos file system.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class CosNConfigKeys extends CommonConfigurationKeys {
    public static final String USER_AGENT = "fs.cosn.user.agent";
    public static final String DEFAULT_USER_AGENT = "cos-hadoop-plugin-v8.1.7";

    public static final String TENCENT_EMR_VERSION_KEY = "fs.emr.version";

    public static final String COSN_STORAGE_CLASS_KEY = "fs.cosn.storage.class";

    public static final String COSN_CREDENTIALS_PROVIDER = "fs.cosn.credentials.provider";
    public static final String COSN_APPID_KEY = "fs.cosn.userinfo.appid";
    public static final String COSN_UIN_KEY = "fs.cosn.userinfo.uin";
    public static final String COSN_REQUEST_ID = "fs.cosn.request.id";
    public static final String COS_REMOTE_CREDENTIAL_PROVIDER_URL = "fs.cosn.remote-credential-provider.url";
    public static final String COS_REMOTE_CREDENTIAL_PROVIDER_PATH = "fs.cosn.remote-credential-provider.path";
    public static final String COSN_USERINFO_SECRET_ID_KEY = "fs.cosn.userinfo.secretId";
    public static final String COSN_USERINFO_SECRET_KEY_KEY = "fs.cosn.userinfo.secretKey";
    public static final String COSN_USERINFO_SESSION_TOKEN = "fs.cosn.userinfo.sessionToken";
    public static final String COSN_REGION_KEY = "fs.cosn.bucket.region";
    public static final String COSN_REGION_PREV_KEY = "fs.cosn.userinfo.region";
    public static final String COSN_ENDPOINT_SUFFIX_KEY = "fs.cosn.bucket.endpoint_suffix";
    public static final String COSN_ENDPOINT_SUFFIX_PREV_KEY = "fs.cosn.userinfo.endpoint_suffix";

    public static final String COSN_USE_HTTPS_KEY = "fs.cosn.useHttps";
    public static final boolean DEFAULT_USE_HTTPS = true;   // 现在 COS 强制使用 https 作为访问协议

    // 这个腾讯内部 L5 负载均衡系统的配置，主要限于内部项目依赖使用
    public static final String COSN_L5_KEY = "fs.cosn.bucket.l5";
    public static final boolean DEFAULT_COSN_USE_L5_ENABLE = false;
    public static final String COSN_USE_L5_ENABLE = "fs.cosn.use.l5.enable";

    public static final String COSN_L5_UPDATE_MAX_RETRIES_KEY = "fs.cosn.l5.update.maxRetries";
    public static final int DEFAULT_COSN_L5_UPDATE_MAX_RETRIES = 5;

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
    public static final int DEFAULT_IO_THREAD_POOL_MAX_SIZE = 2 * Runtime.getRuntime().availableProcessors() + 1;

    public static final String COPY_THREAD_POOL_SIZE_KEY = "fs.cosn.copy_thread_pool";
    public static final int DEFAULT_COPY_THREAD_POOL_SIZE = 3;

    public static final String THREAD_KEEP_ALIVE_TIME_KEY = "fs.cosn.threads.keep_alive_time";
    public static final long DEFAULT_THREAD_KEEP_ALIVE_TIME = 60L;

    public static final String READ_AHEAD_BLOCK_SIZE_KEY = "fs.cosn.read.ahead.block.size";
    public static final long DEFAULT_READ_AHEAD_BLOCK_SIZE = 1 * Unit.MB;
    public static final String READ_AHEAD_QUEUE_SIZE = "fs.cosn.read.ahead.queue.size";
    public static final int DEFAULT_READ_AHEAD_QUEUE_SIZE = 8;
    // used to control getFileStatus list to judge dir whether exist.
    public static final String FILESTATUS_LIST_MAX_KEYS = "fs.cosn.filestatus.list_max_keys";
    public static final int DEFAULT_FILESTATUS_LIST_MAX_KEYS = 2;

    // used by normal bucket to control max keys of list status
    public static final String LISTSTATUS_LIST_MAX_KEYS = "fs.cosn.liststatus.list_max_keys";
    public static final int DEFAULT_LISTSTATUS_LIST_MAX_KEYS = 999;

    // used by posix bucket to control max keys of list status
    public static final String LISTSTATUS_POSIX_BUCKET__LIST_MAX_KEYS = "fs.cosn.liststatus.posix_bucket.list_max_keys";
    public static final int DEFAULT_LISTSTATUS_POSIX_BUCKET_LIST_MAX_KEYS = 5000;

    // used for double check complete mpu in case of return cos client exception but status is 200 ok.
    public static final String COSN_COMPLETE_MPU_CHECK = "fs.cosn.complete.mpu.check";
    public static final boolean DEFAULT_COSN_COMPLETE_MPU_CHECK_ENABLE = true;
    public static final String MAX_CONNECTION_NUM = "fs.cosn.max.connection.num";
    public static final int DEFAULT_MAX_CONNECTION_NUM = 2048;

    public static final String CUSTOMER_DOMAIN = "fs.cosn.customer.domain";
    public static final String COSN_SERVER_SIDE_ENCRYPTION_ALGORITHM = "fs.cosn.server-side-encryption.algorithm";
    public static final String COSN_SERVER_SIDE_ENCRYPTION_KEY = "fs.cosn.server-side-encryption.key";
    public static final String COSN_SERVER_SIDE_ENCRYPTION_CONTEXT = "fs.cosn.server-side-encryption.context";
    public static final String BASE64_PATTERN = "^([A-Za-z0-9+/]{4})*([A-Za-z0-9+/]{4}|[A-Za-z0-9+/]{3}=|[A-Za-z0-9" +
            "+/]{2}==)$";

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

    public static final String COSN_FLUSH_ENABLED = "fs.cosn.flush.enabled";
    public static final boolean DEFAULT_COSN_FLUSH_ENABLED = true;
    public static final String COSN_MAPDISK_DELETEONEXIT_ENABLED = "fs.cosn.map_disk.delete_on_exit.enabled";
    public static final boolean DEFAULT_COSN_MAPDISK_DELETEONEXIT_ENABLED = true;

    // range control, whether meta engine need query own ranger. can be used when transfer from ofs to cos ranger
    public static final String COSN_POSIX_BUCKET_USE_OFS_RANGER_ENABLED = "fs.cosn.posix.bucket.use_ofs_ranger.enabled";
    public static final boolean DEFAULT_COSN_POSIX_BUCKET_USE_OFS_RANGER_ENABLED = false;

}

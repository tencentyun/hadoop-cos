package org.apache.hadoop.fs.cosn;

public final class Constants {
    private Constants() {
    }

    // The block file prefix for multipart upload op.
    public static final String BLOCK_TMP_FILE_PREFIX = "cos_";

    // Suffix for local cache file name
    public static final String BLOCK_TMP_FILE_SUFFIX = "_local_block_cache";

    // Crc32c server response header key
    public static final String CRC32C_RESP_HEADER = "x-cos-hash-crc32c";
    // Crc32c agent request header key
    public static final String CRC32C_REQ_HEADER = "x-cos-crc32c-flag";
    // Crc32c agent request header value
    public static final String CRC32C_REQ_HEADER_VAL = "cosn";

    // Maximum number of blocks uploaded in trunks.
    public static final int MAX_PART_NUM = 10000;
    // The maximum size of a single block.
    public static final long MAX_PART_SIZE = 2 * Unit.GB;
    // The minimum size of a single block.
    public static final long MIN_PART_SIZE = Unit.MB;
    // The maximum size of the buffer is 8GB
    public static final long MAX_BUFFER_SIZE = 2 * Unit.GB;

    // Environments variables for the COS secretId and secretKey.
    public static final String COSN_SECRET_ID_ENV = "COSN_SECRET_ID";
    public static final String COSN_SECRET_KEY_ENV = "COSN_SECRET_KEY";

    public static final String COSN_OFS_CONFIG_PREFIX = "fs.ofs.";
    public static final String COSN_CONFIG_TRANSFER_PREFIX = "fs.cosn.trsf.";

    public static final String COSN_POSIX_BUCKET_FS_COSN_IMPL = "org.apache.hadoop.fs.CosNFileSystem";
    public static final String COSN_POSIX_BUCKET_FS_CHDFS_IMPL="com.qcloud.chdfs.fs.CHDFSHadoopFileSystemAdapter";

    public static final String CUSTOM_AUTHENTICATION = "custom authentication";

    // posix bucket ranger config need to pass through
    public static final String COSN_POSIX_BUCKET_RANGER_POLICY_URL = "fs.ofs.cosn.ranger.policy.url";
    public static final String COSN_POSIX_BUCKET_RANGER_AUTH_JAR_MD5 = "fs.ofs.cosn.ranger.auth.jar.md5";
    public static final String COSN_POSIX_BUCKCET_OFS_RANGER_FLAG = "fs.ofs.ranger.enable.flag";

    public static final String COSN_POSIX_BUCKET_APPID_CONFIG = "fs.ofs.user.appid";
    public static final String COSN_POSIX_BUCKET_REGION_CONFIG = "fs.ofs.bucket.region";

    // ofs relate config
    public static final String COSN_POSIX_BUCKET_SSE_MODE = "fs.ofs.sse.mode";
    public static final String COSN_POSIX_BUCKET_SSE_C_KEY = "fs.ofs.sse.c.key";
    public static final String COSN_POSIX_BUCKET_SSE_KMS_KEYID = "fs.ofs.sse.kms.keyid";
    public static final String COSN_POSIX_BUCKET_SSE_KMS_CONTEXT = "fs.ofs.sse.kms.context";

    // sse relate
    public static final String COSN_SSE_MODE_COS = "SSE-COS";
    public static final String COSN_SSE_MODE_C = "SSE-C";
    public static final String COSN_SSE_MODE_KMS = "SSE-KMS";

    // Prefix for all cosn properties: {@value}.
    public static final String FS_COSN_PREFIX = "fs.cosn.";
    // Prefix for cosn bucket-specific properties: {@value}.
    public static final String FS_COSN_BUCKET_PREFIX = "fs.cosn.bucket.";

}

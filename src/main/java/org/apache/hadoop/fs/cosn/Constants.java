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
}

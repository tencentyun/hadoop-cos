package org.apache.hadoop.fs.cosnative;

public final class Constants {
    private Constants() {
    }

    public static final String BLOCK_TMP_FILE_PREFIX = "cos_";              // the block file prefix for multipart upload op
    public static final String BLOCK_TMP_FILE_SUFFIX = "_local_block_cache";
    public static final int MAX_PART_NUM = 10000;                                    // Maximum number of blocks uploaded in chunks
    public static final long MAX_PART_SIZE = 5 * 1024 * 1024 * 1024;                  // The maximum size of a single block
    public static final long MIN_PART_SIZE = 1 * 1024 * 1024;                         // The minimum size of a single block
}

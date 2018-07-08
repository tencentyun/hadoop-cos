package org.apache.hadoop.fs;

public final class Constants {
    private Constants() {
    }

    public static final String BLOCK_TMP_FILE_PREFIX = "cos_";                    // The block file prefix for multipart upload op
    public static final String BLOCK_TMP_FILE_SUFFIX = "_local_block_cache";    // Suffix for local cache file name
    public static final int MAX_PART_NUM = 10000;                                   // Maximum number of blocks uploaded in chunks
    public static final long MAX_PART_SIZE = 5 * Unit.GB;                           // The maximum size of a single block
    public static final long MIN_PART_SIZE = 1 * Unit.MB;                           // The minimum size of a single block
}

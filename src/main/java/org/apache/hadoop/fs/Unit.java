package org.apache.hadoop.fs;

public final class Unit {
    private Unit() {
    }

    public static final int KB = 1024;
    public static final int MB = 1024 * KB;
    public static final int GB = 1024 * MB;
    public static final long TB = 1024 * GB;
    public static final long PB = 1024 * TB;
}

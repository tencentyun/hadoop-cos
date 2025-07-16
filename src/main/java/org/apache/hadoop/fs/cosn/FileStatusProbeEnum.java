package org.apache.hadoop.fs.cosn;

import java.util.EnumSet;
import java.util.Set;

/**
 * Define the status detection action to be performed according to different situations.
 */
public enum FileStatusProbeEnum {
    // Head the actual path.
    HEAD,

    // Head the path + /.
    DIR_MARKER,

    // List under the path.
    LIST,

    // assert List under the path return true.
    DUMMY_LIST;

    /**
     * Look for files and directories.
     * 1. check if a file with the same name exists.
     * 2. check if a directory (actual directory) with the same name exists.
     * 3. check if a directory (marker directory, the commonPrefix of a object that its key name contains '/') with the same name exists.
     */
    public static final Set<FileStatusProbeEnum> ALL = EnumSet.of(HEAD, DIR_MARKER, LIST);

    /**
     * 1. check if a directory (actual directory) with the same name exists.
     * 2. ASSERT LIST return true, so avoid list operation.
     * 3. avoid list-1 for LIST
     */
    public static final Set<FileStatusProbeEnum> ALL_AND_DUMMY_LIST = EnumSet.of(HEAD, DIR_MARKER, DUMMY_LIST);

    /**
     * Only check if a file with the same name exists.
     */
    public static final Set<FileStatusProbeEnum> HEAD_ONLY = EnumSet.of(HEAD);

    /**
     * Only check if a directory with the same name exists.
     */
    public static final Set<FileStatusProbeEnum> LIST_ONLY = EnumSet.of(LIST);

    /**
     * Only check if a file / directory with the same name exists.
     */
    public static final Set<FileStatusProbeEnum> FILE_DIRECTORY = EnumSet.of(HEAD, DIR_MARKER);


    /**
     * Only check if a directory with the same name exists.
     */
    public static final Set<FileStatusProbeEnum> DIRECTORIES = LIST_ONLY;
}


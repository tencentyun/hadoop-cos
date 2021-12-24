package org.apache.hadoop.fs;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * <p>
 * Holds information on a directory listing for a
 * {@link NativeFileSystemStore}.
 * This includes the {@link FileMetadata files} and directories
 * (their names) contained in a directory.
 * </p>
 * <p>
 * This listing may be returned in chunks, so a <code>priorLastKey</code>
 * is provided so that the next chunk may be requested.
 * </p>
 *
 * @see NativeFileSystemStore#list(String, int)
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class CosNPartialListing {

    private final String priorLastKey;
    private final FileMetadata[] files;
    private final FileMetadata[] commonPrefixes;

    public CosNPartialListing(String priorLastKey, FileMetadata[] files,
                              FileMetadata[] commonPrefixes) {
        this.priorLastKey = priorLastKey;
        this.files = files;
        this.commonPrefixes = commonPrefixes;
    }

    public FileMetadata[] getFiles() {
        return files;
    }

    public FileMetadata[] getCommonPrefixes() {
        return commonPrefixes;
    }

    public String getPriorLastKey() {
        return priorLastKey;
    }

}

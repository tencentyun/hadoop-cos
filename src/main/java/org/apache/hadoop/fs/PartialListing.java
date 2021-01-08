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
public class PartialListing {

    private final String priorLastKey;
    private final FileMetadata[] files;
    private final FileMetadata[] commonPrefixes;
    private CosResultInfo resultInfo;

    public PartialListing(String priorLastKey, FileMetadata[] files,
                          FileMetadata[] commonPrefixes) {
        this.priorLastKey = priorLastKey;
        this.files = files;
        this.commonPrefixes = commonPrefixes;
        this.resultInfo = new CosResultInfo();
    }

    public void setKeySamePrefix(boolean isKeySamePrefix) {
        this.resultInfo.setKeySameToPrefix(isKeySamePrefix);
    }

    public boolean isKeySamePrefix() {
        return this.resultInfo.isKeySameToPrefix();
    }

    public void setRequestID(String requestID) {
        this.resultInfo.setRequestID(requestID);
    }

    public String getRequestID() {
        return this.resultInfo.getRequestID();
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

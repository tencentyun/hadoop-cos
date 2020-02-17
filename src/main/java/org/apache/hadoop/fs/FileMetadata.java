package org.apache.hadoop.fs;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * <p>
 * Holds basic metadata for a file stored in a {@link NativeFileSystemStore}.
 * </p>
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
class FileMetadata {
    private final String key;
    private final long length;
    private final long lastModified;
    private final boolean isFile;
    private final String ETag;
    private final String crc64ecm;
    private final String versionId;

    public FileMetadata(String key, long length, long lastModified) {
        this(key, length, lastModified, true);
    }

    public FileMetadata(String key, long length, long lastModified,
                        boolean isFile) {
        this(key, length, lastModified, isFile, null);
    }

    public FileMetadata(String key, long length, long lastModified, boolean isFile, String ETag) {
        this(key, length, lastModified, isFile, ETag, null, null);
    }

    public FileMetadata(String key, long length, long lastModified, boolean isFile, String eTag, String crc64ecm,
                        String versionId) {
        this.key = key;
        this.length = length;
        this.lastModified = lastModified;
        this.isFile = isFile;
        ETag = eTag;
        this.crc64ecm = crc64ecm;
        this.versionId = versionId;
    }

    public String getKey() {
        return key;
    }

    public long getLength() {
        return length;
    }

    public long getLastModified() {
        return lastModified;
    }

    public String getETag() {
        return ETag;
    }

    public String getVersionId() {
        return versionId;
    }

    public String getCrc64ecm() {
        return crc64ecm;
    }

    @Override
    public String toString() {
        return "FileMetadata[" + key + ", " + length + ", " + lastModified +
                ", file?" + isFile
                + "]";
    }

    public boolean isFile() {
        return isFile;
    }
}

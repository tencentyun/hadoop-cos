package org.apache.hadoop.fs;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import java.util.Map;

/**
 * <p>
 * Holds basic metadata for a file stored in a {@link NativeFileSystemStore}.
 * </p>
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class FileMetadata {
    private final String key;
    private final long length;
    private final long lastModified;
    private final boolean isFile;
    private final String ETag;
    private final String crc64ecm;
    private final String crc32cm;
    private final String versionId;
    private final String storageClass;
    private final Map<String, byte[]> userAttributes;

    public FileMetadata(String key, long length, long lastModified) {
        this(key, length, lastModified, true);
    }

    public FileMetadata(String key, long length, long lastModified,
                        boolean isFile) {
        this(key, length, lastModified, isFile, null);
    }

    public FileMetadata(String key, long length, long lastModified, boolean isFile, String ETag) {
        this(key, length, lastModified, isFile, ETag, null, null, null);
    }

    public FileMetadata(String key, long length, long lastModified, boolean isFile, String eTag, String crc64ecm,
                        String crc32cm, String versionId) {
        this(key, length, lastModified, isFile, eTag, crc64ecm, crc32cm, versionId, null, null);
    }

    public FileMetadata(String key, long length, long lastModified, boolean isFile, String eTag, String crc64ecm,
                        String crc32cm, String versionId, String storageClass) {
        this(key, length, lastModified, isFile, eTag, crc64ecm, crc32cm, versionId, storageClass, null);
    }

    public FileMetadata(String key, long length, long lastModified, boolean isFile, String eTag, String crc64ecm,
                        String crc32cm, String versionId, String storageClass, Map<String, byte[]> userAttributes) {
        this.key = key;
        this.length = length;
        this.lastModified = lastModified;
        this.isFile = isFile;
        this.ETag = eTag;
        this.crc64ecm = crc64ecm;
        this.crc32cm = crc32cm;
        this.versionId = versionId;
        this.storageClass = storageClass;
        this.userAttributes = userAttributes;
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

    public String getCrc32cm() {
        return crc32cm;
    }

    public String getStorageClass() {
        return storageClass;
    }

    public Map<String, byte[]> getUserAttributes() {
        return userAttributes;
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

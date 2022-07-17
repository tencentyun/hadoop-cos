package org.apache.hadoop.fs;

import java.util.Map;

public class CosNSymlinkMetadata extends FileMetadata {
    private String target;

    public CosNSymlinkMetadata(String key, long length, long lastModified, boolean isFile, String eTag,
                               String crc64ecm, String crc32cm,
                               String versionId, String storageClass,
                               String target) {
        super(key, length, lastModified, isFile, eTag, crc64ecm, crc32cm, versionId, storageClass);
        this.target = target;
    }

    public String getTarget() {
        return target;
    }

    public void setTarget(String target) {
        this.target = target;
    }

    @Override
    public boolean isFile() {
        return false;
    }
}

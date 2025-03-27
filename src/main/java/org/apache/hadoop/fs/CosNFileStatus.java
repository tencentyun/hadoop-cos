package org.apache.hadoop.fs;

import org.apache.hadoop.fs.permission.FsPermission;

import javax.annotation.Nullable;
import java.util.Map;

public class CosNFileStatus extends FileStatus {
    private final String ETag;
    private final String crc64ecma;
    private final String crc32cm;
    private final String storageClass;
    private final String versionId;
    private final Map<String, byte[]> userAttributes;

    public CosNFileStatus(long length, boolean isdir, int block_replication, long blocksize, long modification_time,
                          long access_time, FsPermission permission, String owner, String group, Path path) {
        this(length, isdir, block_replication, blocksize, modification_time, access_time, permission, owner, group,
                path, null);
    }

    public CosNFileStatus(long length, boolean isdir, int block_replication, long blocksize, long modification_time,
                          long access_time, FsPermission permission, String owner, String group, Path path,
                          String ETag) {
        this(length, isdir, block_replication, blocksize, modification_time, access_time, permission, owner, group,
                path, ETag, null, null, null, null, null);
    }

    public CosNFileStatus(long length, boolean isdir, int block_replication, long blocksize, long modification_time,
                          long access_time, FsPermission permission, String owner, String group, Path path,
                          String ETag, String crc64ecma, String crc32cm, String versionId, String storageClass, Map<String, byte[]> userAttributes) {
        super(length, isdir, block_replication, blocksize, modification_time, access_time, permission, owner, group,
                path);
        this.ETag = ETag;
        this.crc64ecma = crc64ecma;
        this.crc32cm = crc32cm;
        this.storageClass = storageClass;
        this.versionId = versionId;
        this.userAttributes = userAttributes;
    }

    public String getETag() {
        return ETag;
    }

    @Nullable
    public String getCrc64ecma() {
        return crc64ecma;
    }

    @Nullable
    public String getCrc32cm() {
        return crc32cm;
    }

    public String getStorageClass() {
        return storageClass;
    }

    @Nullable
    public String getVersionId() {
        return versionId;
    }

    @Nullable
    public Map<String, byte[]> getUserAttributes() {
        return userAttributes;
    }
}

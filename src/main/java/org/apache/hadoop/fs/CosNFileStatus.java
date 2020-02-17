package org.apache.hadoop.fs;

import org.apache.hadoop.fs.permission.FsPermission;

public class CosNFileStatus extends FileStatus {
    private String ETag;
    private String crc64ecma;
    private String versionId;

    public CosNFileStatus(long length, boolean isdir, int block_replication, long blocksize, long modification_time,
                          long access_time, FsPermission permission, String owner, String group, Path path) {
        this(length, isdir, block_replication, blocksize, modification_time, access_time, permission, owner, group,
                path, null, null, null);
    }

    public CosNFileStatus(long length, boolean isdir, int block_replication, long blocksize, long modification_time,
                          long access_time, FsPermission permission, String owner, String group, Path path,
                          String ETag) {
        this(length, isdir, block_replication, blocksize, modification_time, access_time, permission, owner, group,
                path, ETag, null, null);
    }

    public CosNFileStatus(long length, boolean isdir, int block_replication, long blocksize, long modification_time,
                          long access_time, FsPermission permission, String owner, String group, Path path,
                          String ETag, String crc64ecma, String versionId) {
        super(length, isdir, block_replication, blocksize, modification_time, access_time, permission, owner, group,
                path);
        this.ETag = ETag;
        this.crc64ecma = crc64ecma;
        this.versionId = versionId;
    }

    public String getETag() {
        return ETag;
    }

    public String getCrc64ecma() {
        return crc64ecma;
    }

    public String getVersionId() {
        return versionId;
    }
}

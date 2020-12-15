package org.apache.hadoop.fs.cosn.ranger.security.authorization;

public class PermissionRequest {
    private ServiceType serviceType;
    private AccessType accessType;

    private String bucketName;
    private String objectKey;

    private String fsMountPoint;
    private String chdfsPath;

    public PermissionRequest(ServiceType serviceType, AccessType accessType,
                             String bucketName, String objectKey, String fsMountPoint, String chdfsPath) {
        this.serviceType = serviceType;
        this.accessType = accessType;
        this.bucketName = bucketName;
        this.objectKey = objectKey;
        this.fsMountPoint = fsMountPoint;
        this.chdfsPath = chdfsPath;
    }

    public ServiceType getServiceType() {
        return serviceType;
    }

    public AccessType getAccessType() {
        return accessType;
    }

    public String getBucketName() {
        return bucketName;
    }

    public String getObjectKey() {
        return objectKey;
    }

    public String getFsMountPoint() {
        return fsMountPoint;
    }

    public String getChdfsPath() {
        return chdfsPath;
    }
}

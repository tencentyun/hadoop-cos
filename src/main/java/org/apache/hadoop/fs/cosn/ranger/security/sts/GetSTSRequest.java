package org.apache.hadoop.fs.cosn.ranger.security.sts;

public class GetSTSRequest {
    private String bucketName;
    private String bucketRegion;
    private String allowPrefix;

    public String getBucketName() {
        return bucketName;
    }

    public void setBucketName(String bucketName) {
        this.bucketName = bucketName;
    }

    public String getBucketRegion() {
        return bucketRegion;
    }

    public void setBucketRegion(String bucketRegion) {
        this.bucketRegion = bucketRegion;
    }

    public String getAllowPrefix() {
        return allowPrefix;
    }

    public void setAllowPrefix(String allowPrefix) {
        this.allowPrefix = allowPrefix;
    }
}
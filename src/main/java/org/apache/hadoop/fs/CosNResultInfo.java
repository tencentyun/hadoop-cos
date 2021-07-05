package org.apache.hadoop.fs;

/**
 * Used to record the cos client query result
 */
public class CosNResultInfo {
    private String requestID;
    private boolean isKeySameToPrefix;

    CosNResultInfo() {
        requestID = "";
        isKeySameToPrefix = false;
    }

    public void setRequestID(String requestID) {
        this.requestID = requestID;
    }
    public String getRequestID() {
        return this.requestID;
    }

    public boolean isKeySameToPrefix() {
        return this.isKeySameToPrefix;
    }

    public void setKeySameToPrefix(boolean isKeySameToPrefix) {
        this.isKeySameToPrefix = isKeySameToPrefix;
    }
}

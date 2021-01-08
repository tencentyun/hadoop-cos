package org.apache.hadoop.fs;

/**
 * Used to record the cos client query result
 */
public class CosResultInfo {
    private String requestID;

    CosResultInfo() {
        requestID = "";
    }

    public void setRequestID(String requestID) {
        this.requestID = requestID;
    }
    public String getRequestID() {
        return this.requestID;
    }
}

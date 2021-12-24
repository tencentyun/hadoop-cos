package org.apache.hadoop.fs;

import com.qcloud.cos.utils.StringUtils;

import java.io.IOException;

public enum CosNEncryptionMethods {

    SSE_COS("SSE-COS", true),
    SSE_C("SSE-C", true),
    SSE_KMS("SSE-KMS", true),
    NONE("", false);

    static final String UNKNOWN_ALGORITHM_MESSAGE
            = "COSN unknown the encryption algorithm ";

    private String method;
    private boolean serverSide;

    CosNEncryptionMethods(String method, final boolean serverSide) {
        this.method = method;
        this.serverSide = serverSide;
    }

    public String getMethod() {
        return method;
    }

    /**
     * Get the encryption mechanism from the value provided.
     * @param name algorithm name
     * @return the method
     * @throws IOException if the algorithm is unknown
     */
    public static CosNEncryptionMethods getMethod(String name) throws IOException {
        if (StringUtils.isNullOrEmpty(name)) {
            return NONE;
        }
        for (CosNEncryptionMethods v : values()) {
            if (v.getMethod().equals(name)) {
                return v;
            }
        }
        throw new IOException(UNKNOWN_ALGORITHM_MESSAGE + name);
    }

    /**
     * Flag to indicate this is a server-side encryption option.
     * @return true if this is server side.
     */
    public boolean isServerSide() {
        return serverSide;
    }

}

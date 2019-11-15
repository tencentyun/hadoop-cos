package org.apache.hadoop.fs;

import java.io.IOException;
import org.apache.commons.lang3.StringUtils;

public enum CosEncryptionMethods {

    SSE_COS("SSE_COS", true),
    SSE_C("SSE_C", true),
    NONE("", false);

    static final String UNKNOWN_ALGORITHM_MESSAGE
            = "COSN unknown the encryption algorithm ";

    private String method;
    private boolean serverSide;

    CosEncryptionMethods(String method, final boolean serverSide) {
        this.method = method;
        this.serverSide = serverSide;
    }

    public String getMethod() {
        return method;
    }

    /**
     * Flag to indicate this is a server-side encryption option.
     * @return true if this is server side.
     */
    public boolean isServerSide() {
        return serverSide;
    }

    /**
     * Get the encryption mechanism from the value provided.
     * @param name algorithm name
     * @return the method
     * @throws IOException if the algorithm is unknown
     */
    public static CosEncryptionMethods getMethod(String name) throws IOException {
        if(StringUtils.isBlank(name)) {
            return NONE;
        }
        for (CosEncryptionMethods v : values()) {
            if (v.getMethod().equals(name)) {
                return v;
            }
        }
        throw new IOException(UNKNOWN_ALGORITHM_MESSAGE + name);
    }
}

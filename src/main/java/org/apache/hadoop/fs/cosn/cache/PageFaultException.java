package org.apache.hadoop.fs.cosn.cache;

import java.io.IOException;

public class PageFaultException extends IOException {
    private static final long serialVersionUID = 1L;

    public PageFaultException(String message) {
        super(message);
    }

    public PageFaultException(String message, Throwable cause) {
        super(message, cause);
    }

    public PageFaultException(Throwable cause) {
        super(cause);
    }
}

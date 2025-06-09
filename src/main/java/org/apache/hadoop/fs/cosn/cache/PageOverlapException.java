package org.apache.hadoop.fs.cosn.cache;

public class PageOverlapException extends PageFaultException {
    public PageOverlapException(String message) {
        super(message);
    }

    public PageOverlapException(String message, Throwable cause) {
        super(message, cause);
    }

    public PageOverlapException(Throwable cause) {
        super(cause);
    }
}

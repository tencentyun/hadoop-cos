package org.apache.hadoop.fs.auth;

import com.qcloud.cos.exception.CosClientException;

public class NoAuthWithCOSException extends CosClientException {
    public NoAuthWithCOSException(String message, Throwable t) {
        super(message, t);
    }

    public NoAuthWithCOSException(String message) {
        super(message);
    }

    public NoAuthWithCOSException(Throwable t) {
        super(t);
    }
}

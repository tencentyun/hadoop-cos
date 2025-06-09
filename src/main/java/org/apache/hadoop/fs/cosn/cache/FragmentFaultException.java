package org.apache.hadoop.fs.cosn.cache;

import java.io.IOException;

public class FragmentFaultException extends IOException {
    private static final long serialVersionUID = 1L;

    public FragmentFaultException(String message) {
        super(message);
    }

    public FragmentFaultException(String message, Throwable cause) {
        super(message, cause);
    }

    public FragmentFaultException(Throwable cause) {
        super(cause);
    }
}

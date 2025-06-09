package org.apache.hadoop.fs.cosn.cache;

public class FragmentOverlapException extends FragmentFaultException {
    public FragmentOverlapException(String message) {
        super(message);
    }

    public FragmentOverlapException(String message, Throwable cause) {
        super(message, cause);
    }

    public FragmentOverlapException(Throwable cause) {
        super(cause);
    }
}

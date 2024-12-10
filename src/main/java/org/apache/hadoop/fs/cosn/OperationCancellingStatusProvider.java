package org.apache.hadoop.fs.cosn;

public interface OperationCancellingStatusProvider {
    boolean isCancelled();
}

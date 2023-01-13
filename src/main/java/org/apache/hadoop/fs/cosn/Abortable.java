package org.apache.hadoop.fs.cosn;

import java.io.IOException;

public interface Abortable {
    void doAbort() throws IOException;
}

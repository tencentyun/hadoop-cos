package org.apache.hadoop.fs;

import java.io.IOException;

public interface Abortable {
    void abort() throws IOException;
}

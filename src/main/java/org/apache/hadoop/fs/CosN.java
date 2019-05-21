package org.apache.hadoop.fs;

import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class CosN extends DelegateToFileSystem {
    public CosN(URI theUri, Configuration conf) throws IOException,
            URISyntaxException {
        super(theUri, new CosFileSystem(), conf, CosFileSystem.SCHEME, false);
    }

    @Override
    public int getUriDefaultPort() {
        return -1;
    }
}

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

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CosN{");
        sb.append("URI =").append(fsImpl.getUri());
        sb.append("; fsImpl=").append(fsImpl);
        sb.append('}');
        return sb.toString();
    }

    /**
     * Close the file system; the FileContext API doesn't have an explicit close.
     */
    @Override
    protected void finalize() throws Throwable {
        fsImpl.close();
        super.finalize();
    }
}

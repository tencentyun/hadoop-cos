package org.apache.hadoop.fs.auth;

import com.qcloud.cos.auth.COSCredentialsProvider;
import org.apache.hadoop.conf.Configuration;

import javax.annotation.Nullable;
import java.net.URI;

/**
 * The base class for COS credential providers which take a URI or
 * configuration in their constructor.
 */
public abstract class AbstractCOSCredentialProvider
        implements COSCredentialsProvider {
    private final URI uri;
    private final Configuration conf;

    public AbstractCOSCredentialProvider(@Nullable URI uri,
                                         Configuration conf) {
        this.uri = uri;
        this.conf = conf;
    }

    public URI getUri() {
        return uri;
    }

    public Configuration getConf() {
        return conf;
    }
}

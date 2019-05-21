package org.apache.hadoop.fs.auth;

import java.net.URI;
import javax.annotation.Nullable;

import com.qcloud.cos.auth.BasicCOSCredentials;
import com.qcloud.cos.auth.COSCredentials;
import com.qcloud.cos.auth.COSCredentialsProvider;
import com.qcloud.cos.utils.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CosNConfigKeys;

/**
 * Get the credentials from the hadoop configuration.
 */
public class SimpleCredentialProvider
        extends AbstractCOSCredentialProvider implements COSCredentialsProvider {
    private String secretId;
    private String secretKey;

    public SimpleCredentialProvider(@Nullable URI uri, Configuration conf) {
        super(uri, conf);
        if (null != conf) {
            this.secretId = conf.get(
                    CosNConfigKeys.COSN_USERINFO_SECRET_ID_KEY
            );
            this.secretKey = conf.get(
                    CosNConfigKeys.COSN_USERINFO_SECRET_KEY_KEY
            );
        }
    }

    @Override
    public COSCredentials getCredentials() {
        if (!StringUtils.isNullOrEmpty(this.secretId)
                && !StringUtils.isNullOrEmpty(this.secretKey)) {
            return new BasicCOSCredentials(this.secretId, this.secretKey);
        }
        return null;
    }

    @Override
    public void refresh() {

    }
}

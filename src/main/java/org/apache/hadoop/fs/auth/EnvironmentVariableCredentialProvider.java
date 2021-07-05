package org.apache.hadoop.fs.auth;

import com.qcloud.cos.auth.BasicCOSCredentials;
import com.qcloud.cos.auth.COSCredentials;
import com.qcloud.cos.auth.COSCredentialsProvider;
import com.qcloud.cos.utils.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.cosn.Constants;
import org.apache.hadoop.fs.CosNConfigKeys;

import javax.annotation.Nullable;
import java.net.URI;

/**
 * The provider obtaining the cos credentials from the environment variables.
 */
public class EnvironmentVariableCredentialProvider
        extends AbstractCOSCredentialProvider implements COSCredentialsProvider {
    private String appId;

    public EnvironmentVariableCredentialProvider(@Nullable URI uri,
                                                 Configuration conf) {
        super(uri, conf);
        if (null != conf) {
            this.appId = conf.get(CosNConfigKeys.COSN_APPID_KEY);
        }
    }

    @Override
    public COSCredentials getCredentials() {
        String secretId = System.getenv(Constants.COSN_SECRET_ID_ENV);
        String secretKey = System.getenv(Constants.COSN_SECRET_KEY_ENV);

        secretId = StringUtils.trim(secretId);
        secretKey = StringUtils.trim(secretKey);

        if (!StringUtils.isNullOrEmpty(secretId)
                && !StringUtils.isNullOrEmpty(secretKey)) {
            if (null != this.appId) {
                return new BasicCOSCredentials(this.appId, secretId, secretKey);
            } else {
                return new BasicCOSCredentials(secretId, secretKey);
            }
        }

        return null;
    }

    @Override
    public void refresh() {
    }

    @Override
    public String toString() {
        return String.format("EnvironmentVariableCredentialProvider{%s, %s}", Constants.COSN_SECRET_ID_ENV,
                Constants.COSN_SECRET_KEY_ENV);
    }
}

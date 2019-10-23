package org.apache.hadoop.fs.auth;

import java.net.URI;
import javax.annotation.Nullable;

import com.qcloud.cos.auth.BasicCOSCredentials;
import com.qcloud.cos.auth.COSCredentials;
import com.qcloud.cos.auth.COSCredentialsProvider;
import com.qcloud.cos.utils.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Constants;

/**
 * The provider obtaining the cos credentials from the environment variables.
 */
public class EnvironmentVariableCredentialProvider
        extends AbstractCOSCredentialProvider implements COSCredentialsProvider {
    public EnvironmentVariableCredentialProvider(@Nullable URI uri,
                                                 Configuration conf) {
        super(uri, conf);
    }

    @Override
    public COSCredentials getCredentials() {
        String secretId = System.getenv(Constants.COSN_SECRET_ID_ENV);
        String secretKey = System.getenv(Constants.COSN_SECRET_KEY_ENV);

        secretId = StringUtils.trim(secretId);
        secretKey = StringUtils.trim(secretKey);

        if (!StringUtils.isNullOrEmpty(secretId)
                && !StringUtils.isNullOrEmpty(secretKey)) {
            return new BasicCOSCredentials(secretId, secretKey);
        }

        return null;
    }

    @Override
    public String toString() {
        return "EnvironmentVariableCredentialProvider{}";
    }
}

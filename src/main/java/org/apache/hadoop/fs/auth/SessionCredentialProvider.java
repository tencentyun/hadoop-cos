package org.apache.hadoop.fs.auth;

import com.qcloud.cos.auth.BasicCOSCredentials;
import com.qcloud.cos.auth.COSCredentials;
import com.qcloud.cos.auth.COSCredentialsProvider;
import com.qcloud.cos.utils.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CosNConfigKeys;

import javax.annotation.Nullable;
import java.net.URI;

/**
 * The provider getting the credential from the specified uri.
 */
public class SessionCredentialProvider
        extends AbstractCOSCredentialProvider implements COSCredentialsProvider {

    private String appId;   // compatible

    public SessionCredentialProvider(@Nullable URI uri, Configuration conf) {
        super(uri, conf);
        if (null != conf) {
            this.appId = conf.get(CosNConfigKeys.COSN_APPID_KEY);
        }
    }

    @Override
    public COSCredentials getCredentials() {
        if (null == super.getUri()) {
            return null;
        }

        String authority = super.getUri().getAuthority();
        if (null == authority) {
            return null;
        }

        int authoritySplitIndex = authority.indexOf('@');
        if (authoritySplitIndex < 0) {
            return null;
        }

        String credential = authority.substring(0, authoritySplitIndex);
        int credentialSplitIndex = credential.indexOf(':');
        if (credentialSplitIndex < 0) {
            return null;
        }
        String secretId = credential.substring(0, credentialSplitIndex);
        String secretKey = credential.substring(credentialSplitIndex + 1);

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
}

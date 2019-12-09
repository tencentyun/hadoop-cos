package org.apache.hadoop.fs.auth;

import java.net.URI;
import javax.annotation.Nullable;

import com.qcloud.cos.auth.BasicCOSCredentials;
import com.qcloud.cos.auth.COSCredentials;
import com.qcloud.cos.auth.COSCredentialsProvider;
import com.qcloud.cos.utils.StringUtils;
import org.apache.hadoop.conf.Configuration;

/**
 * The provider getting the credential from the specified uri.
 */
public class SessionCredentialProvider
        extends AbstractCOSCredentialProvider implements COSCredentialsProvider {

    public SessionCredentialProvider(@Nullable URI uri, Configuration conf) {
        super(uri, conf);
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
            return new BasicCOSCredentials(secretId, secretKey);
        }

        return null;
    }

    @Override
    public void refresh() {

    }

}

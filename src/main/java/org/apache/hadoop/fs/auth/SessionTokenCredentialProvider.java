package org.apache.hadoop.fs.auth;

import com.qcloud.cos.auth.BasicSessionCredentials;
import com.qcloud.cos.auth.COSCredentials;
import com.qcloud.cos.auth.COSCredentialsProvider;
import com.qcloud.cos.utils.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CosNConfigKeys;

import javax.annotation.Nullable;
import java.net.URI;

public class SessionTokenCredentialProvider extends AbstractCOSCredentialProvider
    implements COSCredentialsProvider {
    private String appId;
    private String secretId;
    private String secretKey;
    private String sessionToken;

    public SessionTokenCredentialProvider(@Nullable URI uri, Configuration conf) {
        super(uri, conf);
        if (null != conf) {
            this.appId = conf.get(CosNConfigKeys.COSN_APPID_KEY);
            this.secretId = conf.get(
                    CosNConfigKeys.COSN_USERINFO_SECRET_ID_KEY);
            this.secretKey = conf.get(
                    CosNConfigKeys.COSN_USERINFO_SECRET_KEY_KEY);
            this.sessionToken = conf.get(
                    CosNConfigKeys.COSN_USERINFO_SESSION_TOKEN);
        }
    }

    @Override
    public COSCredentials getCredentials() {
        if (!StringUtils.isNullOrEmpty(this.secretId)
                && !StringUtils.isNullOrEmpty(this.secretKey)) {
            if (null != this.appId) {
                return new BasicSessionCredentials(this.appId, this.secretId, this.secretKey,this.sessionToken);
            } else {
                return new BasicSessionCredentials(this.secretId, this.secretKey, this.sessionToken);
            }
        }
        return null;
    }

    @Override
    public void refresh() {
    }
}

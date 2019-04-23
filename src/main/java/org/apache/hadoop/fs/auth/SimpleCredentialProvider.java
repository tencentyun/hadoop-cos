package org.apache.hadoop.fs.auth;

import com.qcloud.cos.auth.BasicCOSCredentials;
import com.qcloud.cos.auth.COSCredentials;
import com.qcloud.cos.auth.COSCredentialsProvider;
import com.qcloud.cos.exception.CosClientException;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CosNativeFileSystemConfigKeys;


public class SimpleCredentialProvider implements COSCredentialsProvider {
    private String secretId;
    private String secretKey;

    public SimpleCredentialProvider(Configuration conf) {
        this.secretId = conf.get(
                CosNativeFileSystemConfigKeys.COS_SECRET_ID_KEY
        );
        this.secretKey = conf.get(
                CosNativeFileSystemConfigKeys.COS_SECRET_KEY_KEY
        );
    }

    @Override
    public COSCredentials getCredentials() {
        if (!StringUtils.isEmpty(this.secretId) && !StringUtils.isEmpty(this.secretKey)) {
            return new BasicCOSCredentials(this.secretId, this.secretKey);
        }
        throw new CosClientException("secret id or secret key is unset");
    }

    @Override
    public void refresh() {
    }

}

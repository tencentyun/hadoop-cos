package org.apache.hadoop.fs.auth;

import com.qcloud.cos.auth.BasicCOSCredentials;
import com.qcloud.cos.auth.COSCredentials;
import com.qcloud.cos.auth.COSCredentialsProvider;
import com.qcloud.cos.exception.CosClientException;
import com.qcloud.cos.utils.StringUtils;

/**
 * 从环境变量中获取AK和SK
 */
public class EnvironmentVariableCredentialProvider implements COSCredentialsProvider {
    @Override
    public COSCredentials getCredentials() {
        String secretId = System.getenv("COS_SECRET_ID");
        String secretKey = System.getenv("COS_SECRET_KEY");

        secretId = StringUtils.trim(secretId);
        secretKey = StringUtils.trim(secretKey);

        if (!StringUtils.isNullOrEmpty(secretId) && !StringUtils.isNullOrEmpty(secretKey)) {
            return new BasicCOSCredentials(secretId, secretKey);                        // NOTE 这里需要传appid吗
        } else {
            throw new CosClientException(
                    "Unable to load COS credentials from environment variables" +
                            "(COS_SECRET_ID or COS_SECRET_KEY)");
        }
    }

    @Override
    public void refresh() {
    }

    @Override
    public String toString() {
        return "EnvironmentVariableCredentialProvider{}";
    }
}

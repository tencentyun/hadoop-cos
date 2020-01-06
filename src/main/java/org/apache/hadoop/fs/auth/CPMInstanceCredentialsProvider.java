package org.apache.hadoop.fs.auth;

import com.qcloud.cos.auth.*;
import com.qcloud.cos.exception.CosClientException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CosNConfigKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.net.URI;

/**
 * Provide the credentials when the CosN connector is instantiated on Tencent Cloud Physical Machine (CPM)
 */
public class CPMInstanceCredentialsProvider extends AbstractCOSCredentialProvider implements COSCredentialsProvider {
    private static final Logger LOG = LoggerFactory.getLogger(CPMInstanceCredentialsProvider.class);

    private String appId;
    private final COSCredentialsProvider cosCredentialsProvider;

    public CPMInstanceCredentialsProvider(@Nullable URI uri,
                                          Configuration conf) {
        super(uri, conf);
        if (null != conf) {
            this.appId = conf.get(CosNConfigKeys.COSN_APPID_KEY);
        }
        InstanceMetadataCredentialsEndpointProvider endpointProvider =
                new InstanceMetadataCredentialsEndpointProvider(
                        InstanceMetadataCredentialsEndpointProvider.Instance.CPM);
        InstanceCredentialsFetcher instanceCredentialsFetcher = new InstanceCredentialsFetcher(endpointProvider);
        this.cosCredentialsProvider = new InstanceCredentialsProvider(instanceCredentialsFetcher);
    }

    @Override
    public COSCredentials getCredentials() {
        try {
            COSCredentials cosCredentials = this.cosCredentialsProvider.getCredentials();
            // Compatible appId
            if (null != this.appId) {
                if (cosCredentials instanceof InstanceProfileCredentials) {
                    return new InstanceProfileCredentials(this.appId, cosCredentials.getCOSAccessKeyId(),
                            cosCredentials.getCOSSecretKey(),
                            ((InstanceProfileCredentials) cosCredentials).getSessionToken(),
                            ((InstanceProfileCredentials) cosCredentials).getExpiredTime());
                }
            }
            return cosCredentials;
        } catch (CosClientException e) {
            LOG.error("Failed to obtain the credentials from CAMCPMInstanceCredentialsProvider.", e);
        }

        return null;
    }

    @Override
    public void refresh() {
        this.cosCredentialsProvider.refresh();
    }
}

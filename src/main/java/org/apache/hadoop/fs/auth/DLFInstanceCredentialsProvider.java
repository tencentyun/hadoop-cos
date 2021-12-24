package org.apache.hadoop.fs.auth;

import com.google.common.collect.ImmutableMap;
import com.qcloud.cos.auth.COSCredentials;
import com.qcloud.cos.auth.COSCredentialsProvider;
import com.qcloud.cos.auth.HttpCredentialsEndpointProvider;
import com.qcloud.cos.auth.InstanceCredentialsFetcher;
import com.qcloud.cos.auth.InstanceCredentialsProvider;
import com.qcloud.cos.exception.CosClientException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CosNConfigKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.net.URI;
import java.util.Map;

public class DLFInstanceCredentialsProvider extends AbstractCOSCredentialProvider implements COSCredentialsProvider {
    private static final Logger LOG = LoggerFactory.getLogger(DLFInstanceCredentialsProvider.class);
    private final COSCredentialsProvider cosCredentialsProvider;
    private static final String UIN = "Uin";
    private static final String REQUEST_ID = "RequestId";
    private static final String TYPE = "Type";

    private String url;
    private String path;
    private String uin;
    private String requestId;

    public DLFInstanceCredentialsProvider (@Nullable URI uri, Configuration conf) {
        super(uri, conf);
        if (null != conf) {
            this.url = conf.get(CosNConfigKeys.COS_REMOTE_CREDENTIAL_PROVIDER_URL);
            this.path = conf.get(CosNConfigKeys.COS_REMOTE_CREDENTIAL_PROVIDER_PATH);
            this.uin = conf.get(CosNConfigKeys.COSN_UIN_KEY);
            this.requestId = conf.get(CosNConfigKeys.COSN_REQUEST_ID);

        }

        if (uin == null || requestId == null) {
            throw new IllegalArgumentException("uin and request id must be exist");
        }

        Map<String, String> header = ImmutableMap.of(UIN, uin, REQUEST_ID, requestId, TYPE, "DLF");

        HttpCredentialsEndpointProvider endpointProvider = new HttpCredentialsEndpointProvider(url, path, header);
        InstanceCredentialsFetcher instanceCredentialsFetcher = new InstanceCredentialsFetcher(endpointProvider);
        this.cosCredentialsProvider = new InstanceCredentialsProvider(instanceCredentialsFetcher);
    }
    @Override
    public COSCredentials getCredentials() {
        try {
            return this.cosCredentialsProvider.getCredentials();
        } catch (CosClientException e) {
            LOG.error("Failed to obtain the credentials from DLFInstanceCredentialsProvider.", e);
        }

        return null;
    }

    @Override
    public void refresh() {
        this.cosCredentialsProvider.refresh();
    }
}

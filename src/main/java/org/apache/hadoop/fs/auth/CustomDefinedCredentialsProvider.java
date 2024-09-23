package org.apache.hadoop.fs.auth;

import com.qcloud.cos.auth.COSCredentials;
import com.qcloud.cos.auth.COSCredentialsProvider;
import com.qcloud.cos.auth.InstanceCredentialsFetcher;
import com.qcloud.cos.auth.InstanceCredentialsProvider;
import com.qcloud.cos.auth.InstanceMetadataCredentialsEndpointProvider;
import com.qcloud.cos.auth.InstanceProfileCredentials;
import com.qcloud.cos.exception.CosClientException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CosNConfigKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URISyntaxException;

import javax.annotation.Nullable;

/**
 * Fetch credential from a specific url.
 * <p>
 * url response should be like:
 * {
 * "TmpSecretId": "AKIDxxxxxxxxxxxxxxxxxxxx",
 * "TmpSecretKey": "xxxxxxxxxxexxxxxxxxxgA=",
 * "ExpiredTime": 1615590047,
 * "Expiration": "2021-03-12T23:00:47Z",
 * "Token": "xxxxxxxxxxx",
 * "Code": "Success"
 * }
 */
public class CustomDefinedCredentialsProvider extends AbstractCOSCredentialProvider
    implements COSCredentialsProvider {

  private static final Logger LOG = LoggerFactory.getLogger(CVMInstanceCredentialsProvider.class);

  private final String appId;
  private final COSCredentialsProvider cosCredentialsProvider;

  public CustomDefinedCredentialsProvider(@Nullable URI uri, Configuration conf) {
    super(uri, conf);
    if (conf == null) {
      throw new IllegalArgumentException("Configuration is null. Please check the core-site.xml.");
    }
    this.appId = conf.get(CosNConfigKeys.COSN_APPID_KEY);
    final String providerUrl = conf.get(CosNConfigKeys.COS_CUSTOM_CREDENTIAL_PROVIDER_URL);
    if (providerUrl == null) {
      throw new IllegalArgumentException(
          "fs.cosn.remote-credential-provider.url should not be null.");
    }
    InstanceMetadataCredentialsEndpointProvider endpointProvider =
        new InstanceMetadataCredentialsEndpointProvider(null) {
          @Override
          public URI getCredentialsEndpoint() throws URISyntaxException {
            return new URI(providerUrl);
          }
        };
    InstanceCredentialsFetcher instanceCredentialsFetcher =
        new InstanceCredentialsFetcher(endpointProvider);
    this.cosCredentialsProvider = new InstanceCredentialsProvider(instanceCredentialsFetcher);
    // try to fetch credentials and parse.
    getCredentials();
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
      LOG.error("Failed to obtain the credentials from CustomDefinedCredentialsProvider.", e);
    } catch (Exception e) {
      LOG.error("getCredentials failed", e);
    }

    return null;
  }

  @Override
  public void refresh() {
    this.cosCredentialsProvider.refresh();
  }

}

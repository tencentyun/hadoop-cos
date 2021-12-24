package org.apache.hadoop.fs.auth;

import com.qcloud.cos.auth.BasicSessionCredentials;
import com.qcloud.cos.auth.COSCredentials;
import com.qcloud.cos.auth.COSCredentialsProvider;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CosFileSystem;
import org.apache.hadoop.fs.CosNConfigKeys;
import org.apache.hadoop.fs.CosNUtils;
import org.apache.hadoop.fs.cosn.ranger.security.sts.GetSTSResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.net.URI;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class RangerCredentialsProvider extends AbstractCOSCredentialProvider implements COSCredentialsProvider {
    private static final Logger log = LoggerFactory.getLogger(RangerCredentialsProvider.class);
    private RangerCredentialsFetcher rangerCredentialsFetcher;
    private final String bucketName;
    private String bucketRegion;
    private String appId;


    public RangerCredentialsProvider(@Nullable URI uri, Configuration conf) {
        super(uri, conf);
        if (null != conf) {
            this.appId = conf.get(CosNConfigKeys.COSN_APPID_KEY);
        }
        this.bucketName = CosNUtils.formatBucket(uri.getHost(), conf);
        this.bucketRegion = conf.get(CosNConfigKeys.COSN_REGION_KEY);
        if (this.bucketRegion == null || this.bucketRegion.isEmpty()) {
            this.bucketRegion = conf.get(CosNConfigKeys.COSN_REGION_PREV_KEY);
        }

        rangerCredentialsFetcher = new RangerCredentialsFetcher(
                conf.getInt(
                        CosNConfigKeys.COSN_RANGER_TEMP_TOKEN_REFRESH_INTERVAL,
                        CosNConfigKeys.DEFAULT_COSN_RANGER_TEMP_TOKEN_REFRESH_INTERVAL));
    }

    class RangerCredentialsFetcher  {
        private int refreshIntervalSeconds;
        private AtomicReference<COSCredentials> lastCredentialsRef;
        private AtomicLong lastGetCredentialsTimeStamp;

        RangerCredentialsFetcher(int refreshIntervalSeconds) {
            this.refreshIntervalSeconds = refreshIntervalSeconds;
            this.lastCredentialsRef = new AtomicReference<>();
            this.lastGetCredentialsTimeStamp = new AtomicLong();
        }

        COSCredentials getCredentials() {
            if (needSyncFetchNewCredentials()) {
                synchronized (this) {
                    if (needSyncFetchNewCredentials()) {
                        return fetchNewCredentials();
                    }
                }
            }
            return lastCredentialsRef.get();
        }

        private boolean needSyncFetchNewCredentials() {
            if (lastCredentialsRef.get() == null) {
                return true;
            }
            long currentSec = System.currentTimeMillis() / 1000;
            return currentSec - lastGetCredentialsTimeStamp.get() > this.refreshIntervalSeconds;
        }

        private COSCredentials fetchNewCredentials() {
            try {
                GetSTSResponse stsResp = CosFileSystem.rangerQcloudObjectStorageStorageClient.getSTS(bucketRegion,
                        bucketName);

                COSCredentials cosCredentials = null;
                if (appId != null) {
                    cosCredentials =  new BasicSessionCredentials(appId, stsResp.getTempAK(), stsResp.getTempSK(),
                            stsResp.getTempToken());
                } else {
                    cosCredentials = new BasicSessionCredentials(stsResp.getTempAK(), stsResp.getTempSK(),
                            stsResp.getTempToken());
                }

                this.lastCredentialsRef.set(cosCredentials);
                this.lastGetCredentialsTimeStamp.set(System.currentTimeMillis() / 1000);
                return cosCredentials;
            } catch (IOException e) {
                log.error("fetch credentials failed", e);
                return null;
            }
        }
    }

    @Override
    public COSCredentials getCredentials() {
        return rangerCredentialsFetcher.getCredentials();
    }

    @Override
    public void refresh() {
    }
}

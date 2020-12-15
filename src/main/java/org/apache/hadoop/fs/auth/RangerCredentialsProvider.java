package org.apache.hadoop.fs.auth;

import com.qcloud.cos.auth.BasicSessionCredentials;
import com.qcloud.cos.auth.COSCredentials;
import com.qcloud.cos.auth.COSCredentialsProvider;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CosFileSystem;
import org.apache.hadoop.fs.CosNConfigKeys;
import org.apache.hadoop.fs.cosn.ranger.security.sts.GetSTSResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.net.URI;
import java.util.Date;
import java.util.concurrent.atomic.AtomicReference;

public class RangerCredentialsProvider extends AbstractCOSCredentialProvider implements COSCredentialsProvider {
    private static final Logger log = LoggerFactory.getLogger(RangerCredentialsProvider.class);
    private Thread credentialsFetcherDaemonThread;
    private CredentialsFetcherDaemon credentialsFetcherDaemon;
    private final String bucketName;
    private String bucketRegion;

    public RangerCredentialsProvider(@Nullable URI uri, Configuration conf) {
        super(uri, conf);
        this.bucketName = uri.getHost();
        this.bucketRegion = conf.get(CosNConfigKeys.COSN_REGION_KEY);
        if (this.bucketRegion == null || this.bucketRegion.isEmpty()) {
            this.bucketRegion = conf.get(CosNConfigKeys.COSN_REGION_PREV_KEY);
        }

        credentialsFetcherDaemon = new CredentialsFetcherDaemon(
                conf.getInt(
                        CosNConfigKeys.COSN_RANGER_TEMP_TOKEN_REFRESH_INTERVAL,
                        CosNConfigKeys.DEFAULT_COSN_RANGER_TEMP_TOKEN_REFRESH_INTERVAL));
        credentialsFetcherDaemonThread = new Thread(credentialsFetcherDaemon);
        credentialsFetcherDaemonThread.setDaemon(true);
        credentialsFetcherDaemonThread.start();
    }

    class CredentialsFetcherDaemon implements Runnable {
        private int refreshIntervalSeconds;
        private AtomicReference<COSCredentials> lastCredentialsRef;
        private Date lastFreshDate;

        CredentialsFetcherDaemon(int refreshIntervalSeconds) {
            this.refreshIntervalSeconds = refreshIntervalSeconds;
            this.lastCredentialsRef = new AtomicReference<>();
        }

        COSCredentials getCredentials() {
            COSCredentials lastCred = lastCredentialsRef.get();
            if (lastCred == null) {
                return fetchCredentials();
            }
            return lastCred;
        }

        private COSCredentials fetchCredentials() {
            try {
                GetSTSResponse stsResp = CosFileSystem.rangerQcloudObjectStorageStorageClient.getSTS(bucketRegion,
                        bucketName);
                return new BasicSessionCredentials(stsResp.getTempAK(), stsResp.getTempSK(), stsResp.getTempToken());
            } catch (IOException e) {
                log.error("fetch credentials failed", e);
                return null;
            }
        }

        @Override
        public void run() {
            while (true) {
                Date currentDate = new Date();
                if (lastFreshDate == null || (currentDate.getTime() / 1000 - lastFreshDate.getTime() / 1000)
                        < this.refreshIntervalSeconds) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                    }
                    continue;
                }

                COSCredentials newCred = fetchCredentials();
                if (newCred != null) {
                    this.lastFreshDate = currentDate;
                    this.lastCredentialsRef.set(newCred);
                }
            }
        }
    }

    @Override
    public COSCredentials getCredentials() {
        return credentialsFetcherDaemon.getCredentials();
    }

    @Override
    public void refresh() {

    }
}

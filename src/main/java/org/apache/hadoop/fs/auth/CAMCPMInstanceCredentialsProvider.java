package org.apache.hadoop.fs.auth;

import com.qcloud.cos.auth.COSCredentials;
import com.qcloud.cos.auth.COSCredentialsProvider;
import com.qcloud.cos.auth.CPMInstanceProfileCredentialsProvider;
import com.qcloud.cos.exception.CosClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;

/**
 * Provide the credentials when the CosN connector is instantiated on Tencent Cloud Physical Machine (CPM)
 */
public class CAMCPMInstanceCredentialsProvider implements COSCredentialsProvider, Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(CAMCPMInstanceCredentialsProvider.class);

    private final COSCredentialsProvider cosCredentialsProvider = CPMInstanceProfileCredentialsProvider.getInstance();

    public CAMCPMInstanceCredentialsProvider() {
    }

    @Override
    public COSCredentials getCredentials() {
        if (null != this.cosCredentialsProvider) {
            try {
                return this.cosCredentialsProvider.getCredentials();
            } catch (CosClientException e) {
                LOG.error("Failed to obtain the credentials from CAMCPMInstanceCredentialsProvider.", e);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        return null;
    }

    @Override
    public void refresh() {
        if (null != this.cosCredentialsProvider) {
            this.cosCredentialsProvider.refresh();
        } else {
            LOG.debug("The CAMCPMInstanceCredentialsProvider is null. Skip to refresh.");
        }
    }

    @Override
    public void close() throws IOException {
        ((CAMCPMInstanceCredentialsProvider) this.cosCredentialsProvider).close();
    }
}

package org.apache.hadoop.fs.auth;

import com.qcloud.cos.auth.COSCredentials;
import com.qcloud.cos.auth.COSCredentialsProvider;
import com.qcloud.cos.auth.CVMInstanceProfileCredentialsProvider;
import com.qcloud.cos.exception.CosClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;

/**
 * Provide the credentials when the CosN connector is instantiated on Tencent Cloud Virtual Machine(CVM)
 */
public class CAMCVMInstanceCredentialsProvider implements COSCredentialsProvider, Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(CAMCVMInstanceCredentialsProvider.class);

    private final COSCredentialsProvider cosCredentialsProvider = CVMInstanceProfileCredentialsProvider.getInstance();

    public CAMCVMInstanceCredentialsProvider() {
    }

    @Override
    public COSCredentials getCredentials() {
        if (null != this.cosCredentialsProvider) {
            try {
                return this.cosCredentialsProvider.getCredentials();
            } catch (CosClientException e) {
                LOG.error("Failed to obtain the credentials from CAMCVMInstanceCredentialsProvider.", e);
            }
        }

        return null;
    }

    @Override
    public void refresh() {
        if (null != this.cosCredentialsProvider) {
            this.cosCredentialsProvider.refresh();
        } else {
            LOG.debug("The CAMCVMInstanceCredentialsProvider is null. Skip to refresh.");
        }
    }

    @Override
    public void close() throws IOException {
        ((CVMInstanceProfileCredentialsProvider) this.cosCredentialsProvider).close();
    }
}

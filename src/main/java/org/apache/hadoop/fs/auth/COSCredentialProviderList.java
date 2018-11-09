package org.apache.hadoop.fs.auth;

import com.google.common.base.Preconditions;
import com.qcloud.cos.auth.AnonymousCOSCredentials;
import com.qcloud.cos.auth.COSCredentials;
import com.qcloud.cos.auth.COSCredentialsProvider;
import com.qcloud.cos.exception.CosClientException;
import com.qcloud.cos.utils.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class COSCredentialProviderList implements COSCredentialsProvider, AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(COSCredentialProviderList.class);

    private static final String NO_COS_CREDENTIAL_PROVIDERS = new String("No COS Credential Providers");
    private static final String CREDENTIALS_REQUESTED_WHEN_CLOSED = new String("Credentials requested after provider list was closed");

    private final List<COSCredentialsProvider> providers = new ArrayList<>(1);
    private boolean reuseLastProvider = true;
    private COSCredentialsProvider lastProvider;

    private final AtomicInteger refCount = new AtomicInteger(1);            // 引用计数
    private final AtomicBoolean isClosed = new AtomicBoolean(false);

    public COSCredentialProviderList() {
    }

    public COSCredentialProviderList(Collection<COSCredentialsProvider> providers) {
        this.providers.addAll(providers);
    }

    public void add(COSCredentialsProvider provider) {
        this.providers.add(provider);
    }

    public int getRefCount() {
        return this.refCount.get();
    }

    public void checkNotEmpty() {
        if (this.providers.isEmpty()) {
            throw new NoAuthWithCOSException(NO_COS_CREDENTIAL_PROVIDERS);
        }
    }

    public COSCredentialProviderList share() {
        Preconditions.checkState(!this.closed(), "Provider list is closed");
        this.refCount.incrementAndGet();
        return this;
    }

    public boolean closed() {
        return this.isClosed.get();
    }

    @Override
    public COSCredentials getCredentials() {
        if (this.closed()) {
            throw new NoAuthWithCOSException(CREDENTIALS_REQUESTED_WHEN_CLOSED);
        }

        this.checkNotEmpty();

        if (this.reuseLastProvider && this.lastProvider != null) {
            return this.lastProvider.getCredentials();
        }

        for (COSCredentialsProvider provider : this.providers) {
            try {
                COSCredentials credentials = provider.getCredentials();
                if (!StringUtils.isNullOrEmpty(credentials.getCOSAccessKeyId())
                        && !StringUtils.isNullOrEmpty(credentials.getCOSSecretKey())
                        || credentials instanceof AnonymousCOSCredentials) {
                    this.lastProvider = provider;
                    return credentials;
                }
            } catch (CosClientException e) {
                LOG.warn("No credentials provided by {}: {}", provider, e.toString());
                continue;
            }
        }

        throw new NoAuthWithCOSException("No COS Credentials provided by " + this.providers.toString());
    }

    @Override
    public void refresh() {
        if (this.closed()) {
            return;
        }

        for (COSCredentialsProvider provider : this.providers) {
            provider.refresh();
        }
    }

    @Override
    public void close() throws Exception {
        if (this.closed()) {
            return;
        }

        int remainder = this.refCount.decrementAndGet();
        if (remainder != 0) {
            return;
        }
        this.isClosed.set(true);

        for (COSCredentialsProvider provider : this.providers) {
            if (provider instanceof Closeable) {
                ((Closeable) provider).close();
            }
            // TODO autoclosable的情况
        }
    }

}

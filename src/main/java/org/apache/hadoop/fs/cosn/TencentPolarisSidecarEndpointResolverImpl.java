package org.apache.hadoop.fs.cosn;

import com.qcloud.cos.endpoint.DefaultEndpointResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TencentPolarisSidecarEndpointResolverImpl implements TencentPolarisEndpointResolver {
    private static final Logger LOG = LoggerFactory.getLogger(TencentPolarisSidecarEndpointResolverImpl.class);
    private final TencentPolarisSidecarClient polarisClient;
    private final String namespace;
    private final String service;
    private final DefaultEndpointResolver innerEndpointResolver;
    private final TencentPolarisSidecarClient.CallResultStat serviceCallResultStat;
    private String lastID;
    private String lastHost;
    private int lastPort;
    private int maxRetries = 3;

    public TencentPolarisSidecarEndpointResolverImpl(TencentPolarisSidecarClient polarisClient, String namespace, String service) {
        this.polarisClient = polarisClient;
        this.namespace = namespace;
        this.service = service;
        this.innerEndpointResolver = new DefaultEndpointResolver();
        this.serviceCallResultStat = new TencentPolarisSidecarClient.CallResultStat(namespace, service);
    }

    @Override
    public String resolveGeneralApiEndpoint(String generalApiEndpoint) {
        for (int i = 0; i < maxRetries; i++) {
            if (polarisClient != null) {
                TencentPolarisSidecarClient.Instance instance = polarisClient.getOneInstance(namespace, service);
                if (instance != null) {
                    this.lastID = instance.id;
                    this.lastHost = instance.host;
                    this.lastPort = instance.port;
                    return EndpointResolveHelper.buildUrl(instance.host, instance.port);
                } else {
                    LOG.warn("Failed to get any instance from polaris sidecar, namespace: {}, service: {}, retry: {}, maxRetries: {}.",
                            namespace, service, i + 1, this.maxRetries);
                }
            } else {
                LOG.warn("Failed to get any instance from polaris sidecar,as polarisClient is null," +
                        " namespace: {}, service: {}, retry: {}, maxRetries: {}.", namespace, service, i + 1, this.maxRetries);
            }
        }
        return innerEndpointResolver.resolveGeneralApiEndpoint(generalApiEndpoint);
    }

    @Override
    public String resolveGetServiceApiEndpoint(String getServiceApiEndpoint) {
        return innerEndpointResolver.resolveGetServiceApiEndpoint(getServiceApiEndpoint);
    }

    @Override
    public void handle(int status, long costTime) {
        if (null != this.lastHost && this.lastPort > 0) {
            // 上报状态
            TencentPolarisSidecarClient.CallResult item = new TencentPolarisSidecarClient.CallResult();
            item.id = this.lastID;
            item.host = this.lastHost;
            item.port = this.lastPort;
            // 0 表示成功 1 表示失败
            item.retStatus = status == 0 ? 0:1;
            item.retCode = status;
            item.callDelay = costTime;
            this.serviceCallResultStat.updateCallResult(item);
            this.polarisClient.updateResult(this.serviceCallResultStat);
        } else {
            // 无效的上报
            LOG.error("Update polaris service call result failed, host: {}, port: {}, status: {}, costTime: {}",
                    this.lastHost, this.lastPort, status, costTime);
        }
    }

    @Override
    public TencentPolarisSidecarEndpointResolverImpl withMaxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
        return this;
    }
}

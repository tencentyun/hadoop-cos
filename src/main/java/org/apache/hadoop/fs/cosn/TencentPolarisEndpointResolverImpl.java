package org.apache.hadoop.fs.cosn;

import com.google.common.base.Preconditions;
import com.tencent.polaris.api.core.ConsumerAPI;
import com.tencent.polaris.api.exception.PolarisException;
import com.tencent.polaris.api.pojo.Instance;
import com.tencent.polaris.api.pojo.RetStatus;
import com.tencent.polaris.api.rpc.GetOneInstanceRequest;
import com.tencent.polaris.api.rpc.InstancesResponse;
import com.tencent.polaris.api.rpc.ServiceCallResult;
import com.tencent.polaris.factory.api.DiscoveryAPIFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

/**
 * 腾讯内部北极星接入
 */
public class TencentPolarisEndpointResolverImpl implements TencentPolarisEndpointResolver {
    private static final Logger LOG = LoggerFactory.getLogger(TencentPolarisEndpointResolverImpl.class);

    private final String namespace;
    private final String service;

    private final ConsumerAPI consumerAPI = DiscoveryAPIFactory.createConsumerAPI();
    private final ServiceCallResult serviceCallResult;
    private int maxRetries = 3;

    private String lastHost;
    private int lastPort;

    public TencentPolarisEndpointResolverImpl(String namespace, String service) {
        Preconditions.checkArgument(namespace != null && !namespace.isEmpty(), "namespace should not be null or empty");
        Preconditions.checkArgument(service != null && !service.isEmpty(), "service should not be null or empty");
        this.namespace = namespace;
        this.service = service;
        this.serviceCallResult = new ServiceCallResult();
        this.serviceCallResult.setNamespace(namespace);
        this.serviceCallResult.setService(service);
    }

    @Override
    @Nullable
    public String resolveGeneralApiEndpoint(String s) {
        GetOneInstanceRequest getOneInstanceRequest = new GetOneInstanceRequest();
        getOneInstanceRequest.setNamespace(namespace);
        getOneInstanceRequest.setService(service);

        for (int i = 0; i < maxRetries; i++) {
            try {
                InstancesResponse oneInstance = consumerAPI.getOneInstance(getOneInstanceRequest);
                Instance[] instances = oneInstance.getInstances();
                if (instances != null && instances.length > 0) {
                    Instance instance = instances[0];
                    this.lastHost = instance.getHost();
                    this.lastPort = instance.getPort();
                    return EndpointResolveHelper.buildUrl(instance.getHost(), instance.getPort());
                }
                // 更新状态为未找到,这里不知道是不是该更新为超时
                LOG.warn("Failed to get any instance from polaris, namespace: {}, service: {}, retry: {}, maxRetries: {}.",
                        namespace, service, i + 1, this.maxRetries);
            } catch (PolarisException e) {
                LOG.error("Failed to get instance from polaris, namespace: {}, service: {}, retry: {}, maxRetries: {}.",
                        namespace, service, i + 1, this.maxRetries, e);
            }
        }

        LOG.error("Failed to get instance from polaris, namespace: {}, service: {}", namespace, service);
        return null;
    }

    @Override
    public String resolveGetServiceApiEndpoint(String s) {
        return "service.cos.myqcloud.com";
    }

    @Override
    public TencentPolarisEndpointResolverImpl withMaxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
        return this;
    }

    @Override
    public void handle(int status, long costTime) {
        if (null != this.lastHost && this.lastPort > 0) {
            // 上报状态
            this.serviceCallResult.setHost(this.lastHost);
            this.serviceCallResult.setPort(this.lastPort);
            this.serviceCallResult.setRetStatus(status == 0 ? RetStatus.RetSuccess : RetStatus.RetFail);
            this.serviceCallResult.setRetCode(status);
            this.serviceCallResult.setDelay(costTime);
            this.consumerAPI.updateServiceCallResult(serviceCallResult);
        } else {
            // 无效的上报
            LOG.error("Update polaris service call result failed, host: {}, port: {}, status: {}, costTime: {}",
                    this.lastHost, this.lastPort, status, costTime);
        }
    }
}

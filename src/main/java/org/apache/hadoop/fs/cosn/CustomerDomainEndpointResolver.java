package org.apache.hadoop.fs.cosn;

import com.qcloud.cos.endpoint.EndpointBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class CustomerDomainEndpointResolver implements EndpointBuilder {
    private static final Logger log = LoggerFactory.getLogger(CustomerDomainEndpointResolver.class);

    private String endPoint;

    public CustomerDomainEndpointResolver() {
        super();
        endPoint = null;
    }

    public CustomerDomainEndpointResolver(String endPoint) {
        super();
        this.endPoint = endPoint;
    }

    public void setEndpoint(String endPoint) {
        this.endPoint = endPoint;
    }

    public String getEndpoint() {
        return this.endPoint;
    }

    @Override
    public String buildGeneralApiEndpoint(String s) {
        if (this.endPoint != null) {
            return this.endPoint;
        } else {
            log.error("Get customer domain is null");
        }
        return null;
    }

    @Override
    public String buildGetServiceApiEndpoint() {
        return "service.cos.myqcloud.com";
    }
}

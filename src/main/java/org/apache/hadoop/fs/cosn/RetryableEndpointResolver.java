package org.apache.hadoop.fs.cosn;

import com.qcloud.cos.endpoint.EndpointResolver;

public interface RetryableEndpointResolver extends EndpointResolver {
    RetryableEndpointResolver withMaxRetries(int maxRetries);
}

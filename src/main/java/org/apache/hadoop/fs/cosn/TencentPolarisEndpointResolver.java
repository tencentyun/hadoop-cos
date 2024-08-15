package org.apache.hadoop.fs.cosn;

import com.qcloud.cos.http.HandlerAfterProcess;

public interface TencentPolarisEndpointResolver extends RetryableEndpointResolver, HandlerAfterProcess {
}

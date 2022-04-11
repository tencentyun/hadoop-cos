package org.apache.hadoop.fs.cosn;

import com.qcloud.cos.endpoint.EndpointResolver;

public interface TencentCloudL5EndpointResolver extends EndpointResolver {
    public void setModId(int modId);
    public void setCmdId(int cmdId);
    public void updateRouteResult(int status);
}

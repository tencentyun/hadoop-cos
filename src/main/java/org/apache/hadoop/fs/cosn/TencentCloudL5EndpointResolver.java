package org.apache.hadoop.fs.cosn;

import com.qcloud.cos.endpoint.EndpointResolver;
import com.qcloud.cos.http.HandlerAfterProcess;

public interface TencentCloudL5EndpointResolver extends EndpointResolver, HandlerAfterProcess {
    public void setModId(int modId);
    public void setCmdId(int cmdId);
}

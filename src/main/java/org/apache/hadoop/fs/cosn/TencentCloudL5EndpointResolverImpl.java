package org.apache.hadoop.fs.cosn;

import com.tencent.jungle.lb2.L5API;
import com.tencent.jungle.lb2.L5APIException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ThreadLocalRandom;

/**
 * 这个类是专供于腾讯云 L5 解析方式
 */
public class TencentCloudL5EndpointResolverImpl implements TencentCloudL5EndpointResolver {
    private static final Logger LOG = LoggerFactory.getLogger(TencentCloudL5EndpointResolverImpl.class);

    private int modId;
    private int cmdId;
    private String l5IP;
    private int l5Port;
    private long l5Start;

    public TencentCloudL5EndpointResolverImpl() {
        this(-1, -1);
    }

    public TencentCloudL5EndpointResolverImpl(int modId, int cmdId) {
        this.modId = modId;
        this.cmdId = cmdId;
        this.l5IP = null;
        this.l5Port = -1;
        this.l5Start = 0;
    }

    public int getModId() {
        return modId;
    }

    @Override
    public void setModId(int modId) {
        this.modId = modId;
    }

    public int getCmdId() {
        return cmdId;
    }

    @Override
    public void setCmdId(int cmdId) {
        this.cmdId = cmdId;
    }

    @Override
    public void handle(int status, long costMs) {
        // which is used by cos java sdk to refresh
        if (null != l5IP && l5Port > 0) {
            int startUsec = (int)(System.currentTimeMillis() - costMs);
            L5API.L5QOSPacket packet = new L5API.L5QOSPacket();
            packet.ip = this.l5IP;
            packet.port = l5Port;
            packet.cmdid = this.cmdId;
            packet.modid = this.modId;
            packet.start = startUsec;

            // because inner already retry many times, give the control into the java sdk.
            L5API.updateRoute(packet, status);
        } else {
            LOG.error("Update l5 modid: {} cmdid: {} ip: {} port {}, " +
                            "status {}, costMs {} failed.",
                    this.modId, this.cmdId, this.l5IP, this.l5Port, status, costMs);
        }
    }

    @Override
    public String resolveGeneralApiEndpoint(String s) {
        float timeout = 0.2F;
        String cgiIpAddr = null;
        L5API.L5QOSPacket packet = new L5API.L5QOSPacket();
        packet.modid = this.modId;
        packet.cmdid = this.cmdId;

        int maxRetry = 5;
        for (int retryIndex = 1; retryIndex <= maxRetry; ++retryIndex) {
            try {
                packet = L5API.getRoute(packet, timeout);
                if (!packet.ip.isEmpty() && packet.port > 0) {
                    this.l5IP = packet.ip;
                    this.l5Port = packet.port;
                    this.l5Start = packet.start;
                    cgiIpAddr = String.format("%s:%d", packet.ip, packet.port);
                    break;
                }
            } catch (L5APIException e) {
                if (retryIndex != maxRetry) {
                    try {
                        Thread.sleep(ThreadLocalRandom.current().nextLong(1000L, 3000L));
                    } catch (InterruptedException var) {
                    }
                } else {
                    LOG.error("Get l5 modid: {} cmdid: {} failed: ", this.modId, this.cmdId, e);
                }
            }
        }

        return cgiIpAddr;
    }

    @Override
    public String resolveGetServiceApiEndpoint(String s) {
        return "service.cos.myqcloud.com";
    }
}

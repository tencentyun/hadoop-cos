package org.apache.hadoop.fs.cosn;

import com.qcloud.cos.endpoint.EndpointResolver;
import com.tencent.jungle.lb2.L5API;
import com.tencent.jungle.lb2.L5APIException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ThreadLocalRandom;

/**
 * 这个类是专供于腾讯云 L5 解析方式
 */
public class TencentCloudL5EndpointResolver implements EndpointResolver {
    private static final Logger LOG = LoggerFactory.getLogger(TencentCloudL5EndpointResolver.class);

    private int modId;
    private int cmdId;
    private String l5IP;
    private int l5Port;
    private long l5Start;

    public TencentCloudL5EndpointResolver() {
        this(-1, -1);
    }

    public TencentCloudL5EndpointResolver(int modId, int cmdId) {
        this.modId = modId;
        this.cmdId = cmdId;
        this.l5IP = null;
        this.l5Port = -1;
        this.l5Start = 0;
    }

    public int getModId() {
        return modId;
    }

    public void setModId(int modId) {
        this.modId = modId;
    }

    public int getCmdId() {
        return cmdId;
    }

    public void setCmdId(int cmdId) {
        this.cmdId = cmdId;
    }

    public void l5RouteResultUpdate(int status) {
        if (null != l5IP && l5Port > 0) {
            L5API.L5QOSPacket packet = new L5API.L5QOSPacket();
            packet.ip = this.l5IP;
            packet.port = l5Port;
            packet.cmdid = this.cmdId;
            packet.modid = this.modId;
            packet.start = this.l5Start;

            for (int i = 0; i < 5; ++i) {
                L5API.updateRoute(packet, status);
            }
        } else {
            LOG.error("Update l5 modid: {} cmdid: {} ip: {} port {} failed.",
                    this.modId, this.cmdId, this.l5IP, this.l5Port);
        }
    }

    @Override
    public String resolveGeneralApiEndpoint(String s) {
        float timeout = 0.2F;
        String cgiIpAddr = null;
        L5API.L5QOSPacket packet = new L5API.L5QOSPacket();
        packet.modid = this.modId;
        packet.cmdid = this.cmdId;


        for (int i = 0; i < 5; ++i) {
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
                LOG.error("Get l5 modid: {} cmdid: {} failed.", this.modId, this.cmdId);
                try {
                    Thread.sleep(ThreadLocalRandom.current().nextLong(10L, 1000L));
                } catch (InterruptedException var) {
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

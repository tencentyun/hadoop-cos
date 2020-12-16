package org.apache.hadoop.fs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.qcloud.cos.endpoint.EndpointResolver;
import com.tencent.jungle.lb2.L5API;
import com.tencent.jungle.lb2.L5APIException;
import com.tencent.jungle.lb2.L5API.L5QOSPacket;

import java.util.concurrent.ThreadLocalRandom;

public class L5EndpointResolver implements EndpointResolver {
    private static final Logger log = LoggerFactory.getLogger(L5EndpointResolver.class);

    private int modId;
    private int cmdId;
    private String l5Ip;
    private int l5Port;
    private long l5start;


    public L5EndpointResolver(int modId, int cmdId) {
        super();
        this.modId = modId;
        this.cmdId = cmdId;
        l5Ip = null;
        l5Port = -1;
        l5start = 0;
    }


    public void L5RouteResultUpdate(int status) {
        if (l5Ip != null && l5Port > 0) {
            L5QOSPacket packet = new L5QOSPacket();
            packet.ip = l5Ip;
            packet.port = l5Port;
            packet.cmdid = this.cmdId;
            packet.modid = this.modId;
            packet.start = this.l5start;

            for (int i = 0; i < 5; ++i) {
                L5API.updateRoute(packet, status);
            }
        } else {
            log.error("Update l5 modid: {} cmdid: {} ip: {} port {} failed.",
                    this.modId, this.cmdId, this.l5Ip, this.l5Port);
        }
    }

    @Override
    public String resolveGeneralApiEndpoint(String endpoint) {

        float timeout = 0.2F;
        String cgiIpAddr = null;
        L5QOSPacket packet = new L5QOSPacket();
        packet.modid = this.modId;
        packet.cmdid = this.cmdId;


        for (int i = 0; i < 5; ++i) {
            try {
                packet = L5API.getRoute(packet, timeout);
                if (!packet.ip.isEmpty() && packet.port > 0) {
                    l5Ip = packet.ip;
                    l5Port = packet.port;
                    l5start = packet.start;
                    cgiIpAddr = String.format("%s:%d", packet.ip, packet.port);
                    break;
                }
            } catch (L5APIException e) {
                log.error("Get l5 modid: {} cmdid: {} failed.", this.modId, this.cmdId);
                try {
                    Thread.sleep(ThreadLocalRandom.current().nextLong(10L, 1000L));
                } catch (InterruptedException var) {
                }
            }
        }

        return cgiIpAddr;
    }

    @Override
    public String resolveGetServiceApiEndpoint(String arg0) {
        return "service.cos.myqcloud.com";
    }
}

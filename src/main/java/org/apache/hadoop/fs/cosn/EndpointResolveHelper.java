package org.apache.hadoop.fs.cosn;

public final class EndpointResolveHelper {
    public static String buildUrl(String host, int port) {
        return String.format("%s:%d", host, port);
    }
}

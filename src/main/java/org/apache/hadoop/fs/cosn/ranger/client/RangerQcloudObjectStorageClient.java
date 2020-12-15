package org.apache.hadoop.fs.cosn.ranger.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.cosn.ranger.security.authorization.PermissionRequest;
import org.apache.hadoop.fs.cosn.ranger.security.sts.GetSTSResponse;
import org.apache.hadoop.security.token.Token;

import java.io.IOException;

public interface RangerQcloudObjectStorageClient {
    public void init(Configuration conf) throws IOException;

    public String getCanonicalServiceName();

    public boolean checkPermission(PermissionRequest permissionRequest) throws IOException;

    public Token<?> getDelegationToken(String renewer) throws IOException;

    /**
     * Renew the given token.
     *
     * @return the new expiration time
     * @throws IOException
     * @throws InterruptedException
     */
    public long renew(Token<?> token,
                      Configuration conf
    ) throws IOException, InterruptedException;

    /**
     * Cancel the given token
     *
     * @throws IOException
     * @throws InterruptedException
     */

    public void cancel(Token<?> token, Configuration configuration) throws IOException, InterruptedException;

    public GetSTSResponse getSTS(String bucketRegion, String bucketName) throws IOException;

    public void close();
}

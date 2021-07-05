package org.apache.hadoop.fs.cosn.buffer;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CosNNonDirectBufferFactory implements CosNBufferFactory {
    private static final Logger LOG =
            LoggerFactory.getLogger(CosNNonDirectBufferFactory.class);

    @Override
    public CosNByteBuffer create(int size) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(size);
        return new CosNNonDirectBuffer(byteBuffer);
    }

    @Override
    public void release(CosNByteBuffer cosNByteBuffer) {
        if (null == cosNByteBuffer) {
            LOG.debug("The buffer returned is null. Ignore it.");
            return;
        }

        try {
            cosNByteBuffer.close();
        } catch (IOException e) {
            LOG.error("Release the non direct buffer failed.", e);
        }
    }
}

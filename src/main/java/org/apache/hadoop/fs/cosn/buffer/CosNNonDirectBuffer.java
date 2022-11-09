package org.apache.hadoop.fs.cosn.buffer;

import java.nio.ByteBuffer;

/**
 * The direct buffer based on the JVM heap memory.
 */
class CosNNonDirectBuffer extends CosNByteBuffer {

    public CosNNonDirectBuffer(ByteBuffer byteBuffer) {
        super(byteBuffer);
    }

    @Override
    protected boolean isDirect() {
        return false;
    }

    @Override
    protected boolean isMapped() {
        return false;
    }
}

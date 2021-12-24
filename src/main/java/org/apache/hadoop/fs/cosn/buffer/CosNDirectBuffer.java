package org.apache.hadoop.fs.cosn.buffer;

import java.nio.ByteBuffer;

/**
 * The direct buffer.
 */
class CosNDirectBuffer extends CosNByteBuffer {
    public CosNDirectBuffer(ByteBuffer byteBuffer) {
        super(byteBuffer);
    }

    @Override
    boolean isMapped() {
        return false;
    }
}

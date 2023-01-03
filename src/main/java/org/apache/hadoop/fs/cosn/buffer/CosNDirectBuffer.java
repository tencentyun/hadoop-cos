package org.apache.hadoop.fs.cosn.buffer;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * The direct buffer.
 */
class CosNDirectBuffer extends CosNByteBuffer {
    public CosNDirectBuffer(ByteBuffer byteBuffer) {
        super(byteBuffer);
    }

    ByteBuffer getByteBuffer() {
        return super.byteBuffer;
    }

    @Override
    public boolean isDirect() {return true;}

    @Override
    protected boolean isMapped() {
        return false;
    }

    @Override
    public void close() throws IOException {
        super.close();
    }
}

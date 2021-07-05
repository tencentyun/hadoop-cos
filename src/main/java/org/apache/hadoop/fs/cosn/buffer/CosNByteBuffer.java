package org.apache.hadoop.fs.cosn.buffer;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;

import org.apache.hadoop.io.nativeio.NativeIO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.nio.ch.DirectBuffer;

/**
 * The base class for all CosN byte buffers.
 */
public abstract class CosNByteBuffer implements Closeable {
    private static final Logger LOG =
            LoggerFactory.getLogger(CosNByteBuffer.class);
    private ByteBuffer byteBuffer;

    public CosNByteBuffer(ByteBuffer byteBuffer) {
        this.byteBuffer = byteBuffer;
    }

    public ByteBuffer getByteBuffer() {
        return byteBuffer;
    }

    public boolean isDirect() {
        return this.byteBuffer.isDirect();
    }

    abstract boolean isMapped();

    @Override
    public void close() throws IOException {
        if (null == this.byteBuffer) {
            return;
        }
        this.byteBuffer.clear();

        if (this.isMapped()) {
            NativeIO.POSIX.munmap((MappedByteBuffer) this.byteBuffer);
        } else if (this.byteBuffer.isDirect()) {
            ((DirectBuffer) this.byteBuffer).cleaner().clean();
        }

        this.byteBuffer = null;
    }
}

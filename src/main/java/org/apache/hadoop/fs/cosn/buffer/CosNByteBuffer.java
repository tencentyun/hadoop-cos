package org.apache.hadoop.fs.cosn.buffer;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.nio.ch.DirectBuffer;
import sun.nio.ch.FileChannelImpl;

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
            Method method = null;
            try {
                method = FileChannelImpl.class.getDeclaredMethod(
                        "unmap",
                        MappedByteBuffer.class);
                method.setAccessible(true);
                method.invoke(
                        FileChannelImpl.class,
                        (MappedByteBuffer)this.byteBuffer);
            } catch (Exception e) {
                LOG.error("failed to call reflect unmap", e);
                throw new IOException("failed to call reflect unmap", e);
            }
        } else if (this.byteBuffer.isDirect()) {
            ((DirectBuffer) this.byteBuffer).cleaner().clean();
        }

        this.byteBuffer = null;
    }
}

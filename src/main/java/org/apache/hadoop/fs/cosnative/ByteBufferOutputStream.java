package org.apache.hadoop.fs.cosnative;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;

public class ByteBufferOutputStream extends OutputStream {
    static Logger LOG = LoggerFactory.getLogger(ByteBuffer.class);
    private final ByteBuffer byteBuffer;

    public ByteBufferOutputStream(ByteBuffer byteBuffer) throws IOException {
        if (null == byteBuffer) {
            throw new IOException("byte buffer is null");
        }
        this.byteBuffer = byteBuffer;
        this.byteBuffer.clear();
    }

    public ByteBufferOutputStream(byte[] buffer) throws IOException {
        this(ByteBuffer.wrap(buffer));
    }

    @Override
    public void write(int b) {
        byte[] singleBytes = new byte[1];
        singleBytes[0] = (byte) b;
        this.byteBuffer.put(singleBytes, 0, 1);
    }

    @Override
    public void flush() throws IOException {
        if (this.byteBuffer instanceof MappedByteBuffer) {
            ((MappedByteBuffer) this.byteBuffer).force();
        }
    }

    @Override
    public void close() throws IOException {
        this.flush();
        this.byteBuffer.flip();
    }
}

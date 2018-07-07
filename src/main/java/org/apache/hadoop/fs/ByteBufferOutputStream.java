package org.apache.hadoop.fs;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class ByteBufferOutputStream extends OutputStream {
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
    public void close() throws IOException {
        this.flush();
        this.byteBuffer.flip();
    }
}

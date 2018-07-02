package org.apache.hadoop.fs.cosnative;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public class ByteBufferInputStream extends InputStream {
    private final ByteBuffer byteBuffer;

    public ByteBufferInputStream(ByteBuffer byteBuffer) throws IOException {
        if (null == byteBuffer) {
            throw new IOException("byte buffer is null");
        }
        this.byteBuffer = byteBuffer;
    }

    public ByteBufferInputStream(byte[] buffer) throws IOException {
        this(ByteBuffer.wrap(buffer));
    }

    @Override
    public int read() throws IOException {
        if (null == this.byteBuffer) {
            throw new IOException("this byte buffer for InputStream is null");
        }
        if (!this.byteBuffer.hasRemaining()) {
            return -1;
        }
        return this.byteBuffer.get() & 0xFF;
    }

    @Override
    public void close() throws IOException {
        this.byteBuffer.rewind();               // 可重复读取的流
    }
}

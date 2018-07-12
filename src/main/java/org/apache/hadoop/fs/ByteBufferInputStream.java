package org.apache.hadoop.fs;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.InvalidMarkException;

public class ByteBufferInputStream extends InputStream {
    private ByteBuffer byteBuffer;
    private boolean isClosed = true;

    public ByteBufferInputStream(ByteBuffer byteBuffer) throws IOException {
        if (null == byteBuffer) {
            throw new IOException("byte buffer is null");
        }
        this.byteBuffer = byteBuffer;
        this.isClosed = false;
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
    public synchronized void mark(int readLimit) {
        if (!this.markSupported()) {
            return;
        }
        this.byteBuffer.mark();
        // Parameter readLimit is ignored
    }

    @Override
    public boolean markSupported() {
        return true;
    }

    @Override
    public synchronized void reset() throws IOException {
        if (this.isClosed) {
            throw new IOException("Closed in InputStream");
        }
        try {
            this.byteBuffer.reset();
        } catch (InvalidMarkException e) {
            throw new IOException("Invalid mark");
        }
    }

    @Override
    public int available() throws IOException {
        return this.byteBuffer.remaining();
    }

    @Override
    public void close() throws IOException {
        this.byteBuffer.rewind();
        this.byteBuffer = null;
        this.isClosed = true;
    }
}

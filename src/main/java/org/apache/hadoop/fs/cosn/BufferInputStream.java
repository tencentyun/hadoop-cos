package org.apache.hadoop.fs.cosn;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.InvalidMarkException;
import org.apache.hadoop.fs.cosn.buffer.CosNByteBuffer;

public class BufferInputStream extends InputStream {
    private ByteBuffer byteBuffer;
    private boolean isClosed = true;

    public BufferInputStream(CosNByteBuffer buffer) throws IOException {
        if (null == buffer) {
            throw new IOException("byte buffer is null");
        }
        this.byteBuffer = buffer.getByteBuffer();
        this.isClosed = false;
    }

    @Override
    public int read() throws IOException {
        this.checkClosed();

        if (!this.byteBuffer.hasRemaining()) {
            return -1;
        }
        return this.byteBuffer.get() & 0xFF;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        this.checkClosed();

        if (!this.byteBuffer.hasRemaining()) {
            return -1;
        }

        int readLength = Math.min(this.byteBuffer.remaining(), len);
        this.byteBuffer.get(b, off, readLength);
        return readLength;
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
        this.checkClosed();

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

    private void checkClosed() throws IOException {
        if (this.isClosed) {
            throw new IOException(
                    String.format("The BufferInputStream[%d] has been closed", this.hashCode()));
        }
    }
}

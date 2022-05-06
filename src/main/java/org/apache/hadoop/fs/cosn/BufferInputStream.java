package org.apache.hadoop.fs.cosn;

import org.apache.hadoop.fs.cosn.buffer.CosNByteBuffer;

import java.io.IOException;
import java.io.InputStream;
import java.nio.InvalidMarkException;

public class BufferInputStream extends InputStream {
    private CosNByteBuffer buffer;
    private boolean isClosed = true;

    public BufferInputStream(CosNByteBuffer buffer) throws IOException {
        if (null == buffer) {
            throw new IOException("byte buffer is null");
        }
        this.buffer = buffer;
        this.buffer.flipRead();
        this.isClosed = false;
    }

    @Override
    public synchronized int read() throws IOException {
        this.checkOpened();

        if (!this.buffer.hasRemaining()) {
            return -1;
        }
        return this.buffer.get() & 0xFF;
    }

    @Override
    public synchronized int read(byte[] b, int off, int len) throws IOException {
        this.checkOpened();

        if (!this.buffer.hasRemaining()) {
            return -1;
        }

        int readLength = Math.min(this.buffer.remaining(), len);
        this.buffer.get(b, off, readLength);
        return readLength;
    }

    @Override
    public synchronized void mark(int readLimit) {
        if (!this.markSupported()) {
            return;
        }
        this.buffer.mark();
        // Parameter readLimit is ignored
    }

    @Override
    public boolean markSupported() {
        return true;
    }

    @Override
    public synchronized void reset() throws IOException {
        this.checkOpened();

        try {
            this.buffer.reset();
        } catch (InvalidMarkException e) {
            throw new IOException("Invalid mark");
        }
    }

    @Override
    public synchronized int available() throws IOException {
        this.checkOpened();
        return this.buffer.remaining();
    }

    @Override
    public synchronized void close() throws IOException {
        this.isClosed = true;
        this.buffer = null;
    }

    private void checkOpened() throws IOException {
        if (this.isClosed) {
            throw new IOException(
                    String.format("The BufferInputStream[%d] has been closed", this.hashCode()));
        }
    }
}

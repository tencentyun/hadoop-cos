package org.apache.hadoop.fs.cosn;

import org.apache.hadoop.fs.cosn.buffer.CosNByteBuffer;

import java.io.IOException;
import java.io.OutputStream;

/**
 * The input stream class is used for buffered files.
 * The purpose of providing this class is to optimize buffer put performance.
 */
public class BufferOutputStream extends OutputStream {
    private CosNByteBuffer buffer;
    private boolean isFlush;
    private boolean isClosed;

    public BufferOutputStream(CosNByteBuffer buffer) throws IOException {
        if (null == buffer) {
            throw new IOException("buffer is null");
        }
        this.buffer = buffer;
        this.buffer.flipWrite();
        this.isFlush = false;
        this.isClosed = false;
    }

    @Override
    public synchronized void write(int b) throws IOException {
        this.checkOpened();

        if(this.buffer.remaining() == 0){
            throw new IOException("The buffer is full");
        }

        byte[] singleBytes = new byte[1];
        singleBytes[0] = (byte) b;
        this.buffer.put(singleBytes, 0, 1);
        this.isFlush = false;
    }

    @Override
    public synchronized void write(byte[] b, int off, int len) throws IOException {
        this.checkOpened();

        if (b == null) {
            throw new NullPointerException();
        } else if ((off < 0) || (off > b.length) || (len < 0) ||
                ((off + len) > b.length) || ((off + len) < 0)) {
            throw new IndexOutOfBoundsException();
        } else if (len == 0) {
            return;
        }

        this.buffer.put(b, off, len);
        this.isFlush = false;
    }

    @Override
    public synchronized void flush() throws IOException {
        this.checkOpened();

        if (this.isFlush) {
            return;
        }
        // TODO MappedByteBuffer can call the force method to flush.
        this.isFlush = true;
    }

    @Override
    public synchronized void close() throws IOException {
        if (this.isClosed) {
            return;
        }

        this.flush();

        this.isClosed = true;
        this.isFlush = true;
        this.buffer = null;
    }

    private void checkOpened() throws IOException {
        if (this.isClosed) {
            throw new IOException(
                    String.format("The BufferOutputStream[%d] has been closed.", this.hashCode()));
        }
    }
}

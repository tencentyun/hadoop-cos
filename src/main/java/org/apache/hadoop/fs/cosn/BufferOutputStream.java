package org.apache.hadoop.fs.cosn;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import org.apache.hadoop.fs.cosn.buffer.CosNByteBuffer;

/**
 * The input stream class is used for buffered files.
 * The purpose of providing this class is to optimize buffer put performance.
 */
public class BufferOutputStream extends OutputStream {
    private ByteBuffer byteBuffer;
    private boolean isFlush;
    private boolean isClosed;

    public BufferOutputStream(CosNByteBuffer buffer) throws IOException {
        if (null == buffer) {
            throw new IOException("buffer is null");
        }
        this.byteBuffer = buffer.getByteBuffer();
        this.byteBuffer.clear();
        this.isFlush = false;
        this.isClosed = false;
    }

    @Override
    public void write(int b) {
        byte[] singleBytes = new byte[1];
        singleBytes[0] = (byte) b;
        this.byteBuffer.put(singleBytes, 0, 1);
        this.isFlush = false;
    }

    @Override
    public void flush() {
        if (this.isFlush) {
            return;
        }
        this.isFlush = true;
    }

    @Override
    public void close() throws IOException {
        if (this.isClosed) {
            return;
        }
        if (null == this.byteBuffer) {
            throw new IOException("Can not close a null object");
        }

        this.flush();
        this.byteBuffer.flip();
        this.byteBuffer = null;
        this.isFlush = false;
        this.isClosed = true;
    }
}

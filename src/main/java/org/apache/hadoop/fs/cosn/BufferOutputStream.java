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
    public void write(int b) throws IOException {
        this.checkClosed();

        if(this.byteBuffer.remaining() == 0){
            throw new IOException("The buffer is full");
        }

        byte[] singleBytes = new byte[1];
        singleBytes[0] = (byte) b;
        this.byteBuffer.put(singleBytes, 0, 1);
        this.isFlush = false;
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        this.checkClosed();

        // 检查被写入的缓冲区
        if (b == null) {
            throw new NullPointerException();
        } else if ((off < 0) || (off > b.length) || (len < 0) ||
                ((off + len) > b.length) || ((off + len) < 0)) {
            throw new IndexOutOfBoundsException();
        } else if (len == 0) {
            return;
        }

        // 检查缓冲区是否还可以继续写
        if (this.byteBuffer.remaining() < len) {
            throw new IOException(
                    String.format("The buffer remaining[%d] is less than the write length[%d].",
                            this.byteBuffer.remaining(), len));
        }

        this.byteBuffer.put(b, off, len);
        this.isFlush = false;
    }

    @Override
    public void flush() throws IOException {
        this.checkClosed();

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

    private void checkClosed() throws IOException {
        if (this.isClosed) {
            throw new IOException(
                    String.format("The BufferOutputStream[%d] has been closed.", this.hashCode()));
        }
    }
}

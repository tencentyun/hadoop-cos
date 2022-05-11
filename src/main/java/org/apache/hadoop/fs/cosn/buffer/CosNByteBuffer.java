package org.apache.hadoop.fs.cosn.buffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * The base class for all CosN byte buffers.
 */
public abstract class CosNByteBuffer implements Closeable {
    private static final Logger LOG =
            LoggerFactory.getLogger(CosNByteBuffer.class);

    protected ByteBuffer byteBuffer;
    private int lastWritePosition;

    public CosNByteBuffer(ByteBuffer byteBuffer) {
        this.byteBuffer = byteBuffer;
        this.lastWritePosition = this.byteBuffer.position();
    }

    public CosNByteBuffer put(byte b) {
        this.byteBuffer.put(b);
        this.lastWritePosition = this.byteBuffer.position();
        return this;
    }

    public CosNByteBuffer put(byte[] src, int offset, int length) throws IOException {
        // 检查缓冲区是否还可以继续写
        if (this.byteBuffer.remaining() < length) {
            throw new IOException(
                    String.format("The buffer remaining[%d] is less than the write length[%d].",
                            this.byteBuffer.remaining(), length));
        }

        this.byteBuffer.put(src, offset, length);
        this.lastWritePosition = this.byteBuffer.position();
        return this;
    }

    public byte get() {
        return this.byteBuffer.get();
    }

    public CosNByteBuffer get(byte[] dst, int offset, int length) {
        this.byteBuffer.get(dst, offset, length);
        return this;
    }

    public int capacity() {
        return this.byteBuffer.capacity();
    }

    public int position() {
        return this.byteBuffer.position();
    }

    public CosNByteBuffer position(int newPosition) {
        this.byteBuffer.position(newPosition);
        return this;
    }

    public int limit() {
        return this.byteBuffer.limit();
    }

    public CosNByteBuffer limit(int newLimit) {
        this.byteBuffer.limit(newLimit);
        return this;
    }

    public CosNByteBuffer mark() {
        this.byteBuffer.mark();
        return this;
    }

    public CosNByteBuffer reset() {
        this.byteBuffer.reset();
        return this;
    }

    public CosNByteBuffer clear() {
        this.byteBuffer.clear();
        return this;
    }

    public CosNByteBuffer flip() {
        this.byteBuffer.flip();
        return this;
    }

    public CosNByteBuffer rewind() {
        this.byteBuffer.rewind();
        return this;
    }

    public CosNByteBuffer flipRead() {
        this.limit(this.lastWritePosition);
        this.position(0);
        return this;
    }

    public CosNByteBuffer flipWrite() {
        this.position(this.lastWritePosition);
        this.limit(this.byteBuffer.capacity());
        return this;
    }

    public int remaining() {
        return this.byteBuffer.remaining();
    }

    public boolean hasRemaining() {
        return this.byteBuffer.hasRemaining();
    }

    abstract boolean isDirect();

    abstract boolean isMapped();

    @Override
    public void close() throws IOException {
        if (null == this.byteBuffer) {
            return;
        }
        this.byteBuffer.clear();

        this.byteBuffer = null;
        this.lastWritePosition = -1;
    }
}

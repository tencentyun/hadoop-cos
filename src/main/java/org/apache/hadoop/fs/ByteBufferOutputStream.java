package org.apache.hadoop.fs;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class ByteBufferOutputStream extends OutputStream {
    private ByteBuffer byteBuffer;
    private boolean isFlush = false;
    private boolean isClosed = true;

    public ByteBufferOutputStream(ByteBuffer byteBuffer) throws IOException {
        if (null == byteBuffer) {
            throw new IOException("byte buffer is null");
        }
        this.byteBuffer = byteBuffer;
        this.byteBuffer.clear();
        this.isFlush = false;
        this.isClosed = false;
    }

    public ByteBufferOutputStream(byte[] buffer) throws IOException {
        this(ByteBuffer.wrap(buffer));
    }

    @Override
    public void write(int b) {
        byte[] singleBytes = new byte[1];
        singleBytes[0] = (byte) b;
        this.byteBuffer.put(singleBytes, 0, 1);
        this.isFlush = false;
    }

    @Override
    public void flush() throws IOException {
//        if (this.isClosed) {
//            throw new IOException("flush a closed stream");
//        }
//        if (this.isFlush) {
//            return;
//        }
////        super.flush();
//        this.isFlush = true;
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

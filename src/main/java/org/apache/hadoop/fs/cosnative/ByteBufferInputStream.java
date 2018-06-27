package org.apache.hadoop.fs.cosnative;

import org.apache.hadoop.fs.FSExceptionMessages;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public class ByteBufferInputStream extends InputStream {
    private final ByteBuffer byteBuffer;

    public ByteBufferInputStream(ByteBuffer byteBuffer) {
        this.byteBuffer = byteBuffer;
    }

    public ByteBufferInputStream(byte[] buffer){
        this(ByteBuffer.wrap(buffer));
    }

    @Override
    public int read() throws IOException {
        if(null == this.byteBuffer || !this.byteBuffer.hasRemaining()){
            throw new EOFException("The input stream has reached the eof.");
        }

        return this.byteBuffer.get() & 0xFF;
    }

    @Override
    public void close() throws IOException {
        this.byteBuffer.rewind();               // 可重复读取的流
    }
}

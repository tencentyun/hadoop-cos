package org.apache.hadoop.fs;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

public class ByteBufferWrapper {
    private ByteBuffer byteBuffer = null;
    private File file = null;
    private RandomAccessFile randomAccessFile = null;

    ByteBufferWrapper(ByteBuffer byteBuffer) {
        this(byteBuffer, null, null);         // 内存缓冲区
    }

    ByteBufferWrapper(ByteBuffer byteBuffer, RandomAccessFile randomAccessFile, File file) {
        // 磁盘缓冲区
        this.byteBuffer = byteBuffer;
        this.file = file;
        this.randomAccessFile = randomAccessFile;
    }

    public ByteBuffer getByteBuffer() {
        return this.byteBuffer;
    }

    boolean isDiskBuffer() {
        return this.file != null && this.randomAccessFile != null;
    }

    void close() throws IOException {
        if (null != this.byteBuffer) {
            this.byteBuffer.clear();
        }

        if (null != randomAccessFile) {
            this.randomAccessFile.close();
        }

        if (null != this.file && this.file.exists()) {
            this.file.delete();
        }
    }
}

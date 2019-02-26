package org.apache.hadoop.fs;

import sun.nio.ch.FileChannelImpl;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;

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
            this.randomAccessFile.getChannel().close();
            this.randomAccessFile.close();
        }

        if (this.byteBuffer instanceof MappedByteBuffer) {
            try {
                Method method = FileChannelImpl.class.getDeclaredMethod(
                        "unmap",
                        MappedByteBuffer.class);
                method.setAccessible(true);
                method.invoke(
                        FileChannelImpl.class,
                        (MappedByteBuffer) this.byteBuffer);
            } catch (NoSuchMethodException e) {
                throw new IOException(
                        "An exception occurred " +
                                "while releasing MappedByteBuffer, " +
                                "which may cause a memory leak.", e);
            } catch (IllegalAccessException e) {
                throw new IOException(
                        "An exception occurred " +
                                "while releasing MappedByteBuffer, " +
                                "which may cause a memory leak.", e);
            } catch (InvocationTargetException e) {
                throw new IOException(
                        "An exception occurred " +
                                "while releasing MappedByteBuffer, " +
                                "which may cause a memory leak.", e);
            }
        }

        if (null != this.file && this.file.exists()) {
            this.file.delete();
        }
    }
}

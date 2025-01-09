package org.apache.hadoop.fs.cosn.buffer;

import org.apache.hadoop.fs.cosn.buffer.CosNByteBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.nio.ch.FileChannelImpl;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;

/**
 * 专用于随机写的本地缓存
 */
public class CosNRandomAccessMappedBuffer extends CosNByteBuffer {
    private static final Logger LOG = LoggerFactory.getLogger(CosNRandomAccessMappedBuffer.class);

    // 有效的可读长度
    private final RandomAccessFile randomAccessFile;
    private final File file;

    private int maxReadablePosition;

    public CosNRandomAccessMappedBuffer(ByteBuffer byteBuffer,
                                        RandomAccessFile randomAccessFile, File file) {
        super(byteBuffer);
        this.randomAccessFile = randomAccessFile;
        this.file = file;
        this.maxReadablePosition = 0;
    }

    @Override
    public CosNByteBuffer put(byte b) throws IOException {
        super.put(b);
        this.maxReadablePosition = (Math.max(super.nextWritePosition, this.maxReadablePosition));
        return this;
    }

    @Override
    public CosNByteBuffer put(byte[] src, int offset, int length) throws IOException {
        super.put(src, offset, length);
        this.maxReadablePosition = (Math.max(super.nextWritePosition, this.maxReadablePosition));
        return this;
    }

    @Override
    public CosNByteBuffer flipRead() {
        super.limit(this.maxReadablePosition);
        super.position(0);
        return this;
    }

    public int getMaxReadablePosition() {
        return this.maxReadablePosition;
    }

    @Override
    public CosNByteBuffer clear() {
        super.clear();
        this.maxReadablePosition = 0;
        return this;
    }

    @Override
    public void close() throws IOException {
        IOException ioException = null;

        try {
            Method method = FileChannelImpl.class.getDeclaredMethod("unmap", MappedByteBuffer.class);
            method.setAccessible(true);
            method.invoke(FileChannelImpl.class, (MappedByteBuffer) super.byteBuffer);
        } catch (InvocationTargetException invocationTargetException) {
            LOG.error("Failed to invoke the reflect unmap method.", invocationTargetException);
            throw new IOException("Failed to release the mapped buffer.", invocationTargetException);
        } catch (NoSuchMethodException noSuchMethodException) {
            LOG.error("Failed to get the reflect unmap method.", noSuchMethodException);
            ioException = new IOException("Failed to release the mapped buffer.", noSuchMethodException);
        } catch (IllegalAccessException illegalAccessException) {
            LOG.error("Failed to access the reflect unmap method.", illegalAccessException);
            throw new IOException("Failed to release the mapped buffer.", illegalAccessException);
        }

        try {
            if (null != this.randomAccessFile) {
                this.randomAccessFile.close();
            }
        } catch (IOException randomAccessFileClosedException) {
            LOG.error("Failed to close the random access file.", randomAccessFileClosedException);
            ioException = randomAccessFileClosedException;
        }
        if (null != this.file && this.file.exists()) {
            if (!this.file.delete()) {
                LOG.warn("Failed to clean up the temporary file: [{}].",
                        this.file);
            }
        }

        try {
            super.close();
        } catch (IOException superClosedException) {
            // XXX exception chain of responsibility
            ioException = superClosedException;
        }

        if (null != ioException) {
            throw ioException;
        }
    }

    @Override
    protected boolean isDirect() {
        return true;
    }

    @Override
    protected boolean isMapped() {
        return true;
    }
}

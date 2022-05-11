package org.apache.hadoop.fs.cosn.buffer;

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
 * The buffer based on the memory file mapped.
 */
class CosNMappedBuffer extends CosNByteBuffer {
    private static final Logger LOG =
            LoggerFactory.getLogger(MappedByteBuffer.class);

    private File file;
    private RandomAccessFile randomAccessFile;

    public CosNMappedBuffer(ByteBuffer byteBuffer,
                            RandomAccessFile randomAccessFile, File file) {
        super(byteBuffer);
        this.randomAccessFile = randomAccessFile;
        this.file = file;
    }

    @Override
    boolean isDirect() {
        return true;
    }

    @Override
    boolean isMapped() {
        return true;
    }

    @Override
    public void close() throws IOException {
        IOException e = null;

        // unmap
        try {
            Method method = FileChannelImpl.class.getDeclaredMethod("unmap", MappedByteBuffer.class);
            method.setAccessible(true);
            method.invoke(FileChannelImpl.class, (MappedByteBuffer)super.byteBuffer);
        } catch (NoSuchMethodException noSuchMethodException) {
            LOG.error("Failed to get the reflect unmap method.", noSuchMethodException);
            e = new IOException("Failed to release the mapped buffer.", noSuchMethodException);
        } catch (InvocationTargetException invocationTargetException) {
            LOG.error("Failed to invoke the reflect unmap method.", invocationTargetException);
            throw new IOException("Failed to release the mapped buffer.", invocationTargetException);
        } catch (IllegalAccessException illegalAccessException) {
            LOG.error("Failed to access the reflect unmap method.", illegalAccessException);
            throw new IOException("Failed to release the mapped buffer.", illegalAccessException);
        }

        // Memory must be unmapped successfully before files can be deleted.
        // Close the random access file.
        try {
            if (null != this.randomAccessFile) {
                this.randomAccessFile.close();
            }
        } catch (IOException randomAccessFileClosedException) {
            LOG.error("Failed to close the random access file.", randomAccessFileClosedException);
            e = randomAccessFileClosedException;
        }

        // Delete the disk file to release the resource.
        if (null != this.file && this.file.exists()) {
            if (!this.file.delete()) {
                LOG.warn("Failed to clean up the temporary file: [{}].",
                        this.file);
            }
        }

        // Call super close to release the resource of the base class.
        try {
            super.close();
        } catch (IOException superClosedException) {
            // XXX exception chain of responsibility
            e = superClosedException;
        }

        // Finally, throw the error that occurred in the process.
        if (null != e) {
            throw e;
        }
    }
}

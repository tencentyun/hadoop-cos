package org.apache.hadoop.fs.cosn.buffer;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    boolean isMapped() {
        return true;
    }

    @Override
    public void close() throws IOException {
        super.close();

        IOException e = null;
        try {
            // Close the random access file.
            if (null != this.randomAccessFile) {
                this.randomAccessFile.close();
            }
        } catch (IOException e1) {
            LOG.error("Close the random access file failed.", e1);
            e = e1;
        }

        // Delete the disk file to release the resource.
        if (null != this.file && this.file.exists()) {
            if (!this.file.delete()) {
                LOG.warn("Failed to clean up the temporary file: [{}].",
                        this.file);
            }
        }

        // Finally, throw the error that occurred in the process.
        if (null != e) {
            throw e;
        }
    }
}

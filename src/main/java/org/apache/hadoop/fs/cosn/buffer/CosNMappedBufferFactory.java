package org.apache.hadoop.fs.cosn.buffer;

import org.apache.hadoop.fs.cosn.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public class CosNMappedBufferFactory implements CosNBufferFactory {
    private static final Logger LOG =
            LoggerFactory.getLogger(CosNMappedBufferFactory.class);

    private final File tmpDir;

    public CosNMappedBufferFactory(String tmpDir) throws IOException {
        this.tmpDir = CosNMappedBufferFactory.createDir(tmpDir);
    }

    private static File createDir(String tmpDir) throws IOException {
        File file = new File(tmpDir);
        if (!file.exists()) {
            LOG.debug("Buffer dir: [{}] does not exists. create it first.",
                    file);
            if (file.mkdirs()) {
                if (!file.setWritable(true) || !file.setReadable(true)
                        || !file.setExecutable(true)) {
                    LOG.warn("Set the buffer dir: [{}]'s permission [writable,"
                            + "readable, executable] failed.", file);
                }
                LOG.debug("Buffer dir: [{}] is created successfully.",
                        file.getAbsolutePath());
            } else {
                // Once again, check if it has been created successfully.
                // Prevent problems created by multiple processes at the same
                // time.
                if (!file.exists()) {
                    throw new IOException("buffer dir:" + file.getAbsolutePath()
                            + " is created unsuccessfully");
                }
            }
        } else {
            LOG.debug("buffer dir: {} already exists.",
                    file.getAbsolutePath());
        }

        return file;
    }

    @Override
    public CosNByteBuffer create(int size) {
        if (null == this.tmpDir) {
            LOG.error("The tmp dir is null. no mapped buffer will be created.");
            return null;
        }

        if (!this.tmpDir.exists()) {
            LOG.error("The tmp dir does not exist.");
        }

        try {
            File tmpFile = File.createTempFile(
                    Constants.BLOCK_TMP_FILE_PREFIX,
                    Constants.BLOCK_TMP_FILE_SUFFIX,
                    this.tmpDir
            );
            tmpFile.deleteOnExit();
            RandomAccessFile randomAccessFile = new RandomAccessFile(tmpFile,
                    "rw");
            randomAccessFile.setLength(size);
            MappedByteBuffer buf =
                    randomAccessFile.getChannel().map(FileChannel.MapMode.READ_WRITE, 0 , size);
            return new CosNMappedBuffer(buf, randomAccessFile, tmpFile);
        } catch (IOException e) {
            LOG.error("Create tmp file failed. Tmp dir: {}", this.tmpDir, e);
            return null;
        }
    }

    @Override
    public void release(CosNByteBuffer cosNByteBuffer) {
        if (null == cosNByteBuffer) {
            LOG.debug("The buffer returned is null. Ignore it.");
            return;
        }

        try {
            cosNByteBuffer.close();
        } catch (IOException e) {
            LOG.error("Release the mapped byte buffer failed.", e);
        }
    }
}

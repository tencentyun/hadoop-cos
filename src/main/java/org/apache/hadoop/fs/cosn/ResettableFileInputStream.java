package org.apache.hadoop.fs.cosn;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

public class ResettableFileInputStream
        extends InputStream {
    private static final int DEFAULT_BUFFER_SIZE = 1024;

    private final String fileName;
    private int bufferSize;
    private InputStream inputStream;
    private long position;
    private long mark;
    private boolean isMarkSet;

    public ResettableFileInputStream(final File file)
            throws IOException {
        this(file.getCanonicalPath());
    }

    public ResettableFileInputStream(final String filename)
            throws IOException {
        this(filename, DEFAULT_BUFFER_SIZE);
    }

    public ResettableFileInputStream(final String filename, final int bufferSize)
            throws IOException {
        this.bufferSize = bufferSize;
        fileName = filename;
        position = 0;

        inputStream = newStream();
    }

    public void mark(final int readLimit) {
        isMarkSet = true;
        mark = position;
        inputStream.mark(readLimit);
    }

    public boolean markSupported() {
        return true;
    }

    public void reset()
            throws IOException {
        if (!isMarkSet) {
            throw new IOException("Unmarked Stream");
        }
        try {
            inputStream.reset();
        } catch (final IOException ioe) {
            try {
                inputStream.close();
                inputStream = newStream();
                inputStream.skip(mark);
                position = mark;
            } catch (final Exception e) {
                throw new IOException("Cannot reset current Stream: " + e.getMessage());
            }
        }
    }

    protected InputStream newStream()
            throws IOException {
        return new BufferedInputStream(new FileInputStream(fileName), bufferSize);
    }

    public int available()
            throws IOException {
        return inputStream.available();
    }

    public void close() throws IOException {
        inputStream.close();
    }

    public int read() throws IOException {
        position++;
        return inputStream.read();
    }

    public int read(final byte[] bytes, final int offset, final int length)
            throws IOException {
        final int count = inputStream.read(bytes, offset, length);
        position += count;
        return count;
    }

    public long skip(final long count)
            throws IOException {
        position += count;
        return inputStream.skip(count);
    }
}
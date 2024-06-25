package org.apache.hadoop.fs;

import java.io.IOException;
import java.nio.ByteBuffer;

public class CosNFSBufferedFSInputStream extends BufferedFSInputStream implements ByteBufferReadable {

    public CosNFSBufferedFSInputStream(FSInputStream in, int size) {
        super(in, size);
    }

    @Override
    public int read(ByteBuffer byteBuffer) throws IOException {
        final int bufSize = 1024;
        byte[] buf = new byte[bufSize];
        int totalRead = 0;
        while (byteBuffer.hasRemaining()) {
            int maxReadSize = Math.min(bufSize, byteBuffer.remaining());
            int readLen = this.read(buf, 0, maxReadSize);
            if (readLen <= 0) {
                return totalRead == 0 ? -1 : totalRead;
            }
            byteBuffer.put(buf, 0, readLen);
            totalRead += readLen;
        }
        return totalRead;
    }
}

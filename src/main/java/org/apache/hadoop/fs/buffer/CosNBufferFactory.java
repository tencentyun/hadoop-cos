package org.apache.hadoop.fs.buffer;

public interface CosNBufferFactory {
    CosNByteBuffer create(int size);

    void release(CosNByteBuffer cosNByteBuffer);
}

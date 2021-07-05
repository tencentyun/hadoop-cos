package org.apache.hadoop.fs.cosn.buffer;

public interface CosNBufferFactory {
    CosNByteBuffer create(int size);

    void release(CosNByteBuffer cosNByteBuffer);
}

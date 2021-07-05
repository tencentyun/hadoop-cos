package org.apache.hadoop.fs.cosn;

import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigInteger;

/**
 * An etag as a checksum.
 * Consider these suitable for checking if an object has changed, but
 * not suitable for comparing two different objects for equivalence,
 * especially between hadoop compatible filesystem.
 */
public class CRC64Checksum extends FileChecksum {
    private static final String ALGORITHM_NAME = "CRC64";

    private long crc64 = 0;

    public CRC64Checksum() {
    }

    public CRC64Checksum(String crc64ecma) {
        try {
            BigInteger bigInteger = new BigInteger(crc64ecma);
            this.crc64 = bigInteger.longValue();
        } catch (NumberFormatException e) {
            this.crc64 = 0;
        }
    }

    @Override
    public String getAlgorithmName() {
        return CRC64Checksum.ALGORITHM_NAME;
    }

    @Override
    public int getLength() {
        return Long.SIZE / Byte.SIZE;
    }

    @Override
    public byte[] getBytes() {
        return this.crc64 != 0 ? WritableUtils.toByteArray(this) : new byte[0];
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(this.crc64);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.crc64 = dataInput.readLong();
    }

    @Override
    public String toString() {
        return "CRC64Checksum{" +
                "crc64=" + crc64 +
                '}';
    }
}

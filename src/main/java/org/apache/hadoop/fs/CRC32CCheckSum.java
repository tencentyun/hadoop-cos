package org.apache.hadoop.fs;

import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.util.StringUtils;

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
public class CRC32CCheckSum extends FileChecksum {
    private static final String ALGORITHM_NAME = "CRC32C";

    private long crc32c = 0;

    public CRC32CCheckSum() {
    }

    public CRC32CCheckSum(String crc32cecma) {
        try {
            BigInteger bigInteger = new BigInteger(crc32cecma);
            this.crc32c = bigInteger.longValue();
        } catch (NumberFormatException e) {
            this.crc32c = 0;
        }
    }

    @Override
    public String getAlgorithmName() {
        return CRC32CCheckSum.ALGORITHM_NAME;
    }

    @Override
    public int getLength() {
        return Long.SIZE / Byte.SIZE;
    }

    @Override
    public byte[] getBytes() {
        return this.crc32c != 0 ? WritableUtils.toByteArray(this) : new byte[0];
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(this.crc32c);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.crc32c = dataInput.readLong();
    }

    @Override
    public String toString() {
        return "CRC32CChecksum{" +
                "crc32c=" + crc32c +
                '}';
    }
}

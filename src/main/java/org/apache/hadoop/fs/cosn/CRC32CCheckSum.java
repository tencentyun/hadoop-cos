package org.apache.hadoop.fs.cosn;

import org.apache.hadoop.fs.FileChecksum;

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
    private static final String ALGORITHM_NAME = "COMPOSITE-CRC32C";

    private int crc32c = 0;

    public CRC32CCheckSum() {
    }


    public CRC32CCheckSum(String crc32cecma) {
        try {
            BigInteger bigInteger = new BigInteger(crc32cecma);
            this.crc32c = bigInteger.intValue();
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
        return Integer.SIZE / Byte.SIZE;
    }

    @Override
    public byte[] getBytes() {
        return CrcUtil.intToBytes(crc32c);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(this.crc32c);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.crc32c = dataInput.readInt();
    }

    @Override
    public String toString() {
        return getAlgorithmName() + ":" + String.format("0x%08x", crc32c);
    }
}

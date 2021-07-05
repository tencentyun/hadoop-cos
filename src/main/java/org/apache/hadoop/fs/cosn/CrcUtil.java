package org.apache.hadoop.fs.cosn;

import java.io.IOException;

public class CrcUtil {
    private CrcUtil() {
    }

    /**
     * int turn to bytes
     * @return 4-byte array holding the big-endian representation of
     * {@code value}.
     */
    public static byte[] intToBytes(int value) {
        byte[] buf = new byte[4];
        try {
            writeInt(buf, 0, value);
        } catch (IOException ioe) {
            // Since this should only be able to occur from code bugs within this
            // class rather than user input, we throw as a RuntimeException
            // rather than requiring this method to declare throwing IOException
            // for something the caller can't control.
            throw new RuntimeException(ioe);
        }
        return buf;
    }

    /**
     * Writes big-endian representation of {@code value} into {@code buf}
     * starting at {@code offset}. buf.length must be greater than or
     * equal to offset + 4.
     */
    public static void writeInt(byte[] buf, int offset, int value)
            throws IOException {
        if (offset + 4 > buf.length) {
            throw new IOException(String.format(
                    "writeInt out of bounds: buf.length=%d, offset=%d",
                    buf.length, offset));
        }
        buf[offset + 0] = (byte) ((value >>> 24) & 0xff);
        buf[offset + 1] = (byte) ((value >>> 16) & 0xff);
        buf[offset + 2] = (byte) ((value >>> 8) & 0xff);
        buf[offset + 3] = (byte) (value & 0xff);
    }

}

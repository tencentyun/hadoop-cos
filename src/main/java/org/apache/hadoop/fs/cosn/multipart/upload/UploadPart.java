package org.apache.hadoop.fs.cosn.multipart.upload;

import com.qcloud.cos.thirdparty.org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.fs.cosn.MD5Utils;
import org.apache.hadoop.fs.cosn.buffer.CosNByteBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;

public final class UploadPart {
    private static final Logger LOG = LoggerFactory.getLogger(UploadPart.class);
    private final int partNumber;
    private final CosNByteBuffer cosNByteBuffer;
    private final byte[] md5Hash;
    private final boolean isLast;

    public UploadPart(int partNumber, CosNByteBuffer cosNByteBuffer) {
        this(partNumber, cosNByteBuffer, false);
    }

    public UploadPart(int partNumber, CosNByteBuffer cosNByteBuffer, boolean isLast) {
        this.partNumber = partNumber;
        this.cosNByteBuffer = cosNByteBuffer;
        // 计算MD5
        byte[] md5Hash = null;
        try {
            md5Hash = MD5Utils.calculate(cosNByteBuffer);
        } catch (NoSuchAlgorithmException | IOException exception) {
            LOG.warn("Failed to calculate the md5Hash for the part [{}].",
                    partNumber, exception);
        }
        this.md5Hash = md5Hash;
        this.isLast = isLast;
    }

    public UploadPart(int partNumber, CosNByteBuffer cosNByteBuffer, byte[] md5Hash, boolean isLast) {
        this.partNumber = partNumber;
        this.cosNByteBuffer = cosNByteBuffer;
        this.md5Hash = md5Hash;
        this.isLast = isLast;
    }

    public int getPartNumber() {
        return this.partNumber;
    }

    public CosNByteBuffer getCosNByteBuffer() {
        return this.cosNByteBuffer;
    }

    public long getPartSize() {
        return this.cosNByteBuffer.remaining();
    }

    public byte[] getMd5Hash() {
        return this.md5Hash;
    }

    public boolean isLast() {
        return isLast;
    }

    @Override
    public String toString() {
        return String.format("UploadPart{partNumber:%d, partSize: %d, md5Hash: %s, isLast: %b}", this.partNumber, this.cosNByteBuffer.flipRead().remaining(), (this.md5Hash != null ? Hex.encodeHexString(this.md5Hash) : "NULL"), this.isLast);
    }
}

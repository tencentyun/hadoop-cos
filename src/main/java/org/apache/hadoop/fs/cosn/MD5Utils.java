package org.apache.hadoop.fs.cosn;

import org.apache.hadoop.fs.cosn.buffer.CosNByteBuffer;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public final class MD5Utils {
  public static byte[] calculate(CosNByteBuffer buffer)
      throws NoSuchAlgorithmException, IOException {
    if (null == buffer) {
      return null;
    }

    MessageDigest md5 = MessageDigest.getInstance("MD5");
    InputStream inputStream = new DigestInputStream(new BufferInputStream(buffer), md5);
    byte[] chunk = new byte[(int) (4 * Unit.KB)];
    while(inputStream.read(chunk) != -1);
    return md5.digest();
  }

  public static byte[] calculate(String str) throws NoSuchAlgorithmException {
    if (null == str) {
      return null;
    }
    MessageDigest md5 = MessageDigest.getInstance("MD5");
    return md5.digest(str.getBytes(StandardCharsets.UTF_8));
  }
}

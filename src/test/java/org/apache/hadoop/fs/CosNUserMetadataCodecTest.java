package org.apache.hadoop.fs;

import com.qcloud.cos.utils.Base64;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Unit tests of the decoding of the user-defined metadata (xattr, owner/group, etc.).
 * The value is base64 encoded by this store, but an object may also carry a plain text value
 * written by other tools, so both of them have to be supported.
 */
public class CosNUserMetadataCodecTest {

    @Test
    public void testDecodeNullAndEmptyValue() {
        assertNull(CosNativeFileSystemStore.decodeUserMetadataValue(null));
        assertEquals(0, CosNativeFileSystemStore.decodeUserMetadataValue("").length);
    }

    @Test
    public void testDecodeBase64Value() {
        // 写入侧统一用 Base64.encodeAsString 编码，这里必须能原样解回来。
        assertArrayEquals("hello".getBytes(StandardCharsets.UTF_8),
                CosNativeFileSystemStore.decodeUserMetadataValue(Base64.encodeAsString(
                        "hello".getBytes(StandardCharsets.UTF_8))));
        // 二进制的 xattr 值也不能被破坏。
        assertArrayEquals(new byte[]{0, 1, 127, -128},
                CosNativeFileSystemStore.decodeUserMetadataValue(
                        Base64.encodeAsString(new byte[]{0, 1, 127, -128})));
    }

    @Test
    public void testDecodeBase64ValueOfAnyLength() {
        byte[] origin = new byte[256];
        for (int i = 0; i < origin.length; i++) {
            origin[i] = (byte) i;
        }
        for (int length = 1; length <= origin.length; length++) {
            byte[] value = Arrays.copyOf(origin, length);
            assertArrayEquals("length: " + length, value,
                    CosNativeFileSystemStore.decodeUserMetadataValue(Base64.encodeAsString(value)));
        }
    }

    @Test
    public void testDecodePlainTextValue() {
        // 这些都不是合法的 base64 串（长度不是 4 的倍数或含非字母表字符），按明文处理。
        assertPlainText("hadoop");
        assertPlainText("12345");
        assertPlainText("hello world");
        assertPlainText("user@host");
        assertPlainText("中文属主");
    }

    @Test
    public void testDecodeKeepsTheOriginalBytes() {
        // 空值以外的所有明文都按 UTF-8 原样返回，不做任何解码。
        assertArrayEquals("plain-value".getBytes(StandardCharsets.UTF_8),
                CosNativeFileSystemStore.decodeUserMetadataValue("plain-value"));
    }

    private void assertPlainText(String value) {
        assertArrayEquals(value, value.getBytes(StandardCharsets.UTF_8),
                CosNativeFileSystemStore.decodeUserMetadataValue(value));
    }
}

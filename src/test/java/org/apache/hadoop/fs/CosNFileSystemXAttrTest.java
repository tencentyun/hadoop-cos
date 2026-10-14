package org.apache.hadoop.fs;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.junit.Test;

/**
 * Unit tests of the size check of an extended attribute, which is shared by
 * {@link CosNFileSystem#setXAttr(Path, String, byte[], java.util.EnumSet)} and
 * {@link CosNFileSystemExt#setXAttrs(Path, java.util.Map)}.
 */
public class CosNFileSystemXAttrTest {

    @Test
    public void testSizeWithinTheLimit() {
        // name 与 value 的字节数加起来不超过 1024 就允许写入。
        CosNFileSystem.checkXAttrSize("name", new byte[CosNFileSystem.MAX_XATTR_SIZE - "name".length()]);
    }

    @Test(expected = HadoopIllegalArgumentException.class)
    public void testSizeExceedsTheLimit() {
        CosNFileSystem.checkXAttrSize("name", new byte[CosNFileSystem.MAX_XATTR_SIZE]);
    }

    @Test(expected = HadoopIllegalArgumentException.class)
    public void testLongNameExceedsTheLimit() {
        StringBuilder longName = new StringBuilder();
        for (int i = 0; i < CosNFileSystem.MAX_XATTR_SIZE; i++) {
            longName.append('a');
        }
        CosNFileSystem.checkXAttrSize(longName.toString(), new byte[1]);
    }
}

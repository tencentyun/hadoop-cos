package org.apache.hadoop.fs;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Unit tests of {@link CosNFileSystemExt#setXAttrs(Path, Map)}.
 * <p>
 * Only the checks done before the filesystem is touched are covered here: the attributes are
 * validated one by one before any request is sent, so these cases can be tested without the
 * credentials of a real bucket. The cases that really persist the attributes are covered by
 * the integration tests.
 */
public class CosNFileSystemExtTest {

    private final CosNFileSystemExt fs = new CosNFileSystemExt();
    private final Path testPath = new Path("/unittest-dir/set-xattrs");

    @Test
    public void testNothingToDoWhenNoAttributeIsGiven() throws IOException {
        // 空入参在访问后端之前就返回了，不能因为文件系统没有初始化而抛异常。
        this.fs.setXAttrs(this.testPath, null);
        this.fs.setXAttrs(this.testPath, new HashMap<String, byte[]>());
    }

    @Test(expected = HadoopIllegalArgumentException.class)
    public void testEmptyAttributeNameIsRejected() throws IOException {
        this.fs.setXAttrs(this.testPath, attributes("", new byte[]{1}));
    }

    @Test(expected = HadoopIllegalArgumentException.class)
    public void testAttributeSizeExceedsTheLimit() throws IOException {
        // name 与 value 的字节数加起来不能超过 1024。
        this.fs.setXAttrs(this.testPath,
                attributes("attr", new byte[CosNFileSystem.MAX_XATTR_SIZE]));
    }

    @Test(expected = HadoopIllegalArgumentException.class)
    public void testAnyAttributeSizeExceedsTheLimit() throws IOException {
        // 批量设置时，只要有一个属性超限，整批都要拒绝。
        Map<String, byte[]> attributes = attributes("attr-1", new byte[]{1});
        attributes.put("attr-2", new byte[CosNFileSystem.MAX_XATTR_SIZE]);
        this.fs.setXAttrs(this.testPath, attributes);
    }

    private Map<String, byte[]> attributes(String name, byte[] value) {
        Map<String, byte[]> attributes = new HashMap<>();
        attributes.put(name, value);
        return attributes;
    }
}

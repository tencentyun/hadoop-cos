package org.apache.hadoop.fs;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.impl.OpenFileParameters;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Unit tests of the delegation of {@link CosFileSystemExt}.
 * <p>
 * Only the checks done before the filesystem is touched are covered here, so no credentials of a
 * real bucket are needed. The cases that really persist the attributes are covered by
 * {@code ITestCosNFileSystemCreateWithXAttrs} and {@code ITestCosNFileSystemSetXAttrs}.
 */
public class CosFileSystemExtTest {

    private final CosFileSystemExt fs = new CosFileSystemExt();
    private final Path testPath = new Path("/unittest-dir/ext");

    {
        // The builder of createFile reads the default options from the configuration of the filesystem,
        // and the block size from the actual implementation this filesystem delegates to.
        Configuration conf = new Configuration(false);
        this.fs.setConf(conf);
        CosNFileSystemExt delegate = new CosNFileSystemExt();
        delegate.setConf(conf);
        this.fs.actualImplFS = delegate;
    }

    @Test
    public void testCreateXAttrCapability() throws IOException {
        assertTrue(this.fs.hasPathCapability(this.testPath, CosNFileSystemExt.CAPABILITY_CREATE_XATTR));
    }

    @Test
    public void testCreateFileReturnsTheCosBuilder() {
        // 外层返回的也是 CosN 的 builder，否则 must 选项会被 Hadoop 的默认 builder 拒绝。
        CosNFileSystemExt.CosNCreateFileBuilder builder = this.fs.createFile(this.testPath);
        assertNotNull(builder);
    }

    @Test
    public void testNothingToDoWhenNoAttributeIsGiven() throws IOException {
        this.fs.setXAttrs(this.testPath, null);
        this.fs.setXAttrs(this.testPath, new HashMap<String, byte[]>());
    }

    @Test
    public void testEmptyAttributeNameIsRejected() throws IOException {
        // 名字校验在访问后端之前完成，未初始化的文件系统也能测。
        Map<String, byte[]> attributes = new HashMap<>();
        attributes.put("", new byte[]{1});
        try {
            this.fs.setXAttrs(this.testPath, attributes);
            fail("An empty attribute name is expected to be rejected.");
        } catch (HadoopIllegalArgumentException expected) {
            // expected
        }
    }
}

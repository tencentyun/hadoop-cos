package org.apache.hadoop.fs;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests of {@link CosNFileSystemExt#setXAttrs(Path, Map)} and {@link CosNFileSystemExt#createFile(Path)}.
 * <p>
 * Only the checks done before the filesystem is touched are covered here: the attributes are
 * validated before any request is sent, so these cases can be tested without the credentials of
 * a real bucket. The cases that really persist the attributes are covered by the integration tests.
 */
public class CosNFileSystemExtTest {

    private final CosNFileSystemExt fs = new CosNFileSystemExt();
    private final Path testPath = new Path("/unittest-dir/set-xattrs");

    {
        // The builder of createFile reads the default options from the configuration of the filesystem.
        this.fs.setConf(new Configuration(false));
    }

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

    @Test
    public void testCreateXAttrCapability() throws IOException {
        assertTrue(this.fs.hasPathCapability(this.testPath, CosNFileSystemExt.CAPABILITY_CREATE_XATTR));
    }

    @Test
    public void testCreateFileReturnsTheCosBuilder() {
        assertNotNull(this.fs.createFile(this.testPath));
    }

    @Test
    public void testWithOwnerReturnsTheSameBuilder() {
        CosNFileSystemExt.CosNCreateFileBuilder builder = this.fs.createFile(this.testPath);
        assertSame(builder, builder.setOwner("alice", "staff"));
        // 允许只给其中一个，也允许链式地分多次给。
        assertSame(builder, builder.setOwner("alice", null));
        assertSame(builder, builder.setOwner(null, "staff"));
        assertSame(builder, builder.setOwner(null, null));
    }

    @Test(expected = HadoopIllegalArgumentException.class)
    public void testWithOwnerRejectsEmptyUser() {
        this.fs.createFile(this.testPath).setOwner("", "staff");
    }

    @Test(expected = HadoopIllegalArgumentException.class)
    public void testWithOwnerRejectsEmptyGroup() {
        this.fs.createFile(this.testPath).setOwner("alice", "");
    }

    @Test(expected = HadoopIllegalArgumentException.class)
    public void testEmptyOptionNameIsRejected() throws IOException {
        Configuration options = new Configuration(false);
        options.set("", "value");
        CosNFileSystemExt.xattrsFromOptions(options);
    }

    @Test
    public void testEveryOptionIsOneAttribute() {
        // 选项名直接就是属性名，不再需要任何前缀。
        Configuration options = new Configuration(false);
        options.set("owner-tag", "abc");
        options.set("user.checksum", "123");

        Map<String, byte[]> xattrs = CosNFileSystemExt.xattrsFromOptions(options);
        assertEquals(2, xattrs.size());
        assertArrayEquals("abc".getBytes(CosNFileSystem.METADATA_ENCODING), xattrs.get("owner-tag"));
        assertArrayEquals("123".getBytes(CosNFileSystem.METADATA_ENCODING), xattrs.get("user.checksum"));
    }

    @Test
    public void testValuesAreNotVariableSubstituted() {
        // Configuration 默认会替换 ${...}，属性值必须原样保留。
        Configuration options = new Configuration(false);
        options.set("template", "${user.name}-suffix");

        Map<String, byte[]> xattrs = CosNFileSystemExt.xattrsFromOptions(options);
        assertArrayEquals("${user.name}-suffix".getBytes(CosNFileSystem.METADATA_ENCODING),
                xattrs.get("template"));
    }

    @Test
    public void testNoOptionMeansNoAttribute() {
        assertTrue(CosNFileSystemExt.xattrsFromOptions(new Configuration(false)).isEmpty());
        assertTrue(CosNFileSystemExt.xattrsFromOptions(null).isEmpty());
    }

    private Map<String, byte[]> attributes(String name, byte[] value) {
        Map<String, byte[]> attributes = new HashMap<>();
        attributes.put(name, value);
        return attributes;
    }
}

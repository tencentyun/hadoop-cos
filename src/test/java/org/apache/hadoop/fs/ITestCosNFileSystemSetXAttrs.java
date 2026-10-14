package org.apache.hadoop.fs;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Integration tests of {@link CosNFileSystemExt#setXAttrs(Path, Map)}.
 * The attributes are really persisted to the bucket, so the credentials in the core-site.xml
 * are required.
 */
public class ITestCosNFileSystemSetXAttrs extends CosNFileSystemTestBase {

	private Path testPath;
	private CosNFileSystemExt extFs;

	@Before
	public void before() throws IOException {
		super.before();
		testPath = methodPath();
		// 批量设置的接口在 CosNFileSystemExt 上，测试基类里的 fs 是 CosNFileSystem。
		extFs = new CosNFileSystemExt();
		extFs.initialize(fs.getUri(), configuration);
	}

	@After
	public void after() throws IOException {
		if (null != extFs) {
			extFs.close();
			extFs = null;
		}
		super.after();
	}

	@Test
	public void testSetXAttrsOnFile() throws Throwable {
		createBaseFileWithData(10, testPath);

		Map<String, byte[]> attributes = new LinkedHashMap<>();
		attributes.put("batch-attr-1", new byte[]{1, 2, 3});
		attributes.put("batch-attr-2", "hello".getBytes());
		extFs.setXAttrs(testPath, attributes);

		assertArrayEquals(new byte[]{1, 2, 3}, fs.getXAttr(testPath, "batch-attr-1"));
		assertArrayEquals("hello".getBytes(), fs.getXAttr(testPath, "batch-attr-2"));
	}

	@Test
	public void testSetXAttrsKeepsTheExistingAttribute() throws Throwable {
		createBaseFileWithData(10, testPath);
		fs.setXAttr(testPath, "old-attr", new byte[]{9});

		Map<String, byte[]> attributes = new LinkedHashMap<>();
		attributes.put("new-attr", new byte[]{8});
		extFs.setXAttrs(testPath, attributes);

		// 不在本次入参里的属性不能被覆盖丢掉。
		assertArrayEquals(new byte[]{9}, fs.getXAttr(testPath, "old-attr"));
		assertArrayEquals(new byte[]{8}, fs.getXAttr(testPath, "new-attr"));
	}

	@Test
	public void testSetXAttrsOverwritesTheExistingValue() throws Throwable {
		createBaseFileWithData(10, testPath);

		Map<String, byte[]> attributes = new LinkedHashMap<>();
		attributes.put("attr", new byte[]{1});
		extFs.setXAttrs(testPath, attributes);

		Map<String, byte[]> updates = new LinkedHashMap<>();
		updates.put("attr", new byte[]{2});
		extFs.setXAttrs(testPath, updates);

		assertArrayEquals(new byte[]{2}, fs.getXAttr(testPath, "attr"));
	}

	@Test
	public void testSetXAttrsRemovesTheAttributeByNullValue() throws Throwable {
		createBaseFileWithData(10, testPath);

		Map<String, byte[]> attributes = new LinkedHashMap<>();
		attributes.put("attr-to-keep", new byte[]{1});
		attributes.put("attr-to-remove", new byte[]{2});
		extFs.setXAttrs(testPath, attributes);

		Map<String, byte[]> updates = new LinkedHashMap<>();
		updates.put("attr-to-remove", null);
		extFs.setXAttrs(testPath, updates);

		assertArrayEquals(new byte[]{1}, fs.getXAttr(testPath, "attr-to-keep"));
		assertNull(fs.getXAttr(testPath, "attr-to-remove"));
	}

	@Test
	public void testSetXAttrsOnDirectory() throws Throwable {
		fs.mkdirs(testPath);

		Map<String, byte[]> attributes = new LinkedHashMap<>();
		attributes.put("dir-attr", new byte[]{7});
		extFs.setXAttrs(testPath, attributes);

		assertTrue(fs.getFileStatus(testPath).isDirectory());
		assertArrayEquals(new byte[]{7}, fs.getXAttr(testPath, "dir-attr"));
	}
}

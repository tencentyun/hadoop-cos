package org.apache.hadoop.fs;

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

public class ITestCosNFileSystemSetAndGetXAttr extends CosNFileSystemTestBase {

	private Path testPath;

	@Before
	public void before() throws IOException {
		super.before();
		testPath = methodPath();
	}

	@Test
	public void testSetAndGetXAttr() throws Throwable {
		createBaseFileWithData(10, testPath);
		String attrName = "test";
		byte[] attrValue = new byte[] { 1, 2, 3 };
		fs.setXAttr(testPath, attrName, attrValue);
		assertEquals(Arrays.toString(attrValue), Arrays.toString(fs.getXAttr(testPath, attrName)));
	}

	@Test
	public void testSetXAttrAndRename() throws Throwable {
		// 创建源文件并设置 xattr
		createBaseFileWithData(10, testPath);
		String attrName = "test";
		byte[] attrValue = new byte[] { 4, 5, 6 };
		fs.setXAttr(testPath, attrName, attrValue);

		// rename 到新路径
		Path renamedPath = new Path(testPath.getParent(), testPath.getName() + "-renamed");
		assertTrue(fs.rename(testPath, renamedPath));

		// 校验 rename 后 xattr 不丢失
		assertEquals(Arrays.toString(attrValue), Arrays.toString(fs.getXAttr(renamedPath, attrName)));

		// 清理
		fs.delete(renamedPath, false);
	}
}

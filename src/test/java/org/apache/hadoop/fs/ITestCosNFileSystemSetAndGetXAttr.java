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
}

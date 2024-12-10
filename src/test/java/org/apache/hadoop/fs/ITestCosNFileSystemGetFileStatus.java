package org.apache.hadoop.fs;

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class ITestCosNFileSystemGetFileStatus extends CosNFileSystemTestBase {

	private Path testPath;

	@Before
	public void before() throws IOException {
		super.before();
		testPath = methodPath();
	}

	@Test
	public void testGetFileStatus() throws Throwable {
		createBaseFileWithData(10, testPath);
		FileStatus status = fs.getFileStatus(testPath);
		assertEquals(10, status.getLen());
		assertTrue(status.isFile());
	}

	@Test
	public void testGetDirStatus() throws Throwable {
		fs.mkdirs(testPath);
		Path child = new Path(testPath, "child");
		createBaseFileWithData(10, child);
		FileStatus status = fs.getFileStatus(testPath);
		assertTrue(status.isDirectory());
	}
}

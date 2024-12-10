package org.apache.hadoop.fs;

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class ITestCosNFileSystemListStatus extends CosNFileSystemTestBase {

	private Path testPath;

	@Before
	public void before() throws IOException {
		super.before();
		testPath = methodPath();
	}

	@Test
	public void testSingleFileListStatus() throws Throwable {
		createBaseFileWithData(10, testPath);
		FileStatus[] res = fs.listStatus(testPath);
		assertEquals(1, res.length);
		FileStatus fileStatus = fs.getFileStatus(testPath);
		// HCFS: the contents of a FileStatus of a child retrieved via listStatus() are equal to
		// those from a call of getFileStatus() to the same path.
		assertEquals(fileStatus, res[0]);
	}

	@Test
	public void testMultipleFileListStatus() throws Throwable {
		fs.mkdirs(testPath);
		Path child1 = new Path(testPath, "child1");
		createBaseFileWithData(10, child1);
		Path child2 = new Path(testPath, "child2");
		createBaseFileWithData(10, child2);
		FileStatus[] res = fs.listStatus(testPath);
		assertEquals(2, res.length);
	}
}

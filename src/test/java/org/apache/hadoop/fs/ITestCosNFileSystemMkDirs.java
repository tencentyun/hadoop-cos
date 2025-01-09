package org.apache.hadoop.fs;

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class ITestCosNFileSystemMkDirs extends CosNFileSystemTestBase {

	private Path testPath;

	@Before
	public void before() throws IOException {
		super.before();
		testPath = methodPath();
	}

	@Test
	public void testMkdirSingle() throws Throwable {
		fs.mkdirs(testPath);
		assertTrue(fs.getFileStatus(testPath).isDirectory());
	}

	/*
	 * Test to verify create multiple subdirectories.
	 */
	@Test
	public void testMkdirChild() throws Throwable {
		Path child = new Path(testPath, "child");
		fs.mkdirs(child);
		assertTrue(fs.getFileStatus(child).isDirectory());
		assertTrue(fs.getFileStatus(testPath).isDirectory());
	}

	/*
	 * Test to verify create an existing directory.
	 */
	@Test
	public void testMkdirExistFilename() throws Throwable {
		createBaseFileWithData(10, testPath);
		boolean thrown = false;
		try {
			fs.mkdirs(testPath);
		} catch (FileAlreadyExistsException e) {
			thrown = true;
		}
		assertTrue(thrown);
	}

	@Test
	public void testCreateRoot() throws Throwable {
		fs.mkdirs(new Path("/"));
		assertTrue(fs.getFileStatus(new Path("/")).isDirectory());
	}
}

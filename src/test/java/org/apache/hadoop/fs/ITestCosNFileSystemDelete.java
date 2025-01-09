package org.apache.hadoop.fs;

import org.junit.Before;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.IOException;

public class ITestCosNFileSystemDelete extends CosNFileSystemTestBase {
	private Path testPath;

	@Before
	public void before() throws IOException {
		super.before();
		testPath = methodPath();
	}

	@Test
	public void testDeleteFile() throws Throwable {
		createBaseFileWithData(10, testPath);
		assertNotEquals(null, fs.getFileStatus(testPath));
		fs.delete(testPath, false);
		assertTrue(verifyDelete(testPath));
	}

	@Test
	public void testDeleteEmptyDirectory() throws Throwable {
		fs.mkdirs(testPath);
		assertTrue(fs.getFileStatus(testPath).isDirectory());
		fs.delete(testPath, false);
		assertTrue(verifyDelete(testPath));
	}

	@Test
	public void testDeleteNotEmptyDirectory() throws Throwable {
		fs.mkdirs(testPath);
		assertTrue(fs.getFileStatus(testPath).isDirectory());

		Path child = new Path(testPath, "child");
		createBaseFileWithData(10, child);
		Path child1 = new Path(testPath, "child1");
		createBaseFileWithData(10, child1);
		Path child2 = new Path(testPath, "child2");
		createBaseFileWithData(10, child2);
		Path child3 = new Path(testPath, "child3");
		createBaseFileWithData(10, child3);
		Path child4 = new Path(testPath, "child4");
		createBaseFileWithData(10, child4);
		assertEquals(5, fs.listStatus(testPath).length);

		boolean actualThrown = false;
		try {
			fs.delete(testPath, false);
		} catch (IOException e) {
			actualThrown = true;
		}
		assertTrue(actualThrown);
		assertFalse(verifyDelete(testPath));

		fs.delete(testPath, true);
		assertTrue(verifyDelete(testPath));
	}

	private boolean verifyDelete(Path path) {
		boolean actualThrown = false;
		try {
			fs.getFileStatus(testPath);
		} catch (IOException e) {
			actualThrown = true;
		}

		return actualThrown;
	}
}

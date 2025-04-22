package org.apache.hadoop.fs;

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class ITestCosNFileSystemRename extends CosNFileSystemTestBase {

	private Path testPath;

	@Before
	public void before() throws IOException {
		super.before();
		testPath = methodPath();
		fs.mkdirs(testPath);
	}

	@Test
	public void testRenameFile() throws Throwable {
		Path src = new Path(testPath,"src");
		createBaseFileWithData(10, src);
		Path dest = new Path(testPath, "dest");
		assertRenameOutcome(fs, src, dest, true);
		assertTrue(fs.getFileStatus(dest).isFile());
		assertFalse(fs.exists(src));
	}

	@Test
	public void testRenameWithPreExistingDestination() throws Throwable {
		Path src = new Path(testPath,"src");
		createBaseFileWithData(10, src);
		Path dest = new Path(testPath, "dest");
		createBaseFileWithData(10, dest);
		boolean exceptionThrown = false;
		try {
			assertRenameOutcome(fs, src, dest, false);
		} catch (FileAlreadyExistsException e) {
			exceptionThrown = true;
		}
		assertTrue("Expected FileAlreadyExistsException to be thrown", exceptionThrown);
	}

	@Test
	public void testRenameFileUnderDir() throws Throwable {
		Path src = new Path(testPath,"src");
		fs.mkdirs(src);
		String filename = "file1";
		Path file1 = new Path(src, filename);
		createBaseFileWithData(10, file1);

		Path dest = new Path(testPath, "dest");
		assertRenameOutcome(fs, src, dest, true);
		FileStatus[] fileStatus = fs.listStatus(dest);
		assertNotNull("Null file status", fileStatus);
		FileStatus status = fileStatus[0];
		assertEquals("Wrong filename in " + status,
				filename, status.getPath().getName());
	}

	@Test
	public void testRenameDirectory() throws Throwable {
		Path test1 = new Path(testPath,"test1");
		fs.mkdirs(test1);
		fs.mkdirs(new Path(testPath, "test1/test2"));
		Path test3 = new Path(testPath,"test1/test2/test3");
		fs.mkdirs(test3);
		Path file1 = new Path(test3, "file1");
		Path file2 = new Path(test3, "file2");
		createBaseFileWithData(10, file1);
		createBaseFileWithData(10, file2);

		assertRenameOutcome(fs, test1,
				new Path(testPath, "test10"), true);
		assertTrue(fs.exists(new Path(testPath, "test10/test2/test3")));
		assertFalse(fs.exists(test1));
		assertEquals(2, fs.listStatus(new Path(testPath, "test10/test2/test3")).length);
	}

	@Test
	public void testRenameFileToExistDirectory() throws Throwable {
		Path test1 = new Path(testPath,"test1");
		fs.mkdirs(test1);
		Path file1 = new Path(testPath, "file1");
		createBaseFileWithData(10, file1);
		fs.rename(file1, test1);
		assertTrue(fs.exists(new Path(testPath, "test1/file1")));
	}

	@Test
	public void testRenameDestDirParentNotExist() throws Throwable {
		Path test1 = new Path(testPath,"test1");
		fs.mkdirs(test1);
		Path test2 = new Path(testPath,"test2/test3");
		boolean thrown = false;
		try {
			fs.rename(test1, test2);
		} catch (IOException e) {
			thrown = true;
		}
		assertTrue(thrown);
	}

	public static void assertRenameOutcome(FileSystem fs,
			Path source,
			Path dest,
			boolean expectedResult) throws IOException {
		boolean result = fs.rename(source, dest);
		if (expectedResult != result) {
			fail(String.format("Expected rename(%s, %s) to return %b,"
					+ " but result was %b", source, dest, expectedResult, result));
		}
	}
}

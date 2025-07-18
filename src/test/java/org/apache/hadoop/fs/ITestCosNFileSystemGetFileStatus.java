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

	@Test
	public void testGetFileStatusWithMustExist() throws Throwable {
		// 使用到CosNFileSystem新增接口
		assertTrue(fs instanceof CosNFileSystem);
		CosNFileSystem cosfs = (CosNFileSystem) fs;
		fs.mkdirs(testPath);
		Path dir = new Path(testPath, "dir");
		fs.mkdirs(dir);
		FileStatus status = cosfs.getFileStatus(dir, false);
		assertTrue(status.isDirectory());

		Path child = new Path(dir, "child");
		createBaseFileWithData(10, child);
		FileStatus filestatus = cosfs.getFileStatus(child, false);
		assertTrue(filestatus.isFile());
		// can use DUMMY_LIST, "<testPath>/dir/child" file exist, so DUMMY_LIST "<testPath>/dir/"
		FileStatus dirStatus = cosfs.getFileStatus(dir, true);
		assertTrue(dirStatus.isDirectory());
		assertTrue(dirStatus.equals(status));
	}

	@Test
	public void testGetFileStatusWithMustExist2() throws Throwable {
		// 使用coscli提前上传文件对象 ./coscli cp hello.txt cos://<bucket>/a/b/c/hello.txt
		assertTrue(fs instanceof CosNFileSystem);
		CosNFileSystem cosfs = (CosNFileSystem) fs;
		final String dirPath = "/a/b/c";

		Path dir = new Path(dirPath);
		Path filePath = new Path(dirPath, "hello.txt");
		if (!fs.exists(filePath)) {
			// System.out.println("filePath: " + filePath + " not exist!");
			return;
		}

		System.out.println("filePath: " + filePath + " exist.");
		FileStatus dirStatus = cosfs.getFileStatus(dir, true);
		assertTrue(dirStatus.isDirectory());
		FileStatus status = cosfs.getFileStatus(dir);
		assertTrue(status.isDirectory());
		assertTrue(dirStatus.equals(status));
	}

}

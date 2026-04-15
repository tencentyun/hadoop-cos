package org.apache.hadoop.fs;

import org.junit.Before;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * {@link CosNFileSystem#listStatusIterator} 的集成测试。
 * 覆盖：单文件路径、空目录、多文件目录、含子目录、路径不存在、
 * 以及迭代器结果与 listStatus 的一致性。
 */
public class ITestCosNFileSystemListStatusIterator extends CosNFileSystemTestBase {

	private Path testPath;

	@Before
	public void before() throws IOException {
		super.before();
		testPath = methodPath();
	}

	// -----------------------------------------------------------------------
	// 辅助方法
	// -----------------------------------------------------------------------

	/**
	 * 将迭代器中所有元素收集到 List
	 */
	private List<FileStatus> collect(RemoteIterator<FileStatus> it) throws IOException {
		List<FileStatus> result = new ArrayList<>();
		while (it.hasNext()) {
			result.add(it.next());
		}
		return result;
	}

	// -----------------------------------------------------------------------
	// 测试用例
	// -----------------------------------------------------------------------

	/**
	 * 路径指向单个文件时，迭代器应返回该文件自身（单元素），
	 * 且与 getFileStatus() 返回的结果相等。
	 */
	@Test
	public void testSingleFileReturnsItself() throws Throwable {
		createBaseFileWithData(10, testPath);

		List<FileStatus> result = collect(fs.listStatusIterator(testPath));

		assertEquals(1, result.size());
		assertEquals(fs.getFileStatus(testPath), result.get(0));
	}

	/**
	 * 空目录时，迭代器应无任何元素。
	 */
	@Test
	public void testEmptyDirectoryReturnsNoEntries() throws Throwable {
		fs.mkdirs(testPath);

		List<FileStatus> result = collect(fs.listStatusIterator(testPath));

		assertEquals(0, result.size());
	}

	/**
	 * 目录含多个文件时，迭代器应返回所有子文件，数量正确。
	 */
	@Test
	public void testDirectoryWithMultipleFiles() throws Throwable {
		fs.mkdirs(testPath);
		int fileCount = 5;
		for (int i = 0; i < fileCount; i++) {
			createBaseFileWithData(10, new Path(testPath, "file-" + i));
		}

		List<FileStatus> result = collect(fs.listStatusIterator(testPath));

		assertEquals(fileCount, result.size());
		// 所有条目均为文件
		for (FileStatus status : result) {
			assertTrue("期望是文件: " + status.getPath(), status.isFile());
		}
	}

	/**
	 * 目录含子目录时，迭代器应正确返回子目录条目。
	 */
	@Test
	public void testDirectoryWithSubDirectories() throws Throwable {
		fs.mkdirs(testPath);
		Path subDir1 = new Path(testPath, "subdir1");
		Path subDir2 = new Path(testPath, "subdir2");
		fs.mkdirs(subDir1);
		fs.mkdirs(subDir2);
		createBaseFileWithData(10, new Path(testPath, "file1"));

		List<FileStatus> result = collect(fs.listStatusIterator(testPath));

		assertEquals(3, result.size());
		long dirCount = result.stream().filter(FileStatus::isDirectory).count();
		long fileCount = result.stream().filter(FileStatus::isFile).count();
		assertEquals(2, dirCount);
		assertEquals(1, fileCount);
	}

	/**
	 * 路径不存在时，应抛出 FileNotFoundException。
	 */
	@Test(expected = FileNotFoundException.class)
	public void testNonExistentPathThrowsFileNotFoundException() throws Throwable {
		Path nonExistent = new Path(testPath, "non-existent");
		fs.listStatusIterator(nonExistent);
	}

	/**
	 * 迭代器返回的结果集合应与 listStatus() 返回的结果完全一致（路径集合相同）。
	 */
	@Test
	public void testIteratorConsistentWithListStatus() throws Throwable {
		fs.mkdirs(testPath);
		for (int i = 0; i < 3; i++) {
			createBaseFileWithData(10, new Path(testPath, "child-" + i));
		}

		// 通过 listStatus 获取期望结果
		FileStatus[] expected = fs.listStatus(testPath);
		Set<Path> expectedPaths = new HashSet<>();
		for (FileStatus s : expected) {
			expectedPaths.add(s.getPath());
		}

		// 通过 listStatusIterator 获取实际结果
		List<FileStatus> actual = collect(fs.listStatusIterator(testPath));
		Set<Path> actualPaths = new HashSet<>();
		for (FileStatus s : actual) {
			actualPaths.add(s.getPath());
		}

		assertEquals("listStatusIterator 与 listStatus 返回的路径集合不一致", expectedPaths, actualPaths);
	}

	/**
	 * 对单文件调用 listStatusIterator 后，hasNext() 消费完毕应返回 false。
	 */
	@Test
	public void testIteratorExhaustedAfterConsumption() throws Throwable {
		createBaseFileWithData(10, testPath);

		RemoteIterator<FileStatus> it = fs.listStatusIterator(testPath);
		assertTrue(it.hasNext());
		it.next();
		assertFalse("迭代器耗尽后 hasNext() 应返回 false", it.hasNext());
	}
}

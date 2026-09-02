package org.apache.hadoop.fs;

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class ITestCosNFileSystemSetOwner extends CosNFileSystemTestBase {

	private static final String TEST_OWNER = "cosn-test-owner";
	private static final String TEST_GROUP = "cosn-test-group";
	private static final String TEST_GROUP_UPDATED = "cosn-test-group-updated";

	private Path testPath;

	@Before
	public void before() throws IOException {
		super.before();
		testPath = methodPath();
	}

	@Test
	public void testSetOwnerOnFile() throws Throwable {
		createBaseFileWithData(10, testPath);
		fs.setOwner(testPath, TEST_OWNER, TEST_GROUP);

		FileStatus status = fs.getFileStatus(testPath);
		assertEquals(TEST_OWNER, status.getOwner());
		assertEquals(TEST_GROUP, status.getGroup());
	}

	@Test
	public void testSetOwnerOnDirectory() throws Throwable {
		fs.mkdirs(testPath);
		fs.setOwner(testPath, TEST_OWNER, TEST_GROUP);

		FileStatus status = fs.getFileStatus(testPath);
		assertTrue(status.isDirectory());
		assertEquals(TEST_OWNER, status.getOwner());
		assertEquals(TEST_GROUP, status.getGroup());
	}

	@Test
	public void testSetOwnerOnDirectoryWithoutDirObject() throws Throwable {
		// 只写入子对象，父目录没有对应的目录对象，仅仅是对象 key 的前缀。
		Path child = new Path(testPath, "child");
		createBaseFileWithData(10, child);

		fs.setOwner(testPath, TEST_OWNER, TEST_GROUP);

		// 目录对象缺失时会自动补建，属主信息才能落到目录对象上并被读回。
		FileStatus status = fs.getFileStatus(testPath);
		assertTrue(status.isDirectory());
		assertEquals(TEST_OWNER, status.getOwner());
		assertEquals(TEST_GROUP, status.getGroup());
	}

	@Test
	public void testSetOwnerKeepsTheOriginalValueWhenTheArgumentIsNull() throws Throwable {
		createBaseFileWithData(10, testPath);
		fs.setOwner(testPath, TEST_OWNER, TEST_GROUP);
		// owner 传 null 表示不修改，这里只更新 group。
		fs.setOwner(testPath, null, TEST_GROUP_UPDATED);

		FileStatus status = fs.getFileStatus(testPath);
		assertEquals(TEST_OWNER, status.getOwner());
		assertEquals(TEST_GROUP_UPDATED, status.getGroup());
	}

	@Test
	public void testSetOwnerIsPersistedAcrossFileSystemInstances() throws Throwable {
		createBaseFileWithData(10, testPath);
		fs.setOwner(testPath, TEST_OWNER, TEST_GROUP);

		// FileSystem.get 返回的是缓存实例，必须新开一个实例才能验证属主确实持久化到了 COS 上。
		FileSystem newFs = FileSystem.newInstance(configuration);
		try {
			FileStatus status = newFs.getFileStatus(testPath);
			assertEquals(TEST_OWNER, status.getOwner());
			assertEquals(TEST_GROUP, status.getGroup());
		} finally {
			newFs.close();
		}
	}

	@Test
	public void testSetOwnerKeepsTheOwnerAfterRename() throws Throwable {
		createBaseFileWithData(10, testPath);
		fs.setOwner(testPath, TEST_OWNER, TEST_GROUP);

		Path renamedPath = new Path(testPath.getParent(), testPath.getName() + "-renamed");
		assertTrue(fs.rename(testPath, renamedPath));

		FileStatus status = fs.getFileStatus(renamedPath);
		assertEquals(TEST_OWNER, status.getOwner());
		assertEquals(TEST_GROUP, status.getGroup());

		fs.delete(renamedPath, false);
	}

	@Test
	public void testSetOwnerOnRootDirectory() throws Throwable {
		// 根目录没有对应的对象，属主信息无处存放，这里只保证不抛出异常。
		Path rootPath = new Path("/");
		fs.setOwner(rootPath, TEST_OWNER, TEST_GROUP);
		assertTrue(fs.getFileStatus(rootPath).isDirectory());
	}
}

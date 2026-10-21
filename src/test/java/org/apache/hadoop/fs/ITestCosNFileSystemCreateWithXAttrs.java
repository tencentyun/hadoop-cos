package org.apache.hadoop.fs;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.cosn.Abortable;
import org.apache.hadoop.fs.cosn.Constants;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * Integration tests of the create-time extended attributes of {@link CosNFileSystemExt#createFile(Path)}.
 * The objects are really written to the bucket, so the credentials in the core-site.xml are required.
 */
public class ITestCosNFileSystemCreateWithXAttrs extends CosNFileSystemTestBase {
	// 选项名直接就是属性名，不需要前缀。
	private static final String TEXT_ATTR = "attr-text";
	private static final String UTF8_ATTR = "attr-utf8";
	private static final String UTF8_VALUE = "中文-value";

	private Path testPath;
	private CosNFileSystemExt extFs;

	@Before
	public void before() throws IOException {
		super.before();
		testPath = methodPath();
		// 用最小的分块大小，写入几 MB 的数据就能走到分块上传。
		Configuration conf = new Configuration(configuration);
		conf.setLong(CosNConfigKeys.COSN_UPLOAD_PART_SIZE_KEY, Constants.MIN_PART_SIZE);
		extFs = new CosNFileSystemExt();
		extFs.initialize(fs.getUri(), conf);
	}

	@After
	public void after() throws IOException {
		if (null != extFs) {
			extFs.close();
			extFs = null;
		}
		super.after();
	}

	@Test
	public void testCreateSmallFileWithXAttrs() throws Throwable {
		byte[] data = getTestData(10);
		try (FSDataOutputStream out = newBuilderWithXAttrs().build()) {
			out.write(data);
		}

		assertTrue(verifyFile(data, testPath));
		assertXAttrs();
	}

	@Test
	public void testCreateLargeFileWithXAttrs() throws Throwable {
		// 2.5 个分块，会走分块上传，属性在 InitiateMultipartUpload 时声明。
		byte[] data = getTestData((int) (2.5 * Constants.MIN_PART_SIZE));
		try (FSDataOutputStream out = newBuilderWithXAttrs().build()) {
			out.write(data);
			// 关闭之前对象还没有提交，数据和属性是一起生效的。
			assertFalse(fs.exists(testPath));
		}

		assertEquals(data.length, fs.getFileStatus(testPath).getLen());
		assertTrue(verifyFile(data, testPath));
		assertXAttrs();
	}

	@Test
	public void testCreateEmptyFileWithXAttrs() throws Throwable {
		newBuilderWithXAttrs().build().close();

		assertEquals(0, fs.getFileStatus(testPath).getLen());
		assertXAttrs();
	}

	@Test
	public void testAbortSmallFileLeavesNothing() throws Throwable {
		FSDataOutputStream out = newBuilderWithXAttrs().build();
		out.write(getTestData(10));
		((Abortable) out.getWrappedStream()).doAbort();

		assertFalse(fs.exists(testPath));
	}

	@Test
	public void testAbortLargeFileLeavesNothing() throws Throwable {
		FSDataOutputStream out = newBuilderWithXAttrs().build();
		out.write(getTestData((int) (2.5 * Constants.MIN_PART_SIZE)));
		((Abortable) out.getWrappedStream()).doAbort();

		assertFalse(fs.exists(testPath));
	}

	@Test
	public void testOverwriteReplacesTheOldXAttrs() throws Throwable {
		createBaseFileWithData(10, testPath);
		fs.setXAttr(testPath, "old-attr", new byte[]{9});

		byte[] data = getTestData(20);
		try (FSDataOutputStream out = newBuilderWithXAttrs().build()) {
			out.write(data);
		}

		assertTrue(verifyFile(data, testPath));
		assertXAttrs();
		// 覆盖写生成的是新对象，旧对象上的属性不会保留，与 HDFS 的语义一致。
		assertNull(fs.getXAttr(testPath, "old-attr"));
	}

	@Test
	public void testCreateWithoutXAttrs() throws Throwable {
		byte[] data = getTestData(10);
		try (FSDataOutputStream out = extFs.createFile(testPath).recursive().build()) {
			out.write(data);
		}
		assertTrue(verifyFile(data, testPath));
	}

	@Test
	public void testTooLargeXAttrsAreRejectedBeforeWriting() throws Throwable {
		StringBuilder value = new StringBuilder();
		for (int i = 0; i < 1000; i++) {
			value.append('a');
		}
		CosNFileSystemExt.CosNCreateFileBuilder builder = extFs.createFile(testPath).recursive();
		for (int i = 0; i < 4; i++) {
			builder.opt("attr-" + i, value.toString());
		}
		try {
			builder.build();
			fail("The total size of the attributes exceeds the limit.");
		} catch (HadoopIllegalArgumentException expected) {
			// expected
		}
		assertFalse(fs.exists(testPath));
	}

	@Test
	public void testCreateSmallFileWithOwner() throws Throwable {
		byte[] data = getTestData(10);
		try (FSDataOutputStream out = extFs.createFile(testPath).recursive().setOwner("alice", "staff").build()) {
			out.write(data);
		}

		assertTrue(verifyFile(data, testPath));
		assertOwner("alice", "staff");
	}

	@Test
	public void testCreateLargeFileWithOwnerAndXAttrs() throws Throwable {
		// 走分块上传：属主和属性都在 InitiateMultipartUpload 时声明，互不覆盖。
		byte[] data = getTestData((int) (2.5 * Constants.MIN_PART_SIZE));
		try (FSDataOutputStream out = newBuilderWithXAttrs().setOwner("alice", "staff").build()) {
			out.write(data);
			assertFalse(fs.exists(testPath));
		}

		assertTrue(verifyFile(data, testPath));
		assertOwner("alice", "staff");
		assertXAttrs();
	}

	@Test
	public void testCreateWithOwnerOnly() throws Throwable {
		// 只声明 owner：group 没有声明，读回来是文件系统的默认值，而不是 alice 的组。
		try (FSDataOutputStream out = extFs.createFile(testPath).recursive().setOwner("alice", null).build()) {
			out.write(getTestData(10));
		}

		assertEquals("alice", fs.getFileStatus(testPath).getOwner());
	}

	@Test
	public void testAbortWithOwnerLeavesNothing() throws Throwable {
		FSDataOutputStream out = extFs.createFile(testPath).recursive().setOwner("alice", "staff").build();
		out.write(getTestData(10));
		((Abortable) out.getWrappedStream()).doAbort();

		assertFalse(fs.exists(testPath));
	}

	@Test
	public void testEmptyOwnerIsRejectedBeforeCreating() throws Throwable {
		try {
			extFs.createFile(testPath).recursive().setOwner("", "staff");
			fail("An empty user is expected to be rejected.");
		} catch (HadoopIllegalArgumentException expected) {
			// expected
		}
		assertFalse(fs.exists(testPath));
	}

	private CosNFileSystemExt.CosNCreateFileBuilder newBuilderWithXAttrs() {
		return extFs.createFile(testPath)
				.recursive()
				.must(TEXT_ATTR, "hello")
				.opt(UTF8_ATTR, UTF8_VALUE);
	}

	private void assertOwner(String owner, String group) throws IOException {
		FileStatus status = fs.getFileStatus(testPath);
		assertEquals(owner, status.getOwner());
		assertEquals(group, status.getGroup());
	}

	private void assertXAttrs() throws IOException {
		assertArrayEquals("hello".getBytes(), fs.getXAttr(testPath, TEXT_ATTR));
		assertArrayEquals(UTF8_VALUE.getBytes(CosNFileSystem.METADATA_ENCODING), fs.getXAttr(testPath, UTF8_ATTR));
	}
}

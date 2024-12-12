package org.apache.hadoop.fs;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.Arrays;

public class CosNFileSystemTestBase extends CosNFileSystemTestWithTimeout {
	protected static Configuration configuration;
	protected static FileSystem fs;

	protected static final Path unittestDirPath = new Path("/unittest-dir" + RandomStringUtils.randomAlphanumeric(8));
	protected final Path testDirPath = new Path(unittestDirPath, "test-dir");
	protected final Path testFilePath = new Path(unittestDirPath, "test-file");

	@BeforeClass
	public static void beforeClass() throws IOException {
		String configFilePath = System.getProperty("config.file");
		configuration = new Configuration();
		// 初始化文件系统对象，因为 core-site.xml 是在 test 的 resource 下面，因此应该能够正确加载到。
		if (configFilePath != null) {
			// 使用 addResource 方法加载配置文件
			configuration.addResource(new Path(configFilePath));
		}
		// 考虑到是针对 CosNFileSystem 测试，因此强制设置为 CosNFileSystem。
		configuration.set("fs.cosn.impl", "org.apache.hadoop.fs.CosNFileSystem");
		fs = FileSystem.get(configuration);

		if (null != fs && !fs.exists(unittestDirPath)) {
			fs.mkdirs(unittestDirPath);
		}
	}

	@AfterClass
	public static void afterClass() throws IOException {
		if (null != fs && fs.exists(unittestDirPath)) {
			fs.delete(unittestDirPath, true);
		}
		if (null != fs) {
			fs.close();
		}
	}

	@Before
	public void before() throws IOException {
		if (!fs.exists(testDirPath)) {
			fs.mkdirs(testDirPath);
		}
		if (!fs.exists(testFilePath)) {
			try (FSDataOutputStream fsDataOutputStream = fs.create(testFilePath)) {
				fsDataOutputStream.write("Hello, World!".getBytes());
				fsDataOutputStream.write("\n".getBytes());
				fsDataOutputStream.write("Hello, COS!".getBytes());
			}
		}
	}

	@After
	public void after() throws IOException {
		if (fs.exists(testFilePath)) {
			fs.delete(testFilePath, true);
		}
		if (fs.exists(testDirPath)) {
			fs.delete(testDirPath, true);
		}
	}

	/**
	 * Return a path bonded to this method name, unique to this fork during
	 * parallel execution.
	 *
	 * @return a method name unique to (fork, method).
	 * @throws IOException IO problems
	 */
	protected Path methodPath() throws IOException {
		return new Path(unittestDirPath, methodName.getMethodName());
	}

	/*
	 * Helper method that creates test data of size provided by the
	 * "size" parameter.
	 */
	protected static byte[] getTestData(int size) {
		byte[] testData = new byte[size];
		System.arraycopy(RandomStringUtils.randomAlphabetic(size).getBytes(), 0, testData, 0, size);
		return testData;
	}

	// Helper method to create file and write fileSize bytes of data on it.
	protected byte[] createBaseFileWithData(int fileSize, Path testPath) throws Throwable {

		try (FSDataOutputStream createStream = fs.create(testPath)) {
			byte[] fileData = null;

			if (fileSize != 0) {
				fileData = getTestData(fileSize);
				createStream.write(fileData);
			}
			return fileData;
		}
	}

	/*
	 * Helper method to verify  a testFile data.
	 */
	protected boolean verifyFile(byte[] testData, Path testFile) {

		try (FSDataInputStream srcStream = fs.open(testFile)) {

			int baseBufferSize = 2048;
			int testDataSize = testData.length;
			int testDataIndex = 0;

			while (testDataSize > baseBufferSize) {

				if (!verifyFileData(baseBufferSize, testData, testDataIndex, srcStream)) {
					return false;
				}
				testDataIndex += baseBufferSize;
				testDataSize -= baseBufferSize;
			}

			if (!verifyFileData(testDataSize, testData, testDataIndex, srcStream)) {
				return false;
			}

			return true;
		} catch (Exception ex) {
			return false;
		}
	}

	/*
	 * Helper method to verify a file data equal to "dataLength" parameter
	 */
	protected boolean verifyFileData(int dataLength, byte[] testData, int testDataIndex, FSDataInputStream srcStream) {

		try {

			byte[] fileBuffer = new byte[dataLength];
			byte[] testDataBuffer = new byte[dataLength];

			int fileBytesRead = srcStream.read(fileBuffer);

			if (fileBytesRead < dataLength) {
				return false;
			}

			System.arraycopy(testData, testDataIndex, testDataBuffer, 0, dataLength);

			if (!Arrays.equals(fileBuffer, testDataBuffer)) {
				return false;
			}

			return true;

		} catch (Exception ex) {
			return false;
		}

	}
}

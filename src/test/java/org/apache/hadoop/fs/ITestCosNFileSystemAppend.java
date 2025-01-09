package org.apache.hadoop.fs;

import org.apache.hadoop.fs.cosn.Unit;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class ITestCosNFileSystemAppend extends CosNFileSystemTestBase {

	private Path testPath;

	@Before
	public void before() throws IOException {
		super.before();
		testPath = methodPath();
	}

	@Test
	public void testSingleAppend() throws Throwable {
		FSDataOutputStream appendStream = null;
		try {
			int baseDataSize = 50;
			byte[] baseDataBuffer = createBaseFileWithData(baseDataSize, testPath);

			int appendDataSize = 20;
			byte[] appendDataBuffer = getTestData(appendDataSize);
			appendStream = fs.append(testPath, 10);
			appendStream.write(appendDataBuffer);
			appendStream.close();
			byte[] testData = new byte[baseDataSize + appendDataSize];
			System.arraycopy(baseDataBuffer, 0, testData, 0, baseDataSize);
			System.arraycopy(appendDataBuffer, 0, testData, baseDataSize, appendDataSize);

			assertTrue(verifyFile(testData, testPath));
		} finally {
			if (appendStream != null) {
				appendStream.close();
			}
		}
	}

	/*
	 * Test case to verify append to an empty file.
	 */
	@Test
	public void testSingleAppendOnEmptyFile() throws Throwable {
		FSDataOutputStream appendStream = null;

		try {
			createBaseFileWithData(0, testPath);

			int appendDataSize = 20;
			byte[] appendDataBuffer = getTestData(appendDataSize);
			appendStream = fs.append(testPath, 10);
			appendStream.write(appendDataBuffer);
			appendStream.close();

			assertTrue(verifyFile(appendDataBuffer, testPath));
		} finally {
			if (appendStream != null) {
				appendStream.close();
			}
		}
	}

	/*
	 * Tests to verify multiple appends on a Blob.
	 */
	@Test
	public void testMultipleAppends() throws Throwable {
		int baseDataSize = 50;
		byte[] baseDataBuffer = createBaseFileWithData(baseDataSize, testPath);

		int appendDataSize = 100;
		int targetAppendCount = 5;
		byte[] testData = new byte[baseDataSize + (appendDataSize*targetAppendCount)];
		int testDataIndex = 0;
		System.arraycopy(baseDataBuffer, 0, testData, testDataIndex, baseDataSize);
		testDataIndex += baseDataSize;

		int appendCount = 0;

		FSDataOutputStream appendStream = null;

		try {
			while (appendCount < targetAppendCount) {

				byte[] appendDataBuffer = getTestData(appendDataSize);
				appendStream = fs.append(testPath, 30);
				appendStream.write(appendDataBuffer);
				appendStream.close();

				System.arraycopy(appendDataBuffer, 0, testData, testDataIndex, appendDataSize);
				testDataIndex += appendDataSize;
				appendCount++;
			}

			assertTrue(verifyFile(testData, testPath));

		} finally {
			if (appendStream != null) {
				appendStream.close();
			}
		}
	}

	/*
	 * Test to verify we multiple appends on the same stream.
	 */
	@Test
	public void testMultipleAppendsOnSameStream() throws Throwable {
		int baseDataSize = 50;
		byte[] baseDataBuffer = createBaseFileWithData(baseDataSize, testPath);
		int appendDataSize = 100;
		int targetAppendCount = 3;
		byte[] testData = new byte[baseDataSize + (appendDataSize*targetAppendCount)];
		int testDataIndex = 0;
		System.arraycopy(baseDataBuffer, 0, testData, testDataIndex, baseDataSize);
		testDataIndex += baseDataSize;
		int appendCount = 0;

		FSDataOutputStream appendStream = null;

		try {

			while (appendCount < targetAppendCount) {

				appendStream = fs.append(testPath, 50);

				int singleAppendChunkSize = 20;
				int appendRunSize = 0;
				while (appendRunSize < appendDataSize) {

					byte[] appendDataBuffer = getTestData(singleAppendChunkSize);
					appendStream.write(appendDataBuffer);
					System.arraycopy(appendDataBuffer, 0, testData,
							testDataIndex + appendRunSize, singleAppendChunkSize);

					appendRunSize += singleAppendChunkSize;
				}

				appendStream.close();
				testDataIndex += appendDataSize;
				appendCount++;
			}

			assertTrue(verifyFile(testData, testPath));
		} finally {
			if (appendStream != null) {
				appendStream.close();
			}
		}
	}

	/*
	 * Test to verify append big file.
	 */
	@Test
	public void testAppendsBigFile() throws Throwable {
		int baseDataSize = 50;
		byte[] baseDataBuffer = createBaseFileWithData(baseDataSize, testPath);
		int appendDataSize = (int) (8 * Unit.MB); // larger than part size
		int targetAppendCount = 2;
		byte[] testData = new byte[baseDataSize + (appendDataSize*targetAppendCount)];
		int testDataIndex = 0;
		System.arraycopy(baseDataBuffer, 0, testData, testDataIndex, baseDataSize);
		testDataIndex += baseDataSize;
		int appendCount = 0;

		FSDataOutputStream appendStream = null;

		try {

			while (appendCount < targetAppendCount) {
				appendStream = fs.append(testPath, 50);
				byte[] appendDataBuffer = getTestData(appendDataSize);
				appendStream.write(appendDataBuffer);
				System.arraycopy(appendDataBuffer, 0, testData, testDataIndex, appendDataSize);
				appendStream.close();
				testDataIndex += appendDataSize;
				appendCount++;
			}

			assertTrue(verifyFile(testData, testPath));
		} finally {
			if (appendStream != null) {
				appendStream.close();
			}
		}
	}
}

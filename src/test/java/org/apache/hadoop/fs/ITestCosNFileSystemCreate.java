package org.apache.hadoop.fs;

import org.apache.hadoop.fs.cosn.Unit;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

public class ITestCosNFileSystemCreate extends CosNFileSystemTestBase {
	private Path testPath;

	@Before
	public void before() throws IOException {
		super.before();
		testPath = methodPath();
	}

	@Test
	public void testSingleCreate() throws Throwable {
		long[] cases = new long[] {Unit.MB, 10 * Unit.MB};
		for (long c : cases) {
			byte[] baseData = createBaseFileWithData((int) c, testPath);
			FSDataOutputStream outputStream = fs.create(testPath);
			outputStream.write(baseData);
			assertTrue(verifyFile(baseData, testPath));
		}
	}

	@Test
	public void testNotAllowOverwriteCreate() throws Throwable {
		createBaseFileWithData(10, testPath);

		boolean actualException = false;
		byte[] testData = getTestData(10);
		try {
			FSDataOutputStream outputStream = fs.create(testPath, false);
			outputStream.write(testData);
		} catch (FileAlreadyExistsException fe) {
			actualException = true;
		}
		assertTrue(actualException);
	}
}

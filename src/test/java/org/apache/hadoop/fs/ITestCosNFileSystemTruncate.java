package org.apache.hadoop.fs;

import org.apache.hadoop.fs.cosn.Unit;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

public class ITestCosNFileSystemTruncate extends CosNFileSystemTestBase {

	private Path testPath;

	@Before
	public void before() throws IOException {
		super.before();
		testPath = methodPath();
	}

	@Test
	public void testTruncate() throws Throwable {
		long[] cases = new long[] {Unit.MB, 10 * Unit.MB};
		int newLength = (int) Unit.MB - 1;
		for (long c : cases) {
			byte[] baseData = createBaseFileWithData((int) c, testPath);
			fs.truncate(testPath, newLength);
			assertTrue(verifyFile(Arrays.copyOf(baseData, newLength), testPath));
		}
	}

}

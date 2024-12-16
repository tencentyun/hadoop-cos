package org.apache.hadoop.fs;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.junit.rules.Timeout;

public class CosNFileSystemTestWithTimeout extends Assert {
	/**
	 * The name of the current method.
	 */
	@Rule
	public TestName methodName = new TestName();
	/**
	 * Set the timeout for every test.
	 * This is driven by the value returned by {@link #getTestTimeoutMillis()}.
	 */
	@Rule
	public Timeout testTimeout = new Timeout(getTestTimeoutMillis());

	/**
	 * Name the junit thread for the class. This will overridden
	 * before the individual test methods are run.
	 */
	@BeforeClass
	public static void nameTestThread() {
		Thread.currentThread().setName("JUnit");
	}

	/**
	 * Name the thread to the current test method.
	 */
	@Before
	public void nameThread() {
		Thread.currentThread().setName("JUnit-" + methodName.getMethodName());
	}

	/**
	 * Override point: the test timeout in milliseconds.
	 * @return a timeout in milliseconds
	 */
	protected int getTestTimeoutMillis() {
		return 60 * 10 * 1000;
	}

}

package org.apache.hadoop.fs;

import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.util.UUID;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * End-to-end integration tests for the storage class feature.
 *
 * <p>These tests upload real objects to COS using the bucket configuration in
 * {@code core-site.xml}, then verify the storage class reported back by COS
 * matches the configured value. They cover the single upload (PutObject) path,
 * the multipart upload (MPU) path, fallback and normalization.</p>
 */
public class ITestCosNStorageClass extends CosNFileSystemTestWithTimeout {

    private static final int SINGLE_UPLOAD_SIZE = 1024;
    // 10 MB, larger than the default 8 MB part size, so it triggers MPU.
    private static final int MULTIPART_UPLOAD_SIZE = 10 * 1024 * 1024;

    /**
     * Storage classes verified end-to-end against the test bucket (a
     * single-AZ, non-intelligent-tier bucket). Only classes actually supported
     * by this bucket are included; MAZ / intelligent-tier classes are excluded
     * because they would be rejected by the bucket.
     */
    private static final String[] SUPPORTED_STORAGE_CLASSES = {
            CosNStorageClass.STANDARD,
            CosNStorageClass.STANDARD_IA,
            CosNStorageClass.ARCHIVE,
            CosNStorageClass.DEEP_ARCHIVE,
            CosNStorageClass.COLD
    };

    private final Path testBasePath =
            new Path("/storage-class-test-" + UUID.randomUUID());
    private FileSystem fs;

    @After
    public void tearDown() throws IOException {
        if (null != fs) {
            try {
                if (fs.exists(testBasePath)) {
                    fs.delete(testBasePath, true);
                }
            } finally {
                fs.close();
            }
        }
    }

    private FileSystem createFileSystem(String storageClass) throws IOException {
        Configuration conf = new Configuration();
        // core-site.xml under src/test/resources is loaded automatically by
        // Configuration. Force the CosN implementation for consistency.
        conf.set("fs.cosn.impl", "org.apache.hadoop.fs.CosNFileSystem");
        if (null != storageClass) {
            conf.set(CosNConfigKeys.COSN_STORAGE_CLASS_KEY, storageClass);
        }
        return FileSystem.get(conf);
    }

    private Path upload(byte[] data) throws IOException {
        if (!fs.exists(testBasePath)) {
            fs.mkdirs(testBasePath);
        }
        Path path = new Path(testBasePath, "file-" + UUID.randomUUID());
        try (OutputStream out = fs.create(path)) {
            out.write(data);
        }
        return path;
    }

    private String getUploadedStorageClass(Path path) throws IOException {
        FileStatus status = fs.getFileStatus(path);
        assertTrue("Expected a CosNFileStatus but got " + status.getClass().getName(),
                status instanceof CosNFileStatus);
        return ((CosNFileStatus) status).getStorageClass();
    }

    /**
     * Asserts that the storage class reported by COS matches the configured one.
     *
     * <p>STANDARD is the default storage class and COS does not return the
     * {@code x-cos-storage-class} header for it, so a {@code null} value is also
     * treated as STANDARD.</p>
     */
    private void assertUploadedStorageClass(String expected, String actual) {
        if (CosNStorageClass.STANDARD.equals(expected)) {
            assertTrue("Expected STANDARD (or null meaning default STANDARD) but got " + actual,
                    null == actual || CosNStorageClass.STANDARD.equalsIgnoreCase(actual));
        } else {
            assertNotNull("The uploaded object did not report a storage class for " + expected, actual);
            assertTrue("Expected " + expected + " but got " + actual,
                    expected.equalsIgnoreCase(actual));
        }
    }

    @Test
    public void testSingleUploadStorageClassString() throws IOException {
        fs = createFileSystem(CosNStorageClass.STANDARD_IA);
        byte[] data = new byte[SINGLE_UPLOAD_SIZE];
        Path path = upload(data);

        String actual = getUploadedStorageClass(path);
        assertUploadedStorageClass(CosNStorageClass.STANDARD_IA, actual);
    }

    @Test
    public void testMultipartUploadStorageClassString() throws IOException {
        fs = createFileSystem(CosNStorageClass.STANDARD_IA);
        byte[] data = new byte[MULTIPART_UPLOAD_SIZE];
        Path path = upload(data);

        String actual = getUploadedStorageClass(path);
        assertUploadedStorageClass(CosNStorageClass.STANDARD_IA, actual);
    }

    @Test
    public void testStorageClassFallbackToStandard() throws IOException {
        fs = createFileSystem("INVALID_STORAGE_CLASS");
        byte[] data = new byte[SINGLE_UPLOAD_SIZE];
        Path path = upload(data);

        String actual = getUploadedStorageClass(path);
        assertUploadedStorageClass(CosNStorageClass.STANDARD, actual);
    }

    @Test
    public void testStorageClassCaseInsensitiveNormalization() throws IOException {
        fs = createFileSystem("standard_ia");
        byte[] data = new byte[SINGLE_UPLOAD_SIZE];
        Path path = upload(data);

        String actual = getUploadedStorageClass(path);
        assertUploadedStorageClass(CosNStorageClass.STANDARD_IA, actual);
    }

    @Test
    public void testAllSupportedStorageClassesSingleUpload() throws IOException {
        for (String storageClass : SUPPORTED_STORAGE_CLASSES) {
            fs = createFileSystem(storageClass);
            try {
                byte[] data = new byte[SINGLE_UPLOAD_SIZE];
                Path path = upload(data);
                String actual = getUploadedStorageClass(path);
                assertUploadedStorageClass(storageClass, actual);
            } finally {
                fs.close();
                fs = null;
            }
        }
    }
}

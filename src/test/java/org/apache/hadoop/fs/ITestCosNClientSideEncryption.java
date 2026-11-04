package org.apache.hadoop.fs;

import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.UUID;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * End-to-end integration tests for client-side encryption (CSE) combined with
 * multipart upload (MPU).
 *
 * <p>These tests upload real objects larger than the default 8 MB part size to
 * COS with {@code fs.cosn.client-side-encryption.enabled=true}, which forces the
 * multipart upload path. Before the fix, closing the output stream would upload
 * the last part twice, causing the COS SDK to reject the request with:</p>
 *
 * <pre>
 * This part was specified as the last part in a multipart upload,
 * but a previous part was already marked as the last part.
 * </pre>
 *
 * <p>After the fix, these uploads must succeed and the decrypted content read
 * back must match the original bytes.</p>
 */
public class ITestCosNClientSideEncryption extends CosNFileSystemTestWithTimeout {

    // 9 MB spans 2 parts (8 MB + 1 MB), the minimal case that crosses the
    // default 8 MB part size and exercises the last-part-on-close path.
    private static final int TWO_PART_UPLOAD_SIZE = 9 * 1024 * 1024;
    // 24 MB spans 3 parts (8 MB + 8 MB + 8 MB), matching the case in the bug
    // report where part 3 was the last part.
    private static final int THREE_PART_UPLOAD_SIZE = 24 * 1024 * 1024;

    private static File tmpDir;
    private static File publicKeyFile;
    private static File privateKeyFile;

    private final Path testBasePath = new Path("/cse-test-" + UUID.randomUUID());
    private FileSystem fs;

    @BeforeClass
    public static void generateKeyPair() throws Exception {
        tmpDir = new File(System.getProperty("java.io.tmpdir"), "cosn-cse-test-" + UUID.randomUUID());
        if (!tmpDir.mkdirs() && !tmpDir.isDirectory()) {
            throw new IOException("Failed to create temp dir: " + tmpDir);
        }
        publicKeyFile = new File(tmpDir, "public.key");
        privateKeyFile = new File(tmpDir, "private.key");
        CosNUtils.buildAndSaveAsymKeyPair(
                publicKeyFile.getAbsolutePath(), privateKeyFile.getAbsolutePath());
    }

    @AfterClass
    public static void cleanUpKeyPair() {
        if (tmpDir != null) {
            deleteRecursively(tmpDir);
        }
    }

    @After
    public void tearDown() throws IOException {
        if (fs != null) {
            try {
                if (fs.exists(testBasePath)) {
                    fs.delete(testBasePath, true);
                }
            } finally {
                fs.close();
                fs = null;
            }
        }
    }

    private static void deleteRecursively(File file) {
        if (file.isDirectory()) {
            File[] children = file.listFiles();
            if (children != null) {
                for (File child : children) {
                    deleteRecursively(child);
                }
            }
        }
        file.delete();
    }

    private FileSystem createCSEFileSystem() throws IOException {
        Configuration conf = new Configuration();
        // core-site.xml under src/test/resources is loaded automatically by
        // Configuration, providing the bucket, secretId/secretKey and region.
        conf.set("fs.cosn.impl", "org.apache.hadoop.fs.CosNFileSystem");
        conf.setBoolean(CosNConfigKeys.COSN_CLIENT_SIDE_ENCRYPTION_ENABLED, true);
        conf.set(CosNConfigKeys.COSN_CLIENT_SIDE_ENCRYPTION_PUBLIC_KEY_PATH,
                publicKeyFile.getAbsolutePath());
        conf.set(CosNConfigKeys.COSN_CLIENT_SIDE_ENCRYPTION_PRIVATE_KEY_PATH,
                privateKeyFile.getAbsolutePath());
        // Use a fresh instance to avoid caching a non-CSE filesystem created by
        // other tests against the same bucket URI.
        return FileSystem.newInstance(conf);
    }

    private byte[] buildDeterministicData(int size) {
        byte[] data = new byte[size];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) (i % 251);
        }
        return data;
    }

    private Path uploadAndVerify(int size) throws IOException {
        byte[] data = buildDeterministicData(size);

        if (!fs.exists(testBasePath)) {
            fs.mkdirs(testBasePath);
        }
        Path path = new Path(testBasePath, "cse-file-" + UUID.randomUUID());

        // Upload: with client-side encryption enabled and size > 8 MB, this goes
        // through the multipart upload path that previously failed on close.
        try (OutputStream out = fs.create(path)) {
            out.write(data);
        }

        // The CSE metadata must report the original (unencrypted) length.
        assertEquals("Expected the original file size to be restored from CSE metadata",
                size, fs.getFileStatus(path).getLen());

        // Read back and verify the content is decrypted correctly.
        byte[] readBack = new byte[size];
        int offset = 0;
        try (InputStream in = fs.open(path)) {
            int n;
            while (offset < readBack.length && (n = in.read(readBack, offset, readBack.length - offset)) != -1) {
                offset += n;
            }
        }
        assertEquals("Read back fewer bytes than expected", size, offset);
        assertArrayEquals("Decrypted content does not match the original bytes", data, readBack);

        return path;
    }

    @Test
    public void testTwoPartUploadWithClientSideEncryption() throws IOException {
        fs = createCSEFileSystem();
        uploadAndVerify(TWO_PART_UPLOAD_SIZE);
    }

    @Test
    public void testThreePartUploadWithClientSideEncryption() throws IOException {
        fs = createCSEFileSystem();
        uploadAndVerify(THREE_PART_UPLOAD_SIZE);
    }
}

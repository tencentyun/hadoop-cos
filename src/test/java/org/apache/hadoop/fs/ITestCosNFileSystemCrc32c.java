package org.apache.hadoop.fs;

import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Integration tests of {@link CosNFileSystem#getFileChecksum(Path, long)} with the
 * CRC32C checksum enabled.
 * <p>
 * 参考 {@link ITestCosNFileSystemCreateWithXAttrs} 的初始化方式，通过
 * {@code core-site.xml} 里的真实桶信息端到端测试：用开启 crc32c 的文件系统实例真实写入
 * 对象，再调用 {@code getFileChecksum} 拿到 crc32c 校验值。
 */
public class ITestCosNFileSystemCrc32c extends CosNFileSystemTestBase {

    private Path testPath;
    private CosNFileSystem crc32cFs;

    @Before
    public void before() throws IOException {
        super.before();
        testPath = methodPath();
        // 基类的 fs 是按 core-site.xml 默认配置初始化的，未开启 crc32c；
        // 这里新开一个开启了 crc32c 的实例，写文件和读校验值都用它，保证上传和查询都带上
        // x-cos-crc32c-flag，服务端才会计算并返回 crc32c。
        Configuration conf = new Configuration(configuration);
        conf.set(CosNConfigKeys.CRC32C_CHECKSUM_ENABLED, "true");
        conf.set(CosNConfigKeys.CRC64_CHECKSUM_ENABLED, "false");
        crc32cFs = new CosNFileSystem();
        crc32cFs.initialize(fs.getUri(), conf);
    }

    @After
    public void after() throws IOException {
        if (null != crc32cFs) {
            crc32cFs.close();
            crc32cFs = null;
        }
        super.after();
    }

    /**
     * 真实写入一个小文件后，getFileChecksum 应返回 COMPOSITE-CRC32C 校验值。
     */
    @Test
    public void testGetFileChecksumReturnsCrc32c() throws Throwable {
        byte[] data = "hello crc32c checksum".getBytes(StandardCharsets.UTF_8);
        try (FSDataOutputStream out = crc32cFs.create(testPath)) {
            out.write(data);
        }

        FileChecksum checksum = crc32cFs.getFileChecksum(testPath, data.length);

        assertNotNull(checksum);
        assertEquals("COMPOSITE-CRC32C", checksum.getAlgorithmName());
        // CRC32C 是 32 位校验值，长度为 4 字节。
        assertEquals(4, checksum.getLength());
    }
}

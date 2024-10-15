package org.apache.hadoop.fs;

import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CosNFileSystemTest {
    private static Configuration configuration;
    private static FileSystem fs;

    private final Path testDirPath = new Path("/test-dir");
    private final Path testFilePath = new Path("/test-file");
    private final Path testFileSymlinkPath = new Path("/test-symlink");
    private final Path testDirSymlinkPath = new Path("/test-dir-symlink");

    @BeforeClass
    public static void beforeClass() throws IOException {
        // 初始化文件系统对象，因为 core-site.xml 是在 test 的 resource 下面，因此应该能够正确加载到。
        configuration = new Configuration();
        // 考虑到是针对 CosNFileSystem 测试，因此强制设置为 CosNFileSystem。
        configuration.set("fs.cosn.impl", "org.apache.hadoop.fs.CosNFileSystem");
        // 有软连接相关单元测试，因此需要启用软连接支持。
        configuration.set("fs.cosn.support_symlink.enabled", "true");
        fs = FileSystem.get(configuration);
    }

    @AfterClass
    public static void afterClass() throws IOException {
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
            try(FSDataOutputStream fsDataOutputStream = fs.create(testFilePath)) {
                fsDataOutputStream.write("Hello, World!".getBytes());
                fsDataOutputStream.write("\n".getBytes());
                fsDataOutputStream.write("Hello, COS!".getBytes());
            }
        }

        // NOTE 这里需要保证 createSymlink 是可以成功的，所以需要先删除对应的软连接。
        if (fs.exists(testFileSymlinkPath)) {
            fs.delete(testFileSymlinkPath, true);
        }
        if (fs.exists(testDirSymlinkPath)) {
            fs.delete(testDirSymlinkPath, true);
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

    @Test
    public void supportsSymlink() {
        assertTrue(fs.supportsSymlinks());
    }

    @Test
    public void createSymlink() throws IOException {
        // 这里需要保证 createSymlink 是打开的
        assert fs.supportsSymlinks();
        // 创建一个指向文件的软连接
        fs.createSymlink(testFilePath, testFileSymlinkPath, false);
        // 验证软连接是否存在
        assert fs.getFileLinkStatus(testFileSymlinkPath).isSymlink();

        // 创建一个指向目录的软连接
        fs.createSymlink(testDirPath, testDirSymlinkPath, false);
        // 验证软连接是否存在
        assert fs.getFileLinkStatus(testDirSymlinkPath).isSymlink();
    }

    @Test
    public void getFileLinkStatus() throws IOException {
        // 这里需要保证 createSymlink 是打开的
        assert fs.supportsSymlinks();
        // 创建一个指向文件的软连接
        fs.createSymlink(testFilePath, testFileSymlinkPath, false);
        // 验证软连接是否存在
        assert fs.getFileLinkStatus(testFileSymlinkPath).isSymlink();
        // Hadoop Compatible FileSystem 语义要求软连接的FileStatus是软连接，而不是文件。
        assert fs.getFileStatus(testFileSymlinkPath).isSymlink();

        // 创建一个指向目录的软连接
        fs.createSymlink(testDirPath, testDirSymlinkPath, false);
        // 验证软连接是否存在
        assert fs.getFileLinkStatus(testDirSymlinkPath).isSymlink();
        // Hadoop Compatible FileSystem 语义要求软连接的FileStatus是软连接，而不是目录。
        assert fs.getFileStatus(testDirSymlinkPath).isSymlink();
    }

    @Test
    public void getLinkTarget() throws IOException, URISyntaxException {
        // 这里需要保证 createSymlink 是打开的
        assert fs.supportsSymlinks();
        // 创建一个指向文件的软连接
        fs.createSymlink(testFilePath, testFileSymlinkPath, false);
        // 验证软连接是否存在
        assertEquals(testFilePath, new Path(fs.getLinkTarget(testFileSymlinkPath).toUri().getPath()));

        // 创建一个指向目录的软连接
        fs.createSymlink(testDirPath, testDirSymlinkPath, false);
        // 验证软连接是否存在
        assertEquals(testDirPath,  new Path(fs.getLinkTarget(testDirSymlinkPath).toUri().getPath()));
    }
}

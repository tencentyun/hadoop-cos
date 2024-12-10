package org.apache.hadoop.fs;

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ITestCosNFileSystemSymlink extends CosNFileSystemTestBase {
    private final Path testFileSymlinkPath = new Path(unittestDirPath,"test-symlink");
    private final Path testDirSymlinkPath = new Path(unittestDirPath,"test-dir-symlink");

    @Before
    public void before() throws IOException {
        configuration.setBoolean("fs.cosn.support_symlink.enabled", true);
        super.before();
        // NOTE 这里需要保证 createSymlink 是可以成功的，所以需要先删除对应的软连接。
        if (fs.exists(testFileSymlinkPath)) {
            fs.delete(testFileSymlinkPath, true);
        }
        if (fs.exists(testDirSymlinkPath)) {
            fs.delete(testDirSymlinkPath, true);
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
        assert fs.getFileStatus(testDirSymlinkPath).isSymlink();
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
        // 创建一个指x向文件的软连接
        fs.createSymlink(testFilePath, testFileSymlinkPath, false);
        // 验证软连接是否存在
        assertEquals(testFilePath, new Path(fs.getLinkTarget(testFileSymlinkPath).toUri().getPath()));

        // 创建一个指向目录的软连接
        fs.createSymlink(testDirPath, testDirSymlinkPath, false);
        // 验证软连接是否存在
        assertEquals(testDirPath,  new Path(fs.getLinkTarget(testDirSymlinkPath).toUri().getPath()));
    }

    @Test
    public void getFileStatus() throws IOException {
        assert fs.supportsSymlinks();
        fs.createSymlink(testFilePath, testFileSymlinkPath, false);
        assert fs.getFileStatus(testFileSymlinkPath).isSymlink();  // 预期返回的是软连接的FileStatus
        // 关掉软连接支持
        boolean supportSymlink = fs.getConf().getBoolean(CosNConfigKeys.COSN_SUPPORT_SYMLINK_ENABLED, CosNConfigKeys.DEFAULT_COSN_SUPPORT_SYMLINK_ENABLED);
        fs.getConf().setBoolean(CosNConfigKeys.COSN_SUPPORT_SYMLINK_ENABLED, false);
        assert fs.getFileStatus(testFileSymlinkPath).isFile();  // 预期返回的是文件的FileStatus
        fs.getConf().setBoolean(CosNConfigKeys.COSN_SUPPORT_SYMLINK_ENABLED, supportSymlink);
    }
}

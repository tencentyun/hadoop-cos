package org.apache.hadoop.fs;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.cosn.Constants;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

/**
 * Unit tests of {@link CosNFSDataOutputStream#setUserMetadata(Map)}.
 * Nothing is written in these tests, so no backend is needed.
 */
public class CosNFSDataOutputStreamMetadataTest {
    private ExecutorService executorService;
    private CosNFSDataOutputStream stream;

    @Before
    public void setUp() throws IOException {
        Configuration conf = new Configuration(false);
        conf.setBoolean(CosNConfigKeys.COSN_UPLOAD_CHECKS_ENABLED_KEY, false);
        this.executorService = Executors.newSingleThreadExecutor();
        // The store is never touched before the stream is committed.
        this.stream = new CosNFSDataOutputStream(conf, null, "unittest-dir/metadata", this.executorService);
    }

    @After
    public void tearDown() throws IOException {
        this.stream.doAbort();
        this.executorService.shutdownNow();
    }

    @Test
    public void testSetUserMetadata() throws IOException {
        Map<String, byte[]> metadata = new HashMap<>();
        metadata.put("attr-1", new byte[]{1, 2, 3});
        metadata.put("attr-2", "hello".getBytes());
        this.stream.setUserMetadata(metadata);
        assertEquals(2, this.stream.preservedUserMetadata.size());
    }

    @Test
    public void testNullValueIsIgnoredAndEmptyMeansNothing() throws IOException {
        Map<String, byte[]> metadata = new HashMap<>();
        metadata.put("attr-1", null);
        this.stream.setUserMetadata(metadata);
        assertNull(this.stream.preservedUserMetadata);

        this.stream.setUserMetadata(null);
        assertNull(this.stream.preservedUserMetadata);
    }

    @Test(expected = HadoopIllegalArgumentException.class)
    public void testEmptyNameIsRejected() throws IOException {
        Map<String, byte[]> metadata = new HashMap<>();
        metadata.put("", new byte[]{1});
        this.stream.setUserMetadata(metadata);
    }

    @Test(expected = HadoopIllegalArgumentException.class)
    public void testSingleAttributeExceedsTheLimit() throws IOException {
        Map<String, byte[]> metadata = new HashMap<>();
        metadata.put("attr", new byte[CosNFileSystem.MAX_XATTR_SIZE]);
        this.stream.setUserMetadata(metadata);
    }

    @Test
    public void testTotalSizeIsCountedAfterBase64Encoding() throws IOException {
        // 每个属性的原始大小都没超过 1024，但是 base64 编码之后膨胀约 4/3，4 个加起来会超过 COS 的 4KB 限制。
        Map<String, byte[]> metadata = new HashMap<>();
        for (int i = 0; i < 4; i++) {
            metadata.put("attr-" + i, new byte[1000]);
        }
        try {
            this.stream.setUserMetadata(metadata);
            fail("The total size after encoding exceeds the limit, an exception is expected.");
        } catch (HadoopIllegalArgumentException expected) {
            // expected
        }
        assertNull(this.stream.preservedUserMetadata);

        // 3 个属性（编码后约 4020 字节）在限制之内。
        metadata.remove("attr-3");
        this.stream.setUserMetadata(metadata);
        assertEquals(3, this.stream.preservedUserMetadata.size());
    }

    @Test
    public void testRejectedMetadataKeepsTheOriginalOne() throws IOException {
        Map<String, byte[]> metadata = new HashMap<>();
        metadata.put("attr-1", new byte[]{1});
        this.stream.setUserMetadata(metadata);

        Map<String, byte[]> illegal = new HashMap<>();
        illegal.put("", new byte[]{1});
        try {
            this.stream.setUserMetadata(illegal);
            fail("An illegal name is expected to be rejected.");
        } catch (HadoopIllegalArgumentException expected) {
            // expected
        }
        assertEquals(1, this.stream.preservedUserMetadata.size());
    }

    @Test(expected = IllegalStateException.class)
    public void testCanNotSetMetadataAfterCommitted() throws IOException {
        this.stream.committed = true;
        this.stream.setUserMetadata(new HashMap<String, byte[]>());
    }

    @Test(expected = IOException.class)
    public void testCanNotSetMetadataAfterClosed() throws IOException {
        this.stream.doAbort();
        this.stream.setUserMetadata(new HashMap<String, byte[]>());
    }

    @Test
    public void testSetOwnerAndGroup() throws IOException {
        this.stream.setOwner("alice", "staff");
        assertEquals(2, this.stream.preservedUserMetadata.size());
        assertArrayEquals("alice".getBytes(CosNFileSystem.METADATA_ENCODING),
                this.stream.preservedUserMetadata.get(Constants.HADOOP_OWNER_USER_METADATA));
        assertArrayEquals("staff".getBytes(CosNFileSystem.METADATA_ENCODING),
                this.stream.preservedUserMetadata.get(Constants.HADOOP_GROUP_USER_METADATA));
    }

    @Test
    public void testSetOwnerWithNullKeepsTheOtherField() throws IOException {
        // 与 FileSystem#setOwner 一致：null 表示不修改对应的字段。
        this.stream.setOwner("alice", "staff");
        this.stream.setOwner(null, "wheel");
        assertArrayEquals("alice".getBytes(CosNFileSystem.METADATA_ENCODING),
                this.stream.preservedUserMetadata.get(Constants.HADOOP_OWNER_USER_METADATA));
        assertArrayEquals("wheel".getBytes(CosNFileSystem.METADATA_ENCODING),
                this.stream.preservedUserMetadata.get(Constants.HADOOP_GROUP_USER_METADATA));

        this.stream.setOwner("bob", null);
        assertArrayEquals("bob".getBytes(CosNFileSystem.METADATA_ENCODING),
                this.stream.preservedUserMetadata.get(Constants.HADOOP_OWNER_USER_METADATA));
        assertArrayEquals("wheel".getBytes(CosNFileSystem.METADATA_ENCODING),
                this.stream.preservedUserMetadata.get(Constants.HADOOP_GROUP_USER_METADATA));
    }

    @Test
    public void testSetOwnerWithBothNullDoesNothing() throws IOException {
        this.stream.setOwner(null, null);
        assertNull(this.stream.preservedUserMetadata);
    }

    @Test
    public void testSetOwnerOnlyDeclaresTheGivenField() throws IOException {
        this.stream.setOwner("alice", null);
        assertEquals(1, this.stream.preservedUserMetadata.size());
        assertNull(this.stream.preservedUserMetadata.get(Constants.HADOOP_GROUP_USER_METADATA));
    }

    @Test(expected = HadoopIllegalArgumentException.class)
    public void testEmptyUserIsRejected() throws IOException {
        this.stream.setOwner("", "staff");
    }

    @Test(expected = HadoopIllegalArgumentException.class)
    public void testEmptyGroupIsRejected() throws IOException {
        this.stream.setOwner("alice", "");
    }

    @Test
    public void testRejectedOwnerKeepsTheOriginalOne() throws IOException {
        this.stream.setOwner("alice", "staff");
        try {
            this.stream.setOwner("bob", "");
            fail("An empty group is expected to be rejected.");
        } catch (HadoopIllegalArgumentException expected) {
            // expected
        }
        assertArrayEquals("alice".getBytes(CosNFileSystem.METADATA_ENCODING),
                this.stream.preservedUserMetadata.get(Constants.HADOOP_OWNER_USER_METADATA));
    }

    @Test
    public void testSetUserMetadataKeepsTheOwner() throws IOException {
        // 先设置属主，再整体设置属性：属主不能被替换掉。
        this.stream.setOwner("alice", "staff");
        Map<String, byte[]> metadata = new HashMap<>();
        metadata.put("attr-1", new byte[]{1});
        this.stream.setUserMetadata(metadata);

        assertEquals(3, this.stream.preservedUserMetadata.size());
        assertArrayEquals("alice".getBytes(CosNFileSystem.METADATA_ENCODING),
                this.stream.preservedUserMetadata.get(Constants.HADOOP_OWNER_USER_METADATA));
        assertArrayEquals(new byte[]{1}, this.stream.preservedUserMetadata.get("attr-1"));
    }

    @Test
    public void testSetOwnerKeepsTheUserMetadata() throws IOException {
        // 先设置属性，再设置属主：属性不能丢。
        Map<String, byte[]> metadata = new HashMap<>();
        metadata.put("attr-1", new byte[]{1});
        this.stream.setUserMetadata(metadata);
        this.stream.setOwner("alice", "staff");

        assertEquals(3, this.stream.preservedUserMetadata.size());
        assertArrayEquals(new byte[]{1}, this.stream.preservedUserMetadata.get("attr-1"));
        assertArrayEquals("staff".getBytes(CosNFileSystem.METADATA_ENCODING),
                this.stream.preservedUserMetadata.get(Constants.HADOOP_GROUP_USER_METADATA));
    }

    @Test
    public void testOwnerInUserMetadataTakesPrecedence() throws IOException {
        // 调用方在属性里显式给出的 hadoop.owner 以调用方为准，同时没给的 group 保留 setOwner 声明的值。
        this.stream.setOwner("alice", "staff");
        Map<String, byte[]> metadata = new HashMap<>();
        metadata.put(Constants.HADOOP_OWNER_USER_METADATA, "carol".getBytes(CosNFileSystem.METADATA_ENCODING));
        this.stream.setUserMetadata(metadata);

        assertArrayEquals("carol".getBytes(CosNFileSystem.METADATA_ENCODING),
                this.stream.preservedUserMetadata.get(Constants.HADOOP_OWNER_USER_METADATA));
        assertArrayEquals("staff".getBytes(CosNFileSystem.METADATA_ENCODING),
                this.stream.preservedUserMetadata.get(Constants.HADOOP_GROUP_USER_METADATA));
    }

    @Test
    public void testOwnerCountsTowardsTheTotalSize() throws IOException {
        // 3 个 1000 字节的属性编码后约 4026 字节，再加上 200 字符的属主（编码后约 268 字节）就超过 4KB。
        Map<String, byte[]> metadata = new HashMap<>();
        for (int i = 0; i < 3; i++) {
            metadata.put("attr-" + i, new byte[1000]);
        }
        this.stream.setUserMetadata(metadata);
        String longUser = new String(new char[200]).replace('\0', 'a');
        try {
            this.stream.setOwner(longUser, "staff");
            fail("The owner takes the total size over the limit, an exception is expected.");
        } catch (HadoopIllegalArgumentException expected) {
            // expected
        }
        // 被拒绝之后原来的属性保持不变，也没有半截的属主信息。
        assertEquals(3, this.stream.preservedUserMetadata.size());
        assertNull(this.stream.preservedUserMetadata.get(Constants.HADOOP_OWNER_USER_METADATA));
    }

    @Test(expected = IllegalStateException.class)
    public void testCanNotSetOwnerAfterCommitted() throws IOException {
        this.stream.committed = true;
        this.stream.setOwner("alice", "staff");
    }

    @Test(expected = IOException.class)
    public void testCanNotSetOwnerAfterClosed() throws IOException {
        this.stream.doAbort();
        this.stream.setOwner("alice", "staff");
    }
}

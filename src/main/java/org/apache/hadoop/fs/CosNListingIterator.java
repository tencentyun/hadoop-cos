package org.apache.hadoop.fs;

import org.apache.hadoop.fs.cosn.OperationCancellingStatusProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * 懒加载分页迭代器，用于 {@link CosNFileSystem#listStatusIterator} 接口的实现。
 * 每次调用 {@link #hasNext()} 时，若当前批次已耗尽则自动拉取下一页，
 * 避免一次性将所有目录条目加载到内存。
 */
class CosNListingIterator implements RemoteIterator<FileStatus> {

    private static final Logger LOG = LoggerFactory.getLogger(CosNListingIterator.class);

    /** 所属的文件系统实例，用于访问 store、配置及辅助方法 */
    private final CosNFileSystem fs;
    /** 目录的绝对路径 */
    private final Path dirPath;
    /** 目录对应的 COS key 前缀（以 '/' 结尾） */
    private final String prefix;
    /** 单次 List 请求的最大条目数 */
    private final int listMaxLength;
    /** 当前批次尚未消费的 FileStatus 队列 */
    private final Deque<FileStatus> currentBatch = new ArrayDeque<>();
    /** 上一次 List 请求返回的 priorLastKey，null 表示已到最后一页 */
    private String priorLastKey = null;

    CosNListingIterator(CosNFileSystem fs, Path dirPath) throws IOException {
        this.fs = fs;
        this.dirPath = dirPath;
        String key = fs.pathToKey(dirPath);
        if (!key.endsWith(CosNFileSystem.PATH_DELIMITER)) {
            key += CosNFileSystem.PATH_DELIMITER;
        }
        this.prefix = key;
        this.listMaxLength = fs.isPosixBucket()
                ? CosNFileSystem.POSIX_BUCKET_LIST_LIMIT
                : CosNFileSystem.BUCKET_LIST_LIMIT;
        // 立即拉取第一批，以便 hasNext() 可以直接判断
        fetchNextBatch();
    }

    /**
     * 向 COS 发起一次 List 请求，将结果转换为 FileStatus 并填入 currentBatch。
     */
    private void fetchNextBatch() throws IOException {
        URI pathUri = dirPath.toUri();
        CosNPartialListing listing = fs.getNativeStore().list(prefix, listMaxLength, priorLastKey, false);

        // 用于 directoryFirstEnabled 时的同名去重：path -> FileStatus
        // 目录优先：若同名条目已存在文件，则用目录替换
        Map<Path, FileStatus> batchMap = fs.isDirectoryFirstEnabled() ? new LinkedHashMap<>() : null;

        for (FileMetadata fileMetadata : listing.getFiles()) {
            Path subPath = fs.keyToPath(fileMetadata.getKey());
            if (fileMetadata.getKey().equals(prefix)) {
                // 跳过目录自身
                continue;
            }
            FileStatus fileStatus;
            if (fs.supportsSymlinks() && fileMetadata.getLength() < fs.getSymbolicLinkSizeThreshold()) {
                CosNSymlinkMetadata cosNSymlinkMetadata =
                        fs.getNativeStore().retrieveSymlinkMetadata(fileMetadata.getKey());
                if (null != cosNSymlinkMetadata) {
                    cosNSymlinkMetadata.setLength(fileMetadata.getLength());
                    fileStatus = fs.newSymlink(cosNSymlinkMetadata, subPath);
                } else {
                    fileStatus = fs.newFile(fileMetadata, subPath);
                }
            } else {
                fileStatus = fs.newFile(fileMetadata, subPath);
            }
            if (fs.isDirectoryFirstEnabled()) {
                Path qualifiedPath = fileStatus.getPath();
                // 目录优先：若已有目录条目，则不覆盖
                if (!batchMap.containsKey(qualifiedPath) || !batchMap.get(qualifiedPath).isDirectory()) {
                    batchMap.put(qualifiedPath, fileStatus);
                }
            } else {
                currentBatch.add(fileStatus);
            }
        }

        for (FileMetadata commonPrefix : listing.getCommonPrefixes()) {
            Path subPath = fs.keyToPath(commonPrefix.getKey());
            String relativePath = pathUri.relativize(subPath.toUri()).getPath();
            FileStatus directory = fs.newDirectory(commonPrefix, new Path(dirPath, relativePath));
            if (fs.isDirectoryFirstEnabled()) {
                // 目录优先：无论是否已有同名文件，目录都覆盖
                batchMap.put(directory.getPath(), directory);
            } else {
                currentBatch.add(directory);
            }
        }

        if (fs.isDirectoryFirstEnabled()) {
            currentBatch.addAll(batchMap.values());
        }

        priorLastKey = listing.getPriorLastKey();
    }

    @Override
    public boolean hasNext() throws IOException {
        // 检查操作是否已被取消
        OperationCancellingStatusProvider provider = fs.getOperationCancellingStatusProvider();
        if (provider != null && provider.isCancelled()) {
            LOG.warn("The listStatusIterator operation is cancelled. prefix: {}.", prefix);
            throw new IOException(
                    "The listStatusIterator operation is cancelled. prefix: " + prefix);
        }
        // 若当前批次非空，直接返回 true
        if (!currentBatch.isEmpty()) {
            return true;
        }
        // 若线程被中断，停止继续拉取
        if (Thread.currentThread().isInterrupted()) {
            return false;
        }
        // 若还有下一页，则拉取
        if (priorLastKey != null) {
            fetchNextBatch();
            return !currentBatch.isEmpty();
        }
        return false;
    }

    @Override
    public FileStatus next() throws IOException {
        if (!hasNext()) {
            throw new NoSuchElementException("No more entries in listing for: " + dirPath);
        }
        return currentBatch.poll();
    }
}

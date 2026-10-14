package org.apache.hadoop.fs;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.fs.impl.AbstractFSBuilderImpl;
import org.apache.hadoop.fs.impl.OpenFileParameters;
import org.apache.hadoop.util.LambdaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Extends the CosNFileSystem to provide additional functionality for some internal systems, such as GooseFS.
 *
 * This class is compiled over the Hadoop 3.3.0+ version.
 */
public class CosNFileSystemExt extends CosNFileSystem {
    private static final Logger LOG = LoggerFactory.getLogger(CosNFileSystemExt.class);

    @Override
    protected CompletableFuture<FSDataInputStream> openFileWithOptions(Path path, OpenFileParameters parameters)
            throws IOException {
        AbstractFSBuilderImpl.rejectUnknownMandatoryKeys(
                parameters.getMandatoryKeys(),
                Collections.emptySet(),
                "for " + path);
        return LambdaUtils.eval(new CompletableFuture<>(), () ->
                open(path, parameters.getBufferSize(), parameters.getStatus()));
    }

    /**
     * Set the given extended attributes (xattr) of the path in a batch.
     * <p>
     * {@link #setXAttr(Path, String, byte[], java.util.EnumSet)} sets the attributes one by one,
     * and every attribute costs an in-place copy request. Here all the attributes are persisted by
     * only one copy request, so they are set atomically: either all of them take effect, or none
     * of them does. It also means that the {@code XAttrSetFlag} is not checked here, the value of
     * an existing attribute is always replaced.
     * <p>
     * A null value in the map means removing the corresponding attribute.
     *
     * @param f          the path on which to set the attributes
     * @param attributes the extended attributes to be set, null or empty means nothing to do
     * @throws IOException if the extended attributes can not be set
     */
    public void setXAttrs(Path f, Map<String, byte[]> attributes) throws IOException {
        if (null == attributes || attributes.isEmpty()) {
            LOG.debug("No extended attribute to be set. path: {}.", f);
            return;
        }

        for (Map.Entry<String, byte[]> attribute : attributes.entrySet()) {
            if (null == attribute.getKey() || attribute.getKey().isEmpty()) {
                throw new HadoopIllegalArgumentException(
                        "The name of an extended attribute can not be empty. path: " + f);
            }
            if (null != attribute.getValue()) {
                // 值为 null 表示删除这个属性，不需要校验长度。
                checkXAttrSize(attribute.getKey(), attribute.getValue());
            }
        }

        // 与 setXAttr 保持一致，符号连接跟随到它指向的目的对象。
        // FileStatus 中已经携带了连接指向的路径，直接跟着它取一次目的对象的状态即可。
        FileStatus fileStatus = this.getFileStatus(f);
        while (fileStatus.isSymlink()) {
            fileStatus = this.getFileStatus(makeAbsolute(fileStatus.getSymlink()));
        }
        String key = pathToKey(makeAbsolute(fileStatus.getPath()));
        // 普通桶的目录对象是带 '/' 后缀的 key，POSIX 桶的目录就是普通对象。
        if (fileStatus.isDirectory() && !this.isPosixBucket()
                && !key.endsWith(CosNFileSystem.PATH_DELIMITER)) {
            key += CosNFileSystem.PATH_DELIMITER;
        }

        this.getNativeStore().storeAttributes(key, attributes);
        LOG.debug("Set the extended attributes completed. path: {}, attributes: {}.",
                fileStatus.getPath(), attributes.keySet());
    }
}

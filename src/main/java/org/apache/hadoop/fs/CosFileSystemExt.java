package org.apache.hadoop.fs;

import com.qcloud.chdfs.permission.RangerAccessType;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.fs.impl.OpenFileParameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Extends the CosFileSystem to expose the extended functionality of {@link CosNFileSystemExt},
 * which is the actual implementation this file system delegates to.
 * <p>
 * This class is compiled over the Hadoop 3.3.0+ version.
 */
public class CosFileSystemExt extends CosFileSystem {
    private static final Logger LOG = LoggerFactory.getLogger(CosFileSystemExt.class);

    @Override
    protected CompletableFuture<FSDataInputStream> openFileWithOptions(
            Path path, OpenFileParameters parameters) throws IOException {
        return super.actualImplFS.openFileWithOptions(path, parameters);
    }

    /**
     * Create a builder of a new file, which supports declaring the extended attributes of the file
     * at the creation time through the options. Every option is one extended attribute: its key is
     * the name of the attribute, and its value is the UTF-8 text of the attribute value.
     * <pre>
     *   fs.createFile(path)
     *       .recursive()
     *       .must("owner-tag", "abc")
     *       .build();
     * </pre>
     * It is the same builder as the one of {@link CosNFileSystemExt#createFile(Path)}, but returned
     * by this file system, because this is the one the callers usually get from
     * {@link FileSystem#get(org.apache.hadoop.conf.Configuration)}. The attributes are persisted by
     * the actual implementation this file system delegates to.
     */
    @Override
    public CosNFileSystemExt.CosNCreateFileBuilder createFile(Path path) {
        return new CosNFileSystemExt.CosNCreateFileBuilder(this, path).create().overwrite(true);
    }

    @Override
    public boolean hasPathCapability(Path path, String capability) throws IOException {
        if (CosNFileSystemExt.CAPABILITY_CREATE_XATTR.equals(capability)) {
            return true;
        }
        return super.hasPathCapability(path, capability);
    }

    /**
     * Set the given extended attributes (xattr) of the path in a batch.
     * <p>
     * It is delegated to {@link CosNFileSystemExt#setXAttrs(Path, Map)}, where all the attributes
     * are persisted by only one request, so they are set atomically. If the actual implementation
     * is not a {@link CosNFileSystemExt}, the attributes are set one by one by
     * {@link #setXAttr(Path, String, byte[], EnumSet)}, which is not atomic and costs one request
     * for each attribute.
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
        }

        checkInitialized();
        checkPermission(f, RangerAccessType.WRITE);

        if (this.actualImplFS instanceof CosNFileSystemExt) {
            ((CosNFileSystemExt) this.actualImplFS).setXAttrs(f, attributes);
            return;
        }

        LOG.warn("The actual implementation [{}] does not support setting the extended attributes in a batch, " +
                "they are set one by one and not atomically. path: {}.",
                this.actualImplFS.getClass().getName(), f);
        for (Map.Entry<String, byte[]> attribute : attributes.entrySet()) {
            if (null == attribute.getValue()) {
                removeXAttr(f, attribute.getKey());
                continue;
            }
            // XAttrSetFlag 只有 CREATE 和 REPLACE，要支持「存在则覆盖」得先看属性在不在。
            boolean exists = null != getXAttr(f, attribute.getKey());
            setXAttr(f, attribute.getKey(), attribute.getValue(),
                    EnumSet.of(exists ? XAttrSetFlag.REPLACE : XAttrSetFlag.CREATE));
        }
    }
}

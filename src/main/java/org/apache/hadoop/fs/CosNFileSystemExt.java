package org.apache.hadoop.fs;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.cosn.Abortable;
import org.apache.hadoop.fs.impl.AbstractFSBuilderImpl;
import org.apache.hadoop.fs.impl.OpenFileParameters;
import org.apache.hadoop.util.LambdaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Extends the CosNFileSystem to provide additional functionality for some internal systems, such as GooseFS.
 *
 * This class is compiled over the Hadoop 3.3.0+ version.
 */
public class CosNFileSystemExt extends CosNFileSystem {
    private static final Logger LOG = LoggerFactory.getLogger(CosNFileSystemExt.class);

    /**
     * The path capability which tells that {@link #createFile(Path)} supports the create-time xattr.
     */
    public static final String CAPABILITY_CREATE_XATTR = "fs.cosn.capability.create.xattr";

    @Override
    public boolean hasPathCapability(Path path, String capability) throws IOException {
        if (CAPABILITY_CREATE_XATTR.equals(capability)) {
            return true;
        }
        return super.hasPathCapability(path, capability);
    }

    /**
     * Create a builder of a new file, which supports declaring the extended attributes of the file
     * at the creation time through the options. Every option is one extended attribute: its key is
     * the name of the attribute, and its value is the UTF-8 text of the attribute value.
     * <pre>
     *   fs.createFile(path)
     *       .recursive()
     *       .must("owner-tag", "abc")
     *       .opt("checksum", "123")
     *       .build();
     * </pre>
     * The attributes are sent together with the data, by the PutObject request for a small file or by
     * the InitiateMultipartUpload request for a large one, so they take effect atomically with the data
     * when the stream is closed. Nothing is left if the stream is aborted before that.
     * <p>
     * Because the options are flat text pairs, the values are always interpreted as UTF-8 text. Use
     * {@link CosNFSDataOutputStream#setUserMetadata(Map)} or {@link #setXAttrs(Path, Map)} to persist
     * binary values.
     * <p>
     * Except for the extended attributes, it behaves the same as the default builder of the
     * {@link FileSystem}: the file is overwritten by default, and the non-recursive creation goes to
     * {@link #createNonRecursive(Path, org.apache.hadoop.fs.permission.FsPermission, java.util.EnumSet,
     * int, short, long, org.apache.hadoop.util.Progressable)}.
     */
    @Override
    public CosNCreateFileBuilder createFile(Path path) {
        return new CosNCreateFileBuilder(this, path).create().overwrite(true);
    }

    /**
     * Extract the extended attributes from the options of a builder.
     * <p>
     * The options are a Configuration which only holds the keys set by {@code opt()} and
     * {@code must()}: the buffer size, the replication, the flags and so on are kept in the fields of
     * the builder, so they never show up here. That is why every option is taken as one extended
     * attribute, without any namespace prefix: the key is the name, and the value is the UTF-8 text.
     * <p>
     * Package-visible for the unit tests.
     *
     * @param options the options of the builder
     * @return the extended attributes
     * @throws HadoopIllegalArgumentException if a name is empty
     */
    static Map<String, byte[]> xattrsFromOptions(Configuration options) {
        Map<String, byte[]> xattrs = new HashMap<>();
        if (null == options) {
            return xattrs;
        }
        for (Map.Entry<String, String> option : options) {
            String name = option.getKey();
            if (null == name || name.isEmpty()) {
                throw new HadoopIllegalArgumentException("The name of an extended attribute can not be empty.");
            }
            // getRaw 避免值里出现的 ${...} 被当成变量替换掉。
            String value = options.getRaw(name);
            xattrs.put(name, (null == value ? "" : value).getBytes(CosNFileSystem.METADATA_ENCODING));
        }
        return xattrs;
    }

    /**
     * The builder returned by {@link #createFile(Path)}.
     */
    public static final class CosNCreateFileBuilder
            extends FSDataOutputStreamBuilder<FSDataOutputStream, CosNCreateFileBuilder> {

        private CosNCreateFileBuilder(FileSystem fileSystem, Path path) {
            super(fileSystem, path);
        }

        @Override
        public CosNCreateFileBuilder getThisBuilder() {
            return this;
        }

        @Override
        public FSDataOutputStream build() throws IOException {
            // 先解析属性，有问题直接失败，不会创建出输出流。
            Map<String, byte[]> xattrs = xattrsFromOptions(getOptions());
            if (!getFlags().contains(CreateFlag.CREATE) && !getFlags().contains(CreateFlag.OVERWRITE)) {
                throw new PathIOException(getPath().toString(),
                        "Only the create or overwrite flag is supported, the append is not supported");
            }

            FSDataOutputStream out;
            if (isRecursive()) {
                out = getFS().create(getPath(), getPermission(), getFlags(), getBufferSize(), getReplication(),
                        getBlockSize(), getProgress(), getChecksumOpt());
            } else {
                out = getFS().createNonRecursive(getPath(), getPermission(), getFlags(), getBufferSize(),
                        getReplication(), getBlockSize(), getProgress());
            }
            if (xattrs.isEmpty()) {
                return out;
            }

            OutputStream wrapped = out.getWrappedStream();
            if (!(wrapped instanceof CosNFSDataOutputStream)) {
                abortQuietly(wrapped);
                throw new UnsupportedOperationException(
                        "The output stream of " + getPath() + " does not support the create-time extended attributes.");
            }
            try {
                ((CosNFSDataOutputStream) wrapped).setUserMetadata(xattrs);
            } catch (IOException | RuntimeException e) {
                // 还没有写入任何数据，中止掉就不会留下任何东西。
                abortQuietly(wrapped);
                throw e;
            }
            return out;
        }

        private static void abortQuietly(OutputStream stream) {
            if (!(stream instanceof Abortable)) {
                return;
            }
            try {
                ((Abortable) stream).doAbort();
            } catch (IOException e) {
                LOG.warn("Failed to abort the output stream.", e);
            }
        }
    }

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

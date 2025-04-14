package org.apache.hadoop.fs;

import org.apache.hadoop.fs.impl.AbstractFSBuilderImpl;
import org.apache.hadoop.fs.impl.OpenFileParameters;
import org.apache.hadoop.util.LambdaUtils;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;

/**
 * Extends the CosNFileSystem to provide additional functionality for some internal systems, such as GooseFS.
 *
 * This class is compiled over the Hadoop 3.3.0+ version.
 */
public class CosNFileSystemExt extends CosNFileSystem {
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
}

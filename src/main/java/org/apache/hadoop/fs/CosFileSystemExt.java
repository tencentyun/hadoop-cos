package org.apache.hadoop.fs;

import org.apache.hadoop.fs.impl.OpenFileParameters;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

public class CosFileSystemExt extends CosFileSystem {
    @Override
    protected CompletableFuture<FSDataInputStream> openFileWithOptions(
            Path path, OpenFileParameters parameters) throws IOException {
        return super.actualImplFS.openFileWithOptions(path, parameters);
    }
}

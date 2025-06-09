package org.apache.hadoop.fs.cosn.cache;

import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

public interface FragmentCache {
    class Fragment {
        private final String filePath;
        private final long startOffsetInFile;
        private final byte[] content;

        public Fragment(String filePath, long startOffsetInFile, byte[] content) {
            this.filePath = filePath;
            this.startOffsetInFile = startOffsetInFile;
            this.content = content;
        }

        public String getFilePath() {
            return filePath;
        }

        public long getStartOffsetInFile() {
            return startOffsetInFile;
        }

        public byte[] getContent() {
            return content;
        }
    }

    void initialize(Configuration configuration) throws IOException;
    void put(Fragment fragment) throws IOException;
    Fragment get(String filePath, long startOffsetInFile) throws IOException;
    boolean contains(String filePath, long startOffsetInFile) throws IOException;
    void remove(String filePath, long startOffsetInFile) throws IOException;
    void remove(String filePath) throws IOException;
    void clear() throws IOException;
    void close();
}

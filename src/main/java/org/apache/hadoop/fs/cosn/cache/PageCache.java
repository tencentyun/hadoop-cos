package org.apache.hadoop.fs.cosn.cache;

import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

public interface PageCache {
    class Page {
        private final String filePath;
        private final long offsetInFile;
        private final byte[] content;

        public Page(String filePath, long offsetInFile, byte[] content) {
          this.filePath = filePath;
          this.offsetInFile = offsetInFile;
            this.content = content;
        }

        public String getFilePath() {
            return filePath;
        }

        public long getOffsetInFile() {
            return offsetInFile;
        }

        public byte[] getContent() {
            return content;
        }
    }

    void initialize(Configuration configuration) throws IOException;
    void put(Page page) throws IOException;
    Page get(String filePath, long startOffsetInFile) throws IOException;
    boolean contains(String filePath, long startOffsetInFile) throws IOException;
    void remove(String filePath, long startOffsetInFile) throws IOException;
    void remove(String filePath) throws IOException;
    void clear() throws IOException;
    void close();
}

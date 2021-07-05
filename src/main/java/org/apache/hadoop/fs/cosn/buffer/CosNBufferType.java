package org.apache.hadoop.fs.cosn.buffer;

/**
 * The type of upload buffer.
 */
public enum CosNBufferType {
    NON_DIRECT_MEMORY("non_direct_memory"),
    DIRECT_MEMORY("direct_memory"),
    MAPPED_DISK("mapped_disk");

    private final String name;

    CosNBufferType(String str) {
        this.name = str;
    }

    public String getName() {
        return name;
    }

    public static CosNBufferType typeFactory(String typeName) {
        if (typeName.compareToIgnoreCase(NON_DIRECT_MEMORY.getName()) == 0) {
            return NON_DIRECT_MEMORY;
        }
        if (typeName.compareToIgnoreCase(DIRECT_MEMORY.getName()) == 0) {
            return DIRECT_MEMORY;
        }
        if (typeName.compareToIgnoreCase(MAPPED_DISK.getName()) == 0) {
            return MAPPED_DISK;
        }

        return null;
    }
}

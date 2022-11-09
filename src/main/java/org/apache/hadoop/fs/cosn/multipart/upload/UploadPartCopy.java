package org.apache.hadoop.fs.cosn.multipart.upload;

public final class UploadPartCopy {
  private final String srcKey;

  private final String destKey;
  private final int partNumber;
  private final long firstByte;
  private final long lastByte;

  public UploadPartCopy(String srcKey, String destKey, int partNumber, long firstByte, long lastByte) {
    this.srcKey = srcKey;
    this.destKey = destKey;
    this.partNumber = partNumber;
    this.firstByte = firstByte;
    this.lastByte = lastByte;
  }

  public String getSrcKey() {
    return srcKey;
  }

  public String getDestKey() {
    return destKey;
  }

  public int getPartNumber() {
    return partNumber;
  }

  public long getFirstByte() {
    return firstByte;
  }

  public long getLastByte() {
    return lastByte;
  }

  @Override
  public String toString() {
    return "UploadPartCopy{" +
        "srcKey='" + srcKey + '\'' +
        ", partNumber=" + partNumber +
        ", firstByte=" + firstByte +
        ", lastByte=" + lastByte +
        '}';
  }
}

package org.apache.hadoop.fs.cosn;

import java.io.IOException;

public interface Abortable {
    /**
     * hadoop-3.3.0+ 也有在输出流的基类中定义 abort 接口，因此为了实现该接口的类出现跟继承的输出流
     * 基类发生冲突，因此定义为 doAbort。
     *
     * @throws IOException 中断 CosN 的底层发生错误。
     */
    void doAbort() throws IOException;
}

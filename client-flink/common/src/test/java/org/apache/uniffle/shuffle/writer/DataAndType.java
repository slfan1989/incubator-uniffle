package org.apache.uniffle.shuffle.writer;

import org.apache.flink.runtime.io.network.buffer.Buffer;

import java.nio.ByteBuffer;

public class DataAndType {
    private final ByteBuffer data;
    private final Buffer.DataType dataType;

    DataAndType(ByteBuffer data, Buffer.DataType dataType) {
        this.data = data;
        this.dataType = dataType;
    }
}

package org.apache.uniffle.shuffle.writer;

import org.apache.flink.runtime.io.network.buffer.Buffer;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class BufferWithChannel {

    private final Buffer buffer;

    private final int channelIndex;

    BufferWithChannel(Buffer buffer, int channelIndex) {
        this.buffer = checkNotNull(buffer);
        this.channelIndex = channelIndex;
    }

    public Buffer getBuffer() {
        return buffer;
    }

    public int getChannelIndex() {
        return channelIndex;
    }
}

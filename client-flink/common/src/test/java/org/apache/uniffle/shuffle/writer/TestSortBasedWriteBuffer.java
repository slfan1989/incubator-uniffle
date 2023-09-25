package org.apache.uniffle.shuffle.writer;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Queue;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestSortBasedWriteBuffer {


  @Test
  public void testWriteAndReadSortBuffer() throws Exception {
        int numSubpartitions = 10;
        int bufferSize = 1024;
        int bufferPoolSize = 1000;
        Random random = new Random(1111);

        // used to store data written to and read from sort buffer for correctness check
        Queue<DataAndType>[] dataWritten = new Queue[numSubpartitions];
        Queue<Buffer>[] buffersRead = new Queue[numSubpartitions];
        for (int i = 0; i < numSubpartitions; ++i) {
            dataWritten[i] = new ArrayDeque<>();
            buffersRead[i] = new ArrayDeque<>();
        }

        int[] numBytesWritten = new int[numSubpartitions];
        int[] numBytesRead = new int[numSubpartitions];
        Arrays.fill(numBytesWritten, 0);
        Arrays.fill(numBytesRead, 0);

        // fill the sort buffer with randomly generated data
        int totalBytesWritten = 0;
        WriteBuffer sortBuffer = createWriteBuffer(
            bufferPoolSize,
            bufferSize,
            numSubpartitions,
            getRandomSubpartitionOrder(numSubpartitions));

        while (true) {
            // record size may be larger than buffer size so a record may span multiple segments
            int recordSize = random.nextInt(bufferSize * 4 - 1) + 1;
            byte[] bytes = new byte[recordSize];

            // fill record with random value
            random.nextBytes(bytes);
            ByteBuffer record = ByteBuffer.wrap(bytes);

            // select a random subpartition to write
            int subpartition = random.nextInt(numSubpartitions);

            // select a random data type
            boolean isBuffer = random.nextBoolean() || recordSize > bufferSize;
            Buffer.DataType dataType = isBuffer ? Buffer.DataType.DATA_BUFFER : Buffer.DataType.EVENT_BUFFER;
            if (sortBuffer.append(record, subpartition, dataType)) {
               if(sortBuffer.isFinished())
               sortBuffer.finish();
               break;
            }
            record.rewind();
            dataWritten[subpartition].add(new DataAndType(record, dataType));
            numBytesWritten[subpartition] += recordSize;
            totalBytesWritten += recordSize;
        }

        // read all data from the sort buffer
        while (sortBuffer.hasRemaining()) {
            MemorySegment readBuffer = MemorySegmentFactory.allocateUnpooledSegment(bufferSize);
            BufferWithChannel bufferAndChannel =
                    sortBuffer.getNextBuffer(readBuffer);
            int subpartition = bufferAndChannel.getChannelIndex();
            buffersRead[subpartition].add(bufferAndChannel.getBuffer());
            numBytesRead[subpartition] += bufferAndChannel.getBuffer().readableBytes();
        }

        assertEquals(totalBytesWritten, sortBuffer.numTotalBytes());
        //checkWriteReadResult(
        //        numSubpartitions, numBytesWritten, numBytesRead, dataWritten, buffersRead);
    }


    private WriteBuffer createWriteBuffer(
            int bufferPoolSize, int bufferSize, int numSubpartitions, int[] customReadOrder)
            throws IOException {
        NetworkBufferPool globalPool = new NetworkBufferPool(bufferPoolSize, bufferSize);
        BufferPool bufferPool = globalPool.createBufferPool(bufferPoolSize, bufferPoolSize);

        return new SortBasedWriteBuffer(bufferPool, numSubpartitions, bufferSize, 10, customReadOrder);
    }

    public static int[] getRandomSubpartitionOrder(int numSubpartitions) {
        Random random = new Random(1111);
        int[] subpartitionReadOrder = new int[numSubpartitions];
        int shift = random.nextInt(numSubpartitions);
        for (int i = 0; i < numSubpartitions; ++i) {
            subpartitionReadOrder[i] = (i + shift) % numSubpartitions;
        }
        return subpartitionReadOrder;
    }
}

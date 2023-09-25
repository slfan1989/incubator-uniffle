package org.apache.uniffle.shuffle.writer;

import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.util.function.SupplierWithException;
import org.apache.uniffle.client.api.ShuffleWriteClient;
import org.apache.uniffle.shuffle.RssShuffleDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class RssShuffleOutputGate {

  private static final Logger LOG = LoggerFactory.getLogger(RssShuffleOutputGate.class);

  private final RssShuffleDescriptor shuffleDesc;

  private final int numSubs;

  private ShuffleWriteClient shuffleWriteClient;

  // private final BufferPacker bufferPacker;

  protected final SupplierWithException<BufferPool, IOException> bufferPoolFactory;

  protected BufferPool bufferPool;

  public RssShuffleOutputGate(
      RssShuffleDescriptor shuffleDesc,
      int numSubs,
      int bufferSize,
      String dataPartitionFactoryName,
      SupplierWithException<BufferPool, IOException> bufferPoolFactory) {

      this.shuffleDesc = shuffleDesc;
      this.numSubs = numSubs;
      this.bufferPoolFactory = bufferPoolFactory;
      // this.bufferPacker = new BufferPacker(shuffleWriteClient::write);
    }
}

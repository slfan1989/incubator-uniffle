package org.apache.uniffle.shuffle;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.AkkaOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.shuffle.*;
import org.apache.uniffle.client.api.CoordinatorClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;

public class RssShuffleMaster implements ShuffleMaster<RssShuffleDescriptor> {

  private static final Logger LOG = LoggerFactory.getLogger(RssShuffleMaster.class);

  private static final int MAX_RETRY_TIMES = 3;

  private final ShuffleMasterContext shuffleMasterContext;

  // Job level configuration will be supported in the future
  private final String partitionFactory;

  private final Map<JobID, CoordinatorClient> shuffleClients = new HashMap<>();

  private final AtomicBoolean isClosed = new AtomicBoolean(false);

  private final ScheduledThreadPoolExecutor executor =
      new ScheduledThreadPoolExecutor(1,
      runnable -> new Thread(runnable, "remote-shuffle-master-executor"));



  public RssShuffleMaster(ShuffleMasterContext shuffleMasterContext) {
    this.shuffleMasterContext = shuffleMasterContext;
    this.executor.setRemoveOnCancelPolicy(true);
    this.partitionFactory = null;
        // configuration.getString(PluginOptions.DATA_PARTITION_FACTORY_NAME);

  }

  @Override
  public void start() throws Exception {
    ShuffleMaster.super.start();
  }

  @Override
  public void close() {
    if (isClosed.compareAndSet(false, true)) {
      executor.execute(
              () -> {
                for (CoordinatorClient clientWithListener : shuffleClients.values()) {
                  try {
                    clientWithListener.close();
                  } catch (Throwable throwable) {
                    LOG.error("Failed to close shuffle client.", throwable);
                  }
                }
                shuffleClients.clear();



                try {
                  executor.shutdown();
                } catch (Throwable throwable) {
                  LOG.error("Failed to close the shuffle master executor.", throwable);
                }
              });
    }
  }

  @Override
  public void registerJob(JobShuffleContext context) {
  }

  @Override
  public void unregisterJob(JobID jobID) {
     ShuffleMaster.super.unregisterJob(jobID);
  }

  @Override
  public MemorySize computeShuffleMemorySizeForTask(
    TaskInputsOutputsDescriptor taskInputsOutputsDescriptor) {
    return ShuffleMaster.super.computeShuffleMemorySizeForTask(taskInputsOutputsDescriptor);
  }

    @Override
  public CompletableFuture<RssShuffleDescriptor> registerPartitionWithProducer(
    JobID jobID, PartitionDescriptor partitionDescriptor, ProducerDescriptor producerDescriptor) {
    return null;
  }

  @Override
  public void releasePartitionExternally(ShuffleDescriptor shuffleDescriptor) {
  }
}

package org.apache.uniffle.shuffle.descriptor;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.uniffle.shuffle.resource.RssShuffleResource;

import java.util.Optional;

public class RssShuffleDescriptor implements ShuffleDescriptor  {

  private final ResultPartitionID resultPartitionID;

  private final JobID jobId;

  private final RssShuffleResource rssShuffleResource;

  public RssShuffleDescriptor(
      ResultPartitionID resultPartitionID,
      JobID jobId,
      RssShuffleResource shuffleResource) {
    this.resultPartitionID = resultPartitionID;
    this.jobId = jobId;
    this.rssShuffleResource = shuffleResource;
  }

  @Override
  public ResultPartitionID getResultPartitionID() {
    return resultPartitionID;
  }

  @Override
  public Optional<ResourceID> storesLocalResourcesOn() {
    return Optional.empty();
  }

  public JobID getJobId() {
    return jobId;
  }
}
